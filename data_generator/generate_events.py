import uuid
import random
import pandas as pd 
from typing import Optional, Dict 
from datetime import datetime, timedelta

SUBSCRIPTION_PLANS = {
    "basic" : 10.0,
    "pro" : 20.0,
    "premium" : 100.0
    }

BILLING_CYCLE = ["monthly","annually"]

REGIONS = ["us-east","us-west","us-central"]

ANNUAL_MULTIPLIER = 11 #Discounted for annual users like in most SaaS platforms like Uber,Spotify,etc. 

def generate_customers(n: int) -> list[dict]:
    """Generate customer IDs"""""
    # Customers structure: [{customer_id : customer_0001, plan : pro, billing_cycle : monthly, regions : us-west}]
    plans = list(SUBSCRIPTION_PLANS.keys())
    plans_weights = [0.6, 0.3, 0.1]  # most users are basic 
    region_weights = [0.5, 0.3, 0.2]  # more users on east coast
    billing_weights = [0.7,0.3] # most users do monthly subscriptions
    customers = []
    for i in range(1,n+1):
        customers.append({"customer_id" : f"customer_{i:04d}",
                         "plan" : random.choices(plans, weights=plans_weights, k=1)[0],
                         "billing_cycle": random.choices(BILLING_CYCLE, weights=billing_weights, k=1)[0],
                         "region" : random.choices(REGIONS, weights=region_weights, k=1)[0]})
    return customers

def generate_billing_event(event_id : str ,customer_id : str, event_type : str, amount : float,
                            event_ts : datetime, updated_at : datetime, 
                            metadata : Optional[Dict] = None) -> dict: 
    """Generate a single SaaS billing event (Stripe like)"""
    return {
        "event_id": event_id,
        "customer_id": customer_id,
        "event_type" : event_type,
        "amount" : amount,
        "event_ts": event_ts, #immutable  
        "updated_at": updated_at, #mutable will showcase its purpose in generate_updates.py file
        "metadata": metadata or {}
    }

def generate_billing_events(num_customers : int = 400, 
                            months_back: int = 36,cancellations: Optional[Dict[str, datetime]] = None) -> pd.DataFrame:
    """
    Generate initial SaaS billing events (no late updates yet).
    """
    customers = generate_customers(num_customers)
    today = datetime.utcnow().date()
    start_month = today.replace(day=1) # Past 30 days
    events = [] # Container for all billing events before dataframe conversion
    random.seed(42) # Deterministic Randomness 

    cancellations = cancellations or {}

    for month_offset in range(months_back):
        # Move backwards month-by-month
        event_month = start_month - pd.DateOffset(months = month_offset)
        event_date = event_month.to_pydatetime()

        for customer in customers:
            customer_id = customer["customer_id"]
            #stop billing after cancellation
            canceled_at = cancellations.get(customer_id)
            if canceled_at and event_date >= canceled_at:
                continue

            billing_cycle = customer["billing_cycle"]
            #Monthly invoices should be billed every month 
            if billing_cycle == "monthly":
                should_bill = True
                val = SUBSCRIPTION_PLANS[customer["plan"]]
            else:
                #Annual billing: once per year, same month as start_month
                should_bill = (event_date.month == start_month.month)
                val = SUBSCRIPTION_PLANS[customer["plan"]]*ANNUAL_MULTIPLIER
            

            if should_bill: 
                event = generate_billing_event(
                    event_id = str(uuid.uuid4()), 
                    customer_id =  customer["customer_id"],
                    event_type = "invoice_paid",
                    amount = val,
                    event_ts = event_date,
                    updated_at= event_date,
                    metadata={
                        "plan": customer["plan"],
                        "billing_cycle" : billing_cycle, 
                        "region" : customer["region"]
                        }
                    )
                events.append(event)
    df = pd.DataFrame(events)
    df = df.sort_values("event_ts").reset_index(drop=True)
    return df

if __name__ == "__main__":
    df = generate_billing_events()
    print(f"\nTotal events generated: {len(df)}\n")
    print(df)