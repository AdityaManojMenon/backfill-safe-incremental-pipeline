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

def generate_customers(n: int) -> list[dict]:
    """Generate customer IDs"""""
    # Customers structure: [{customer_id : customer_0001, plan : pro, billing_cycle : monthly, regions : us-west}]
    plans = list(SUBSCRIPTION_PLANS.keys())
    plans_weights = [0.6, 0.3, 0.1]  # most users are basic 
    region_weights = [0.5, 0.3, 0.2]  # more users on east coast
    customers = []
    for i in range(1,n+1):
        customers.append({"customer_id" : f"customer_{i:04d}",
                         "plan" : random.choices(plans, weights=plans_weights, k=1)[0],
                         "billing_cycle": random.choice(BILLING_CYCLE),
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
        "event_ts": event_ts,
        "updated_at": updated_at,
        "metadata": metadata or {}
    }



def generate_billing_events(num_customers : int = 50, days_back: int = 30,
                            event_probability: float = 0.3
) -> pd.DataFrame:
    """
    Generate initial SaaS billing events (no late updates yet).
    """
    customers = generate_customers(num_customers)
    today = datetime.utcnow().date()
    start_date = today - timedelta(days=days_back) # Past 30 days
    events = [] # Container for all billing events before dataframe conversion
    random.seed(42) # Deterministic Randomness 
    

    for day_offset in range(days_back):
        event_date = start_date + timedelta(days = day_offset)
        for customer in customers:
            if random.random() < event_probability: 
                event = generate_billing_event(
                    event_id = str(uuid.uuid4()), 
                    customer_id =  customer["customer_id"],
                    event_type = "invoice_paid",
                    amount = SUBSCRIPTION_PLANS[customer["plan"]],
                    event_ts = datetime.combine(event_date, datetime.min.time()),
                    updated_at= datetime.combine(event_date, datetime.min.time()),
                    metadata={
                        "plan": customer["plan"],
                        "billing_cycle" : customer["billing_cycle"], 
                        "region" : customer["region"]
                        }
                    )
                events.append(event)
    return pd.DataFrame(events)

df = generate_billing_events()

print(f"\nTotal events generated: {len(df)}\n")
print(df.head(10))

    





