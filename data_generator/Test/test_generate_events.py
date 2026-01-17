import pandas as pd
from data_generator.generate_events import generate_billing_events

df = generate_billing_events()

# Pick 3 random customers
random_customers = (
    df["customer_id"]
    .drop_duplicates()
    .sample(3, random_state=42)
    .tolist()
)

print("Selected customers:", random_customers)

# Show full billing history for each
for cust in random_customers:
    print(f"\n===== Billing history for {cust} =====")
    print(
        df[df["customer_id"] == cust]
        .sort_values("event_ts")
    )
