import pandas as pd
from data_generator.generate_events import generate_billing_events

df = generate_billing_events()

monthly_count = (
    df[df["metadata"].apply(lambda x: x["billing_cycle"] == "monthly")]
    .groupby("customer_id")
)

annual_count = (
    df[df["metadata"].apply(lambda x: x["billing_cycle"] == "annually")]
    .groupby("customer_id")
    .size()
)

print("Monthly invoices per customer:")
print(monthly_count.describe())

print("\nAnnual invoices per customer:")
print(annual_count.describe())

print("\nTotal events:", len(df))

