import pandas as pd
from data_generator.generate_events import generate_billing_events

def test_event_schema(df):
    required_cols = {
        "event_id", "customer_id", "event_type",
        "amount", "event_ts", "updated_at", "metadata"
    }
    assert required_cols.issubset(df.columns)

def test_event_ts_immutable(df):
    assert (df["event_ts"] <= df["updated_at"]).all()

def test_positive_invoice_amounts(df):
    paid = df[df["event_type"] == "invoice_paid"]
    assert (paid["amount"] > 0).all()


