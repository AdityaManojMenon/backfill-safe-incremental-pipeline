import pandas as pd
from data_generator.generate_events import generate_billing_events
from data_generator.generate_updates import generate_updates

def test_late_arriving_updates(updated_df, original_df):
    changed = updated_df[
        updated_df["updated_at"] > original_df["updated_at"]
    ]
    assert len(changed) > 0

def test_refunds_are_negative(updated_df):
    refunds = updated_df[updated_df["event_type"] == "invoice_refunded"]
    assert (refunds["amount"] < 0).all()

def test_proration_metadata(updated_df):
    proration = updated_df[
        updated_df["metadata"].apply(
            lambda m: m.get("adjustment_reason") == "proration"
        )
    ]
    assert proration.shape[0] > 0

def test_idempotency(df):
    from data_generator.generate_updates import generate_updates
    once = generate_updates(df)
    twice = generate_updates(df)
    pd.testing.assert_frame_equal(once, twice)