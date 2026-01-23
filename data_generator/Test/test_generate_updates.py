import pandas as pd
from data_generator.generate_events import generate_billing_events
from data_generator.generate_updates import generate_updates

df = generate_billing_events()
df_updated = generate_updates(df)
mask = df_updated["event_ts"] != df_updated["updated_at"]
changed_ids = df_updated.loc[mask, "event_id"]

before = df[df["event_id"].isin(changed_ids)].copy()
after  = df_updated[df_updated["event_id"].isin(changed_ids)].copy()

merged = before.merge(
    after,
    on="event_id",
    suffixes=("_before", "_after")
)

cols = [
    "event_id",
    "customer_id_before",
    "event_type_before", "event_type_after",
    "amount_before", "amount_after",
    "event_ts_before", "updated_at_before", "updated_at_after",
    "metadata_before", "metadata_after",
]
print(merged[cols].head(20))
