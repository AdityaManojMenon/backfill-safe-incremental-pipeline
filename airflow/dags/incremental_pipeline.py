from data_generator.generate_events import generate_billing_events
from data_generator.generate_updates import generate_updates

# -------------------------
# Phase 1: Base invoices
# -------------------------
df_base = generate_billing_events(
    num_customers=400,
    months_back=36
)

# -------------------------
# Phase 2: Late updates
# -------------------------
df_updated = generate_updates(df_base)

# -------------------------
# Phase 3: Extract cancellations
# -------------------------
cancellations = (
    df_updated.loc[
        df_updated["metadata"].apply(
            lambda m: m.get("adjustment_reason") == "cancellation"
        ),
        ["customer_id", "updated_at"]
    ]
    .drop_duplicates("customer_id")
    .set_index("customer_id")["updated_at"]
    .to_dict()
)

# -------------------------
# Phase 4: Regenerate future invoices with lifecycle enforcement
# -------------------------
df_lifecycle_aware = generate_billing_events(
    num_customers=400,
    months_back=36,
    cancellations=cancellations
)
