import uuid
import random 
import numpy as np
import pandas as pd
from typing import Dict, Optional
from datetime import datetime, timedelta

#Already defined in generate events file
PLAN_PRICES = {
    "basic": 10,
    "pro": 20,
    "premium": 100
}

#Update Distribution
NO_CHANGE_PCT = 0.75
REFUND_PCT = 0.12
PRORATION_PCT = 0.08
DISCOUNT_PCT = 0.05
MAX_UPDATE_LAG_MONTHS = 6  # updates can arrive up to 6 months later

def generate_updates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply late-arriving corrections to historical subscription invoices.
    Updates occur in-place (same event_id, later updated_at).
    |-- 75% (no change) -- 12% (refund) -- 8% (proration) -- 5% (discount) --|
    """
    random.seed(42)
    updated_df = df.copy()
    #Vectorize random generation (fast) and deterministic because of the random.seed(42)
    rolls = np.random.random(len(updated_df))

    #Mask based update selection instead of using python for loop and itterows() for improved efficiency using numpy and pandas
    no_change_mask = rolls < NO_CHANGE_PCT
    refund_mask = ((rolls >= NO_CHANGE_PCT) & (rolls < NO_CHANGE_PCT + REFUND_PCT))
    proration_mask = ((rolls >= NO_CHANGE_PCT + REFUND_PCT) & (rolls < NO_CHANGE_PCT + REFUND_PCT + PRORATION_PCT))
    discount_mask = rolls >= NO_CHANGE_PCT + REFUND_PCT + PRORATION_PCT

    #Late arriving update lag
    lag_months = pd.Series(
    np.random.randint(1, MAX_UPDATE_LAG_MONTHS + 1, size=len(updated_df)),
    index=updated_df.index
)
    
    #Apply change only to rows that change
    change_mask = ~no_change_mask #all rows not in no_change_mask
    updated_df.loc[change_mask, "updated_at"] = (
    updated_df.loc[change_mask]
    .apply(lambda r: r["updated_at"] + pd.DateOffset(months=int(lag_months.loc[r.name])), axis=1)
)
    #Refund
    updated_df.loc[refund_mask, "event_type"] = "invoice_refunded"
    updated_df.loc[refund_mask, "amount"] = -updated_df.loc[refund_mask, "amount"].abs()
    updated_df.loc[refund_mask, "metadata"] = updated_df.loc[refund_mask, "metadata"].apply(
    lambda m: {**m, "adjustment_reason": "cancellation"}
)

    #Proration
    direction = np.random.choice(["Upgrade","Downgrade"], size = proration_mask.sum())

    
    return updated_df

