from __future__ import annotations

import pandas as pd

from data import client, queries
from utils.constants import BASELINE_LOOKBACK_DAYS


def fetch_baseline(account_id: str | None = None) -> pd.DataFrame:
    """Per-user baseline of distinct countries and IPs over the prior N days."""
    kql = queries.build_baseline_kql(account_id=account_id)
    df = client.query(kql, timespan=queries.timespan_for_days(BASELINE_LOOKBACK_DAYS))
    if df.empty:
        return pd.DataFrame(columns=["User", "BaselineCountries", "BaselineIps"])
    for col in ("BaselineCountries", "BaselineIps"):
        if col in df.columns:
            df[col] = df[col].apply(lambda v: set(v) if isinstance(v, list) else set())
    return df
