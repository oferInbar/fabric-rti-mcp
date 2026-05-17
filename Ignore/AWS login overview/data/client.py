from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from defender_ah_mcp.services.hunting.hunting_service import run_hunting_query  # noqa: E402

from utils.constants import CACHE_TTL_SECONDS  # noqa: E402


def _results_to_dataframe(payload: dict[str, Any]) -> pd.DataFrame:
    results = payload.get("Results") or payload.get("results") or []
    if not results:
        schema = payload.get("Schema") or payload.get("schema") or []
        cols = [c.get("Name") or c.get("name") for c in schema]
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(results)
    for col in df.columns:
        if "Time" in col or col.endswith("Seen"):
            try:
                df[col] = pd.to_datetime(df[col], utc=True, errors="ignore")
            except Exception:
                pass
    return df


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner="Querying Defender Advanced Hunting…")
def query(kql: str, timespan: str = "P7D", max_results: int = 5000) -> pd.DataFrame:
    payload = run_hunting_query(query=kql, timespan=timespan, max_results=max_results)
    if payload.get("error"):
        raise RuntimeError(f"Hunting query failed: {payload}")
    return _results_to_dataframe(payload)
