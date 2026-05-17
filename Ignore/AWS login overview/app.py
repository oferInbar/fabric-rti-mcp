from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

_APP_DIR = Path(__file__).resolve().parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

from utils.env_bootstrap import load_mcp_env  # noqa: E402

_APPLIED_ENV = load_mcp_env()

from data import client, queries  # noqa: E402
from risk import baselines, scoring  # noqa: E402
from utils.constants import (  # noqa: E402
    DEFAULT_TIME_WINDOW_DAYS,
    MAX_LOGIN_ROWS,
    TIME_WINDOW_CHOICES,
)
from utils.geo import is_aws_ip, is_service_user_agent  # noqa: E402
from views.drilldown import render_drilldown  # noqa: E402
from views.map_view import render_map  # noqa: E402
from views.user_panel import render_user_panel  # noqa: E402

st.set_page_config(page_title="AWS Login Overview", layout="wide")
st.title("AWS Login Overview")
st.caption("Source: Microsoft Defender Advanced Hunting → AWSCloudTrail")


def _filter_human_logins(df: pd.DataFrame, exclude_aws_ips: bool, exclude_sdk_ua: bool) -> pd.DataFrame:
    if df.empty:
        return df
    out = df
    if exclude_aws_ips:
        out = out[~out["SourceIpAddress"].apply(is_aws_ip)]
    if exclude_sdk_ua:
        out = out[~out["UserAgent"].apply(is_service_user_agent)]
    return out.reset_index(drop=True)


with st.sidebar:
    st.header("Filters")
    days = st.select_slider(
        "Time window (days)",
        options=list(TIME_WINDOW_CHOICES),
        value=DEFAULT_TIME_WINDOW_DAYS,
    )
    risk_threshold = st.slider("Min risk score", 0.0, 1.0, 0.0, 0.05)
    account_id = st.text_input("AWS Account ID filter (optional)", value="").strip() or None
    exclude_aws_ips = st.checkbox("Exclude AWS-owned source IPs", value=True)
    exclude_sdk_ua = st.checkbox("Exclude aws-sdk-* / AWS Internal user agents", value=True)
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    with st.expander("Graph endpoint (debug)"):
        st.caption(
            f"Loaded {len(_APPLIED_ENV)} env vars from MCP config" if _APPLIED_ENV else "MCP config not auto-loaded"
        )
        st.code(
            "\n".join(
                f"{k}={os.environ.get(k, '<unset>')}"
                for k in (
                    "DEFENDER_GRAPH_API_BASE_URL",
                    "DEFENDER_GRAPH_TOKEN_SCOPE",
                    "DEFENDER_GRAPH_TENANT_ID",
                    "HUNTING_ENDPOINT",
                    "HUNTING_QUERY_FIELD_NAME",
                    "AH_MODE",
                )
            ),
            language="bash",
        )


try:
    logins_raw = client.query(
        queries.build_logins_kql(days=days, account_id=account_id),
        timespan=queries.timespan_for_days(days),
        max_results=MAX_LOGIN_ROWS,
    )
    baseline_df = baselines.fetch_baseline(account_id=account_id)
    alerts_raw = client.query(queries.build_alerts_kql(), timespan="P30D", max_results=5000)
except Exception as e:  # noqa: BLE001
    st.error(f"Failed to query Advanced Hunting: {e}")
    st.stop()

logins = _filter_human_logins(logins_raw, exclude_aws_ips, exclude_sdk_ua)

heuristic_df = scoring.heuristic_score(logins, baseline_df)
alerts_df = scoring.alert_score(alerts_raw)
user_risk = scoring.combine_scores(heuristic_df, alerts_df)

if not user_risk.empty and risk_threshold > 0:
    keep = set(user_risk[user_risk["Risk"] >= risk_threshold]["User"])
    logins = logins[logins["User"].isin(keep)].reset_index(drop=True)
    user_risk = user_risk[user_risk["User"].isin(keep)].reset_index(drop=True)

c_top1, c_top2, c_top3, c_top4 = st.columns(4)
c_top1.metric("Sign-ins", len(logins))
c_top2.metric("Distinct users", logins["User"].nunique() if not logins.empty else 0)
c_top3.metric("Distinct countries", logins["Country"].nunique() if not logins.empty else 0)
c_top4.metric(
    "Impossible-travel users",
    int(heuristic_df["ImpossibleTravel"].sum()) if not heuristic_df.empty else 0,
)

selection = render_map(logins, user_risk)

selected_geo = selection["geo"] if selection else []
selected_user = st.session_state.get("selected_user")

col_left, col_right = st.columns([3, 4], gap="large")
with col_left:
    user_pick = render_drilldown(logins, user_risk, selected_geo)
    if user_pick:
        st.session_state["selected_user"] = user_pick
        selected_user = user_pick

with col_right:
    if selected_user:
        render_user_panel(selected_user, days, logins, user_risk)
    else:
        st.caption("Select a user from the drilldown table to open the deep-dive panel.")

with st.expander("Top users by risk"):
    if not user_risk.empty:
        st.dataframe(
            user_risk[
                [
                    "User",
                    "Risk",
                    "HeuristicScore",
                    "AlertScore",
                    "AlertCount",
                    "ImpossibleTravel",
                    "Logins",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )
