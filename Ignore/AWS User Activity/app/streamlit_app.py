"""AWS User Activity dashboard.

Run with:
    streamlit run "Ignore/AWS User Activity/app/streamlit_app.py"

Auth model: this app calls the same `run_hunting_query` function the
Defender Advanced Hunting MCP server uses, so it inherits the MCP
server's Graph base URL / scope / endpoint and credential chain (set via
DEFENDER_GRAPH_* / HUNTING_* env vars, optionally auto-loaded from
~/.copilot/mcp-config.json via utils.env_bootstrap).

Rows implemented: 1 (Health & Volume), 2 (Authentication & Access),
3 (IAM / Privilege Changes), 5 (Cross-Account & Region), 6 (Top-N
Drill-downs). Rows 4 (Behavioral Anomalies) and 7 (Identity
Correlation) are deferred — they depend on per-user baselines and an
AWS-principal -> Entra resolution helper that haven't been built yet.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

_APP_DIR = Path(__file__).resolve().parent
if str(_APP_DIR) not in sys.path:
    sys.path.insert(0, str(_APP_DIR))

from utils.env_bootstrap import load_mcp_env  # noqa: E402

_APPLIED_ENV = load_mcp_env()

from data import client, queries  # noqa: E402
from risk import scoring  # noqa: E402
from utils.constants import (  # noqa: E402
    BASELINE_LOOKBACK_DAYS,
    BASELINE_TIMESPANS,
    DEFAULT_TIME_WINDOW,
    MAX_LOGIN_ROWS,
    MAX_RESULTS,
    TIME_WINDOWS,
    TIMESPANS,
    load_allowlist,
)
from utils.geo import is_aws_ip, is_service_user_agent  # noqa: E402
from views.drilldown import render_drilldown  # noqa: E402
from views.map_view import render_map  # noqa: E402
from views.user_panel import render_user_panel  # noqa: E402

st.set_page_config(page_title="AWS User Activity", layout="wide")
st.title("AWS User Activity")
st.caption("Source: Microsoft Defender Advanced Hunting → AWSCloudTrail")


@st.cache_data(ttl=600)
def _account_options() -> list[str]:
    df = client.query(queries.accounts_in_scope("7d"), timespan="P7D", max_results=200)
    if df.empty:
        return []
    return df["UserIdentityAccountId"].dropna().astype(str).tolist()


allowlist = load_allowlist()

with st.sidebar:
    st.header("Filters")
    window = st.select_slider(
        "Time range",
        options=list(TIME_WINDOWS.keys()),
        value=DEFAULT_TIME_WINDOW,
    )
    try:
        account_options = _account_options()
    except Exception as e:  # noqa: BLE001
        st.error(f"Could not load account list: {e}")
        account_options = []
    accounts = st.multiselect(
        "AWS accounts (empty = full Organization)",
        options=account_options,
    )
    human_only = st.toggle("Exclude workload roles (allowlist)", value=False)
    st.markdown("**Row 7 — Geo & deep-dive filters**")
    exclude_aws_ips = st.checkbox("Exclude AWS-owned source IPs", value=True)
    exclude_sdk_ua = st.checkbox("Exclude aws-sdk-* / AWS Internal user agents", value=True)
    risk_threshold = st.slider("Min risk score (map filter)", 0.0, 1.0, 0.0, 0.05)
    if st.button("Refresh data", width="stretch"):
        st.cache_data.clear()
        st.rerun()
    with st.expander("Graph endpoint (debug)"):
        st.caption(
            f"Loaded {len(_APPLIED_ENV)} env vars from MCP config"
            if _APPLIED_ENV
            else "MCP config not auto-loaded"
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

window_kql = TIME_WINDOWS[window]
window_iso = TIMESPANS[window]
accounts_arg = accounts or None
# Bin size scales with the selected window so the timeline stays legible.
_bin = "15m" if window in {"1h", "4h"} else "1h" if window == "24h" else "1d"


def _resolve(name: str, results: dict[str, object]) -> pd.DataFrame:
    """Unwrap a `query_many` result: surface per-job errors as inline
    warnings rather than killing the whole page."""
    v = results.get(name)
    if isinstance(v, Exception):
        st.warning(f"**{name}** — {v}")
        from data.client import HuntingQueryError  # local import to avoid cycles

        if isinstance(v, HuntingQueryError):
            with st.expander(f"Details · {name}", expanded=False):
                status = v.payload.get("status_code")
                if status:
                    st.markdown(f"**HTTP status:** `{status}`")
                detail = v.payload.get("detail")
                if detail not in (None, "", "{}"):
                    st.markdown("**Server detail:**")
                    st.code(str(detail), language="json")
                msg = v.payload.get("message")
                if msg:
                    st.markdown(f"**Transport message:** `{msg}`")
                st.markdown(f"**Attempts:** {v.attempts}")
                st.markdown("**KQL:**")
                st.code(v.kql, language="kusto")
        return pd.DataFrame()
    return v if isinstance(v, pd.DataFrame) else pd.DataFrame()


# ---------------------------------------------------------------------------
# Build all queries up-front and dispatch them in parallel. This collapses
# ~14 sequential Graph round-trips into one wall-clock batch.
# ---------------------------------------------------------------------------
jobs: dict[str, dict] = {
    "volume": dict(
        kql=queries.volume_tiles(window_kql, allowlist, accounts_arg, human_only),
        timespan=window_iso,
    ),
    "top_principals": dict(
        kql=queries.top_principals(window_kql, allowlist, accounts_arg, human_only=human_only),
        timespan=window_iso,
        max_results=MAX_RESULTS,
    ),
    "login_outcome": dict(
        kql=queries.login_outcome_timeline(window_kql, allowlist, accounts_arg, _bin, human_only=human_only),
        timespan=window_iso,
    ),
    "top_failed": dict(
        kql=queries.top_failed_logins(window_kql, allowlist, accounts_arg, human_only=human_only),
        timespan=window_iso,
    ),
    "logins_by_region": dict(
        kql=queries.logins_by_region(window_kql, allowlist, accounts_arg, human_only=human_only),
        timespan=window_iso,
    ),
    "new_geo": dict(
        kql=queries.new_geo_per_user(window_kql, allowlist, accounts_arg, human_only=human_only),
        timespan=BASELINE_TIMESPANS[window],
    ),
    "iam_timeline": dict(
        kql=queries.iam_sensitive_events_timeline(window_kql, accounts_arg, _bin),
        timespan=window_iso,
    ),
    "iam_table": dict(
        kql=queries.iam_events_table(window_kql, accounts_arg),
        timespan=window_iso,
        max_results=MAX_RESULTS,
    ),
    "cross_account": dict(
        kql=queries.cross_account_access(window_kql, allowlist, accounts_arg, human_only=human_only),
        timespan=window_iso,
    ),
    "region_heatmap": dict(
        kql=queries.region_heatmap(window_kql, allowlist, accounts_arg, human_only=human_only),
        timespan=window_iso,
        max_results=2000,
    ),
    "top_errors": dict(
        kql=queries.top_errors(window_kql, allowlist, accounts_arg, human_only=human_only),
        timespan=window_iso,
    ),
    "top_event_sources": dict(
        kql=queries.top_event_sources(window_kql, allowlist, accounts_arg, human_only=human_only),
        timespan=window_iso,
    ),
    "top_source_ips": dict(
        kql=queries.top_source_ips(window_kql, allowlist, accounts_arg, human_only=human_only),
        timespan=window_iso,
    ),
    # Row 7 — geo / risk inputs
    "geo_logins": dict(
        kql=queries.build_logins_kql(accounts_arg, max_rows=MAX_LOGIN_ROWS),
        timespan=window_iso,
        max_results=MAX_LOGIN_ROWS,
    ),
    "geo_baseline": dict(
        kql=queries.build_baseline_kql(accounts_arg),
        timespan=f"P{BASELINE_LOOKBACK_DAYS}D",
        max_results=MAX_LOGIN_ROWS,
    ),
    "geo_alerts": dict(
        kql=queries.build_alerts_kql(),
        timespan="P30D",
        max_results=5000,
    ),
}

# Defender Advanced Hunting throttles >~6 concurrent queries per tenant
# (excess submissions time out at ~30s). Dispatch in ordered batches of 6
# aligned with page sections so the top of the dashboard renders while the
# next batch is still in flight. See files/bench_parallel.py for the
# experiment that produced these numbers.
#
# Convention: within each batch, queries whose results are rendered inside
# a collapsed `st.expander` (expanded=False) are placed LAST. The thread
# pool services jobs in submission order, so this lets the visible
# widgets finish first and keeps the perceived render time tight even
# when a single query in the batch is slow.
_BATCHES: list[tuple[str, list[str]]] = [
    # `top_principals` -> expander (line ~271), kept last.
    ("Rows 1–2", ["volume", "login_outcome", "top_failed", "logins_by_region", "new_geo", "top_principals"]),
    # `iam_table` -> expander (line ~351), kept last.
    ("Rows 3 & 5", ["iam_timeline", "cross_account", "region_heatmap", "iam_table"]),
    # No expander-only queries in this batch (the row-7 expander reuses
    # geo_* results that are already needed for the visible map/drilldown).
    ("Rows 6 & 7", ["top_errors", "top_event_sources", "top_source_ips", "geo_logins", "geo_baseline", "geo_alerts"]),
]
results: dict[str, object] = {}


def _run_next_batch(idx: int) -> None:
    label, names = _BATCHES[idx]
    batch_jobs = {n: jobs[n] for n in names if n in jobs}
    if not batch_jobs:
        return
    with st.spinner(
        f"Running batch {idx + 1}/{len(_BATCHES)} — {label} "
        f"({len(batch_jobs)} queries)…"
    ):
        results.update(client.query_many(batch_jobs))


_run_next_batch(0)


# ---------------------------------------------------------------------------
# Row 1 — Health & Volume
# ---------------------------------------------------------------------------
st.subheader("Row 1 — Health & Volume")
volume_df = _resolve("volume", results)
if volume_df.empty:
    st.info("No events in selected window.")
else:
    row = volume_df.iloc[0]
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric(f"Total events ({window})", int(row.get("TotalEvents", 0)))
    c2.metric("Distinct principals", int(row.get("DistinctPrincipals", 0)))
    c3.metric("Failed API calls", int(row.get("FailedApiCalls", 0)))
    c4.metric("Root account events", int(row.get("RootEvents", 0)))
    c5.metric("Console logins", int(row.get("ConsoleLogins", 0)))
    c6.metric("MFA-less logins", int(row.get("MfaLessLogins", 0)))

with st.expander(f"Top principals (last {window})", expanded=False):
    top_df = _resolve("top_principals", results)
    st.dataframe(top_df, width="stretch", hide_index=True)

# ---------------------------------------------------------------------------
# Row 2 — Authentication & Access
# ---------------------------------------------------------------------------
st.subheader("Row 2 — Authentication & Access")

c1, c2 = st.columns([3, 2])

with c1:
    st.markdown("**Login outcome timeline**")
    timeline_df = _resolve("login_outcome", results)
    if timeline_df.empty:
        st.info("No ConsoleLogin events.")
    else:
        timeline_df["TimeGenerated"] = pd.to_datetime(timeline_df["TimeGenerated"])
        fig = px.area(
            timeline_df,
            x="TimeGenerated",
            y="Events",
            color="Outcome",
            color_discrete_map={
                "Success": "#2ca02c",
                "Success (no MFA)": "#ff7f0e",
                "Failure": "#d62728",
            },
        )
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width="stretch")

with c2:
    st.markdown("**Top 10 users by failed logins**")
    failed_df = _resolve("top_failed", results)
    st.dataframe(failed_df, width="stretch", hide_index=True, height=300)

c3, c4 = st.columns(2)

with c3:
    st.markdown("**Console logins by AWS region**")
    region_df = _resolve("logins_by_region", results)
    if region_df.empty:
        st.info("No ConsoleLogin events.")
    else:
        fig = px.bar(
            region_df,
            x="AWSRegion",
            y="Events",
            color="Outcome",
            color_discrete_map={"Success": "#2ca02c", "Failure": "#d62728"},
        )
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, width="stretch")

with c4:
    st.markdown(f"**New (user, source IP) pairs in last {window}** vs 7d baseline")
    new_geo_df = _resolve("new_geo", results)
    st.dataframe(new_geo_df, width="stretch", hide_index=True, height=300)

# ---------------------------------------------------------------------------
# Row 3 — Identity & Privilege Changes (IAM)
# ---------------------------------------------------------------------------
_run_next_batch(1)
st.subheader("Row 3 — IAM & Privilege Changes")

iam_timeline_df = _resolve("iam_timeline", results)
if iam_timeline_df.empty:
    st.info("No sensitive IAM events in window.")
else:
    iam_timeline_df["TimeGenerated"] = pd.to_datetime(iam_timeline_df["TimeGenerated"])
    fig = px.bar(
        iam_timeline_df,
        x="TimeGenerated",
        y="Events",
        color="EventName",
    )
    fig.update_layout(height=280, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig, width="stretch")

with st.expander("Sensitive IAM events (recent)", expanded=False):
    iam_table_df = _resolve("iam_table", results)
    st.dataframe(iam_table_df, width="stretch", hide_index=True)

# ---------------------------------------------------------------------------
# Row 5 — Cross-Account & Region
# ---------------------------------------------------------------------------
st.subheader("Row 5 — Cross-Account & Region")

st.markdown("**Cross-account access**")
xacct_df = _resolve("cross_account", results)
if xacct_df.empty:
    st.info("No cross-account events (caller account == resource account).")
else:
    st.dataframe(xacct_df, width="stretch", hide_index=True, height=280)

st.markdown("**Activity by user × region (top 25 users)**")
heat_df = _resolve("region_heatmap", results)
if heat_df.empty:
    st.info("No region activity.")
else:
    pivot = heat_df.pivot_table(
        index="UserIdentityArn",
        columns="Region",
        values="Events",
        aggfunc="sum",
        fill_value=0,
    )
    pivot.index = pivot.index.map(lambda s: s.rsplit("/", 1)[-1] if s else "(empty)")
    fig = px.imshow(
        pivot,
        color_continuous_scale="Blues",
        aspect="auto",
        labels=dict(color="Events"),
    )
    fig.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig, width="stretch")

# ---------------------------------------------------------------------------
# Row 6 — Top-N Drill-downs
# ---------------------------------------------------------------------------
_run_next_batch(2)
st.subheader("Row 6 — Top-N Drill-downs")

c7, c8, c9 = st.columns(3)

with c7:
    st.markdown("**Top error codes**")
    st.dataframe(_resolve("top_errors", results), width="stretch", hide_index=True, height=360)

with c8:
    st.markdown("**Top event sources (services)**")
    st.dataframe(_resolve("top_event_sources", results), width="stretch", hide_index=True, height=360)

with c9:
    st.markdown("**Top source IPs**")
    st.dataframe(_resolve("top_source_ips", results), width="stretch", hide_index=True, height=360)

# ---------------------------------------------------------------------------
# Row 7 — Geo map & Entity Deep-Dive
# ---------------------------------------------------------------------------
st.subheader("Row 7 — Geo map & entity deep-dive")


def _filter_human_logins(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df
    if exclude_aws_ips:
        out = out[~out["SourceIpAddress"].apply(is_aws_ip)]
    if exclude_sdk_ua:
        out = out[~out["UserAgent"].apply(is_service_user_agent)]
    return out.reset_index(drop=True)


logins_raw = _resolve("geo_logins", results)
baseline_df = _resolve("geo_baseline", results)
alerts_raw = _resolve("geo_alerts", results)

if not baseline_df.empty:
    for col in ("BaselineCountries", "BaselineIps"):
        if col in baseline_df.columns:
            baseline_df[col] = baseline_df[col].apply(
                lambda v: set(v) if isinstance(v, list) else set()
            )

logins = _filter_human_logins(logins_raw)
heuristic_df = scoring.heuristic_score(logins, baseline_df)
alerts_df = scoring.alert_score(alerts_raw)
user_risk = scoring.combine_scores(heuristic_df, alerts_df)

if not user_risk.empty and risk_threshold > 0:
    keep = set(user_risk[user_risk["Risk"] >= risk_threshold]["User"])
    logins = logins[logins["User"].isin(keep)].reset_index(drop=True)
    user_risk = user_risk[user_risk["User"].isin(keep)].reset_index(drop=True)

g1, g2, g3, g4 = st.columns(4)
g1.metric("Sign-ins", len(logins))
g2.metric("Distinct users", logins["User"].nunique() if not logins.empty else 0)
g3.metric("Distinct countries", logins["Country"].nunique() if not logins.empty else 0)
g4.metric(
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
        render_user_panel(selected_user, window_iso, logins, user_risk)
    else:
        st.caption("Select a user from the drilldown table to open the deep-dive panel.")

with st.expander("Top users by risk"):
    if not user_risk.empty:
        st.dataframe(
            user_risk[
                ["User", "Risk", "HeuristicScore", "AlertScore", "AlertCount",
                 "ImpossibleTravel", "Logins"]
            ],
            width="stretch",
            hide_index=True,
        )

st.caption(
    "Row 4 (Behavioral Anomalies) and the full identity correlation "
    "(`ResolveAwsPrincipal()` + IAM-user→UPN watchlist + cross-cloud "
    "impossible travel) are deferred to Phases 4–5. See AnalyticsPlan.md §9."
)
