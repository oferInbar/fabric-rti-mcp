from __future__ import annotations

import pandas as pd
import streamlit as st


def render_drilldown(
    logins: pd.DataFrame,
    user_risk: pd.DataFrame,
    geo_selection: list[tuple[str, str]],
) -> str | None:
    """Render the list of users in the selected geography. Returns selected user."""
    if not geo_selection:
        st.caption("Click a city on the map to drill down into users.")
        return None

    labels = ", ".join(f"{city or '?'}, {country or '?'}" for country, city in geo_selection)
    st.subheader(f"Users active in: {labels}")

    mask = pd.Series(False, index=logins.index)
    for country, city in geo_selection:
        mask |= (logins["Country"] == country) & (logins["City"] == city)
    subset = logins[mask]
    if subset.empty:
        st.info("No matching sign-ins.")
        return None

    per_user = (
        subset.groupby("User")
        .agg(
            Logins=("SourceIpAddress", "count"),
            DistinctIPs=("SourceIpAddress", "nunique"),
            LastSeen=("TimeGenerated", "max"),
            AWSAccount=("UserIdentityAccountId", "first"),
        )
        .reset_index()
    )
    if not user_risk.empty:
        per_user = per_user.merge(
            user_risk[["User", "Risk", "HeuristicScore", "AlertScore", "AlertCount"]],
            on="User",
            how="left",
        )
    per_user = per_user.sort_values("Risk", ascending=False, na_position="last")

    event = st.dataframe(
        per_user,
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
        key="drilldown_table",
    )
    rows = (event or {}).get("selection", {}).get("rows") if isinstance(event, dict) else None
    if rows:
        return per_user.iloc[rows[0]]["User"]
    return None
