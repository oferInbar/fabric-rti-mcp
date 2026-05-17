from __future__ import annotations

import pandas as pd
import streamlit as st

from utils.identity import friendly_user_name


def _risk_badge(risk: float) -> str:
    if pd.isna(risk):
        return "·"
    if risk >= 0.7:
        return f"🔴 {risk:.2f}"
    if risk >= 0.4:
        return f"🟠 {risk:.2f}"
    if risk > 0:
        return f"🟡 {risk:.2f}"
    return f"⚪ {risk:.2f}"


def render_drilldown(
    logins: pd.DataFrame,
    user_risk: pd.DataFrame,
    geo_selection: list[tuple[str, str]],
) -> str | None:
    """List users active in the selected geography. Returns selected user ARN."""
    if not geo_selection:
        st.caption("Click a city on the map to drill down into users.")
        return None

    labels = ", ".join(f"{city or '?'}, {country or '?'}" for country, city in geo_selection)
    st.subheader(f"Users active in: {labels}")

    geo_key = tuple(sorted(geo_selection))
    if st.session_state.get("drilldown_geo_key") != geo_key:
        st.session_state["drilldown_geo_key"] = geo_key
        st.session_state.pop("drilldown_selected_user", None)

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
        )
        .reset_index()
    )
    if not user_risk.empty:
        per_user = per_user.merge(
            user_risk[["User", "Risk"]],
            on="User",
            how="left",
        )
    else:
        per_user["Risk"] = float("nan")
    per_user = per_user.sort_values("Risk", ascending=False, na_position="last")

    selected_user = st.session_state.get("drilldown_selected_user")
    st.caption(f"{len(per_user)} user(s). Click **Investigate** to open a deep-dive.")

    max_rows = 25
    for _, row in per_user.head(max_rows).iterrows():
        arn = row["User"]
        friendly = friendly_user_name(arn)
        is_selected = arn == selected_user
        with st.container(border=True):
            col_name, col_meta, col_btn = st.columns([5, 3, 2])
            with col_name:
                marker = "▸ " if is_selected else ""
                st.markdown(f"**{marker}{friendly}**")
                st.caption(arn)
            with col_meta:
                st.markdown(
                    f"{_risk_badge(row['Risk'])} &nbsp;·&nbsp; "
                    f"{int(row['Logins'])} logins &nbsp;·&nbsp; "
                    f"{int(row['DistinctIPs'])} IP(s)"
                )
                st.caption(f"Last seen: {row['LastSeen']}")
            with col_btn:
                label = "✅ Selected" if is_selected else "🔍 Investigate →"
                if st.button(label, key=f"drill_btn_{arn}", width="stretch", disabled=is_selected):
                    st.session_state["drilldown_selected_user"] = arn
                    st.rerun()

    if len(per_user) > max_rows:
        st.caption(f"+{len(per_user) - max_rows} more not shown.")

    return st.session_state.get("drilldown_selected_user")

