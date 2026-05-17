from __future__ import annotations

import pandas as pd
import streamlit as st

from data import client, queries


def render_user_panel(
    user: str,
    days: int,
    logins: pd.DataFrame,
    user_risk: pd.DataFrame,
) -> None:
    st.subheader(f"👤 {user}")

    risk_row = user_risk.loc[user_risk["User"] == user]
    if not risk_row.empty:
        r = risk_row.iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Risk", f"{r['Risk']:.2f}")
        c2.metric("Heuristic", f"{r['HeuristicScore']:.2f}")
        c3.metric("Alert score", f"{r['AlertScore']:.2f}")
        c4.metric("Defender alerts", int(r["AlertCount"]))

        flags = []
        if r.get("ImpossibleTravel"):
            flags.append("🚀 Impossible travel")
        if r.get("NewCountries"):
            flags.append(f"🌍 New countries: {', '.join(r['NewCountries'])}")
        if r.get("NewIps"):
            ips = r["NewIps"]
            preview = ", ".join(ips[:5]) + (f" (+{len(ips) - 5})" if len(ips) > 5 else "")
            flags.append(f"🆕 New IPs: {preview}")
        if flags:
            for f in flags:
                st.markdown(f"- {f}")

    user_logins = logins[logins["User"] == user].copy()
    if user_logins.empty:
        st.info("No sign-ins for this user in the selected window.")
        return

    st.markdown("**Identity**")
    ident = user_logins.iloc[0]
    st.write({
        "AWS Account": ident.get("UserIdentityAccountId"),
        "Identity Type": ident.get("UserIdentityType"),
        "First seen (window)": str(user_logins["TimeGenerated"].min()),
        "Last seen (window)": str(user_logins["TimeGenerated"].max()),
    })

    st.markdown("**Recent IPs**")
    ip_table = (
        user_logins.groupby(["SourceIpAddress", "Country", "City"])
        .agg(
            FirstSeen=("TimeGenerated", "min"),
            LastSeen=("TimeGenerated", "max"),
            Logins=("Day", "count"),
        )
        .reset_index()
        .sort_values("LastSeen", ascending=False)
    )
    st.dataframe(ip_table, use_container_width=True, hide_index=True)

    st.markdown("**AWS Regions hit**")
    regions = (
        user_logins.groupby("AWSRegion")
        .size()
        .reset_index(name="Logins")
        .sort_values("Logins", ascending=False)
    )
    st.dataframe(regions, use_container_width=True, hide_index=True)

    with st.expander("Top events & user-agents (deep dive)"):
        try:
            detail = client.query(
                queries.build_user_detail_kql(user, days),
                timespan=queries.timespan_for_days(days),
                max_results=500,
            )
            st.dataframe(detail, use_container_width=True, hide_index=True)
        except Exception as e:  # noqa: BLE001
            st.error(f"User detail query failed: {e}")

    with st.expander("Underlying KQL (logins)"):
        st.code(queries.build_logins_kql(days), language="kusto")
