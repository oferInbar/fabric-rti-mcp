from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def _aggregate_by_city(
    logins: pd.DataFrame, user_risk: pd.DataFrame
) -> pd.DataFrame:
    if logins.empty:
        return logins
    risk_lookup = dict(zip(user_risk["User"], user_risk["Risk"])) if not user_risk.empty else {}
    df = logins.copy()
    df["UserRisk"] = df["User"].map(risk_lookup).fillna(0.0)
    agg = df.groupby(["Country", "City", "Latitude", "Longitude"], dropna=False).agg(
        Logins=("User", "count"),
        Users=("User", "nunique"),
        MaxRisk=("UserRisk", "max"),
        SampleUsers=("User", lambda s: ", ".join(sorted(set(s))[:5])),
    ).reset_index()
    return agg


def render_map(logins: pd.DataFrame, user_risk: pd.DataFrame) -> dict | None:
    """Render the world map and return the click selection (or None)."""
    st.subheader("AWS sign-ins — world map")
    if logins.empty:
        st.info("No sign-ins in the selected window.")
        return None

    agg = _aggregate_by_city(logins, user_risk)
    fig = px.scatter_geo(
        agg,
        lat="Latitude",
        lon="Longitude",
        size="Logins",
        color="MaxRisk",
        color_continuous_scale="RdYlGn_r",
        range_color=(0.0, 1.0),
        hover_name="City",
        hover_data={
            "Country": True,
            "Logins": True,
            "Users": True,
            "MaxRisk": ":.2f",
            "SampleUsers": True,
            "Latitude": False,
            "Longitude": False,
        },
        projection="natural earth",
        size_max=40,
    )
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=520)

    selection = st.plotly_chart(
        fig,
        use_container_width=True,
        on_select="rerun",
        selection_mode=("points", "box", "lasso"),
        key="map_chart",
    )
    points = (selection or {}).get("selection", {}).get("points") if isinstance(selection, dict) else None
    if not points:
        return None

    selected_keys = []
    for p in points:
        idx = p.get("point_index")
        if idx is None or idx >= len(agg):
            continue
        row = agg.iloc[idx]
        selected_keys.append((row["Country"], row["City"]))
    return {"geo": selected_keys, "agg": agg}
