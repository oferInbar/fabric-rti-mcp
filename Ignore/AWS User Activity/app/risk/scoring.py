from __future__ import annotations

from math import asin, cos, radians, sin, sqrt

import pandas as pd

from utils.constants import (
    ALERT_SEVERITY_SCORE,
    MIN_DISTANCE_KM,
    RISK_WEIGHTS,
    SPEED_THRESHOLD_KMH,
)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    lat1r, lat2r = radians(lat1), radians(lat2)
    dlat = lat2r - lat1r
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(lat1r) * cos(lat2r) * sin(dlon / 2) ** 2
    return 2 * r * asin(sqrt(a))


def _impossible_travel_for_user(group: pd.DataFrame) -> bool:
    g = group.sort_values("TimeGenerated").reset_index(drop=True)
    for i in range(1, len(g)):
        prev, cur = g.iloc[i - 1], g.iloc[i]
        if prev["SourceIpAddress"] == cur["SourceIpAddress"]:
            continue
        dist_km = _haversine_km(
            prev["Latitude"], prev["Longitude"], cur["Latitude"], cur["Longitude"]
        )
        if dist_km < MIN_DISTANCE_KM:
            continue
        hours = (cur["TimeGenerated"] - prev["TimeGenerated"]).total_seconds() / 3600.0
        if hours <= 0:
            continue
        if dist_km / hours > SPEED_THRESHOLD_KMH:
            return True
    return False


def heuristic_score(logins: pd.DataFrame, baseline: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "User", "HeuristicScore", "ImpossibleTravel",
        "NewCountries", "NewIps", "Countries", "Ips", "Logins",
    ]
    if logins.empty:
        return pd.DataFrame(columns=cols)

    base_lookup = {
        row["User"]: (row.get("BaselineCountries") or set(), row.get("BaselineIps") or set())
        for _, row in baseline.iterrows()
    } if not baseline.empty else {}

    rows = []
    for user, g in logins.groupby("User"):
        countries = set(g["Country"].dropna().unique())
        ips = set(g["SourceIpAddress"].dropna().unique())
        b_countries, b_ips = base_lookup.get(user, (set(), set()))
        new_countries = countries - b_countries
        new_ips = ips - b_ips

        r_travel = 1.0 if _impossible_travel_for_user(g) else 0.0
        r_country = min(1.0, len(new_countries) * 0.5) if b_countries else 0.0
        r_ip = min(1.0, len(new_ips) / 5.0) if b_ips else 0.0

        score = (
            RISK_WEIGHTS["impossible_travel"] * r_travel
            + RISK_WEIGHTS["new_country"] * r_country
            + RISK_WEIGHTS["new_ip"] * r_ip
        )
        score = max(0.0, min(1.0, score))

        rows.append({
            "User": user,
            "HeuristicScore": round(score, 3),
            "ImpossibleTravel": bool(r_travel),
            "NewCountries": sorted(new_countries),
            "NewIps": sorted(new_ips),
            "Countries": sorted(countries),
            "Ips": sorted(ips),
            "Logins": int(len(g)),
        })
    return pd.DataFrame(rows)


def alert_score(alerts: pd.DataFrame) -> pd.DataFrame:
    if alerts.empty:
        return pd.DataFrame(columns=["User", "AlertScore", "AlertCount", "AlertTitles"])

    def _score(sevs: object) -> float:
        if not isinstance(sevs, list):
            return 0.0
        return max((ALERT_SEVERITY_SCORE.get(s, 0.0) for s in sevs), default=0.0)

    out = alerts.copy()
    out["AlertScore"] = out["Severities"].apply(_score)
    out["AlertTitles"] = out.get("Titles", pd.Series([[]] * len(out)))
    return out[["User", "AlertScore", "AlertCount", "AlertTitles"]]


def combine_scores(heuristic: pd.DataFrame, alerts: pd.DataFrame) -> pd.DataFrame:
    if heuristic.empty:
        return heuristic
    merged = heuristic.merge(alerts, on="User", how="left")
    merged["AlertScore"] = merged["AlertScore"].fillna(0.0)
    merged["AlertCount"] = merged["AlertCount"].fillna(0).astype(int)
    merged["Risk"] = merged[["HeuristicScore", "AlertScore"]].max(axis=1).round(3)
    return merged.sort_values("Risk", ascending=False).reset_index(drop=True)
