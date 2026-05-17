from __future__ import annotations

from pathlib import Path

import yaml


CACHE_TTL_SECONDS = 300
DEFAULT_TIME_WINDOW = "24h"
TIME_WINDOWS = {"1h": "1h", "24h": "24h", "7d": "7d", "30d": "30d"}
TIMESPANS = {"1h": "PT1H", "24h": "P1D", "7d": "P7D", "30d": "P30D"}
# Approximate calendar-day equivalent of each window, used by analytics
# that operate in day units (e.g. baseline lookback math).
WINDOW_DAYS = {"1h": 1, "24h": 1, "7d": 7, "30d": 30}

# For analytics that compare a recent window vs a 7-day baseline immediately
# preceding it, the API-level `timespan` must cover BOTH windows together,
# since `AWSCloudTrail` is a Bronze Logs table and rejects in-query time
# filters on subqueries.
BASELINE_TIMESPANS = {"1h": "P7D", "24h": "P8D", "7d": "P14D", "30d": "P37D"}

MAX_RESULTS = 10000

# --- Geo / risk-scoring (ported from "AWS login overview") ---
HUMAN_IDENTITY_TYPES = ("IAMUser", "AssumedRole", "FederatedUser", "Root")
SPEED_THRESHOLD_KMH = 900.0
MIN_DISTANCE_KM = 500.0
BASELINE_LOOKBACK_DAYS = 30
MAX_LOGIN_ROWS = 5000

RISK_WEIGHTS = {
    "impossible_travel": 0.45,
    "new_country": 0.30,
    "new_ip": 0.25,
}

ALERT_SEVERITY_SCORE = {
    "Informational": 0.1,
    "Low": 0.3,
    "Medium": 0.6,
    "High": 0.85,
    "Critical": 1.0,
}

ALLOWLIST_PATH = Path(__file__).resolve().parent.parent / "workload_allowlist.yaml"


def load_allowlist(path: Path = ALLOWLIST_PATH) -> dict:
    with path.open() as fh:
        data = yaml.safe_load(fh) or {}
    return {
        "roles": frozenset(entry["name"] for entry in data.get("roles", [])),
        "role_patterns": tuple(data.get("role_patterns", [])),
        "iam_user_service_accounts": frozenset(
            data.get("iam_user_service_accounts", [])
        ),
    }
