SPEED_THRESHOLD_KMH = 900.0
MIN_DISTANCE_KM = 500.0

DEFAULT_TIME_WINDOW_DAYS = 7
TIME_WINDOW_CHOICES = (1, 7, 14, 30)

BASELINE_LOOKBACK_DAYS = 30

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

HUMAN_IDENTITY_TYPES = ("IAMUser", "AssumedRole", "FederatedUser", "Root")

CACHE_TTL_SECONDS = 300

MAX_LOGIN_ROWS = 5000
