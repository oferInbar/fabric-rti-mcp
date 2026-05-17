from __future__ import annotations

from utils.constants import HUMAN_IDENTITY_TYPES, MAX_LOGIN_ROWS

_HUMAN_TYPES_KQL = ",".join(f'"{t}"' for t in HUMAN_IDENTITY_TYPES)


def _clean_kql(query: str) -> str:
    """Drop blank lines (left behind by empty optional filter snippets) and
    trailing whitespace so the rendered KQL is one contiguous pipeline."""
    return "\n".join(
        line.rstrip() for line in query.splitlines() if line.strip()
    ).strip()


def _timespan(days: int) -> str:
    return f"P{int(days)}D"


def build_logins_kql(days: int, account_id: str | None = None) -> str:
    """
    Synthetic 'login' = first AWSCloudTrail event per (user, IP, calendar day).
    Filters: human-ish identity types, IPv4 only.
    AWS-owned IP and SDK-UA exclusion happens client-side (utils.geo).
    """
    account_filter = (
        f'| where UserIdentityAccountId == "{account_id}"' if account_id else ""
    )
    return _clean_kql(f"""
AWSCloudTrail
| where UserIdentityType in ({_HUMAN_TYPES_KQL})
{account_filter}
| where isnotempty(SourceIpAddress)
| where SourceIpAddress matches regex @"^\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}$"
| extend User = coalesce(UserIdentityUserName, UserIdentityArn)
| where isnotempty(User)
| extend Day = startofday(TimeGenerated)
| summarize TimeGenerated=min(TimeGenerated),
            EventName=take_any(EventName),
            UserAgent=take_any(UserAgent),
            AWSRegion=take_any(AWSRegion),
            Events=count()
            by User, UserIdentityAccountId, UserIdentityType, SourceIpAddress, Day
| extend Geo = geo_info_from_ip_address(SourceIpAddress)
| extend Country = tostring(Geo.country),
         City = tostring(Geo.city),
         Latitude = todouble(Geo.latitude),
         Longitude = todouble(Geo.longitude)
| where isnotnull(Latitude) and isnotnull(Longitude)
| project TimeGenerated, Day, User, UserIdentityAccountId, UserIdentityType,
          SourceIpAddress, Country, City, Latitude, Longitude,
          AWSRegion, EventName, UserAgent, Events
| order by TimeGenerated desc
| take {MAX_LOGIN_ROWS}
""")


def build_baseline_kql(account_id: str | None = None) -> str:
    """30-day prior baseline (Country, IP) sets per user."""
    account_filter = (
        f'| where UserIdentityAccountId == "{account_id}"' if account_id else ""
    )
    return _clean_kql(f"""
AWSCloudTrail
| where UserIdentityType in ({_HUMAN_TYPES_KQL})
{account_filter}
| where isnotempty(SourceIpAddress)
| where SourceIpAddress matches regex @"^\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}$"
| extend User = coalesce(UserIdentityUserName, UserIdentityArn)
| where isnotempty(User)
| extend Geo = geo_info_from_ip_address(SourceIpAddress)
| extend Country = tostring(Geo.country)
| summarize BaselineCountries = make_set(Country, 200),
            BaselineIps = make_set(SourceIpAddress, 1000)
            by User
""")


def build_alerts_kql() -> str:
    """Defender alerts per user."""
    return _clean_kql("""
AlertEvidence
| where EntityType == "User"
| extend User = coalesce(AccountUpn, AccountObjectId, AccountSid)
| where isnotempty(User)
| summarize AlertCount=count(),
            LastAlertTime=max(Timestamp),
            Severities=make_set(Severity, 20),
            Titles=make_set(Title, 10)
            by User
""")


def build_user_detail_kql(user: str, days: int) -> str:
    """Top events / IPs / regions for a single user."""
    safe_user = user.replace("'", "''")
    return _clean_kql(f"""
AWSCloudTrail
| where UserIdentityType in ({_HUMAN_TYPES_KQL})
| extend User = coalesce(UserIdentityUserName, UserIdentityArn)
| where User == '{safe_user}'
| extend Geo = geo_info_from_ip_address(SourceIpAddress)
| summarize Events=count(),
            FirstSeen=min(TimeGenerated),
            LastSeen=max(TimeGenerated),
            TopEvents=make_set(EventName, 25),
            AWSRegions=make_set(AWSRegion, 25),
            UserAgents=make_set(UserAgent, 10),
            Country=any(tostring(Geo.country)),
            City=any(tostring(Geo.city))
            by SourceIpAddress
| order by LastSeen desc
| take 200
""")


def timespan_for_days(days: int) -> str:
    return _timespan(days)
