"""KQL query library for AWS User Activity dashboard.

All queries target AWSCloudTrail. Workload roles, service IAM users, and
non-human UserIdentityType values are filtered out when `human_only=True`.

Rows implemented:
  Row 1 — Health & Volume          (volume_tiles, top_principals)
  Row 2 — Authentication & Access  (login_outcome_timeline, top_failed_logins,
                                    logins_by_region, new_geo_per_user)
  Row 3 — IAM / Privilege Changes  (iam_sensitive_events, iam_events_table)
  Row 5 — Cross-Account & Region   (cross_account_access, region_heatmap)
  Row 6 — Top-N Drill-downs        (top_errors, top_event_sources, top_source_ips)
"""

from __future__ import annotations


def _clean_kql(query: str) -> str:
    """Normalize a rendered KQL string.

    Optional filter clauses (e.g. allowlist, account filter) are interpolated
    on their own line and may be empty, which leaves blank lines in the
    final query. Some Advanced Hunting parsers treat a blank line as a
    statement terminator and reject the trailing pipeline. Strip blank
    lines and trailing whitespace so the rendered KQL is always a single
    contiguous pipeline.
    """
    return "\n".join(
        line.rstrip() for line in query.splitlines() if line.strip()
    ).strip()


def _allowlist_filter_clause(allowlist: dict, human_only: bool = True) -> str:
    """KQL snippet that excludes workload roles + service IAM users +
    non-human UserIdentityType values.

    Operates on a row that already has `_roleName` and `UserIdentityUserName`
    extended. When `human_only=False`, returns an empty string so callers
    can use the same call shape regardless of toggle state.
    """
    if not human_only:
        return ""
    roles = sorted(allowlist.get("roles", ()))
    patterns = allowlist.get("role_patterns", ())
    iam_users = sorted(allowlist.get("iam_user_service_accounts", ()))

    exact_roles = ", ".join(f'"{r}"' for r in roles)
    pattern_clauses = " or ".join(
        f'_roleName matches regex @"(?i){p}"' for p in patterns
    )
    iam_clause = ", ".join(f'"{u}"' for u in iam_users)

    parts = []
    # Non-human UserIdentityType values. These are AWS-internal callers
    # (AWSService), cross-account chained calls (AWSAccount), unattributable
    # events (Unknown), and OIDC workload federation (WebIdentityUser —
    # always UUID-shaped session names in this tenant, never humans).
    parts.append(
        'UserIdentityType in ("AWSService", "AWSAccount", "Unknown", "WebIdentityUser")'
    )
    if exact_roles:
        parts.append(f"_roleName in ({exact_roles})")
    if pattern_clauses:
        parts.append(f"({pattern_clauses})")
    if iam_clause:
        parts.append(f"UserIdentityUserName in ({iam_clause})")

    return "| where not(" + " or ".join(parts) + ")"


def _base_scope(time_window: str, accounts: list[str] | None) -> str:
    """Compose the outer table scope (table + role-name extend + optional
    account filter).

    NOTE: We deliberately do NOT add `| where TimeGenerated > ago(...)`.
    `AWSCloudTrail` is a Bronze Logs table — the API-level `timespan`
    parameter is the only legitimate way to scope its time range, and a
    redundant in-query `where` clause is both unnecessary and a refactor
    landmine (it would silently turn illegal if this scope were ever
    moved inside a `let` subquery or `union` leg). `time_window` is kept
    as a parameter for callers that bind it into KQL constructs that ARE
    allowed (e.g. `extend Bucket = iff(TimeGenerated > ago(...), ...)`).
    """
    parts = [
        "AWSCloudTrail",
        "| extend _roleName = tostring(split(SessionIssuerArn, '/')[-1])",
    ]
    if accounts:
        ids = ", ".join(f'"{a}"' for a in accounts)
        parts.append(f"| where UserIdentityAccountId in ({ids})")
    return "\n".join(parts)


def volume_tiles(
    time_window: str,
    allowlist: dict,
    accounts: list[str] | None = None,
    human_only: bool = False,
) -> str:
    scope = _base_scope(time_window, accounts)
    allow = _allowlist_filter_clause(allowlist, human_only)
    # `tostring(AdditionalEventData)` is inlined inside the `MfaLessLogins`
    # countif so the dynamic→string conversion is only paid for
    # ConsoleLogin rows (countif short-circuits on `and`), instead of
    # being extended onto every row first.
    return _clean_kql(f"""
{scope}
{allow}
| summarize
    TotalEvents       = count(),
    DistinctPrincipals= dcount(UserIdentityArn),
    FailedApiCalls    = countif(isnotempty(ErrorCode)),
    RootEvents        = countif(UserIdentityType == "Root"),
    ConsoleLogins     = countif(EventName == "ConsoleLogin"),
    MfaLessLogins     = countif(EventName == "ConsoleLogin" and tostring(AdditionalEventData) has_cs '"MFAUsed":"No"')
""")


def top_principals(
    time_window: str,
    allowlist: dict,
    accounts: list[str] | None = None,
    limit: int = 25,
    human_only: bool = False,
) -> str:
    scope = _base_scope(time_window, accounts)
    allow = _allowlist_filter_clause(allowlist, human_only)
    return _clean_kql(f"""
{scope}
{allow}
| summarize Events = count(),
            Failures = countif(isnotempty(ErrorCode)),
            DistinctEvents = dcount(EventName),
            DistinctRegions = dcount(AWSRegion),
            SampleIp = any(SourceIpAddress)
        by UserIdentityArn, UserIdentityType, UserIdentityAccountId
| top {limit} by Events desc
""")


def accounts_in_scope(time_window: str) -> str:
    """Populate the multi-select account filter (§7)."""
    return _clean_kql(f"""
AWSCloudTrail
| where TimeGenerated > ago({time_window})
| where isnotempty(UserIdentityAccountId)
| summarize Events = count() by UserIdentityAccountId
| order by Events desc
""")


# ---------------------------------------------------------------------------
# Row 2 — Authentication & Access
# ---------------------------------------------------------------------------

def login_outcome_timeline(
    time_window: str,
    allowlist: dict,
    accounts: list[str] | None = None,
    bin_size: str = "1h",
    human_only: bool = False,
) -> str:
    scope = _base_scope(time_window, accounts)
    allow = _allowlist_filter_clause(allowlist, human_only)
    # Filter to ConsoleLogin BEFORE stringifying the dynamic columns —
    # `tostring(ResponseElements)` / `tostring(AdditionalEventData)` is
    # expensive and only needed for login rows.
    return _clean_kql(f"""
{scope}
{allow}
| where EventName == "ConsoleLogin"
| extend ResponseStr = tostring(ResponseElements)
| extend AddlStr = tostring(AdditionalEventData)
| extend Outcome = case(
    ResponseStr has "Failure", "Failure",
    AddlStr has_cs '"MFAUsed":"No"', "Success (no MFA)",
    "Success")
| summarize Events = count() by bin(TimeGenerated, {bin_size}), Outcome
| order by TimeGenerated asc
""")


def top_failed_logins(
    time_window: str,
    allowlist: dict,
    accounts: list[str] | None = None,
    limit: int = 10,
    human_only: bool = False,
) -> str:
    scope = _base_scope(time_window, accounts)
    allow = _allowlist_filter_clause(allowlist, human_only)
    return _clean_kql(f"""
{scope}
{allow}
| where EventName == "ConsoleLogin" and tostring(ResponseElements) has "Failure"
| summarize Failures = count(),
            DistinctIps = dcount(SourceIpAddress),
            SampleIp = any(SourceIpAddress),
            LastSeen = max(TimeGenerated)
        by UserIdentityArn, UserIdentityAccountId
| top {limit} by Failures desc
""")


def logins_by_region(
    time_window: str,
    allowlist: dict,
    accounts: list[str] | None = None,
    human_only: bool = False,
) -> str:
    scope = _base_scope(time_window, accounts)
    allow = _allowlist_filter_clause(allowlist, human_only)
    return _clean_kql(f"""
{scope}
{allow}
| where EventName == "ConsoleLogin"
| extend Outcome = iff(tostring(ResponseElements) has "Failure", "Failure", "Success")
| summarize Events = count(),
            DistinctUsers = dcount(UserIdentityArn)
        by AWSRegion, Outcome
| order by Events desc
""")


def new_geo_per_user(
    time_window: str,
    allowlist: dict,
    accounts: list[str] | None = None,
    baseline_days: int = 7,
    limit: int = 50,
    human_only: bool = False,
) -> str:
    """Users whose source IP first appeared in the recent window (no baseline match).

    Recent window  = the trailing `time_window` (e.g. '24h', '7d').
    Baseline window= the `baseline_days` immediately preceding `recent`.

    Implementation note: `AWSCloudTrail` is a Bronze Logs table in the
    Defender Advanced Hunting virtual schema. The time range MUST come
    from the API-level `timespan` parameter — `where TimeGenerated > …`
    on a Bronze table is rejected ("Bronze Logs table query is missing
    time range"), which also rules out two-`let`-subquery anti-join
    patterns since the inner subquery's `where` is illegal.

    Workaround: the caller passes a single `timespan` covering BOTH
    windows (`time_window + baseline_days`), and the query uses
    `extend Bucket = iff(TimeGenerated > ago(time_window), "recent",
    "baseline")` to partition rows. `extend` on `TimeGenerated` is
    allowed (it's a computed projection, not a time-range filter).
    Then summarize by (user, ip) and keep keys with zero baseline hits.
    """
    allow = _allowlist_filter_clause(allowlist, human_only)
    account_filter = ""
    if accounts:
        ids = ", ".join(f'"{a}"' for a in accounts)
        account_filter = f"| where UserIdentityAccountId in ({ids})"
    return _clean_kql(f"""
AWSCloudTrail
| where EventName == "ConsoleLogin"
| extend _roleName = tostring(split(SessionIssuerArn, '/')[-1])
{account_filter}
{allow}
| extend Bucket = iff(TimeGenerated > ago({time_window}), "recent", "baseline")
| summarize FirstSeenInRecent = minif(TimeGenerated, Bucket == "recent"),
            BaselineHits = countif(Bucket == "baseline"),
            RecentHits   = countif(Bucket == "recent")
        by UserIdentityArn, SourceIpAddress
| where BaselineHits == 0 and RecentHits > 0
| summarize FirstSeen = min(FirstSeenInRecent),
            NewIps = make_set(SourceIpAddress, 10),
            Events = sum(RecentHits)
        by UserIdentityArn
| top {limit} by Events desc
""")


# ---------------------------------------------------------------------------
# Row 3 — Identity & Privilege Changes (IAM)
# ---------------------------------------------------------------------------

_IAM_SENSITIVE_EVENTS = (
    "CreateUser", "CreateAccessKey", "CreateLoginProfile",
    "UpdateLoginProfile", "AttachUserPolicy", "AttachRolePolicy",
    "PutUserPolicy", "PutRolePolicy", "CreatePolicyVersion",
    "SetDefaultPolicyVersion", "DeleteAccountPasswordPolicy",
    "UpdateAccountPasswordPolicy", "DeactivateMFADevice",
    "DeleteVirtualMFADevice", "EnableMFADevice",
    "DeleteAccessKey", "UpdateAccessKey",
)


def iam_sensitive_events_timeline(
    time_window: str,
    accounts: list[str] | None = None,
    bin_size: str = "1h",
) -> str:
    """IAM-sensitive event volume over time. No allowlist — IAM changes
    from any principal (workload or human) deserve visibility."""
    scope = _base_scope(time_window, accounts)
    events = ", ".join(f'"{e}"' for e in _IAM_SENSITIVE_EVENTS)
    return _clean_kql(f"""
{scope}
| where EventName in ({events})
| summarize Events = count() by bin(TimeGenerated, {bin_size}), EventName
| order by TimeGenerated asc
""")


def iam_events_table(
    time_window: str,
    accounts: list[str] | None = None,
    limit: int = 100,
) -> str:
    scope = _base_scope(time_window, accounts)
    events = ", ".join(f'"{e}"' for e in _IAM_SENSITIVE_EVENTS)
    return _clean_kql(f"""
{scope}
| where EventName in ({events})
| project TimeGenerated, EventName, UserIdentityArn, UserIdentityAccountId,
          AWSRegion, SourceIpAddress, ErrorCode
| top {limit} by TimeGenerated desc
""")


# ---------------------------------------------------------------------------
# Row 5 — Cross-Account & Region
# ---------------------------------------------------------------------------

def cross_account_access(
    time_window: str,
    allowlist: dict,
    accounts: list[str] | None = None,
    limit: int = 50,
    human_only: bool = False,
) -> str:
    """Events where the caller account != the resource-owning account."""
    scope = _base_scope(time_window, accounts)
    allow = _allowlist_filter_clause(allowlist, human_only)
    return _clean_kql(f"""
{scope}
{allow}
| where isnotempty(RecipientAccountId)
    and isnotempty(UserIdentityAccountId)
    and RecipientAccountId != UserIdentityAccountId
| summarize Events = count(),
            DistinctEvents = dcount(EventName),
            SampleEvent = any(EventName),
            LastSeen = max(TimeGenerated)
        by UserIdentityArn, UserIdentityAccountId, RecipientAccountId
| top {limit} by Events desc
""")


def region_heatmap(
    time_window: str,
    allowlist: dict,
    accounts: list[str] | None = None,
    user_limit: int = 25,
    human_only: bool = False,
) -> str:
    """Top-N users x region matrix (for heatmap rendering)."""
    scope = _base_scope(time_window, accounts)
    allow = _allowlist_filter_clause(allowlist, human_only)
    return _clean_kql(f"""
{scope}
{allow}
| where isnotempty(AWSRegion)
| summarize Events = count() by UserIdentityArn, AWSRegion
| summarize TotalEvents = sum(Events),
            Regions = make_bag(pack(AWSRegion, Events))
        by UserIdentityArn
| top {user_limit} by TotalEvents desc
| mv-expand Region = bag_keys(Regions) to typeof(string)
| extend Events = toint(Regions[Region])
| project UserIdentityArn, Region, Events
""")


# ---------------------------------------------------------------------------
# Row 6 — Top-N Drill-downs
# ---------------------------------------------------------------------------

def top_errors(
    time_window: str,
    allowlist: dict,
    accounts: list[str] | None = None,
    limit: int = 15,
    human_only: bool = False,
) -> str:
    scope = _base_scope(time_window, accounts)
    allow = _allowlist_filter_clause(allowlist, human_only)
    return _clean_kql(f"""
{scope}
{allow}
| where isnotempty(ErrorCode)
| summarize Events = count(),
            DistinctUsers = dcount(UserIdentityArn),
            SampleUser = any(UserIdentityArn),
            LastSeen = max(TimeGenerated)
        by ErrorCode
| top {limit} by Events desc
""")


def top_event_sources(
    time_window: str,
    allowlist: dict,
    accounts: list[str] | None = None,
    limit: int = 15,
    human_only: bool = False,
) -> str:
    scope = _base_scope(time_window, accounts)
    allow = _allowlist_filter_clause(allowlist, human_only)
    return _clean_kql(f"""
{scope}
{allow}
| summarize Events = count(),
            DistinctUsers = dcount(UserIdentityArn),
            DistinctEvents = dcount(EventName)
        by EventSource
| top {limit} by Events desc
""")


def top_source_ips(
    time_window: str,
    allowlist: dict,
    accounts: list[str] | None = None,
    limit: int = 25,
    human_only: bool = False,
) -> str:
    scope = _base_scope(time_window, accounts)
    allow = _allowlist_filter_clause(allowlist, human_only)
    return _clean_kql(f"""
{scope}
{allow}
| where isnotempty(SourceIpAddress)
| summarize Events = count(),
            DistinctUsers = dcount(UserIdentityArn),
            DistinctEvents = dcount(EventName),
            Failures = countif(isnotempty(ErrorCode)),
            LastSeen = max(TimeGenerated)
        by SourceIpAddress
| top {limit} by Events desc
""")


# ---------------------------------------------------------------------------
# Row 7 — Geo & Entity Deep Dive  (ported/adapted from "AWS login overview")
# ---------------------------------------------------------------------------
_HUMAN_TYPES_KQL = ",".join(f'"{t}"' for t in ("IAMUser", "AssumedRole", "FederatedUser", "Root"))


def _accounts_filter(accounts: list[str] | None) -> str:
    if not accounts:
        return ""
    ids = ", ".join(f'"{a}"' for a in accounts)
    return f"| where UserIdentityAccountId in ({ids})"


def build_logins_kql(
    accounts: list[str] | None = None,
    max_rows: int = 5000,
) -> str:
    """One synthetic 'login' per (user, IP, calendar day). Time range is
    set via the API-level `timespan` (Bronze constraint)."""
    return _clean_kql(f"""
AWSCloudTrail
| where UserIdentityType in ({_HUMAN_TYPES_KQL})
{_accounts_filter(accounts)}
| where isnotempty(SourceIpAddress)
| where SourceIpAddress matches regex @"^\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}$"
| project TimeGenerated, UserIdentityUserName, UserIdentityArn,
          UserIdentityAccountId, UserIdentityType, SourceIpAddress,
          EventName, UserAgent, AWSRegion
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
| take {int(max_rows)}
""")


def build_baseline_kql(accounts: list[str] | None = None) -> str:
    """Per-user baseline (countries + IPs) for the timespan you pass at the
    API level (typically 30 days). Bronze: no in-body time filter."""
    return _clean_kql(f"""
AWSCloudTrail
| where UserIdentityType in ({_HUMAN_TYPES_KQL})
{_accounts_filter(accounts)}
| where isnotempty(SourceIpAddress)
| where SourceIpAddress matches regex @"^\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}\\.\\d{{1,3}}$"
| project UserIdentityUserName, UserIdentityArn, SourceIpAddress
| extend User = coalesce(UserIdentityUserName, UserIdentityArn)
| where isnotempty(User)
| extend Geo = geo_info_from_ip_address(SourceIpAddress)
| extend Country = tostring(Geo.country)
| summarize BaselineCountries = make_set(Country, 200),
            BaselineIps = make_set(SourceIpAddress, 1000)
        by User
""")


def build_alerts_kql() -> str:
    """Defender alerts per user. Pass timespan='P30D' at the API level."""
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


def build_user_detail_kql(user: str) -> str:
    """Per-IP event summary for a single user. Time range via API timespan."""
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
