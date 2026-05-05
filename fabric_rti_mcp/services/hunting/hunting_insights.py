import concurrent.futures
from typing import Any

from fabric_rti_mcp.services.hunting.hunting_service import run_hunting_query

_MAX_INSIGHT_RESULTS = 20


def get_device_insights(
    device_ids: list[str],
) -> dict[str, Any]:
    """
    Given one or more device IDs, returns enriched context by running multiple
    Advanced Hunting queries in parallel.

    Gathers:
    - General device information (OS, health, onboarding status)
    - Software vulnerabilities (known CVEs)
    - Recent logon history (past 7 days)
    - Alert evidence (past 30 days)

    :param device_ids: List of Defender device IDs to enrich.
    :return: Dictionary with sections: device_info, vulnerabilities, logon_history, alert_evidence.
        Each section contains the query results or an error message if the query failed.
    """
    ids_literal = _build_kql_array(device_ids)

    queries = {
        "device_info": f"""
            let deviceIds = {ids_literal};
            DeviceInfo
            | where DeviceId in (deviceIds)
            | summarize arg_max(Timestamp, *) by DeviceId
            | project Timestamp, DeviceId, DeviceName, OSPlatform, OSVersion,
                      OnboardingStatus, SensorHealthState, IsAzureADJoined,
                      AadDeviceId, MachineGroup, PublicIP, ClientVersion
            | take {_MAX_INSIGHT_RESULTS}
        """,
        "vulnerabilities": f"""
            let deviceIds = {ids_literal};
            DeviceTvmSoftwareVulnerabilities
            | where DeviceId in (deviceIds)
            | summarize VulnCount=count(),
                        CriticalCount=countif(VulnerabilitySeverityLevel == "Critical"),
                        HighCount=countif(VulnerabilitySeverityLevel == "High")
                        by DeviceId, DeviceName, SoftwareName, SoftwareVersion
            | top {_MAX_INSIGHT_RESULTS} by VulnCount desc
        """,
        "logon_history": f"""
            let deviceIds = {ids_literal};
            DeviceLogonEvents
            | where Timestamp > ago(7d)
            | where DeviceId in (deviceIds)
            | where ActionType == "LogonSuccess"
            | summarize LogonCount=dcount(Timestamp)
                        by DeviceId, LogonType, AccountName, AccountDomain, IsLocalAdmin
            | top {_MAX_INSIGHT_RESULTS} by LogonCount desc
        """,
        "alert_evidence": f"""
            let deviceIds = {ids_literal};
            AlertEvidence
            | where Timestamp > ago(30d)
            | where DeviceId in (deviceIds)
            | join kind=leftouter (
                AlertInfo
                | where Timestamp > ago(30d)
                | project AlertId, Title, Severity, Category
            ) on AlertId
            | summarize AlertCount=count(), Titles=make_set(Title, 5),
                        Severities=make_set(Severity, 5) by DeviceId
            | take {_MAX_INSIGHT_RESULTS}
        """,
    }

    return _execute_parallel_queries(queries)


def get_user_insights(
    user_ids: list[str],
) -> dict[str, Any]:
    """
    Given one or more user identifiers (UPN or on-premises SID), returns enriched
    context by first resolving identities and then running multiple Advanced Hunting
    queries in parallel.

    Gathers:
    - Identity resolution and general user information
    - Device logon history (past 7 days)
    - Alert evidence (past 30 days)
    - AAD/Entra ID risk events (past 30 days)

    :param user_ids: List of user identifiers (AccountUpn or OnPremSid).
    :return: Dictionary with sections: identity_info, logon_history, alert_evidence, risk_events.
        Each section contains the query results or an error message if the query failed.
    """
    ids_literal = _build_kql_array(user_ids)

    queries = {
        "identity_info": f"""
            let userIds = {ids_literal};
            IdentityInfo
            | where AccountUpn in (userIds) or OnPremSid in (userIds)
            | extend UserKey = coalesce(AccountUpn, OnPremSid)
            | summarize arg_max(Timestamp, *) by UserKey
            | project Timestamp, AccountUpn, AccountDisplayName, AccountName, AccountDomain,
                      OnPremSid, AccountObjectId, IsAccountEnabled, CreatedDateTime,
                      IdentityEnvironment, Type
            | take {_MAX_INSIGHT_RESULTS}
        """,
        "logon_history": f"""
            let userIds = {ids_literal};
            DeviceLogonEvents
            | where Timestamp > ago(7d)
            | where AccountUpn in (userIds) or AccountSid in (userIds)
            | where ActionType == "LogonSuccess"
            | summarize LogonCount=dcount(Timestamp)
                        by AccountName, AccountDomain, AccountSid, DeviceId, DeviceName,
                           LogonType, IsLocalAdmin
            | top {_MAX_INSIGHT_RESULTS} by LogonCount desc
        """,
        "alert_evidence": f"""
            let userIds = {ids_literal};
            AlertEvidence
            | where Timestamp > ago(30d)
            | where EntityType == "User"
            | where AccountUpn in (userIds) or AccountSid in (userIds)
            | join kind=leftouter (
                AlertInfo
                | where Timestamp > ago(30d)
                | project AlertId, Title, Severity, Category
            ) on AlertId
            | extend UserKey = coalesce(AccountUpn, AccountSid)
            | summarize AlertCount=count(), Titles=make_set(Title, 5),
                        Severities=make_set(Severity, 5) by UserKey
            | take {_MAX_INSIGHT_RESULTS}
        """,
        "risk_events": f"""
            let userIds = {ids_literal};
            AADUserRiskEvents
            | where ActivityDateTime > ago(30d)
            | where UserPrincipalName in (userIds)
            | where RiskLevel in ("high", "medium")
            | project UserPrincipalName, ActivityDateTime, RiskLevel, RiskEventType,
                      RiskState, IpAddress
            | top {_MAX_INSIGHT_RESULTS} by ActivityDateTime desc
        """,
    }

    return _execute_parallel_queries(queries)


def _build_kql_array(values: list[str]) -> str:
    """Build a KQL dynamic array literal from a list of strings, with proper escaping."""
    escaped = [v.replace("'", "\\'") for v in values]
    items = ", ".join(f"'{v}'" for v in escaped)
    return f"dynamic([{items}])"


def _execute_parallel_queries(queries: dict[str, str]) -> dict[str, Any]:
    """Execute multiple hunting queries in parallel using threads."""
    results: dict[str, Any] = {}

    def _run_query(name: str, kql: str) -> tuple[str, Any]:
        try:
            result = run_hunting_query(kql)
            return name, result
        except Exception as e:
            return name, {"error": str(e)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_run_query, name, kql): name for name, kql in queries.items()}
        for future in concurrent.futures.as_completed(futures):
            name, result = future.result()
            results[name] = result

    return results
