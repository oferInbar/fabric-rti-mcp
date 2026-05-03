from typing import Any

from fabric_rti_mcp.graph_api_http_client import GraphHttpClientCache

HUNTING_ENDPOINT = "/security/runHuntingQuery"


def run_hunting_query(
    query: str,
    timespan: str | None = None,
) -> dict[str, Any]:
    """
    Run an advanced hunting query using the Microsoft Graph Security API.

    This executes a KQL query against Microsoft 365 Defender data to proactively
    look for threats across devices, emails, apps, and identities.

    The query targets tables in the advanced hunting schema such as
    DeviceProcessEvents, EmailEvents, IdentityLogonEvents, CloudAppEvents, etc.

    Requires ThreatHunting.Read.All permission.

    :param query: The hunting query in Kusto Query Language (KQL).
        Must reference tables from the Microsoft 365 Defender advanced hunting schema.
    :param timespan: Optional time interval in ISO 8601 format. Default is 30 days.
        Examples:
        - "P30D" — last 30 days
        - "P7D" — last 7 days
        - "2024-01-01T00:00:00Z/2024-01-31T23:59:59Z" — specific date range
        - "2024-01-01T00:00:00Z/P30D" — start date and duration
    :return: A dictionary containing:
        - schema: List of column definitions (name and type)
        - results: List of result rows as dictionaries

    Examples:

    # Find recent PowerShell-initiated processes:
    run_hunting_query(
        "DeviceProcessEvents "
        "| where InitiatingProcessFileName =~ 'powershell.exe' "
        "| project Timestamp, FileName, InitiatingProcessFileName "
        "| order by Timestamp desc "
        "| limit 10"
    )

    # Find failed sign-ins in the last 7 days:
    run_hunting_query(
        "IdentityLogonEvents "
        "| where ActionType == 'LogonFailed' "
        "| summarize FailedAttempts=count() by AccountUpn "
        "| order by FailedAttempts desc "
        "| limit 20",
        timespan="P7D"
    )
    """
    payload: dict[str, Any] = {"Query": query}

    if timespan:
        payload["Timespan"] = timespan

    return GraphHttpClientCache.get_client().make_request(
        "POST",
        HUNTING_ENDPOINT,
        payload,
    )
