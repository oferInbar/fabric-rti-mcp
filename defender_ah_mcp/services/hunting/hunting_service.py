import logging
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from defender_ah_mcp.graph_api_http_client import GraphHttpClientCache

logger = logging.getLogger(__name__)

HUNTING_ENDPOINT = "/security/runHuntingQuery"
HUNTING_SCHEMA_ENDPOINT = "/security/runHuntingQuery/schema"

# Payload field names vary between deployments (Graph vs partner gateways).
# Allow overriding keys for non-standard deployments.
_QUERY_FIELD = os.getenv("HUNTING_QUERY_FIELD_NAME", "Query")
_TIMESPAN_FIELD = os.getenv("HUNTING_TIMESPAN_FIELD_NAME", "Timespan")
_START_TIME_FIELD = os.getenv("HUNTING_START_TIME_FIELD_NAME", "startTime")
_END_TIME_FIELD = os.getenv("HUNTING_END_TIME_FIELD_NAME", "endTime")

# Allow overriding endpoints for non-standard deployments (e.g., partner gateways).
_HUNTING_ENDPOINT = os.getenv("HUNTING_ENDPOINT", HUNTING_ENDPOINT)
_SCHEMA_ENDPOINT = os.getenv("HUNTING_SCHEMA_ENDPOINT", HUNTING_SCHEMA_ENDPOINT)

_SECTION_DISPLAY_NAMES: dict[str, str] = {
    "Usx": "Sentinel built-in tables (USX)",
    "CustomLogs": "Sentinel custom logs (CustomLogs)",
}


class _HuntingSchemaCache:
    """Module-level cache for the Advanced Hunting schema. Fetches once per process lifetime."""

    _schema: list[dict[str, Any]] | None = None

    @classmethod
    def get_schema(cls) -> list[dict[str, Any]]:
        if cls._schema is None:
            client = GraphHttpClientCache.get_client()
            response = client.make_request("GET", _SCHEMA_ENDPOINT)
            if isinstance(response, list):
                cls._schema = response
            else:
                # Don't cache errors — let subsequent calls retry
                return [response]
        return cls._schema

    @classmethod
    def clear(cls) -> None:
        """Clear the cached schema (useful for testing)."""
        cls._schema = None


_DEFAULT_MAX_RESULTS = 500


_ISO_DURATION_RE = re.compile(
    r"^P"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?"
    r"$"
)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _format_dt(dt: datetime) -> str:
    # Graph endpoints typically accept ISO8601 with 'Z'
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso8601_duration(duration: str) -> timedelta | None:
    """Parse a limited ISO8601 duration (PnDTnHnMnS) into timedelta.

    Notes:
    - Does NOT support months/years.
    - Returns None if the format isn't supported.
    """

    m = _ISO_DURATION_RE.match(duration)
    if not m:
        return None

    parts = {k: int(v) if v else 0 for k, v in m.groupdict().items()}
    if all(v == 0 for v in parts.values()):
        return None

    return timedelta(days=parts["days"], hours=parts["hours"], minutes=parts["minutes"], seconds=parts["seconds"])


def run_hunting_query(
    query: str,
    timespan: str | None = None,
    startTime: str | None = None,
    endTime: str | None = None,
    max_results: int | None = None,
) -> dict[str, Any]:
    """
    Run an advanced hunting query using the Microsoft Graph Security API.

    This executes a KQL query against Microsoft 365 Defender data to proactively
    look for threats across devices, emails, apps, and identities.

    The query targets tables in the advanced hunting schema such as
    DeviceProcessEvents, EmailEvents, IdentityLogonEvents, CloudAppEvents, etc.

    IMPORTANT: Before writing any query, call get_hunting_schema() first to discover
    available tables, columns, and their types. Do NOT guess table or column names —
    the schema varies per tenant and may include custom tables (e.g., SAP, custom logs).

    TIP: To discover the output schema of a query before running it fully, append
    "| getschema" to the query. For example:
        "DeviceProcessEvents | where InitiatingProcessFileName =~ 'powershell.exe' | getschema"
    This returns the column names and types of the query result without fetching all rows.

    Requires ThreatHunting.Read.All permission.

    :param query: The hunting query in KQL.
        Must reference tables from the Microsoft 365 Defender advanced hunting schema.
    :param timespan: Optional time interval in ISO 8601 format. Default is 30 days.
        Ignored when startTime is provided.
        Examples:
        - "P30D" — last 30 days
        - "P7D" — last 7 days
        - "2024-01-01T00:00:00Z/2024-01-31T23:59:59Z" — specific date range
        - "2024-01-01T00:00:00Z/P30D" — start date and duration
    :param startTime: Optional start of the query time window in ISO 8601 datetime format
        (e.g. "2024-01-01T00:00:00Z"). Takes precedence over timespan when provided.
        If endTime is omitted, the window extends 30 days from startTime.
    :param endTime: Optional end of the query time window in ISO 8601 datetime format
        (e.g. "2024-01-31T23:59:59Z"). Only used when startTime is also provided.
    :param max_results: Optional maximum number of result rows to return. Default is 500.
        When the result set exceeds this limit, the response includes a `_truncation_info`
        field indicating the results were truncated. Set to 0 or None to disable truncation.
    :return: A dictionary containing:
        - schema: List of column definitions (name and type)
        - results: List of result rows as dictionaries
        - _truncation_info (if truncated): Object with truncated, returned, and message fields

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

    # Find sign-ins within a specific date range:
    run_hunting_query(
        "IdentityLogonEvents | where ActionType == 'LogonFailed' | limit 20",
        startTime="2024-01-01T00:00:00Z",
        endTime="2024-01-31T23:59:59Z"
    )

    # Look up CVE details (use when a user asks about a specific CVE):
    run_hunting_query(
        "DeviceTvmSoftwareVulnerabilitiesKB "
        "| where CveId == 'CVE-2024-1234' "
        "| project CveId, CvssScore, EpssScore, IsExploitAvailable, "
        "VulnerabilitySeverityLevel, PublishedDate, VulnerabilityDescription, AffectedSoftware"
    )
    """
    payload: dict[str, Any] = {_QUERY_FIELD: query}

    # Some backends do not enforce the requested time window unless start/end are supplied.
    # We therefore prefer sending startTime/endTime when possible, in addition to Timespan.
    if startTime:
        payload[_START_TIME_FIELD] = startTime
        if endTime:
            payload[_END_TIME_FIELD] = endTime
            payload[_TIMESPAN_FIELD] = f"{startTime}/{endTime}"
        else:
            # Preserve the previous behavior: 30 days from the start
            payload[_TIMESPAN_FIELD] = f"{startTime}/P30D"
    elif timespan:
        payload[_TIMESPAN_FIELD] = timespan

        # If timespan is an explicit interval, also provide start/end.
        if "/" in timespan:
            start, end = timespan.split("/", 1)
            payload[_START_TIME_FIELD] = start

            # end can be an ISO datetime or an ISO duration (start/duration)
            if end.upper().startswith("P"):
                try:
                    start_dt = datetime.fromisoformat(start.replace("Z", "+00:00"))
                except ValueError:
                    start_dt = None
                delta = _parse_iso8601_duration(end)
                if start_dt is not None and delta is not None:
                    payload[_END_TIME_FIELD] = _format_dt(start_dt + delta)
            else:
                payload[_END_TIME_FIELD] = end

        # If a duration is provided, also compute explicit start/end.
        else:
            delta = _parse_iso8601_duration(timespan)
            if delta is not None:
                now = _utcnow()
                payload[_START_TIME_FIELD] = _format_dt(now - delta)
                payload[_END_TIME_FIELD] = _format_dt(now)
    logger.info("Running hunting query with payload: %s", payload)
    response = GraphHttpClientCache.get_client().make_request(
        "POST",
        _HUNTING_ENDPOINT,
        payload,
    )

    effective_max = max_results if max_results is not None else _DEFAULT_MAX_RESULTS
    if effective_max and isinstance(response, dict) and "results" in response:
        results = response["results"]
        if isinstance(results, list) and len(results) > effective_max:
            total = len(results)
            response["results"] = results[:effective_max]
            response["_truncation_info"] = {
                "truncated": True,
                "total_available": total,
                "returned": effective_max,
                "message": (
                    f"Results truncated to {effective_max} rows out of {total} available. "
                    "For complete results, refine your query with additional filters or increase max_results."
                ),
            }

    return response


def get_hunting_schema() -> str:
    """Retrieves the Advanced Hunting schema available to this tenant.

    This returns a compact summary of all available tables grouped by section,
    showing table names and column counts.

    The schema can include **Microsoft Defender** Advanced Hunting tables (e.g. Devices,
    Emails, Identity, Alerts) and, when enabled in your environment, **Microsoft Sentinel**
    tables surfaced via Advanced Hunting (e.g. sections like "Sentinel built-in tables (USX)"
    and "Sentinel custom logs (CustomLogs)").

    Use get_table_schema(table_name) to get the full column details for a specific table.

    IMPORTANT: Call this tool BEFORE writing any hunting query. The schema reveals
    tenant-specific tables (including custom/solution tables like SAP, custom logs, etc.)
    that cannot be guessed.

    :return: Markdown-formatted list of tables grouped by section.
    """
    schema = _HuntingSchemaCache.get_schema()

    # If we got an error response, return it as text
    if len(schema) == 1 and "error" in schema[0]:
        return f"Error fetching schema: {schema[0]}"

    # Group tables by section
    sections: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for table in schema:
        section = table.get("TableSection", "Other")
        section = _SECTION_DISPLAY_NAMES.get(section, section)
        name = table.get("Name", "Unknown")
        col_count = len(table.get("Schema", []))
        sections[section].append((name, col_count))

    # Format as markdown
    total_tables = sum(len(tables) for tables in sections.values())
    lines = [f"# Advanced Hunting Schema ({total_tables} tables)\n"]
    for section in sorted(sections.keys()):
        lines.append(f"## {section}")
        for table_name, col_count in sorted(sections[section]):
            lines.append(f"- {table_name} ({col_count} columns)")
        lines.append("")

    return "\n".join(lines)


def get_table_schema(table_name: str) -> str:
    """
    Retrieves the detailed column schema for a single Advanced Hunting table.

    Returns column names, types, entity annotations, and descriptions in a
    compact format suitable for writing KQL queries.

    :param table_name: Exact name of the table (e.g., "DeviceProcessEvents").
        Use get_hunting_schema() to discover available table names.
    :return: Markdown-formatted column listing for the table.
    """
    schema = _HuntingSchemaCache.get_schema()

    # If we got an error response, return it as text
    if len(schema) == 1 and "error" in schema[0]:
        return f"Error fetching schema: {schema[0]}"

    # Find the requested table (case-insensitive match)
    table = None
    for t in schema:
        if t.get("Name", "").lower() == table_name.lower():
            table = t
            break

    if table is None:
        available = sorted(t.get("Name", "") for t in schema)
        return f"Table '{table_name}' not found. Available tables:\n" + ", ".join(available)

    # Format columns
    section = table.get("TableSection", "Unknown")
    section = _SECTION_DISPLAY_NAMES.get(section, section)
    lines = [f"## {table['Name']} ({section})\n"]
    for col in table.get("Schema", []):
        name = col.get("Name", "")
        col_type = col.get("Type", "")
        entity = col.get("Entity")
        desc = col.get("Description", "")

        entity_part = f" [{entity}]" if entity else ""
        desc_part = f" - {desc}" if desc else ""
        lines.append(f"  {name}: {col_type}{entity_part}{desc_part}")

    return "\n".join(lines)


def validate_hunting_query(query: str) -> dict[str, Any]:
    """
    Validates a KQL query against Advanced Hunting without returning full results.

    Appends '| getschema' to the query and executes it. The engine validates syntax
    and semantics (table/column existence) but only returns the output column schema,
    not the actual data rows. This is significantly cheaper than a full execution.

    Use this tool to verify a query is correct BEFORE running it with run_hunting_query.
    This is especially useful in agentic loops where LLM-generated KQL may contain errors.

    :param query: The KQL query to validate.
    :return: A dictionary with:
        - valid (bool): Whether the query is syntactically and semantically correct.
        - output_schema (list): If valid, the list of output columns with Name and Type.
            Each entry has 'ColumnName', 'ColumnOrdinal', 'DataType', and 'ColumnType'.
        - error (str): If invalid, the error message from the engine.
    """
    cleaned = query.strip().rstrip(";").strip()

    # `let` statements must remain at the top level — wrapping them in parentheses
    # is a KQL syntax error.  For queries that contain `let`, we append `| getschema`
    # to the final expression instead of wrapping the whole thing.
    _LET_RE = re.compile(r"(?:^|\n)\s*let\s+", re.IGNORECASE)
    validation_query = f"{cleaned}\n| getschema" if _LET_RE.search(cleaned) else f"({cleaned}) | getschema"

    try:
        result = run_hunting_query(validation_query, timespan="P1D", max_results=0)
    except Exception as e:
        return {"valid": False, "error": str(e)}

    if isinstance(result, dict):
        if "error" in result:
            error_val = result["error"]
            # The Graph API may return the error as a nested dict, a string, or
            # a plain boolean flag.  Always surface a human-readable message.
            if isinstance(error_val, dict):
                error_msg = error_val.get("message") or error_val.get("Message") or str(error_val)
            elif isinstance(error_val, str):
                error_msg = error_val
            else:
                # Boolean or other non-string sentinel — fall back to adjacent detail fields.
                error_msg = result.get("detail") or result.get("message") or str(error_val)
            return {"valid": False, "error": error_msg}
        if result.get("ErrorCode"):
            return {"valid": False, "error": result.get("ErrorMessage", str(result.get("ErrorCode")))}

    schema_rows = result.get("results", []) if isinstance(result, dict) else []
    return {"valid": True, "output_schema": schema_rows}
