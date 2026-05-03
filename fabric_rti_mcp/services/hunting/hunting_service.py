import os
from collections import defaultdict
from typing import Any

from fabric_rti_mcp.graph_api_http_client import GraphHttpClientCache

HUNTING_ENDPOINT = "/security/runHuntingQuery"
HUNTING_SCHEMA_ENDPOINT = "/security/runHuntingQuery/schema"

# The official Microsoft Graph Security API uses "Query" and "Timespan" as payload keys.
# Some deployments expect "queryText" instead. Set HUNTING_QUERY_FIELD_NAME to override.
_QUERY_FIELD = os.getenv("HUNTING_QUERY_FIELD_NAME", "Query")
_TIMESPAN_FIELD = "Timespan"

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

    IMPORTANT: Before writing any query, call get_hunting_schema() first to discover
    available tables, columns, and their types. Do NOT guess table or column names —
    the schema varies per tenant and may include custom tables (e.g., SAP, custom logs).

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
    payload: dict[str, Any] = {_QUERY_FIELD: query}

    if timespan:
        payload[_TIMESPAN_FIELD] = timespan

    return GraphHttpClientCache.get_client().make_request(
        "POST",
        _HUNTING_ENDPOINT,
        payload,
    )


def get_hunting_schema() -> str:
    """
    Retrieves the Advanced Hunting schema from Microsoft 365 Defender.

    Returns a compact summary of all available tables grouped by section,
    showing table names and column counts. Use get_table_schema(table_name)
    to get the full column details for a specific table.

    IMPORTANT: Call this tool BEFORE writing any hunting query. The schema reveals
    tenant-specific tables (including custom/solution tables like SAP, custom logs, etc.)
    that cannot be guessed. Use get_table_schema() to inspect column details for query writing.

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
