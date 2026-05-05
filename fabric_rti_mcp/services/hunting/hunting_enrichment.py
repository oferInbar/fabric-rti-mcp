import re
from typing import Any

# ---------------------------------------------------------------------------
# Static mappings
# ---------------------------------------------------------------------------

_TABLE_PIVOTS: dict[str, dict[str, str]] = {
    "DeviceProcessEvents": {
        "description": "Check network activity for the same devices",
        "kql_template": "DeviceNetworkEvents | where DeviceId in ({device_ids}) | project Timestamp, DeviceId, RemoteIP, RemotePort, InitiatingProcessFileName | order by Timestamp desc | limit 100",
    },
    "DeviceNetworkEvents": {
        "description": "Check file activity for the same devices",
        "kql_template": "DeviceFileEvents | where DeviceId in ({device_ids}) | project Timestamp, DeviceId, FileName, FolderPath, SHA256, ActionType | order by Timestamp desc | limit 100",
    },
    "DeviceFileEvents": {
        "description": "Check process activity for the same devices",
        "kql_template": "DeviceProcessEvents | where DeviceId in ({device_ids}) | project Timestamp, DeviceId, FileName, ProcessCommandLine, InitiatingProcessFileName | order by Timestamp desc | limit 100",
    },
    "EmailAttachmentInfo": {
        "description": "Check email events for the same messages",
        "kql_template": "EmailEvents | where NetworkMessageId in ({message_ids}) | project Timestamp, SenderFromAddress, RecipientEmailAddress, Subject, DeliveryAction | order by Timestamp desc | limit 100",
    },
    "IdentityLogonEvents": {
        "description": "Check identity directory changes",
        "kql_template": "IdentityDirectoryEvents | where AccountUpn in ({upns}) | project Timestamp, AccountUpn, ActionType, TargetAccountUpn | order by Timestamp desc | limit 100",
    },
    "AlertInfo": {
        "description": "Get evidence details for these alerts",
        "kql_template": "AlertEvidence | where AlertId in ({alert_ids}) | project AlertId, EntityType, EvidenceRole, FileName, SHA256, RemoteIP | limit 100",
    },
}

_COLUMN_PIVOTS: list[dict[str, Any]] = [
    {
        "columns": ["SHA1", "SHA256"],
        "description": "Look up file reputation across devices",
        "kql_template": "DeviceFileEvents | where SHA1 == '{hash}' or SHA256 == '{hash}' | summarize dcount(DeviceId), min(Timestamp), max(Timestamp) by FileName, SHA256",
    },
    {
        "columns": ["RemoteIP"],
        "description": "Sweep for this IP across all telemetry",
        "kql_template": "union DeviceNetworkEvents, EmailEvents | where RemoteIP == '{ip}' | summarize count() by $table, bin(Timestamp, 1h) | order by Timestamp desc",
    },
    {
        "columns": ["AccountUpn"],
        "description": "Check sign-in activity for these users",
        "kql_template": "IdentityLogonEvents | where AccountUpn in ({upns}) | project Timestamp, AccountUpn, ActionType, LogonType, Application | order by Timestamp desc | limit 100",
    },
    {
        "columns": ["DeviceId"],
        "description": "Get device details and health",
        "kql_template": "DeviceInfo | where DeviceId in ({device_ids}) | summarize arg_max(Timestamp, *) by DeviceId | project DeviceId, DeviceName, OSPlatform, OSVersion, MachineGroup",
    },
]

_ACTION_MAPPINGS: list[dict[str, Any]] = [
    {
        "action": "IsolateMachine",
        "entity_type": "Device",
        "id_columns": ["DeviceId", "DeviceName"],
        "description": "Isolate a device from the network to contain a threat",
    },
    {
        "action": "CollectInvestigationPackage",
        "entity_type": "Device",
        "id_columns": ["DeviceId", "DeviceName"],
        "description": "Collect forensic investigation package from a device",
    },
    {
        "action": "RunAntivirusScan",
        "entity_type": "Device",
        "id_columns": ["DeviceId", "DeviceName"],
        "description": "Trigger an antivirus scan on a device",
    },
    {
        "action": "RestrictAppExecution",
        "entity_type": "Device",
        "id_columns": ["DeviceId", "DeviceName"],
        "description": "Restrict application execution on a device to Microsoft-signed binaries only",
    },
    {
        "action": "StopAndQuarantineFile",
        "entity_type": "File",
        "id_columns": ["SHA1", "SHA256"],
        "description": "Stop a process and quarantine its file on devices",
    },
    {
        "action": "AllowFile",
        "entity_type": "File",
        "id_columns": ["SHA1", "SHA256"],
        "description": "Allow a file that was previously blocked",
    },
    {
        "action": "BlockFile",
        "entity_type": "File",
        "id_columns": ["SHA1", "SHA256"],
        "description": "Block a file from running across the organization",
    },
    {
        "action": "MarkUserAsCompromised",
        "entity_type": "User",
        "id_columns": ["AccountUpn", "AccountObjectId"],
        "description": "Mark a user account as compromised in Azure AD",
    },
    {
        "action": "DisableUser",
        "entity_type": "User",
        "id_columns": ["AccountUpn", "AccountObjectId"],
        "description": "Disable a user account",
    },
    {
        "action": "ForceUserPasswordReset",
        "entity_type": "User",
        "id_columns": ["AccountUpn", "AccountObjectId"],
        "description": "Force a password reset for a user account",
    },
    {
        "action": "MoveEmailToFolder",
        "entity_type": "Email",
        "id_columns": ["NetworkMessageId"],
        "description": "Move emails to junk or deleted items folder",
    },
    {
        "action": "DeleteEmail",
        "entity_type": "Email",
        "id_columns": ["NetworkMessageId"],
        "description": "Soft-delete or hard-delete emails",
    },
    {
        "action": "TriggerEmailInvestigation",
        "entity_type": "Email",
        "id_columns": ["NetworkMessageId"],
        "description": "Trigger an automated investigation on emails",
    },
]

# Table name extraction pattern — matches the first bare identifier before a pipe or whitespace.
_TABLE_NAME_RE = re.compile(r"^\s*(\w+)\s*(?:\||$)", re.MULTILINE)


def _is_datetime_type(type_str: str) -> bool:
    return "datetime" in type_str.lower()


def _is_numeric_type(type_str: str) -> bool:
    lower = type_str.lower()
    return any(t in lower for t in ("int", "long", "real", "double", "decimal"))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def analyze_hunting_results(
    schema: list[dict[str, str]],
    results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Performs deterministic heuristic analysis on Advanced Hunting query results.

    Analyzes the result set to produce structured metadata useful for visualization
    and exploration:
    - Identifies the best column for timeline visualization
    - Suggests appropriate chart type based on data shape
    - Computes column statistics (cardinality, min/max for dates/numbers)
    - Generates filter suggestions (columns with low-to-medium cardinality and their top values)

    :param schema: List of column definitions from the query result, each with 'Name' and 'Type'.
    :param results: List of result row dictionaries.
    :return: Dictionary with timeline_column, chart_type, column_stats, filter_suggestions.
    """
    datetime_cols: list[str] = []
    numeric_cols: list[str] = []
    col_types: dict[str, str] = {}

    for col in schema:
        name = col.get("Name", "")
        col_type = col.get("Type", "")
        col_types[name] = col_type
        if _is_datetime_type(col_type):
            datetime_cols.append(name)
        if _is_numeric_type(col_type):
            numeric_cols.append(name)

    # Timeline column: prefer "Timestamp" if present, else first datetime column.
    timeline_column: str | None = None
    if "Timestamp" in datetime_cols:
        timeline_column = "Timestamp"
    elif datetime_cols:
        timeline_column = datetime_cols[0]

    # Column statistics
    column_stats: dict[str, dict[str, Any]] = {}
    for col in schema:
        name = col.get("Name", "")
        values = [row.get(name) for row in results if row.get(name) is not None]
        distinct_values = set(str(v) for v in values)
        stats: dict[str, Any] = {"distinct_count": len(distinct_values)}

        if _is_datetime_type(col_types.get(name, "")):
            str_values = sorted(str(v) for v in values if v is not None)
            if str_values:
                stats["min"] = str_values[0]
                stats["max"] = str_values[-1]

        if _is_numeric_type(col_types.get(name, "")):
            try:
                num_values = [float(v) for v in values if v is not None]
                if num_values:
                    stats["min"] = min(num_values)
                    stats["max"] = max(num_values)
            except (ValueError, TypeError):
                pass

        column_stats[name] = stats

    # Chart type heuristic
    chart_type = "table"
    if len(results) == 1:
        chart_type = "card"
    elif timeline_column and numeric_cols:
        chart_type = "line"
    elif any(
        1 < column_stats.get(c, {}).get("distinct_count", 0) <= 20
        for c in col_types
        if not _is_datetime_type(col_types[c]) and not _is_numeric_type(col_types[c])
    ):
        chart_type = "bar"

    # Filter suggestions: columns with cardinality between 2 and 20
    filter_suggestions: list[dict[str, Any]] = []
    for col in schema:
        name = col.get("Name", "")
        stats = column_stats.get(name, {})
        distinct_count = stats.get("distinct_count", 0)
        if 2 <= distinct_count <= 20:
            values = sorted({str(row.get(name)) for row in results if row.get(name) is not None})[:10]
            filter_suggestions.append({"column": name, "distinct_count": distinct_count, "sample_values": values})

    return {
        "timeline_column": timeline_column,
        "chart_type": chart_type,
        "column_stats": column_stats,
        "filter_suggestions": filter_suggestions,
    }


def suggest_hunting_followups(
    query: str,
    schema: list[dict[str, str]],
) -> list[dict[str, str]]:
    """Generates follow-up hunting query suggestions based on tables and columns used in the original query.

    Uses security-domain heuristics to suggest related pivot queries.

    :param query: The original KQL query text.
    :param schema: The result schema (list of column defs with 'Name' and 'Type').
    :return: List of suggestions, each with 'description' and 'kql_template' keys.
    """
    suggestions: list[dict[str, str]] = []
    seen_descriptions: set[str] = set()

    # Table-based pivots
    match = _TABLE_NAME_RE.search(query)
    if match:
        table_name = match.group(1)
        pivot = _TABLE_PIVOTS.get(table_name)
        if pivot and pivot["description"] not in seen_descriptions:
            suggestions.append({"description": pivot["description"], "kql_template": pivot["kql_template"]})
            seen_descriptions.add(pivot["description"])

    # Column-based pivots
    schema_col_names = {col.get("Name", "") for col in schema}
    for pivot in _COLUMN_PIVOTS:
        if any(c in schema_col_names for c in pivot["columns"]):
            if pivot["description"] not in seen_descriptions:
                suggestions.append({"description": pivot["description"], "kql_template": pivot["kql_template"]})
                seen_descriptions.add(pivot["description"])

    return suggestions[:5]


def get_available_hunting_actions(
    column_names: list[str],
) -> list[dict[str, str]]:
    """Maps result column names to available Microsoft 365 Defender response actions.

    Given the columns present in a hunting query result, returns the set of
    response actions that can be taken on entities identified in those columns.

    :param column_names: List of column names from the query result schema.
    :return: List of available actions with 'action', 'entity_type', 'id_column', and 'description'.
    """
    column_names_lower = {c.lower(): c for c in column_names}
    actions: list[dict[str, str]] = []

    for mapping in _ACTION_MAPPINGS:
        for id_col in mapping["id_columns"]:
            original_name = column_names_lower.get(id_col.lower())
            if original_name is not None:
                actions.append(
                    {
                        "action": mapping["action"],
                        "entity_type": mapping["entity_type"],
                        "id_column": original_name,
                        "description": mapping["description"],
                    }
                )
                break

    return actions
