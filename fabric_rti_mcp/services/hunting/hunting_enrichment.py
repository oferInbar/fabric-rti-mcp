import json
import re
from typing import Any

from mcp.server.fastmcp import Context
from mcp.types import SamplingMessage, TextContent

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

# Priority-ranked datetime column names (higher index = lower priority).
_DATETIME_PRIORITY = [
    "Timestamp",
    "EventTime",
    "TimeGenerated",
    "ActivityDateTime",
    "CreatedDateTime",
    "StartTime",
    "EndTime",
    "LastSeen",
    "FirstSeen",
]

# Column name patterns indicating entity/categorical columns (suitable for bar charts).
_ENTITY_COLUMN_PATTERNS = re.compile(
    r"(AccountUpn|AccountName|DeviceName|DeviceId|FileName|RemoteIP|"
    r"SenderFromAddress|RecipientEmailAddress|InitiatingProcessFileName|"
    r"ActionType|Category|Severity|LogonType|Application)",
    re.IGNORECASE,
)

# Column name patterns indicating numeric/aggregate columns.
_NUMERIC_COLUMN_PATTERNS = re.compile(r"(Count|Total|Sum|Avg|Rate|Percentage|Score|Attempts|Duration)", re.IGNORECASE)


def _is_datetime_type(type_str: str) -> bool:
    return "datetime" in type_str.lower()


def _is_numeric_type(type_str: str) -> bool:
    lower = type_str.lower()
    return any(t in lower for t in ("int", "long", "real", "double", "decimal"))


def _rank_datetime_column(name: str) -> int:
    """Lower rank = higher priority for timeline visualization."""
    try:
        return _DATETIME_PRIORITY.index(name)
    except ValueError:
        return len(_DATETIME_PRIORITY)


def _compute_summary_stats(
    results: list[dict[str, Any]], col_types: dict[str, str], column_stats: dict[str, dict[str, Any]]
) -> dict[str, Any]:
    """Compute structured summary statistics for the result set."""
    summary: dict[str, Any] = {"total_rows": len(results)}

    # Time range
    for col_name, col_type in col_types.items():
        if _is_datetime_type(col_type) and col_name in column_stats:
            stats = column_stats[col_name]
            if "min" in stats and "max" in stats:
                summary["time_range"] = {"column": col_name, "min": stats["min"], "max": stats["max"]}
                break

    # Top entities: find low-cardinality entity columns and their top values
    top_entities: dict[str, list[str]] = {}
    for col_name in col_types:
        if not _ENTITY_COLUMN_PATTERNS.search(col_name):
            continue
        stats = column_stats.get(col_name, {})
        distinct = stats.get("distinct_count", 0)
        if 1 <= distinct <= 50:
            values = [row.get(col_name) for row in results if row.get(col_name) is not None]
            # Count occurrences and take top 5
            counts: dict[str, int] = {}
            for v in values:
                sv = str(v)
                counts[sv] = counts.get(sv, 0) + 1
            top = sorted(counts.keys(), key=lambda k: counts[k], reverse=True)[:5]
            top_entities[col_name] = top
    if top_entities:
        summary["top_entities"] = top_entities

    # Concentration hints: flag if >80% of rows share a single value in any entity column
    anomaly_hints: list[str] = []
    for col_name, top_vals in top_entities.items():
        if top_vals:
            top_val = top_vals[0]
            count = sum(1 for row in results if str(row.get(col_name, "")) == top_val)
            if len(results) > 2 and count / len(results) > 0.8:
                anomaly_hints.append(f"{count}/{len(results)} rows have {col_name}='{top_val}'")
    if anomaly_hints:
        summary["anomaly_hints"] = anomaly_hints

    return summary


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
    - Produces structured summary stats (top entities, time range, anomaly hints)

    :param schema: List of column definitions from the query result, each with 'Name' and 'Type'.
    :param results: List of result row dictionaries.
    :return: Dictionary with timeline_column, chart_type, column_stats, filter_suggestions, summary.
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

    # Timeline column: rank by known priority, then fallback to first datetime.
    timeline_column: str | None = None
    if datetime_cols:
        timeline_column = min(datetime_cols, key=_rank_datetime_column)

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

    # Chart type heuristic (enhanced with column-name semantics)
    chart_type = "table"
    has_entity_col = any(_ENTITY_COLUMN_PATTERNS.search(c) for c in col_types)
    has_aggregate_col = any(_NUMERIC_COLUMN_PATTERNS.search(c) for c in col_types if _is_numeric_type(col_types[c]))

    if len(results) == 1:
        chart_type = "card"
    elif timeline_column and numeric_cols:
        chart_type = "line"
    elif has_entity_col and has_aggregate_col:
        # Entity + aggregate pattern (e.g., AccountUpn + FailedAttempts) → bar chart
        chart_type = "bar"
    elif any(
        1 < column_stats.get(c, {}).get("distinct_count", 0) <= 30
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

    # Structured summary
    summary = _compute_summary_stats(results, col_types, column_stats)

    return {
        "timeline_column": timeline_column,
        "chart_type": chart_type,
        "column_stats": column_stats,
        "filter_suggestions": filter_suggestions,
        "summary": summary,
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


_SUMMARIZE_SYSTEM_PROMPT = (
    "You are a security analyst. Given hunting query results, produce a concise JSON response with:\n"
    '- "summary_bullets": 2-4 key findings as short sentences (focus on what matters for security)\n'
    '- "recommended_chart": the best chart type for this data ("line", "bar", "pie", "table", or "card")\n'
    '- "grouping_column": the best column to group/visualize by\n'
    "Respond ONLY with valid JSON, no markdown."
)

_MAX_RESULTS_FOR_SUMMARY = 30
_MAX_SUMMARY_TOKENS = 400


async def summarize_hunting_results(
    query: str,
    schema: list[dict[str, str]],
    results: list[dict[str, Any]],
    user_prompt: str | None = None,
    ctx: Context | None = None,
) -> dict[str, Any]:
    """
    Generates natural language summarization of hunting query results using the client LLM.

    Uses MCP sampling to request the connected client's language model to analyze the results
    and produce human-readable findings. This is complementary to analyze_hunting_results which
    provides deterministic statistics — this tool adds semantic understanding.

    Requires the MCP client to support sampling (createMessage). If sampling is unavailable,
    returns a fallback response indicating the feature is not supported.

    :param query: The original KQL query that produced the results.
    :param schema: List of column definitions from the query result, each with 'Name' and 'Type'.
    :param results: List of result row dictionaries (first 30 rows are used).
    :param user_prompt: Optional original user question for context.
    :param ctx: MCP Context (auto-injected by FastMCP).
    :return: Dictionary with:
        - summary_bullets (list[str]): Key findings in natural language.
        - recommended_chart (str): Semantically appropriate chart type.
        - grouping_column (str | None): Best column for visualization grouping.
        - source (str): "llm" if produced by sampling, "unavailable" if sampling failed.
    """
    if ctx is None:
        return _fallback_response("No MCP context available for sampling")

    # Truncate results for token efficiency
    truncated = results[:_MAX_RESULTS_FOR_SUMMARY]
    schema_summary = ", ".join(f"{c.get('Name', '')}:{c.get('Type', '')}" for c in schema)

    user_content = f"Query: {query}\nSchema: {schema_summary}\nResults ({len(truncated)} of {len(results)} rows):\n"
    user_content += json.dumps(truncated, default=str)
    if user_prompt:
        user_content = f"User question: {user_prompt}\n\n{user_content}"

    try:
        response = await ctx.session.create_message(
            messages=[SamplingMessage(role="user", content=TextContent(type="text", text=user_content))],
            max_tokens=_MAX_SUMMARY_TOKENS,
            system_prompt=_SUMMARIZE_SYSTEM_PROMPT,
        )

        # Parse the LLM response
        text = response.content.text if hasattr(response.content, "text") else str(response.content)
        parsed = json.loads(text)
        return {
            "summary_bullets": parsed.get("summary_bullets", []),
            "recommended_chart": parsed.get("recommended_chart", "table"),
            "grouping_column": parsed.get("grouping_column"),
            "source": "llm",
        }
    except json.JSONDecodeError:
        # LLM returned non-JSON; use raw text as a single bullet
        return {
            "summary_bullets": [text.strip()] if text else [],
            "recommended_chart": "table",
            "grouping_column": None,
            "source": "llm",
        }
    except Exception as e:
        return _fallback_response(f"Sampling not supported or failed: {e}")


def _fallback_response(reason: str) -> dict[str, Any]:
    return {
        "summary_bullets": [],
        "recommended_chart": None,
        "grouping_column": None,
        "source": "unavailable",
        "reason": reason,
    }
