from __future__ import annotations

import re
from dataclasses import dataclass

_DEFAULT_KUSTO_SUFFIX = ".kusto.windows.net"


def _normalize_cluster_uri(raw: str) -> str:
    """Expand a short cluster name to a full URI if needed.

    ``"asiusagetelemetryprod.eastus"`` → ``"https://asiusagetelemetryprod.eastus.kusto.windows.net"``
    """
    if raw.startswith("http://"):
        raise ValueError(f"HTTP cluster URIs are not allowed — use HTTPS: '{raw}'")
    if raw.startswith("https://"):
        return raw
    if raw.endswith(_DEFAULT_KUSTO_SUFFIX):
        return f"https://{raw}"
    return f"https://{raw}{_DEFAULT_KUSTO_SUFFIX}"


_CLUSTER_PREFIX = re.compile(r"""^cluster\((['"])([^'"]+)\1\)\.""", re.IGNORECASE)
_DATABASE_PREFIX = re.compile(r"""^database\((['"])([^'"]+)\1\)\.""", re.IGNORECASE)


_PARAM_PRECONFIGURED = "{param} is pre-configured. No need to provide it."
_PARAM_REQUIRED = "{param} is REQUIRED — you must provide the {description}."
_PARAM_OPTIONAL = "{param} is optional. {fallback}"

_TOOL_DESCRIPTION_HEADER = """\
Retrieves KQL query examples that semantically resemble the user's prompt.

IMPORTANT: Call this tool BEFORE writing any KQL query. The returned shots contain
expert-written KQL examples that reveal the correct databases, tables, column names,
and query patterns for this cluster. Without this context, you are likely to query
the wrong table or database.

Use this to:
- Discover which databases and tables contain the data you need
- Learn the correct column names and schema for a given domain
- Find proven query patterns as starting points"""


@dataclass(slots=True, frozen=True)
class ShotsTableRef:
    """Parsed reference to a shots table, optionally including cluster and database."""

    table_name: str
    cluster_uri: str | None = None
    database: str | None = None

    @property
    def is_fully_qualified(self) -> bool:
        return self.cluster_uri is not None and self.database is not None

    def describe_params(self, has_default_cluster: bool = False) -> list[str]:
        """Return per-parameter description lines indicating what is pre-configured vs required."""
        param_lines: list[str] = []

        if self.cluster_uri or has_default_cluster:
            param_lines.append(_PARAM_PRECONFIGURED.format(param="cluster_uri"))
        else:
            param_lines.append(_PARAM_REQUIRED.format(param="cluster_uri", description="Kusto cluster URI"))

        param_lines.append(_PARAM_PRECONFIGURED.format(param="shots_table_name"))

        if self.database:
            param_lines.append(_PARAM_PRECONFIGURED.format(param="database"))
        else:
            param_lines.append(
                _PARAM_OPTIONAL.format(param="database", fallback="If not provided, the default database will be used.")
            )

        return param_lines

    @staticmethod
    def _describe_unconfigured_params(has_default_cluster: bool = False) -> list[str]:
        """Return parameter descriptions when no ShotsTableRef is configured."""
        param_lines: list[str] = []

        if has_default_cluster:
            param_lines.append(_PARAM_PRECONFIGURED.format(param="cluster_uri"))
        else:
            param_lines.append(_PARAM_REQUIRED.format(param="cluster_uri", description="Kusto cluster URI"))

        param_lines.append(_PARAM_REQUIRED.format(param="shots_table_name", description="shots table name"))
        param_lines.append(
            _PARAM_OPTIONAL.format(param="database", fallback="If not provided, the default database will be used.")
        )

        return param_lines

    @staticmethod
    def build_tool_description(
        shots_table_ref: ShotsTableRef | None, embedding_configured: bool, has_default_cluster: bool = False
    ) -> str:
        """Build the full tool description for kusto_get_shots."""
        lines = [_TOOL_DESCRIPTION_HEADER, ""]

        if shots_table_ref:
            lines.extend(shots_table_ref.describe_params(has_default_cluster))
        else:
            lines.extend(ShotsTableRef._describe_unconfigured_params(has_default_cluster))

        if embedding_configured:
            lines.append(_PARAM_PRECONFIGURED.format(param="embedding_endpoint"))
        else:
            lines.append(
                _PARAM_OPTIONAL.format(
                    param="embedding_endpoint", fallback="If not set, the cluster's default embedding will be used."
                )
            )

        return "\n".join(lines)

    @staticmethod
    def parse(value: str) -> ShotsTableRef:
        """Parse a shots table reference which may be a plain table name or a FQN.

        Supported formats:
          - Plain table name: ``"MCP_SHOTS"``
          - Database-qualified: ``"database('MyDB').MCP_SHOTS"``
          - Fully qualified:  ``"cluster('https://host.kusto.windows.net').database('MyDB').MCP_SHOTS"``
          - Short cluster name: ``"cluster('host.eastus').database('MyDB').MCP_SHOTS"``
            (auto-expanded to ``https://host.eastus.kusto.windows.net``)
        """
        text_to_parse = value.strip()
        if not text_to_parse:
            raise ValueError("KUSTO_SHOTS_TABLE must not be blank.")

        cluster_uri = None
        database = None

        cluster_match = _CLUSTER_PREFIX.match(text_to_parse)
        if cluster_match:
            cluster_uri = _normalize_cluster_uri(cluster_match.group(2))
            text_to_parse = text_to_parse[cluster_match.end() :]

        db_match = _DATABASE_PREFIX.match(text_to_parse)
        if db_match:
            database = db_match.group(2)
            text_to_parse = text_to_parse[db_match.end() :]

        table_name = text_to_parse.strip()
        if not table_name:
            raise ValueError(f"KUSTO_SHOTS_TABLE is missing the table name: '{value}'")

        return ShotsTableRef(table_name=table_name, cluster_uri=cluster_uri, database=database)
