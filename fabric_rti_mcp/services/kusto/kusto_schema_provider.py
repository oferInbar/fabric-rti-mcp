from __future__ import annotations

import functools
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

from fabric_rti_mcp.config import logger
from fabric_rti_mcp.services.kusto.kusto_connection import sanitize_uri

_ADX_PROXY_HOST = "ade.loganalytics.io"
SCHEMA_FILE_ENV_VAR = "KUSTO_SCHEMA_FILE"
_BUNDLED_SCHEMA = Path(__file__).parents[3] / "schema.json"


def is_adx_proxy(cluster_uri: str) -> bool:
    """Check if cluster_uri points to a Log Analytics ADX proxy."""
    try:
        hostname = urlparse(sanitize_uri(cluster_uri)).hostname or ""
        return hostname.lower() == _ADX_PROXY_HOST
    except Exception:
        return False


@dataclass(slots=True, frozen=True)
class StaticColumnSchema:
    Name: str
    Type: str
    Description: str = ""


@functools.lru_cache(maxsize=1)
def _load_schema() -> tuple[dict[str, tuple[StaticColumnSchema, ...]], dict[str, str]]:
    """Load a static schema file and build a flat lookup.

    Returns (canonical_lookup, lower_to_canonical) where:
    - canonical_lookup maps canonical table names to column tuples
    - lower_to_canonical maps lowered names to canonical names for case-insensitive access
    """
    schema_path = os.getenv(SCHEMA_FILE_ENV_VAR)
    if not schema_path:
        schema_path = str(_BUNDLED_SCHEMA) if _BUNDLED_SCHEMA.exists() else None
    if not schema_path:
        return {}, {}

    try:
        with open(schema_path) as f:
            raw = json.load(f)
    except Exception as e:
        logger.error(f"Failed to load schema from {schema_path}: {e}")
        return {}, {}

    lookup: dict[str, tuple[StaticColumnSchema, ...]] = {}
    raw_schema: Any = raw.get("Schema", {})
    if not isinstance(raw_schema, dict):
        return {}, {}
    schema_section = cast(dict[str, Any], raw_schema)
    for tables_val in schema_section.values():
        if not isinstance(tables_val, dict):
            continue
        tables = cast(dict[str, Any], tables_val)
        for tbl_name, tbl_columns in tables.items():
            if isinstance(tbl_columns, list):
                col_list = cast(list[dict[str, Any]], tbl_columns)
                lookup[str(tbl_name)] = tuple(StaticColumnSchema(**col) for col in col_list)

    lower_to_canonical = {name.lower(): name for name in lookup}
    logger.info(f"Loaded static schema: {len(lookup)} tables from {schema_path}")
    return lookup, lower_to_canonical


def _resolve_table(table_name: str) -> tuple[StaticColumnSchema, ...] | None:
    """Case-insensitive table lookup. Returns columns or None."""
    lookup, lower_map = _load_schema()
    canonical = lower_map.get(table_name.lower())
    if canonical is None:
        return None
    return lookup.get(canonical)


def get_static_table_names() -> list[str]:
    """Return all table names from the static schema (canonical casing)."""
    lookup, _ = _load_schema()
    return list(lookup.keys())


def get_static_table_schema(table_name: str) -> list[dict[str, str]] | None:
    """Return column schema for a table, or None if not in the static schema."""
    columns = _resolve_table(table_name)
    if columns is None:
        return None
    return [asdict(c) for c in columns]


def format_static_table_list(table_names: list[str]) -> dict[str, Any]:
    """Format a list of table names into the same shape as a kusto_response from `.show tables`."""
    columns = [
        {"ColumnName": "TableName", "ColumnType": "string"},
        {"ColumnName": "DatabaseName", "ColumnType": "string"},
        {"ColumnName": "Folder", "ColumnType": "string"},
        {"ColumnName": "DocString", "ColumnType": "string"},
    ]
    rows = [[name, "", "", ""] for name in table_names]
    return {"format": "kusto_response", "data": {"columns": columns, "rows": rows}}


def format_static_cslschema(table_name: str, columns: list[dict[str, str]]) -> dict[str, Any]:
    """Format static column schema into the same shape as `.show table X cslschema`."""
    csl_parts = [f"{col['Name']}:{col['Type']}" for col in columns]
    csl_string = ", ".join(csl_parts)
    result_columns = [
        {"ColumnName": "TableName", "ColumnType": "string"},
        {"ColumnName": "Schema", "ColumnType": "string"},
        {"ColumnName": "DatabaseName", "ColumnType": "string"},
        {"ColumnName": "Folder", "ColumnType": "string"},
        {"ColumnName": "DocString", "ColumnType": "string"},
    ]
    rows = [[table_name, csl_string, "", "", ""]]
    return {"format": "kusto_response", "data": {"columns": result_columns, "rows": rows}}


def format_static_database_entities(table_names: list[str]) -> dict[str, Any]:
    """Format a list of table names into the same shape as `.show databases entities`."""
    result_columns = [
        {"ColumnName": "EntityName", "ColumnType": "string"},
        {"ColumnName": "EntityType", "ColumnType": "string"},
        {"ColumnName": "Folder", "ColumnType": "string"},
        {"ColumnName": "DocString", "ColumnType": "string"},
        {"ColumnName": "CslInputSchema", "ColumnType": "string"},
        {"ColumnName": "Content", "ColumnType": "string"},
        {"ColumnName": "CslOutputSchema", "ColumnType": "string"},
    ]
    rows: list[list[str]] = []
    for name in table_names:
        cols = _resolve_table(name)
        csl = ""
        if cols:
            csl = ", ".join(f"{c.Name}:{c.Type}" for c in cols)
        rows.append([name, "Table", "", "", csl, "", csl])
    return {"format": "kusto_response", "data": {"columns": result_columns, "rows": rows}}
