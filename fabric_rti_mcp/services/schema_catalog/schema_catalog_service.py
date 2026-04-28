from __future__ import annotations

import json
import math
import os
import re
from dataclasses import dataclass, field
from typing import Any

from fabric_rti_mcp.config import logger

# ── Synonym map for token-overlap scoring ────────────────────────────────────

_SYNONYMS: dict[str, list[str]] = {
    "logon": ["signin", "login", "authentication", "auth"],
    "signin": ["logon", "login", "authentication", "auth"],
    "login": ["logon", "signin", "authentication"],
    "auth": ["logon", "signin", "login", "authentication"],
    "authentication": ["logon", "signin", "login", "auth"],
    "email": ["mail", "message", "outlook"],
    "mail": ["email", "message"],
    "network": ["connection", "traffic", "ip", "dns", "socket"],
    "dns": ["network", "domain", "resolution"],
    "process": ["executable", "binary", "program", "cmdline"],
    "file": ["document", "attachment", "path"],
    "device": ["machine", "computer", "endpoint", "host"],
    "endpoint": ["device", "machine", "computer", "host"],
    "identity": ["user", "account", "principal"],
    "user": ["identity", "account", "principal"],
    "account": ["identity", "user", "principal"],
    "threat": ["malware", "attack", "adversary", "intrusion"],
    "malware": ["threat", "attack", "virus"],
    "geo": ["geolocation", "location", "country", "ip", "travel"],
    "geolocation": ["geo", "location", "country", "ip"],
    "travel": ["location", "geo", "ip", "geolocation", "impossible"],
    "impossible": ["travel", "anomaly", "location", "geo"],
    "anomaly": ["impossible", "travel", "unusual", "detection"],
    "alert": ["detection", "incident", "finding"],
    "incident": ["alert", "detection", "case"],
    "vulnerability": ["cve", "patch", "exposure", "tvm"],
    "cloud": ["azure", "aws", "saas"],
    "exposure": ["vulnerability", "attack", "risk"],
}

# ── Data models ───────────────────────────────────────────────────────────────


@dataclass
class ColumnInfo:
    name: str
    type: str
    description: str


@dataclass
class TableInfo:
    name: str
    group: str
    columns: list[ColumnInfo]
    aliases: list[str] = field(default_factory=list)


@dataclass
class SchemaPack:
    name: str
    generated_at: str | None
    tables: dict[str, TableInfo]


# ── Catalog state ─────────────────────────────────────────────────────────────

_catalog: dict[str, SchemaPack] | None = None


def _get_catalog() -> dict[str, SchemaPack]:
    global _catalog
    if _catalog is None:
        _catalog = {}
        _load_catalog()
    return _catalog


def _load_catalog() -> None:
    """Load schema packs from environment configuration into the global catalog."""
    assert _catalog is not None
    path = os.environ.get("SCHEMA_CATALOG_PATH")
    if not path:
        return
    try:
        pack = _load_pack_from_file(path)
        _catalog[pack.name] = pack
        logger.info(f"Schema catalog: loaded pack '{pack.name}' with {len(pack.tables)} tables from '{path}'")
    except Exception as exc:
        logger.error(f"Schema catalog: failed to load from '{path}': {exc}")


def _load_pack_from_file(file_path: str) -> SchemaPack:
    """
    Load a schema pack from a JSON file.

    Supported format:
    {
      "PackName": "optional_override",       // optional
      "GeneratedAtUtc": "2026-...",          // optional
      "Aliases": {"TableName": ["alias1"]},  // optional per-table aliases
      "Schema": {
        "GroupName": {
          "TableName": [{"Name": "col", "Type": "string", "Description": "..."}]
        }
      }
    }
    """
    with open(file_path, encoding="utf-8") as f:
        raw: dict[str, Any] = json.load(f)

    pack_name: str = raw.get("PackName") or _slugify(os.path.splitext(os.path.basename(file_path))[0])
    generated_at: str | None = raw.get("GeneratedAtUtc")
    aliases_map: dict[str, list[str]] = raw.get("Aliases", {})

    tables: dict[str, TableInfo] = {}
    for group_name, group_tables in raw.get("Schema", {}).items():
        for table_name, columns_raw in group_tables.items():
            columns = [
                ColumnInfo(
                    name=col["Name"],
                    type=col.get("Type", ""),
                    description=col.get("Description", ""),
                )
                for col in columns_raw
            ]
            tables[table_name] = TableInfo(
                name=table_name,
                group=group_name,
                columns=columns,
                aliases=aliases_map.get(table_name, []),
            )

    return SchemaPack(name=pack_name, generated_at=generated_at, tables=tables)


def _slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def is_configured() -> bool:
    """Return True if the schema catalog is configured via environment variables."""
    return bool(os.environ.get("SCHEMA_CATALOG_PATH"))


# ── Scoring helpers ───────────────────────────────────────────────────────────


def _tokenize(text: str) -> set[str]:
    return set(re.findall(r"[a-z]+", text.lower()))


def _expand_tokens(tokens: set[str]) -> set[str]:
    expanded = set(tokens)
    for token in tokens:
        expanded.update(_SYNONYMS.get(token, []))
    return expanded


def _score_tables(pack: SchemaPack, prompt_tokens: set[str]) -> list[tuple[str, float]]:
    scores: list[tuple[str, float]] = []
    for table_name, table in pack.tables.items():
        score = _table_score(table, prompt_tokens)
        if score > 0:
            scores.append((table_name, score))
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores


def _table_score(table: TableInfo, prompt_tokens: set[str]) -> float:
    name_tokens = _tokenize(table.name)
    name_overlap = len(prompt_tokens & name_tokens) / max(len(name_tokens), 1)

    alias_overlap = 0.0
    if table.aliases:
        alias_tokens: set[str] = set()
        for alias in table.aliases:
            alias_tokens.update(_tokenize(alias))
        alias_overlap = len(prompt_tokens & alias_tokens) / max(len(alias_tokens), 1)

    col_score = sum(
        math.log1p(len(prompt_tokens & (_tokenize(col.name) | _tokenize(col.description)))) for col in table.columns
    )
    col_score /= max(len(table.columns), 1)

    return name_overlap * 3.0 + alias_overlap * 2.0 + col_score


def _score_columns(columns: list[ColumnInfo], prompt_tokens: set[str]) -> list[ColumnInfo]:
    scored = [(col, len(prompt_tokens & (_tokenize(col.name) | _tokenize(col.description)))) for col in columns]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [col for col, _ in scored]


# ── Tool implementations ──────────────────────────────────────────────────────


def schema_list_packs() -> list[dict[str, Any]]:
    """
    List all available schema packs in the catalog.

    :return: List of objects with pack metadata:
        * name — pack identifier to use in other schema tools
        * table_count — number of tables in the pack
        * generated_at — ISO timestamp when the schema was generated (or null)

    Example:
    [{"name": "defender_xdr_advanced_hunting", "table_count": 107, "generated_at": "2026-04-28T09:54:45Z"}]
    """
    return [
        {
            "name": pack.name,
            "table_count": len(pack.tables),
            "generated_at": pack.generated_at,
        }
        for pack in _get_catalog().values()
    ]


def schema_list_tables(pack: str) -> list[str]:
    """
    List all table names available in the specified schema pack.

    :param pack: Schema pack name as returned by schema_list_packs.
    :return: Sorted list of table names.
    """
    catalog = _get_catalog()
    if pack not in catalog:
        raise ValueError(f"Unknown pack '{pack}'. Available: {list(catalog.keys())}")
    return sorted(catalog[pack].tables.keys())


def schema_get_table(pack: str, table_name: str) -> dict[str, Any]:
    """
    Get the full column schema for a specific table.

    :param pack: Schema pack name as returned by schema_list_packs.
    :param table_name: Table name as returned by schema_list_tables.
    :return: Object with table metadata and all columns:
        * table — table name
        * group — logical group the table belongs to (e.g. "MdatpTables")
        * columns — list of {name, type, description}

    Example:
    {
      "table": "DeviceFileEvents",
      "group": "MdatpTables",
      "columns": [{"name": "Timestamp", "type": "datetime", "description": "..."}, ...]
    }
    """
    catalog = _get_catalog()
    if pack not in catalog:
        raise ValueError(f"Unknown pack '{pack}'. Available: {list(catalog.keys())}")
    tables = catalog[pack].tables
    if table_name not in tables:
        raise ValueError(f"Unknown table '{table_name}' in pack '{pack}'")
    t = tables[table_name]
    return {
        "table": t.name,
        "group": t.group,
        "columns": [{"name": c.name, "type": c.type, "description": c.description} for c in t.columns],
    }


def schema_search(pack: str, prompt: str, top_k: int = 5, top_k_columns: int = 15) -> dict[str, Any]:
    """
    Search for schema tables most relevant to a natural-language query intent.
    Call this before writing KQL to get the correct tables and columns to use.

    Uses token-overlap scoring with synonym expansion (e.g. "sign-in" matches logon tables).

    :param pack: Schema pack name as returned by schema_list_packs.
    :param prompt: Natural language description of the query intent,
        e.g. "impossible travel for user sign-ins" or "email attachments with malware".
    :param top_k: Maximum number of tables to return. Defaults to 5.
    :param top_k_columns: Maximum number of columns to return per table. Defaults to 15.
    :return: Compact context object:
        * tables — ordered list of matching table names (most relevant first)
        * matches — list of {table, group, columns:[{name,type,description}]}
        * notes — summary of the search

    Critical: Only use tables and columns returned here when generating KQL.
    If a needed column is missing, call schema_get_table for the full column list.
    """
    catalog = _get_catalog()
    if pack not in catalog:
        raise ValueError(f"Unknown pack '{pack}'. Available: {list(catalog.keys())}")

    prompt_tokens = _expand_tokens(_tokenize(prompt))
    scored = _score_tables(catalog[pack], prompt_tokens)
    top = scored[:top_k]

    matches = []
    for table_name, _ in top:
        t = catalog[pack].tables[table_name]
        top_cols = _score_columns(t.columns, prompt_tokens)[:top_k_columns]
        matches.append(
            {
                "table": t.name,
                "group": t.group,
                "columns": [{"name": c.name, "type": c.type, "description": c.description} for c in top_cols],
            }
        )

    return {
        "tables": [m["table"] for m in matches],
        "matches": matches,
        "notes": f"Returned top {len(matches)} of {len(catalog[pack].tables)} tables most relevant to: '{prompt}'",
    }
