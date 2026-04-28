import json
import os
import tempfile
from typing import Any
from unittest.mock import patch

import pytest

from fabric_rti_mcp.services.schema_catalog import schema_catalog_service
from fabric_rti_mcp.services.schema_catalog.schema_catalog_service import (
    SchemaPack,
    TableInfo,
    ColumnInfo,
    _load_pack_from_file,
    _slugify,
    schema_get_table,
    schema_list_packs,
    schema_list_tables,
    schema_search,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

SAMPLE_SCHEMA_JSON: dict[str, Any] = {
    "GeneratedAtUtc": "2026-01-01T00:00:00Z",
    "Schema": {
        "AadTables": {
            "EntraIdSignInEvents": [
                {"Name": "Timestamp", "Type": "datetime", "Description": "Date and time of the sign-in"},
                {"Name": "AccountObjectId", "Type": "string", "Description": "Unique identifier for the account"},
                {"Name": "IPAddress", "Type": "string", "Description": "IP address of the client"},
                {"Name": "CountryCode", "Type": "string", "Description": "Country of origin based on IP"},
                {"Name": "City", "Type": "string", "Description": "City of origin based on IP"},
                {"Name": "Latitude", "Type": "real", "Description": "Latitude of the sign-in location"},
                {"Name": "Longitude", "Type": "real", "Description": "Longitude of the sign-in location"},
                {"Name": "IsInteractive", "Type": "bool", "Description": "Whether sign-in was interactive"},
            ]
        },
        "MdatpTables": {
            "DeviceFileEvents": [
                {"Name": "Timestamp", "Type": "datetime", "Description": "Date and time of the event"},
                {"Name": "DeviceId", "Type": "string", "Description": "Unique identifier for the device"},
                {"Name": "FileName", "Type": "string", "Description": "Name of the file"},
                {"Name": "FolderPath", "Type": "string", "Description": "Path to the file"},
                {"Name": "SHA256", "Type": "string", "Description": "SHA-256 hash of the file"},
            ],
            "DeviceProcessEvents": [
                {"Name": "Timestamp", "Type": "datetime", "Description": "Date and time of the process event"},
                {"Name": "DeviceId", "Type": "string", "Description": "Unique identifier for the device"},
                {"Name": "ProcessCommandLine", "Type": "string", "Description": "Command line used to run the process"},
                {"Name": "InitiatingProcessFileName", "Type": "string", "Description": "Parent process file name"},
            ],
        },
        "OfficeTables": {
            "EmailEvents": [
                {"Name": "Timestamp", "Type": "datetime", "Description": "Date and time of the email event"},
                {"Name": "SenderFromAddress", "Type": "string", "Description": "Sender email address"},
                {"Name": "RecipientEmailAddress", "Type": "string", "Description": "Recipient email address"},
                {"Name": "Subject", "Type": "string", "Description": "Subject of the email"},
                {"Name": "ThreatNames", "Type": "string", "Description": "Names of detected threats"},
            ]
        },
    },
}


@pytest.fixture
def schema_file(tmp_path: Any) -> str:
    """Write sample schema JSON to a temp file and return the path."""
    path = tmp_path / "DefenderSchema.json"
    path.write_text(json.dumps(SAMPLE_SCHEMA_JSON), encoding="utf-8")
    return str(path)


@pytest.fixture(autouse=True)
def reset_catalog() -> Any:
    """Reset the in-memory catalog before each test."""
    schema_catalog_service._catalog = None
    yield
    schema_catalog_service._catalog = None


# ── Loading tests ─────────────────────────────────────────────────────────────


def test_load_pack_derives_name_from_filename(schema_file: str) -> None:
    pack = _load_pack_from_file(schema_file)
    assert pack.name == "defenderschema"


def test_load_pack_uses_explicit_pack_name(tmp_path: Any) -> None:
    data = dict(SAMPLE_SCHEMA_JSON, PackName="my_custom_pack")
    path = tmp_path / "schema.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    pack = _load_pack_from_file(str(path))
    assert pack.name == "my_custom_pack"


def test_load_pack_table_count(schema_file: str) -> None:
    pack = _load_pack_from_file(schema_file)
    assert len(pack.tables) == 4


def test_load_pack_column_count(schema_file: str) -> None:
    pack = _load_pack_from_file(schema_file)
    assert len(pack.tables["EntraIdSignInEvents"].columns) == 8


def test_load_pack_group_assignment(schema_file: str) -> None:
    pack = _load_pack_from_file(schema_file)
    assert pack.tables["DeviceFileEvents"].group == "MdatpTables"


def test_load_pack_generated_at(schema_file: str) -> None:
    pack = _load_pack_from_file(schema_file)
    assert pack.generated_at == "2026-01-01T00:00:00Z"


def test_load_pack_aliases(tmp_path: Any) -> None:
    data = dict(SAMPLE_SCHEMA_JSON, Aliases={"EntraIdSignInEvents": ["logon", "signin"]})
    path = tmp_path / "schema.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    pack = _load_pack_from_file(str(path))
    assert pack.tables["EntraIdSignInEvents"].aliases == ["logon", "signin"]


def test_slugify() -> None:
    assert _slugify("DefenderSchema") == "defenderschema"
    assert _slugify("My-Schema File") == "my_schema_file"


# ── schema_list_packs ─────────────────────────────────────────────────────────


def test_schema_list_packs_empty_when_no_env(monkeypatch: Any) -> None:
    monkeypatch.delenv("SCHEMA_CATALOG_PATH", raising=False)
    result = schema_list_packs()
    assert result == []


def test_schema_list_packs_returns_metadata(schema_file: str, monkeypatch: Any) -> None:
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    result = schema_list_packs()
    assert len(result) == 1
    assert result[0]["table_count"] == 4
    assert result[0]["generated_at"] == "2026-01-01T00:00:00Z"
    assert "name" in result[0]


# ── schema_list_tables ────────────────────────────────────────────────────────


def test_schema_list_tables_sorted(schema_file: str, monkeypatch: Any) -> None:
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    pack_name = schema_list_packs()[0]["name"]
    tables = schema_list_tables(pack_name)
    assert tables == sorted(tables)
    assert "EntraIdSignInEvents" in tables
    assert "DeviceFileEvents" in tables


def test_schema_list_tables_unknown_pack(schema_file: str, monkeypatch: Any) -> None:
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    with pytest.raises(ValueError, match="Unknown pack"):
        schema_list_tables("nonexistent_pack")


# ── schema_get_table ──────────────────────────────────────────────────────────


def test_schema_get_table_returns_columns(schema_file: str, monkeypatch: Any) -> None:
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    pack_name = schema_list_packs()[0]["name"]
    result = schema_get_table(pack_name, "DeviceFileEvents")
    assert result["table"] == "DeviceFileEvents"
    assert result["group"] == "MdatpTables"
    col_names = [c["name"] for c in result["columns"]]
    assert "FileName" in col_names
    assert "SHA256" in col_names


def test_schema_get_table_unknown_table(schema_file: str, monkeypatch: Any) -> None:
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    pack_name = schema_list_packs()[0]["name"]
    with pytest.raises(ValueError, match="Unknown table"):
        schema_get_table(pack_name, "NonExistentTable")


# ── schema_search ─────────────────────────────────────────────────────────────


def test_schema_search_signin_travel(schema_file: str, monkeypatch: Any) -> None:
    """Sign-in/travel prompt should surface EntraIdSignInEvents."""
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    pack_name = schema_list_packs()[0]["name"]
    result = schema_search(pack_name, "impossible travel for user sign-ins")
    assert "EntraIdSignInEvents" in result["tables"]
    # Geolocation columns should appear
    col_names = [c["name"] for m in result["matches"] if m["table"] == "EntraIdSignInEvents" for c in m["columns"]]
    assert any(c in col_names for c in ("CountryCode", "Latitude", "Longitude", "IPAddress"))


def test_schema_search_email_malware(schema_file: str, monkeypatch: Any) -> None:
    """Email/malware prompt should surface EmailEvents."""
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    pack_name = schema_list_packs()[0]["name"]
    result = schema_search(pack_name, "email attachments with malware")
    assert "EmailEvents" in result["tables"]


def test_schema_search_process_execution(schema_file: str, monkeypatch: Any) -> None:
    """Process execution prompt should surface DeviceProcessEvents."""
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    pack_name = schema_list_packs()[0]["name"]
    result = schema_search(pack_name, "suspicious process execution command line")
    assert "DeviceProcessEvents" in result["tables"]


def test_schema_search_top_k_respected(schema_file: str, monkeypatch: Any) -> None:
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    pack_name = schema_list_packs()[0]["name"]
    result = schema_search(pack_name, "device file process email signin", top_k=2)
    assert len(result["tables"]) <= 2
    assert len(result["matches"]) <= 2


def test_schema_search_top_k_columns_respected(schema_file: str, monkeypatch: Any) -> None:
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    pack_name = schema_list_packs()[0]["name"]
    result = schema_search(pack_name, "sign-in", top_k=5, top_k_columns=3)
    for match in result["matches"]:
        assert len(match["columns"]) <= 3


def test_schema_search_result_structure(schema_file: str, monkeypatch: Any) -> None:
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    pack_name = schema_list_packs()[0]["name"]
    result = schema_search(pack_name, "device events")
    assert "tables" in result
    assert "matches" in result
    assert "notes" in result
    for match in result["matches"]:
        assert "table" in match
        assert "group" in match
        assert "columns" in match
        for col in match["columns"]:
            assert "name" in col
            assert "type" in col
            assert "description" in col


def test_schema_search_unknown_pack(schema_file: str, monkeypatch: Any) -> None:
    monkeypatch.setenv("SCHEMA_CATALOG_PATH", schema_file)
    with pytest.raises(ValueError, match="Unknown pack"):
        schema_search("nonexistent", "some query")
