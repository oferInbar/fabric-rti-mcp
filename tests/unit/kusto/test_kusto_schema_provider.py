import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest

from fabric_rti_mcp.services.kusto.kusto_schema_provider import (
    _load_schema,  # pyright: ignore[reportPrivateUsage]
    format_static_cslschema,
    format_static_database_entities,
    format_static_table_list,
    get_static_table_names,
    get_static_table_schema,
    is_adx_proxy,
)

PROXY_URI = (
    "https://ade.loganalytics.io/subscriptions/99005f96-e572-4035-b476-836fd9d83d64"
    "/resourcegroups/CyberSOC/providers/microsoft.operationalinsights/workspaces/CyberSOC"
)
REGULAR_URI = "https://help.kusto.windows.net"
FABRIC_URI = "https://mycluster.z1.kusto.fabric.microsoft.com"

from typing import Any

SAMPLE_SCHEMA: dict[str, Any] = {
    "GeneratedAtUtc": "2024-01-01",
    "Schema": {
        "TestCategory": {
            "AlertEvidence": [
                {"Name": "Timestamp", "Type": "datetime", "Description": "Time of event"},
                {"Name": "AlertId", "Type": "string", "Description": "Alert identifier"},
            ],
            "DeviceInfo": [
                {"Name": "DeviceName", "Type": "string", "Description": "Name of device"},
            ],
        }
    },
}


# --- is_adx_proxy ---


class TestIsAdxProxy:
    def test_proxy_uri(self) -> None:
        assert is_adx_proxy(PROXY_URI) is True

    def test_regular_cluster(self) -> None:
        assert is_adx_proxy(REGULAR_URI) is False

    def test_fabric_cluster(self) -> None:
        assert is_adx_proxy(FABRIC_URI) is False

    def test_empty_string(self) -> None:
        assert is_adx_proxy("") is False

    def test_garbage(self) -> None:
        assert is_adx_proxy("not-a-uri") is False

    def test_case_insensitive(self) -> None:
        assert is_adx_proxy("https://ADE.LOGANALYTICS.IO/foo") is True


# --- Schema loading and lookup ---


@pytest.fixture(autouse=True)
def _clear_schema_cache() -> None:  # noqa: PT004  # pyright: ignore[reportUnusedFunction]
    """Clear the lru_cache between tests."""
    _load_schema.cache_clear()


class TestSchemaLoading:
    def test_no_env_var(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            names = get_static_table_names()
        assert names == []

    def test_missing_file(self) -> None:
        with patch.dict(os.environ, {"KUSTO_SCHEMA_FILE": "/nonexistent/schema.json"}):
            names = get_static_table_names()
        assert names == []

    def test_loads_tables(self, tmp_path: Path) -> None:
        schema_file = tmp_path / "schema.json"
        schema_file.write_text(json.dumps(SAMPLE_SCHEMA))

        with patch.dict(os.environ, {"KUSTO_SCHEMA_FILE": str(schema_file)}):
            names = get_static_table_names()
        assert sorted(names) == ["AlertEvidence", "DeviceInfo"]

    def test_case_insensitive_lookup(self, tmp_path: Path) -> None:
        schema_file = tmp_path / "schema.json"
        schema_file.write_text(json.dumps(SAMPLE_SCHEMA))

        with patch.dict(os.environ, {"KUSTO_SCHEMA_FILE": str(schema_file)}):
            result = get_static_table_schema("alertevidence")
        assert result is not None
        assert result[0]["Name"] == "Timestamp"

    def test_unknown_table_returns_none(self, tmp_path: Path) -> None:
        schema_file = tmp_path / "schema.json"
        schema_file.write_text(json.dumps(SAMPLE_SCHEMA))

        with patch.dict(os.environ, {"KUSTO_SCHEMA_FILE": str(schema_file)}):
            result = get_static_table_schema("SigninLogs")
        assert result is None


# --- Format helpers ---


class TestFormatHelpers:
    def test_format_table_list(self) -> None:
        result = format_static_table_list(["AlertEvidence", "DeviceInfo"])
        assert result["format"] == "kusto_response"
        assert len(result["data"]["rows"]) == 2
        assert result["data"]["rows"][0][0] == "AlertEvidence"
        assert result["data"]["columns"][0]["ColumnName"] == "TableName"

    def test_format_cslschema(self) -> None:
        columns = [
            {"Name": "Timestamp", "Type": "datetime", "Description": ""},
            {"Name": "AlertId", "Type": "string", "Description": ""},
        ]
        result = format_static_cslschema("AlertEvidence", columns)
        assert result["format"] == "kusto_response"
        assert result["data"]["rows"][0][0] == "AlertEvidence"
        assert "Timestamp:datetime" in result["data"]["rows"][0][1]
        assert "AlertId:string" in result["data"]["rows"][0][1]

    def test_format_database_entities(self, tmp_path: Path) -> None:
        schema_file = tmp_path / "schema.json"
        schema_file.write_text(json.dumps(SAMPLE_SCHEMA))

        with patch.dict(os.environ, {"KUSTO_SCHEMA_FILE": str(schema_file)}):
            result = format_static_database_entities(["AlertEvidence", "DeviceInfo"])
        assert result["format"] == "kusto_response"
        assert len(result["data"]["rows"]) == 2
        # EntityType should be "Table"
        assert result["data"]["rows"][0][1] == "Table"
        # CslInputSchema should have column info
        assert "Timestamp:datetime" in result["data"]["rows"][0][4]

    def test_format_empty_list(self) -> None:
        result = format_static_table_list([])
        assert result["data"]["rows"] == []
