from collections.abc import Generator
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from fabric_rti_mcp.services.hunting import hunting_service


@pytest.fixture()
def mock_http_client(monkeypatch: pytest.MonkeyPatch) -> Generator[MagicMock, None, None]:
    client: MagicMock = MagicMock()

    def fake_get_client(*_: object) -> MagicMock:
        return client

    monkeypatch.setattr(hunting_service.GraphHttpClientCache, "get_client", fake_get_client)
    yield client


def test_run_hunting_query_basic(mock_http_client: MagicMock) -> None:
    """Test basic hunting query with only query parameter."""
    expected_response: dict[str, Any] = {
        "schema": [
            {"name": "Timestamp", "type": "DateTime"},
            {"name": "FileName", "type": "String"},
        ],
        "results": [
            {"Timestamp": "2024-03-26T09:39:50Z", "FileName": "cmd.exe"},
        ],
    }
    mock_http_client.make_request.return_value = expected_response

    query = "DeviceProcessEvents | where FileName == 'cmd.exe' | limit 1"
    result = hunting_service.run_hunting_query(query)

    assert result == expected_response
    mock_http_client.make_request.assert_called_once_with(
        "POST",
        "/security/runHuntingQuery",
        {"Query": query},
    )


def test_run_hunting_query_with_timespan(mock_http_client: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test hunting query with optional timespan parameter."""
    expected_response: dict[str, Any] = {
        "schema": [{"name": "Timestamp", "type": "DateTime"}],
        "results": [],
    }
    mock_http_client.make_request.return_value = expected_response

    monkeypatch.setattr(hunting_service, "_utcnow", lambda: datetime(2024, 1, 8, tzinfo=timezone.utc))

    query = "DeviceProcessEvents | limit 10"
    timespan = "P7D"
    result = hunting_service.run_hunting_query(query, timespan=timespan)

    assert result == expected_response
    mock_http_client.make_request.assert_called_once_with(
        "POST",
        "/security/runHuntingQuery",
        {
            "Query": query,
            "Timespan": "P7D",
            "startTime": "2024-01-01T00:00:00Z",
            "endTime": "2024-01-08T00:00:00Z",
        },
    )


def test_run_hunting_query_with_date_range_timespan(mock_http_client: MagicMock) -> None:
    """Test hunting query with explicit date range timespan."""
    mock_http_client.make_request.return_value = {"schema": [], "results": []}

    query = "IdentityLogonEvents | limit 5"
    timespan = "2024-01-01T00:00:00Z/2024-01-31T23:59:59Z"
    result = hunting_service.run_hunting_query(query, timespan=timespan)

    assert result == {"schema": [], "results": []}
    mock_http_client.make_request.assert_called_once_with(
        "POST",
        "/security/runHuntingQuery",
        {
            "Query": query,
            "Timespan": timespan,
            "startTime": "2024-01-01T00:00:00Z",
            "endTime": "2024-01-31T23:59:59Z",
        },
    )


def test_run_hunting_query_no_timespan_omits_field(mock_http_client: MagicMock) -> None:
    """Test that Timespan is not sent when not provided."""
    mock_http_client.make_request.return_value = {"schema": [], "results": []}

    hunting_service.run_hunting_query("DeviceProcessEvents | limit 1")

    call_args = mock_http_client.make_request.call_args
    payload = call_args[0][2]
    assert "Timespan" not in payload
    assert "Query" in payload


def test_run_hunting_query_returns_error_response(mock_http_client: MagicMock) -> None:
    """Test that Graph API error responses are passed through."""
    error_response: dict[str, Any] = {
        "error": True,
        "status_code": 403,
        "detail": "Forbidden: insufficient permissions",
    }
    mock_http_client.make_request.return_value = error_response

    result = hunting_service.run_hunting_query("DeviceProcessEvents | limit 1")

    assert result == error_response


def test_run_hunting_query_complex_kql(mock_http_client: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test with a realistic complex hunting query."""
    mock_http_client.make_request.return_value = {
        "schema": [
            {"name": "AccountUpn", "type": "String"},
            {"name": "FailedAttempts", "type": "Int64"},
        ],
        "results": [
            {"AccountUpn": "user@contoso.com", "FailedAttempts": 42},
        ],
    }

    query = (
        "IdentityLogonEvents "
        "| where ActionType == 'LogonFailed' "
        "| summarize FailedAttempts=count() by AccountUpn "
        "| order by FailedAttempts desc "
        "| limit 20"
    )
    monkeypatch.setattr(hunting_service, "_utcnow", lambda: datetime(2024, 1, 8, tzinfo=timezone.utc))

    result = hunting_service.run_hunting_query(query, timespan="P7D")

    assert result["results"][0]["FailedAttempts"] == 42
    mock_http_client.make_request.assert_called_once_with(
        "POST",
        "/security/runHuntingQuery",
        {
            "Query": query,
            "Timespan": "P7D",
            "startTime": "2024-01-01T00:00:00Z",
            "endTime": "2024-01-08T00:00:00Z",
        },
    )


def test_run_hunting_query_with_start_and_end_time(mock_http_client: MagicMock) -> None:
    """Test that startTime + endTime are combined into a Timespan interval."""
    mock_http_client.make_request.return_value = {"schema": [], "results": []}

    hunting_service.run_hunting_query(
        "DeviceProcessEvents | limit 1",
        startTime="2024-01-01T00:00:00Z",
        endTime="2024-01-31T23:59:59Z",
    )

    mock_http_client.make_request.assert_called_once_with(
        "POST",
        "/security/runHuntingQuery",
        {
            "Query": "DeviceProcessEvents | limit 1",
            "Timespan": "2024-01-01T00:00:00Z/2024-01-31T23:59:59Z",
            "startTime": "2024-01-01T00:00:00Z",
            "endTime": "2024-01-31T23:59:59Z",
        },
    )


def test_run_hunting_query_with_start_time_only(mock_http_client: MagicMock) -> None:
    """Test that startTime alone produces a 30-day window from that start."""
    mock_http_client.make_request.return_value = {"schema": [], "results": []}

    hunting_service.run_hunting_query("DeviceProcessEvents | limit 1", startTime="2024-01-01T00:00:00Z")

    mock_http_client.make_request.assert_called_once_with(
        "POST",
        "/security/runHuntingQuery",
        {
            "Query": "DeviceProcessEvents | limit 1",
            "Timespan": "2024-01-01T00:00:00Z/P30D",
            "startTime": "2024-01-01T00:00:00Z",
        },
    )


def test_run_hunting_query_start_time_overrides_timespan(mock_http_client: MagicMock) -> None:
    """Test that startTime takes precedence over timespan when both are provided."""
    mock_http_client.make_request.return_value = {"schema": [], "results": []}

    hunting_service.run_hunting_query(
        "DeviceProcessEvents | limit 1",
        timespan="P7D",
        startTime="2024-01-01T00:00:00Z",
        endTime="2024-01-15T00:00:00Z",
    )

    mock_http_client.make_request.assert_called_once_with(
        "POST",
        "/security/runHuntingQuery",
        {
            "Query": "DeviceProcessEvents | limit 1",
            "Timespan": "2024-01-01T00:00:00Z/2024-01-15T00:00:00Z",
            "startTime": "2024-01-01T00:00:00Z",
            "endTime": "2024-01-15T00:00:00Z",
        },
    )


def test_run_hunting_query_custom_field_name(mock_http_client: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that HUNTING_QUERY_FIELD_NAME env var overrides the query field name."""
    monkeypatch.setattr(hunting_service, "_QUERY_FIELD", "queryText")
    mock_http_client.make_request.return_value = {"schema": [], "results": []}

    monkeypatch.setattr(hunting_service, "_utcnow", lambda: datetime(2024, 1, 8, tzinfo=timezone.utc))

    query = "DeviceProcessEvents | limit 1"
    hunting_service.run_hunting_query(query, timespan="P7D")

    mock_http_client.make_request.assert_called_once_with(
        "POST",
        "/security/runHuntingQuery",
        {
            "queryText": query,
            "Timespan": "P7D",
            "startTime": "2024-01-01T00:00:00Z",
            "endTime": "2024-01-08T00:00:00Z",
        },
    )


def test_get_hunting_schema(mock_http_client: MagicMock) -> None:
    """Test retrieving the Advanced Hunting schema returns compact markdown."""
    schema_response = [
        {
            "Name": "DeviceProcessEvents",
            "TableSection": "Devices",
            "HasData": True,
            "Schema": [
                {"Name": "Timestamp", "Type": "DateTime", "Description": "Timestamp", "Entity": None},
                {"Name": "DeviceId", "Type": "String", "Description": "Device ID", "Entity": "Machine"},
            ],
        },
        {
            "Name": "EmailEvents",
            "TableSection": "Emails",
            "HasData": True,
            "Schema": [
                {"Name": "Timestamp", "Type": "DateTime", "Description": "Timestamp", "Entity": None},
            ],
        },
    ]
    mock_http_client.make_request.return_value = schema_response
    hunting_service._HuntingSchemaCache.clear()

    result = hunting_service.get_hunting_schema()

    assert "# Advanced Hunting Schema (2 tables)" in result
    assert "## Devices" in result
    assert "- DeviceProcessEvents (2 columns)" in result
    assert "## Emails" in result
    assert "- EmailEvents (1 columns)" in result


def test_get_hunting_schema_error(mock_http_client: MagicMock) -> None:
    """Test that error responses are returned as text."""
    error_response = {"error": True, "status_code": 403, "detail": "Forbidden"}
    mock_http_client.make_request.return_value = error_response
    hunting_service._HuntingSchemaCache.clear()

    result = hunting_service.get_hunting_schema()

    assert "Error fetching schema" in result


def test_get_table_schema(mock_http_client: MagicMock) -> None:
    """Test retrieving schema for a single table."""
    schema_response = [
        {
            "Name": "DeviceProcessEvents",
            "TableSection": "Devices",
            "HasData": True,
            "Schema": [
                {"Name": "Timestamp", "Type": "DateTime", "Description": "Date and time", "Entity": None},
                {"Name": "DeviceId", "Type": "String", "Description": "Device ID", "Entity": "Machine"},
                {"Name": "FileName", "Type": "String", "Description": "", "Entity": None},
            ],
        },
    ]
    mock_http_client.make_request.return_value = schema_response
    hunting_service._HuntingSchemaCache.clear()

    result = hunting_service.get_table_schema("DeviceProcessEvents")

    assert "## DeviceProcessEvents (Devices)" in result
    assert "  Timestamp: DateTime - Date and time" in result
    assert "  DeviceId: String [Machine] - Device ID" in result
    assert "  FileName: String" in result
    # No trailing " - " for empty description
    assert "FileName: String\n" in result or "FileName: String" in result


def test_get_table_schema_case_insensitive(mock_http_client: MagicMock) -> None:
    """Test that table lookup is case-insensitive."""
    schema_response = [
        {
            "Name": "DeviceProcessEvents",
            "TableSection": "Devices",
            "HasData": True,
            "Schema": [
                {"Name": "Timestamp", "Type": "DateTime", "Description": "Timestamp", "Entity": None},
            ],
        },
    ]
    mock_http_client.make_request.return_value = schema_response
    hunting_service._HuntingSchemaCache.clear()

    result = hunting_service.get_table_schema("deviceprocessevents")

    assert "## DeviceProcessEvents (Devices)" in result


def test_get_table_schema_not_found(mock_http_client: MagicMock) -> None:
    """Test error message when table is not found."""
    schema_response = [
        {
            "Name": "DeviceProcessEvents",
            "TableSection": "Devices",
            "HasData": True,
            "Schema": [],
        },
    ]
    mock_http_client.make_request.return_value = schema_response
    hunting_service._HuntingSchemaCache.clear()

    result = hunting_service.get_table_schema("NonExistentTable")

    assert "not found" in result
    assert "DeviceProcessEvents" in result


def test_schema_cache_reuses_result(mock_http_client: MagicMock) -> None:
    """Test that schema is fetched only once and cached."""
    schema_response = [
        {
            "Name": "DeviceProcessEvents",
            "TableSection": "Devices",
            "HasData": True,
            "Schema": [{"Name": "Timestamp", "Type": "DateTime", "Description": "", "Entity": None}],
        },
    ]
    mock_http_client.make_request.return_value = schema_response
    hunting_service._HuntingSchemaCache.clear()

    # Call both tools
    hunting_service.get_hunting_schema()
    hunting_service.get_table_schema("DeviceProcessEvents")

    # API called only once
    mock_http_client.make_request.assert_called_once_with("GET", hunting_service._SCHEMA_ENDPOINT)
