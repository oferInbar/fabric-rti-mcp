from collections.abc import Generator
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


def test_run_hunting_query_with_timespan(mock_http_client: MagicMock) -> None:
    """Test hunting query with optional timespan parameter."""
    expected_response: dict[str, Any] = {
        "schema": [{"name": "Timestamp", "type": "DateTime"}],
        "results": [],
    }
    mock_http_client.make_request.return_value = expected_response

    query = "DeviceProcessEvents | limit 10"
    timespan = "P7D"
    result = hunting_service.run_hunting_query(query, timespan=timespan)

    assert result == expected_response
    mock_http_client.make_request.assert_called_once_with(
        "POST",
        "/security/runHuntingQuery",
        {"Query": query, "Timespan": "P7D"},
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
        {"Query": query, "Timespan": timespan},
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


def test_run_hunting_query_complex_kql(mock_http_client: MagicMock) -> None:
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
    result = hunting_service.run_hunting_query(query, timespan="P7D")

    assert result["results"][0]["FailedAttempts"] == 42
    mock_http_client.make_request.assert_called_once_with(
        "POST",
        "/security/runHuntingQuery",
        {"Query": query, "Timespan": "P7D"},
    )
