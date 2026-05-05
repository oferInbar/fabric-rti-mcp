from collections.abc import Generator
from typing import Any
from unittest.mock import MagicMock

import pytest

from fabric_rti_mcp.services.hunting import hunting_service
from fabric_rti_mcp.services.hunting.hunting_insights import (
    _build_kql_array,
    get_device_insights,
    get_user_insights,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_run_query(monkeypatch: pytest.MonkeyPatch) -> Generator[MagicMock, None, None]:
    mock = MagicMock()
    monkeypatch.setattr("fabric_rti_mcp.services.hunting.hunting_insights.run_hunting_query", mock)
    yield mock


@pytest.fixture()
def mock_http_client(monkeypatch: pytest.MonkeyPatch) -> Generator[MagicMock, None, None]:
    client: MagicMock = MagicMock()

    def fake_get_client(*_: object) -> MagicMock:
        return client

    monkeypatch.setattr(hunting_service.GraphHttpClientCache, "get_client", fake_get_client)
    yield client


# ---------------------------------------------------------------------------
# _build_kql_array
# ---------------------------------------------------------------------------


def test_build_kql_array_single() -> None:
    result = _build_kql_array(["abc123"])
    assert result == "dynamic(['abc123'])"


def test_build_kql_array_multiple() -> None:
    result = _build_kql_array(["id1", "id2", "id3"])
    assert result == "dynamic(['id1', 'id2', 'id3'])"


def test_build_kql_array_escapes_quotes() -> None:
    result = _build_kql_array(["it's", "test"])
    assert result == "dynamic(['it\\'s', 'test'])"


def test_build_kql_array_empty() -> None:
    result = _build_kql_array([])
    assert result == "dynamic([])"


# ---------------------------------------------------------------------------
# get_device_insights
# ---------------------------------------------------------------------------


def test_device_insights_returns_all_sections(mock_run_query: MagicMock) -> None:
    mock_run_query.return_value = {"schema": [], "results": []}
    result = get_device_insights(["device1"])
    assert "device_info" in result
    assert "vulnerabilities" in result
    assert "logon_history" in result
    assert "alert_evidence" in result


def test_device_insights_calls_query_four_times(mock_run_query: MagicMock) -> None:
    mock_run_query.return_value = {"schema": [], "results": []}
    get_device_insights(["device1"])
    assert mock_run_query.call_count == 4


def test_device_insights_handles_query_failure(mock_run_query: MagicMock) -> None:
    """One failing query should not prevent others from returning."""
    call_count = 0

    def side_effect(query: str, **kwargs: Any) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("Query failed")
        return {"schema": [], "results": []}

    mock_run_query.side_effect = side_effect
    result = get_device_insights(["device1"])
    # Should have 4 keys — one with error, three with results
    assert len(result) == 4
    error_sections = [k for k, v in result.items() if isinstance(v, dict) and "error" in v]
    assert len(error_sections) == 1


def test_device_insights_multiple_devices(mock_run_query: MagicMock) -> None:
    mock_run_query.return_value = {"schema": [], "results": []}
    get_device_insights(["device1", "device2", "device3"])
    for call_args in mock_run_query.call_args_list:
        query = call_args[0][0]
        assert "device1" in query
        assert "device2" in query
        assert "device3" in query


# ---------------------------------------------------------------------------
# get_user_insights
# ---------------------------------------------------------------------------


def test_user_insights_returns_all_sections(mock_run_query: MagicMock) -> None:
    mock_run_query.return_value = {"schema": [], "results": []}
    result = get_user_insights(["user@contoso.com"])
    assert "identity_info" in result
    assert "logon_history" in result
    assert "alert_evidence" in result
    assert "risk_events" in result


def test_user_insights_calls_query_four_times(mock_run_query: MagicMock) -> None:
    mock_run_query.return_value = {"schema": [], "results": []}
    get_user_insights(["user@contoso.com"])
    assert mock_run_query.call_count == 4


def test_user_insights_handles_query_failure(mock_run_query: MagicMock) -> None:
    call_count = 0

    def side_effect(query: str, **kwargs: Any) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        if call_count == 2:
            raise Exception("Query failed")
        return {"schema": [], "results": []}

    mock_run_query.side_effect = side_effect
    result = get_user_insights(["user@contoso.com"])
    assert len(result) == 4
    error_sections = [k for k, v in result.items() if isinstance(v, dict) and "error" in v]
    assert len(error_sections) == 1


# ---------------------------------------------------------------------------
# run_hunting_query truncation
# ---------------------------------------------------------------------------


def test_truncation_default_max(mock_http_client: MagicMock) -> None:
    """Results exceeding default max (500) should be truncated."""
    results = [{"id": i} for i in range(600)]
    mock_http_client.make_request.return_value = {"schema": [], "results": results}
    response = hunting_service.run_hunting_query("query")
    assert "_truncation_info" in response
    assert response["_truncation_info"]["truncated"] is True
    assert response["_truncation_info"]["returned"] == 500
    assert response["_truncation_info"]["total_available"] == 600
    assert len(response["results"]) == 500


def test_truncation_custom_max(mock_http_client: MagicMock) -> None:
    results = [{"id": i} for i in range(50)]
    mock_http_client.make_request.return_value = {"schema": [], "results": results}
    response = hunting_service.run_hunting_query("query", max_results=10)
    assert "_truncation_info" in response
    assert len(response["results"]) == 10


def test_no_truncation_when_under_max(mock_http_client: MagicMock) -> None:
    results = [{"id": i} for i in range(5)]
    mock_http_client.make_request.return_value = {"schema": [], "results": results}
    response = hunting_service.run_hunting_query("query")
    assert "_truncation_info" not in response
    assert len(response["results"]) == 5


def test_truncation_disabled_with_zero(mock_http_client: MagicMock) -> None:
    results = [{"id": i} for i in range(1000)]
    mock_http_client.make_request.return_value = {"schema": [], "results": results}
    response = hunting_service.run_hunting_query("query", max_results=0)
    assert "_truncation_info" not in response
    assert len(response["results"]) == 1000
