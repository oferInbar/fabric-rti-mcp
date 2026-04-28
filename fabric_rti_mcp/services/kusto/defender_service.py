from __future__ import annotations

import os
from typing import Any

import requests

from fabric_rti_mcp.config import logger

DEFENDER_QUERY_URL = "https://security.microsoft.com/apiproxy/mtp/huntingService/queryExecutor?useFanOut=false"
DEFENDER_URI_SCHEME = "defender://"


def is_defender_uri(cluster_uri: str) -> bool:
    return cluster_uri.strip().lower().startswith(DEFENDER_URI_SCHEME)


def _get_tenant_id(cluster_uri: str) -> str:
    return cluster_uri.strip().removeprefix(DEFENDER_URI_SCHEME).strip("/")


def _get_cookies() -> dict[str, str]:
    sccauth = os.environ.get("DEFENDER_SCCAUTH")
    if not sccauth:
        raise RuntimeError(
            "DEFENDER_SCCAUTH environment variable not set. "
            "Get it from browser DevTools: open security.microsoft.com → DevTools → Network → "
            "run a query → find queryExecutor request → copy sccauth cookie value."
        )
    xsrf = os.environ.get("DEFENDER_XSRF_TOKEN", "")
    sess_id = os.environ.get("DEFENDER_SESS_ID", "")

    cookies: dict[str, str] = {"sccauth": sccauth}
    if xsrf:
        cookies["XSRF-TOKEN"] = xsrf
    if sess_id:
        cookies["s.SessID"] = sess_id
    return cookies


def defender_query(query: str, cluster_uri: str, database: str | None = None) -> dict[str, Any]:
    """Execute a KQL query against Defender Advanced Hunting via the portal API."""
    tenant_id = _get_tenant_id(cluster_uri) or os.environ.get("DEFENDER_TENANT_ID", "")
    cookies = _get_cookies()
    xsrf_token = os.environ.get("DEFENDER_XSRF_TOKEN", "")

    headers: dict[str, str] = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "tenant-id": tenant_id,
        "x-tid": tenant_id,
    }
    if xsrf_token:
        headers["x-xsrf-token"] = xsrf_token

    body: dict[str, Any] = {
        "QueryText": query,
        "EncodedQueryText": query,
        "StartTime": None,
        "EndTime": None,
        "MaxRecordCount": None,
        "TenantIds": None,
        "tenantIds": None,
        "selectedWorkspaces": {},
    }

    logger.info(f"Defender query: {query[:100]}...")
    resp = requests.post(DEFENDER_QUERY_URL, headers=headers, cookies=cookies, json=body, timeout=120)

    if resp.status_code == 401 or resp.status_code == 403:
        raise RuntimeError(
            f"Defender auth failed ({resp.status_code}). Cookie likely expired. "
            "Refresh DEFENDER_SCCAUTH from browser DevTools."
        )
    resp.raise_for_status()

    data = resp.json()
    return _format_response(data)


def _format_response(data: dict[str, Any]) -> dict[str, Any]:
    """Convert Defender portal response to match KustoFormatter output format."""
    results = data.get("Results", [])

    if not results:
        return {"format": "json", "data": []}

    # Results are already list-of-dicts from the portal API
    # Just clean up the column names to match schema if needed
    return {"format": "json", "data": results}


def defender_list_tables(cluster_uri: str) -> list[dict[str, str]]:
    """List available Defender tables by querying the schema endpoint."""
    try:
        result = defender_query("search * | take 0 | getschema", cluster_uri)
        tables: set[str] = set()
        for row in result.get("data", []):
            table = row.get("TableName") or row.get("tableName")
            if table:
                tables.add(table)
        return [{"TableName": t, "EntityType": "Table"} for t in sorted(tables)]
    except Exception as e:
        logger.warning(f"Failed to list Defender tables dynamically: {e}")
        # Fall back to static schema
        from fabric_rti_mcp.services.kusto.kusto_schema_provider import get_static_table_names

        return [{"TableName": t, "EntityType": "Table"} for t in get_static_table_names()]
