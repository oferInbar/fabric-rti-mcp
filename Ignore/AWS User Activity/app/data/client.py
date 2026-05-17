"""Advanced Hunting client — wraps the MCP server's run_hunting_query so
the Streamlit app uses the exact same Graph call path (and credentials)
as the MCP tools.

`query_many` fans the per-row queries out across a thread pool so the
dashboard renders in one round-trip of wall-clock time rather than 10+.
The underlying `query` is `@st.cache_data`-wrapped, so cached results
short-circuit before the thread pool is dispatched (Streamlit's
cache_data is thread-safe).
"""

from __future__ import annotations

import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

_REPO_ROOT = Path(__file__).resolve().parents[4]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from defender_ah_mcp.services.hunting.hunting_service import run_hunting_query  # noqa: E402

from utils.constants import CACHE_TTL_SECONDS  # noqa: E402

_MAX_PARALLEL_QUERIES = 6
_RETRY_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_MAX_ATTEMPTS = 3
_RETRY_BASE_DELAY_SECONDS = 1.5

logger = logging.getLogger("aws_user_activity.client")
if not logger.handlers:
    logger.setLevel(logging.INFO)


def _results_to_dataframe(payload: dict[str, Any]) -> pd.DataFrame:
    results = payload.get("Results") or payload.get("results") or []
    if not results:
        schema = payload.get("Schema") or payload.get("schema") or []
        cols = [c.get("Name") or c.get("name") for c in schema]
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(results)
    for col in df.columns:
        if "Time" in col or col.endswith("Seen"):
            try:
                df[col] = pd.to_datetime(df[col], utc=True)
            except (ValueError, TypeError):
                pass
    return df


def _extract_error_message(payload: dict[str, Any]) -> str:
    """Pull the most informative text we can out of an error payload."""
    detail = payload.get("detail")
    if detail:
        try:
            parsed = json.loads(detail) if isinstance(detail, str) else detail
            if isinstance(parsed, dict):
                err = parsed.get("error")
                if isinstance(err, dict):
                    msg = err.get("message") or err.get("Message")
                    code = err.get("code") or err.get("Code")
                    if msg:
                        return f"{code}: {msg}" if code else str(msg)
                if parsed.get("ErrorMessage"):
                    return str(parsed["ErrorMessage"])
        except (json.JSONDecodeError, TypeError):
            pass
        if isinstance(detail, str) and detail.strip() not in ("", "{}"):
            return detail
    msg = payload.get("message")
    if msg:
        return str(msg)
    return ""


def _is_retryable(payload: dict[str, Any]) -> bool:
    if not isinstance(payload, dict) or not payload.get("error"):
        return False
    status = payload.get("status_code")
    if isinstance(status, int) and status in _RETRY_STATUS_CODES:
        return True
    # Empty detail / empty message — gateway hiccup; worth one retry.
    detail = payload.get("detail")
    if detail in (None, "", "{}"):
        msg = payload.get("message")
        if msg in (None, ""):
            return True
    return False


class HuntingQueryError(RuntimeError):
    """Raised when a hunting query fails. Carries a structured payload for logging."""

    def __init__(self, payload: dict[str, Any], kql: str, attempts: int):
        self.payload = payload
        self.kql = kql
        self.attempts = attempts
        status = payload.get("status_code")
        msg = _extract_error_message(payload) or "(no error detail returned)"
        prefix = f"HTTP {status}" if status else "error"
        suffix = f" (after {attempts} attempt{'s' if attempts != 1 else ''})"
        super().__init__(f"{prefix}: {msg}{suffix}")


def _normalize_kql(kql: str) -> str:
    """Strip blank lines from a KQL query.

    The Advanced Hunting API treats blank lines as block delimiters and
    silently truncates the query at the first one — yielding HTTP 400
    with an empty `{}` body when downstream operators (`case`, `summarize
    … by Outcome`) reference identifiers that only existed in the
    truncated tail. Our query builders sometimes interpolate optional
    clauses (`_allowlist_filter_clause` returns `""` when
    `human_only=False`) which produces a stray blank line, so we
    normalize centrally here."""
    return "\n".join(line for line in kql.splitlines() if line.strip())


def _run_with_retry(kql: str, timespan: str, max_results: int) -> dict[str, Any]:
    """Call run_hunting_query, retrying on transient failures with
    exponential backoff + jitter.

    Defender Advanced Hunting sometimes returns HTTP 400 with an empty `{}`
    body when the tenant query queue is hot — `_is_retryable` treats that
    signature as retryable. With 3 batches firing in parallel we need ≥2
    retries to ride through a brief queue spike."""
    import random  # local import: only needed on the retry path

    kql = _normalize_kql(kql)
    last_payload: dict[str, Any] | None = None
    attempts_made = 0
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        attempts_made = attempt
        try:
            payload = run_hunting_query(query=kql, timespan=timespan, max_results=max_results)
        except Exception as exc:  # noqa: BLE001
            payload = {"error": True, "message": f"{type(exc).__name__}: {exc}"}

        if not (isinstance(payload, dict) and payload.get("error")):
            if attempt > 1:
                logger.info("Hunting query succeeded on retry (attempt %d).", attempt)
            return payload

        last_payload = payload
        retryable = _is_retryable(payload)
        logger.warning(
            "Hunting query failed (attempt %d/%d, retryable=%s): status=%s msg=%r kql=%r",
            attempt,
            _MAX_ATTEMPTS,
            retryable,
            payload.get("status_code"),
            _extract_error_message(payload),
            kql[:200] + ("…" if len(kql) > 200 else ""),
        )
        if attempt < _MAX_ATTEMPTS and retryable:
            # Exponential backoff (1.5s, 3s, …) plus 0–0.5s jitter so the
            # three render-order batches don't all retry in lockstep.
            delay = _RETRY_BASE_DELAY_SECONDS * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
            time.sleep(delay)
            continue
        break

    assert last_payload is not None
    raise HuntingQueryError(last_payload, kql, attempts=attempts_made)


@st.cache_data(ttl=CACHE_TTL_SECONDS, show_spinner=False)
def query(kql: str, timespan: str = "P7D", max_results: int = 5000) -> pd.DataFrame:
    payload = _run_with_retry(kql, timespan, max_results)
    return _results_to_dataframe(payload)


def query_many(
    jobs: dict[str, dict[str, Any]],
) -> dict[str, pd.DataFrame | Exception]:
    """Run several hunting queries in parallel.

    `jobs` maps an arbitrary name → kwargs for `query()`
    (must include `kql`; may include `timespan`, `max_results`).

    Returns a dict with the same keys. On per-job failure the value is
    the captured Exception (caller decides whether to surface it inline
    or stop the whole page).
    """
    if not jobs:
        return {}

    def _run(name: str, kw: dict[str, Any]) -> tuple[str, pd.DataFrame | Exception]:
        try:
            return name, query(**kw)
        except Exception as e:  # noqa: BLE001
            logger.error("Job %r failed: %s", name, e)
            return name, e

    results: dict[str, pd.DataFrame | Exception] = {}
    workers = min(_MAX_PARALLEL_QUERIES, len(jobs))
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="ah-query") as ex:
        futures = [ex.submit(_run, name, kw) for name, kw in jobs.items()]
        for fut in futures:
            name, value = fut.result()
            results[name] = value
    return results
