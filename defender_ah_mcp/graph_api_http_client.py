import asyncio
import os
import time
from collections.abc import Coroutine
from typing import Any, cast

import httpx
from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import ChainedTokenCredential, ClientSecretCredential, DefaultAzureCredential

from defender_ah_mcp.config import logger

GRAPH_API_BASE_URL_DEFAULT = "https://graph.microsoft.com/v1.0"
GRAPH_TOKEN_SCOPE_DEFAULT = "https://graph.microsoft.com/.default"


class GraphEnvVarNames:
    tenant_id = "DEFENDER_GRAPH_TENANT_ID"
    client_id = "DEFENDER_GRAPH_CLIENT_ID"
    client_secret = "DEFENDER_GRAPH_CLIENT_SECRET"
    api_base_url = "DEFENDER_GRAPH_API_BASE_URL"
    token_scope = "DEFENDER_GRAPH_TOKEN_SCOPE"
    auth_prefer_default = "DEFENDER_GRAPH_AUTH_PREFER_DEFAULT"
    access_token = "DEFENDER_GRAPH_ACCESS_TOKEN"


class _StaticTokenCredential(TokenCredential):
    """Returns a pre-acquired bearer token, mimicking pass-through scenarios.

    Useful for local development when the host has already obtained a Graph
    token (e.g. via `az account get-access-token`) and wants to inject it
    into the container without exposing client secrets.
    """

    def __init__(self, token: str, expires_on: int | None = None) -> None:
        # Default 50-minute lifetime — Graph tokens are usually ~1h.
        self._token = AccessToken(token, expires_on or int(time.time()) + 50 * 60)

    def get_token(self, *scopes: str, **_: Any) -> AccessToken:  # noqa: D401
        return self._token


class GraphAPIHttpClient:
    """
    Azure Identity-based HTTP client for Microsoft Graph APIs.
    Handles authentication transparently using Azure credential providers.

    Supports two authentication modes:
    - Client credentials (app-only): set DEFENDER_GRAPH_TENANT_ID, DEFENDER_GRAPH_CLIENT_ID,
      and DEFENDER_GRAPH_CLIENT_SECRET environment variables.
    - Default Azure credential: falls back to az login / managed identity / etc.
    """

    def __init__(self, api_base_url: str | None = None):
        self.api_base_url = (
            api_base_url or os.getenv(GraphEnvVarNames.api_base_url, GRAPH_API_BASE_URL_DEFAULT)
        ).rstrip("/")
        self.credential = self._get_credential()
        self.token_scope = os.getenv(GraphEnvVarNames.token_scope, GRAPH_TOKEN_SCOPE_DEFAULT)

    def _get_credential(
        self,
    ) -> TokenCredential:
        access_token = os.getenv(GraphEnvVarNames.access_token)
        if access_token:
            logger.info(
                "Using pre-acquired access token from DEFENDER_GRAPH_ACCESS_TOKEN "
                "(mimicking bearer pass-through)"
            )
            return _StaticTokenCredential(access_token)

        tenant_id = os.getenv(GraphEnvVarNames.tenant_id)
        client_id = os.getenv(GraphEnvVarNames.client_id)
        client_secret = os.getenv(GraphEnvVarNames.client_secret)
        prefer_default = os.getenv(GraphEnvVarNames.auth_prefer_default, "").lower() in ("true", "1", "yes")

        has_client_creds = bool(tenant_id and client_id and client_secret)
        default_cred = DefaultAzureCredential(
            exclude_shared_token_cache_credential=True,
            exclude_interactive_browser_credential=False,
        )

        if prefer_default and has_client_creds:
            # Try az login / managed identity first, fall back to app registration
            logger.info("Using ChainedTokenCredential: DefaultAzureCredential → ClientSecretCredential")
            app_cred = ClientSecretCredential(
                tenant_id=tenant_id,  # type: ignore[arg-type]
                client_id=client_id,  # type: ignore[arg-type]
                client_secret=client_secret,  # type: ignore[arg-type]
            )
            return ChainedTokenCredential(default_cred, app_cred)

        if has_client_creds:
            logger.info("Using client credentials for Graph API authentication")
            return ClientSecretCredential(
                tenant_id=tenant_id,  # type: ignore[arg-type]
                client_id=client_id,  # type: ignore[arg-type]
                client_secret=client_secret,  # type: ignore[arg-type]
            )

        logger.info("Using DefaultAzureCredential for Graph API authentication")
        return default_cred

    def _get_access_token(self) -> str:
        try:
            token = self.credential.get_token(self.token_scope)
            if not token:
                raise Exception("Failed to acquire token from Azure credential")
            logger.debug(f"Successfully acquired Graph API token (expires: {token.expires_on})")
            return token.token
        except Exception as e:
            logger.error(f"Failed to get Graph API access token: {e}")
            raise Exception(f"Authentication failed: {e}")

    def _get_headers(self, extra_headers: dict[str, str] | None = None) -> dict[str, str]:
        access_token = self._get_access_token()
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if extra_headers:
            headers.update(extra_headers)
        return headers

    def _run_async_operation(self, coro: Coroutine[Any, Any, Any]) -> Any:
        try:
            asyncio.get_running_loop()
            import concurrent.futures

            def run_in_thread() -> Any:
                new_loop = asyncio.new_event_loop()
                asyncio.set_event_loop(new_loop)
                try:
                    return new_loop.run_until_complete(coro)
                finally:
                    new_loop.close()

            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_in_thread)
                return future.result()

        except RuntimeError:
            return asyncio.run(coro)

    async def make_request_async(
        self,
        method: str,
        endpoint: str,
        payload: dict[str, Any] | None = None,
        timeout: int = 30,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        url = f"{self.api_base_url}{endpoint}"
        headers = self._get_headers(extra_headers)

        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                if method.upper() == "GET":
                    response = await client.get(url, headers=headers)
                elif method.upper() == "POST":
                    response = await client.post(url, json=payload, headers=headers)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                if response.status_code >= 400:
                    error_detail = response.text
                    logger.error(f"Graph API error {response.status_code}: {error_detail}")
                    return {"error": True, "status_code": response.status_code, "detail": error_detail}

                if response.status_code == 204:
                    return {"success": True, "message": "Operation completed successfully"}

                try:
                    return cast(dict[str, Any], response.json())
                except Exception:
                    return {"success": True, "message": response.text}

        except Exception as e:
            logger.error(f"Error making Graph API request: {e}")
            return {"error": True, "message": str(e)}

    def make_request(
        self,
        method: str,
        endpoint: str,
        payload: dict[str, Any] | None = None,
        timeout: int = 30,
        extra_headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            self._run_async_operation(self.make_request_async(method, endpoint, payload, timeout, extra_headers)),
        )


class GraphHttpClientCache:
    """Connection cache for Graph API clients using Azure Identity."""

    _connection: GraphAPIHttpClient | None = None

    @classmethod
    def get_client(cls) -> GraphAPIHttpClient:
        if cls._connection is None:
            cls._connection = GraphAPIHttpClient()
            logger.info(f"Created Graph API connection for: {cls._connection.api_base_url}")
        return cls._connection
