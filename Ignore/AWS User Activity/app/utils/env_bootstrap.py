from __future__ import annotations

import json
import os
from pathlib import Path

DEFAULT_MCP_CONFIG_PATH = Path.home() / ".copilot" / "mcp-config.json"
DEFAULT_SERVER_NAME = "defender-ah-mcp-vh-dev-zava-corp"

RELEVANT_PREFIXES = ("DEFENDER_GRAPH_", "HUNTING_", "AH_MODE")


def load_mcp_env(
    config_path: Path = DEFAULT_MCP_CONFIG_PATH,
    server_name: str = DEFAULT_SERVER_NAME,
    override: bool = False,
) -> dict[str, str]:
    """
    Copy the named MCP server's env vars (DEFENDER_GRAPH_*, HUNTING_*,
    AH_MODE) into os.environ so the in-process hunting service inherits the
    same Graph base URL / scope / endpoint overrides the MCP server uses.

    Silent no-op if the config file or server entry is missing.
    """
    if not config_path.exists():
        return {}
    try:
        cfg = json.loads(config_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    server = (cfg.get("mcpServers") or {}).get(server_name)
    if not server:
        return {}
    env = server.get("env") or {}
    applied: dict[str, str] = {}
    for key, value in env.items():
        if not isinstance(value, str):
            continue
        if not key.startswith(RELEVANT_PREFIXES):
            continue
        if override or key not in os.environ:
            os.environ[key] = value
            applied[key] = value
    return applied
