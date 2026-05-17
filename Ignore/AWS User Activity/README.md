# AWS User Activity Dashboard

Streamlit app that visualizes AWS user activity from Microsoft Defender
Advanced Hunting. The app calls the same `run_hunting_query` function the
Defender Advanced Hunting MCP server uses — so it inherits the MCP
server's Graph credential chain, base URL, scope, and endpoint overrides.
See [AnalyticsPlan.md](./AnalyticsPlan.md) for the analytical design.

## Status

Phase 1 (Health & Volume): ✅ scaffold + Row 1 tiles + top-principals
table. Workload-role allowlist seeded from a 7d probe of `AWSCloudTrail`.

## How auth works

The app imports `defender_ah_mcp.services.hunting.hunting_service.run_hunting_query`
directly (same module the MCP server's tools call). On startup,
`utils/env_bootstrap.py` copies the `DEFENDER_GRAPH_*`, `HUNTING_*`, and
`AH_MODE` env vars from `~/.copilot/mcp-config.json` (server name
`defender-ah-mcp-vh-dev-zava-corp`) into the current process, so the
in-process hunting service uses the exact same Graph endpoint and token
acquisition path as the MCP server. No separate Entra app, no MSAL flow,
no manual token plumbing.

Override server name or config path by editing `utils/env_bootstrap.py`
(`DEFAULT_MCP_CONFIG_PATH`, `DEFAULT_SERVER_NAME`), or pre-set the env
vars yourself before launching Streamlit.

## Run

```bash
pip install -r requirements.txt
streamlit run "Ignore/AWS User Activity/app/streamlit_app.py"
```

Open <http://localhost:8501>. The sidebar's "Graph endpoint (debug)"
expander shows which env vars were picked up from the MCP config.

## Layout

```
.
├── AnalyticsPlan.md           # design doc — source of truth
├── README.md
├── requirements.txt
└── app/
    ├── __init__.py
    ├── streamlit_app.py       # entry point
    ├── workload_allowlist.yaml
    ├── data/
    │   ├── client.py          # wraps run_hunting_query, returns DataFrame
    │   └── queries.py         # Phase 1 KQL queries
    └── utils/
        ├── env_bootstrap.py   # copies MCP server env into os.environ
        └── constants.py       # time windows, allowlist loader
```
