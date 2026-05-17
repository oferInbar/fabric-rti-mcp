# AWS Login Overview

A Streamlit app that visualizes AWS sign-in activity sourced from Microsoft 365
Defender Advanced Hunting (`AWSCloudTrail` table). It reuses the
`defender_ah_mcp` package for the Graph Security API auth and query path.

## What it does

- Pulls AWS sign-ins (synthetic logins: first event per user + IP + day) from
  the last 1 / 7 / 14 / 30 days.
- Plots them on an interactive world map, sized by sign-in volume and colored
  by per-user risk.
- Click a city → see users active there with risk scores.
- Click a user → side panel with identity, recent IPs, AWS regions, top events
  and linked Defender alerts.

## Risk score

`risk = max(heuristic_score, alert_score)` where:

- **heuristic_score** = weighted combination of:
  - impossible travel (consecutive sign-ins exceeding 900 km/h),
  - new countries (versus 30-day baseline),
  - new IPs (versus 30-day baseline).
- **alert_score** = severity-weighted score derived from `AlertEvidence`
  rows joined on `AccountUpn` / `AccountObjectId` / `AccountSid`.

AWS-owned source IPs and `aws-sdk-*` / `AWS Internal` user agents are
filtered out by default to suppress backend/service noise.

## Setup

From the repository root:

```bash
pip install -e .                                           # install defender_ah_mcp
pip install -r "Ignore/AWS login overview/requirements.txt"
```

### Credentials

The app reuses the MCP server's Graph auth (`GraphAPIHttpClient`). On startup
it will **auto-load** the env block of the `defender-ah-mcp-vh-dev-zava-corp`
server from `~/.copilot/mcp-config.json` (only `DEFENDER_GRAPH_*`, `HUNTING_*`,
and `AH_MODE` keys; existing env vars are not overridden). This means the
partner-gateway base URL and token scope used by the MCP server are picked up
automatically.

To override, set any of these in your shell before launching:

- `DEFENDER_GRAPH_TENANT_ID`
- `DEFENDER_GRAPH_CLIENT_ID` / `DEFENDER_GRAPH_CLIENT_SECRET` (optional;
  falls back to `DefaultAzureCredential` / `az login`)
- `DEFENDER_GRAPH_API_BASE_URL` (e.g. partner gateway URL)
- `DEFENDER_GRAPH_TOKEN_SCOPE`
- `HUNTING_ENDPOINT` / `HUNTING_SCHEMA_ENDPOINT` / `HUNTING_QUERY_FIELD_NAME`

The sidebar has a **Graph endpoint (debug)** expander that shows the active
values.

## Run

```bash
cd "Ignore/AWS login overview"
streamlit run app.py
```

## Notes

- The bundled AWS IP-range list in `utils/geo.py` is a static snapshot.
  For a full list, refresh from
  <https://ip-ranges.amazonaws.com/ip-ranges.json>.
- Results are cached in-memory for 5 minutes (`@st.cache_data`).
- Click **Refresh data** in the sidebar to clear caches.
