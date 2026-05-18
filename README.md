## 🎯 Overview

A Model Context Protocol (MCP) server for **Microsoft 365 Defender Advanced Hunting**. It exposes the [Advanced Hunting API](https://learn.microsoft.com/en-us/graph/api/security-security-runhuntingquery) (via Microsoft Graph Security) as MCP tools that agents can call to investigate threats across devices, identities, email, and cloud apps.

> [!NOTE]
> This project was forked from the Defender Advanced Hunting MCP server and stripped down to focus exclusively on Advanced Hunting / Vibe Hunting workflows.

### 🔍 How It Works

- 🔄 **MCP Protocol**: Exposes Advanced Hunting capabilities as MCP tools
- 🏗️ **Natural Language to KQL**: AI agents can translate prompts into Defender hunting queries
- 💡 **Secure Authentication**: Uses Azure Identity (DefaultAzureCredential or bearer-token HTTP auth)
- 🧠 **Enrichment**: Optional Vibe Hunting mode adds device/user insights, follow-up suggestions, and result analysis

### ✨ Modes

The server registers tools according to the `AH_MODE` environment variable (parsed case-insensitively):

| `AH_MODE` value                          | Tools registered                |
| ---------------------------------------- | ------------------------------- |
| unset / empty / `AdvancedHunting`        | Core hunting tools only         |
| `VibeHunting` (or legacy `true` / `1`)   | Core hunting + enrichment tools |

Unknown values are ignored with a warning and fall back to "core only".

#### Core Advanced Hunting tools
- **`run_hunting_query`** — Execute a KQL hunting query against Defender data
- **`validate_hunting_query`** — Validate KQL without running it (uses `| getschema`)
- **`get_hunting_schema`** — List all Advanced Hunting tables grouped by section
- **`get_table_schema`** — Get column details for a specific table

#### Vibe Hunting enrichment tools
- **`get_device_insights`** — Enrich device IDs with general info, vulnerabilities, logon history, and alert evidence
- **`get_user_insights`** — Enrich user identifiers (UPN/SID) with identity info, logons, alerts, and risk events
- **`analyze_hunting_results`** — Deterministic heuristics on result sets (chart hints, column stats, filter suggestions)
- **`summarize_hunting_results`** — Natural-language summary of results via MCP sampling
- **`suggest_hunting_followups`** — Pivot/follow-up KQL suggestions based on tables and columns used
- **`get_available_hunting_actions`** — Map result columns to applicable Defender response actions

### 🔍 Example Prompts

- "Find processes launched by PowerShell in the last 7 days"
- "Show me failed sign-in attempts across identities"
- "Hunt for suspicious email attachments in the last 30 days"
- "List devices with unsigned process executions"
- "Enrich the top 5 devices from the previous result and suggest follow-ups"

## Getting Started

### Prerequisites
1. Python 3.10+
2. An account with `ThreatHunting.Read.All` permission on Microsoft Graph
3. [`uv`](https://docs.astral.sh/uv/getting-started/installation/) (recommended)
4. [`az` CLI](https://learn.microsoft.com/cli/azure/install-azure-cli) (for `DefaultAzureCredential` login)

### Install from source

Clone this fork and check out the `advanced-hunter-demo` branch (the working branch for this server):

**macOS / Linux / WSL:**
```bash
git clone --branch advanced-hunter-demo https://github.com/oferInbar/fabric-rti-mcp.git
cd fabric-rti-mcp
uv sync          # or: pip install -e ".[dev]"
```

**Windows (PowerShell):**
```powershell
git clone --branch advanced-hunter-demo https://github.com/oferInbar/fabric-rti-mcp.git
cd fabric-rti-mcp
uv sync          # or: pip install -e ".[dev]"
```

> Stay on `advanced-hunter-demo` — `main` tracks the upstream Microsoft fabric-rti-mcp repo and does not contain the Advanced Hunting server.

### MCP client configuration (stdio)

Set `--directory` to the absolute path of your clone. Use forward slashes on macOS/Linux and either forward slashes or escaped backslashes on Windows.

**macOS / Linux:**
```json
{
  "mcp": {
    "servers": {
      "defender-hunting": {
        "command": "uv",
        "args": ["run", "--directory", "/path/to/fabric-rti-mcp", "defender-advanced-hunting-mcp"],
        "env": {
          "AH_MODE": "VibeHunting"
        }
      }
    }
  }
}
```

**Windows:**
```json
{
  "mcp": {
    "servers": {
      "defender-hunting": {
        "command": "uv",
        "args": ["run", "--directory", "C:/Users/you/code/fabric-rti-mcp", "defender-advanced-hunting-mcp"],
        "env": {
          "AH_MODE": "VibeHunting"
        }
      }
    }
  }
}
```

The server itself is pure Python and runs on macOS, Linux, and Windows. Only path syntax differs between platforms.

## ⚙️ Configuration

### Advanced Hunting tools

| Variable | Description | Default |
|----------|-------------|---------|
| `AH_MODE` | Tool selection. `AdvancedHunting` (or unset) → core only; `VibeHunting` → core + enrichment. Legacy `true`/`1` maps to `VibeHunting`. Parsed case-insensitively; unknown values warn and fall back to core only. | unset (core only) |
| `HUNTING_ENDPOINT` | Override the Graph Security hunting endpoint path | `/security/runHuntingQuery` |
| `HUNTING_SCHEMA_ENDPOINT` | Override the schema discovery endpoint path | `/security/runHuntingQuery/schema` |
| `HUNTING_QUERY_FIELD_NAME` | Override the request payload field name for the query | `Query` |
| `HUNTING_TIMESPAN_FIELD_NAME` | Override the request payload field name for the timespan | `Timespan` |
| `HUNTING_START_TIME_FIELD_NAME` | Override the request payload field name for `startTime` | `startTime` |
| `HUNTING_END_TIME_FIELD_NAME` | Override the request payload field name for `endTime` | `endTime` |

### Microsoft Graph authentication

The server uses `azure-identity` to obtain Graph tokens. Either app-only client credentials or `DefaultAzureCredential` can be used.

| Variable | Description | Default |
|----------|-------------|---------|
| `DEFENDER_GRAPH_TENANT_ID` | App-only: tenant ID | none |
| `DEFENDER_GRAPH_CLIENT_ID` | App-only: client ID | none |
| `DEFENDER_GRAPH_CLIENT_SECRET` | App-only: client secret | none |
| `DEFENDER_GRAPH_API_BASE_URL` | Override Graph base URL | `https://graph.microsoft.com/v1.0` |
| `DEFENDER_GRAPH_TOKEN_SCOPE` | Override token scope | `https://graph.microsoft.com/.default` |
| `DEFENDER_GRAPH_AUTH_PREFER_DEFAULT` | Force `DefaultAzureCredential` even if client secrets are set | `false` |

If none of the client-credential variables are set, the server falls back to `DefaultAzureCredential` (Azure CLI, managed identity, environment, etc.).

### HTTP mode

| Variable | Description | Default |
|----------|-------------|---------|
| `DEFENDER_AH_TRANSPORT` | `stdio` or `http` | `stdio` |
| `DEFENDER_AH_HTTP_HOST` | Host to bind | `127.0.0.1` |
| `DEFENDER_AH_HTTP_PORT` | Port to bind (also honors `PORT` / `FUNCTIONS_CUSTOMHANDLER_PORT`) | `3000` |
| `DEFENDER_AH_HTTP_PATH` | MCP endpoint path | `/mcp` |
| `DEFENDER_AH_STATELESS_HTTP` | Enable stateless HTTP mode | `false` |
| `DEFENDER_AH_CORS_ORIGINS` | Allowed CORS origins | `*` |
| `DEFENDER_AH_AI_FOUNDRY_COMPATIBILITY_SCHEMA` | Simplify schemas for AI Foundry compatibility | `false` |
| `DEFENDER_AH_INSTRUCTIONS` | Override the default server instructions string | none |
| `DEFENDER_AH_DISABLE_AUTH` | Disable auth middleware (HTTP mode) | `false` |

### OBO Flow

When the MCP server sits behind a gateway (e.g., APIM) that forwards user tokens with a different audience, the server can perform an On-Behalf-Of token exchange:

| Variable | Description | Default |
|----------|-------------|---------|
| `USE_OBO_FLOW` | Enable OBO exchange | `false` |
| `DEFENDER_AH_AZURE_TENANT_ID` | Tenant ID | Microsoft tenant |
| `DEFENDER_AH_ENTRA_APP_CLIENT_ID` | Entra App client ID | none |
| `DEFENDER_AH_USER_MANAGED_IDENTITY_CLIENT_ID` | UMI client ID for federated credential | none |
| `DEFENDER_AH_TOKEN_AUDIENCE` | Target audience for the exchanged token | `https://graph.microsoft.com/.default` |

## 🔑 Authentication

The server uses [Azure Identity](https://learn.microsoft.com/en-us/azure/developer/python/sdk/authentication/credential-chains?tabs=dac) for token acquisition. In HTTP mode, clients send a bearer token in the `Authorization` header; the middleware stores it in a request-scoped context for downstream Graph calls (and optionally performs OBO exchange).

## 🐛 Local debugging

```bash
uv sync                                     # or: pip install -e ".[dev]"
uv run defender-advanced-hunting-mcp        # stdio transport (default)
```

For HTTP transport, set `DEFENDER_AH_TRANSPORT=http` before running. The same commands work on macOS, Linux, and Windows (PowerShell or cmd).

## 🐳 Docker

A `Dockerfile` is included. The image runs the server in **HTTP transport** mode (stdio doesn't fit a container deployment model since the MCP client launches the process directly).

### Build

```bash
docker build -t defender-ah-mcp .
```

### One-shot helper script

```bash
./scripts/run-docker.sh                          # build + run with .env
./scripts/run-docker.sh --tenant <tenant-id>     # az login first, then build + run
./scripts/run-docker.sh --no-build --port 8080   # skip build, expose on 8080
```

The script verifies Docker, checks for `.env`, optionally runs `az login` against a tenant (and caches a Graph token for `DefaultAzureCredential`), builds the image, and starts the container.

### Run

```bash
docker run --rm -p 3000:3000 \
  -e AH_MODE=VibeHunting \
  -e DEFENDER_GRAPH_TENANT_ID=<tenant> \
  -e DEFENDER_GRAPH_CLIENT_ID=<client-id> \
  -e DEFENDER_GRAPH_CLIENT_SECRET=<client-secret> \
  defender-ah-mcp
```

The MCP endpoint is then available at `http://localhost:3000/mcp`.

### Auth options inside the container

- **App-only (recommended for containers)** — set `DEFENDER_GRAPH_TENANT_ID`, `DEFENDER_GRAPH_CLIENT_ID`, `DEFENDER_GRAPH_CLIENT_SECRET`.
- **Bearer pass-through** — clients send `Authorization: Bearer <token>` to `/mcp`; the middleware forwards it to Graph (optionally via OBO if `USE_OBO_FLOW=true`).
- **Managed identity** — when running on Azure (ACA, App Service, AKS with workload identity), no secrets are needed; `DefaultAzureCredential` picks up the assigned identity. Set `DEFENDER_AH_AUTH_PREFER_DEFAULT=true` if app-only env vars are also present.
- **Azure CLI / interactive login** — not supported inside the container; use one of the above instead.

> Don't bake credentials into the image. Pass them via `-e`, `--env-file`, or your orchestrator's secret store.

### MCP client config (HTTP)

```json
{
  "mcp": {
    "servers": {
      "defender-hunting": {
        "url": "http://localhost:3000/mcp"
      }
    }
  }
}
```

## 🧪 Tests

```bash
make precommit   # ruff format + ruff check + ty + pytest
```

## 🛡️ Security Note

Credentials are handled by the official Azure Identity SDK — tokens are never stored or persisted by this server.

## 👥 Contributing

This project welcomes contributions and suggestions. Most contributions require you to agree to a Contributor License Agreement (CLA). For details, visit https://cla.opensource.microsoft.com.

## 🤝 Code of Conduct

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com).

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft trademarks or logos is subject to and must follow [Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general). Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship. Any use of third-party trademarks or logos are subject to those third-party's policies.
