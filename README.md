## 🎯 Overview

A Model Context Protocol (MCP) server for **Microsoft 365 Defender Advanced Hunting**. It exposes the [Advanced Hunting API](https://learn.microsoft.com/en-us/graph/api/security-security-runhuntingquery) (via Microsoft Graph Security) as MCP tools that agents can call to investigate threats across devices, identities, email, and cloud apps.

> [!NOTE]
> This project was forked from the Microsoft Fabric RTI MCP server and stripped down to focus exclusively on Advanced Hunting / Vibe Hunting workflows.

### 🔍 How It Works

- 🔄 **MCP Protocol**: Exposes Advanced Hunting capabilities as MCP tools
- 🏗️ **Natural Language to KQL**: AI agents can translate prompts into Defender hunting queries
- 💡 **Secure Authentication**: Uses Azure Identity (DefaultAzureCredential or bearer-token HTTP auth)
- 🧠 **Enrichment**: Optional Vibe Hunting mode adds device/user insights, follow-up suggestions, and result analysis

### ✨ Modes

The server registers tools according to the `AH_MODE` environment variable:

| `AH_MODE` value         | Tools registered                                                                                 |
| ----------------------- | ------------------------------------------------------------------------------------------------ |
| unset / `AdvancedHunting` (default) | Core hunting tools only                                                              |
| `VibeHunting` (or legacy `true`/`1`) | Core hunting + enrichment tools                                                     |

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
3. `uv` (recommended) — see https://docs.astral.sh/uv/getting-started/installation/

### Install from source

```bash
git clone <this repo>
cd fabric-rti-mcp
pip install -e ".[dev]"
```

### MCP client configuration (stdio)

```json
{
  "mcp": {
    "servers": {
      "defender-hunting": {
        "command": "uv",
        "args": ["--directory", "/path/to/fabric-rti-mcp", "run", "-m", "fabric_rti_mcp.server"],
        "env": {
          "AH_MODE": "VibeHunting"
        }
      }
    }
  }
}
```

## ⚙️ Configuration

### Advanced Hunting tools

| Variable | Description | Default |
|----------|-------------|---------|
| `AH_MODE` | `AdvancedHunting` (core) or `VibeHunting` (core + enrichment). Legacy `true`/`1` maps to `VibeHunting`. | unset → core only |
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
| `FABRIC_GRAPH_TENANT_ID` | App-only: tenant ID | none |
| `FABRIC_GRAPH_CLIENT_ID` | App-only: client ID | none |
| `FABRIC_GRAPH_CLIENT_SECRET` | App-only: client secret | none |
| `FABRIC_GRAPH_API_BASE_URL` | Override Graph base URL | `https://graph.microsoft.com/v1.0` |
| `FABRIC_GRAPH_TOKEN_SCOPE` | Override token scope | `https://graph.microsoft.com/.default` |
| `FABRIC_GRAPH_AUTH_PREFER_DEFAULT` | Force `DefaultAzureCredential` even if client secrets are set | `false` |

If none of the client-credential variables are set, the server falls back to `DefaultAzureCredential` (Azure CLI, managed identity, environment, etc.).

### HTTP mode

| Variable | Description | Default |
|----------|-------------|---------|
| `FABRIC_RTI_TRANSPORT` | `stdio` or `http` | `stdio` |
| `FABRIC_RTI_HTTP_HOST` | Host to bind | `127.0.0.1` |
| `FABRIC_RTI_HTTP_PORT` | Port to bind (also honors `PORT` / `FUNCTIONS_CUSTOMHANDLER_PORT`) | `3000` |
| `FABRIC_RTI_HTTP_PATH` | MCP endpoint path | `/mcp` |
| `FABRIC_RTI_STATELESS_HTTP` | Enable stateless HTTP mode | `false` |
| `FABRIC_RTI_CORS_ORIGINS` | Allowed CORS origins | `*` |
| `FABRIC_RTI_AI_FOUNDRY_COMPATIBILITY_SCHEMA` | Simplify schemas for AI Foundry compatibility | `false` |
| `FABRIC_RTI_INSTRUCTIONS` | Override the default server instructions string | none |
| `FABRIC_RTI_DISABLE_AUTH` | Disable auth middleware (HTTP mode) | `false` |

### OBO Flow

When the MCP server sits behind a gateway (e.g., APIM) that forwards user tokens with a different audience, the server can perform an On-Behalf-Of token exchange:

| Variable | Description | Default |
|----------|-------------|---------|
| `USE_OBO_FLOW` | Enable OBO exchange | `false` |
| `FABRIC_RTI_MCP_AZURE_TENANT_ID` | Tenant ID | Microsoft tenant |
| `FABRIC_RTI_MCP_ENTRA_APP_CLIENT_ID` | Entra App client ID | none |
| `FABRIC_RTI_MCP_USER_MANAGED_IDENTITY_CLIENT_ID` | UMI client ID for federated credential | none |
| `FABRIC_RTI_MCP_TOKEN_AUDIENCE` | Target audience for the exchanged token | `https://graph.microsoft.com/.default` |
| `FABRIC_RTI_MCP_KUSTO_AUDIENCE` | **Deprecated** alias for `FABRIC_RTI_MCP_TOKEN_AUDIENCE` | none |

## 🔑 Authentication

The server uses [Azure Identity](https://learn.microsoft.com/en-us/azure/developer/python/sdk/authentication/credential-chains?tabs=dac) for token acquisition. In HTTP mode, clients send a bearer token in the `Authorization` header; the middleware stores it in a request-scoped context for downstream Graph calls (and optionally performs OBO exchange).

## 🐛 Local debugging

```bash
pip install -e ".[dev]"
python -m fabric_rti_mcp.server --stdio
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
