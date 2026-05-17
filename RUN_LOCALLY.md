# Running the Defender AH MCP Server Locally (VS Code)

Quick guide for running this MCP server from a local clone of the
[`oferInbar/fabric-rti-mcp`](https://github.com/oferInbar/fabric-rti-mcp) branch
inside VS Code (Copilot / MCP-enabled extensions).

## 1. Prerequisites

- macOS / Linux / WSL
- [Python 3.10+](https://www.python.org/)
- [`uv`](https://docs.astral.sh/uv/) (`brew install uv`)
- [`az` CLI](https://learn.microsoft.com/cli/azure/install-azure-cli)
- VS Code with an MCP-aware extension (e.g. GitHub Copilot Chat)

## 2. Clone the branch

```bash
git clone https://github.com/oferInbar/fabric-rti-mcp.git
cd fabric-rti-mcp
git checkout removing-kusto-legacy-skills   # or your branch of choice
uv sync
```

## 3. Sign in to the ZAVA-CORP tenant

The server uses `DefaultAzureCredential` and requires an interactive Azure login
against the **ZAVA-CORP** tenant:

```bash
az login --tenant 0527ecb7-06fb-4769-b324-fd4a3bb865eb
```

Verify:

```bash
az account show --query '{tenantId:tenantId, user:user.name}'
```

The `tenantId` should be `0527ecb7-06fb-4769-b324-fd4a3bb865eb`.

## 4. Recommended MCP config

Add the following entry to your MCP config file
(e.g. `~/.copilot/mcp-config.json` or VS Code `mcp.json`).
Update `--directory` to match the absolute path of your local clone.

```json
{
  "defender-ah-mcp-vh-dev-zava-corp": {
    "command": "uv",
    "args": [
      "run",
      "--directory",
      "/Users/oferinbar/Documents/Gits/fabric-rti-mcp",
      "defender-advanced-hunting-mcp"
    ],
    "env": {
      "DEFENDER_GRAPH_TENANT_ID": "0527ecb7-06fb-4769-b324-fd4a3bb865eb",
      "DEFENDER_GRAPH_API_BASE_URL": "https://partnersgw.securitycenter.windows.com/api/mdgw/gaia/medeinaapi",
      "DEFENDER_GRAPH_TOKEN_SCOPE": "https://securitycenter.microsoft.com/mtp/.default",
      "DEFENDER_GRAPH_AUTH_PREFER_DEFAULT": "true",
      "AH_MODE": "VibeHunting",
      "HUNTING_QUERY_FIELD_NAME": "queryText",
      "HUNTING_ENDPOINT": "/hunting/RunHuntingQuery",
      "HUNTING_SCHEMA_ENDPOINT": "/hunting/GetHuntingSchema",
      "KUSTO_SERVICE_URI": "https://asiusagetelemetryprod.eastus.kusto.windows.net/",
      "KUSTO_SERVICE_DEFAULT_DB": "ReportingProd",
      "KUSTO_SHOTS_TABLE": "CommonKnowledge",
      "AZ_OPENAI_EMBEDDING_ENDPOINT": "https://onesocoptimize.openai.azure.com/openai/deployments/text-embedding-ada-002/embeddings?api-version=2023-05-15;impersonate"
    }
  }
}
```

## 5. Launch in VS Code

1. Reload VS Code (or restart the MCP client) so the new server is picked up.
2. Open the MCP / Copilot tools panel — `defender-ah-mcp-vh-dev-zava-corp` should appear as available.
3. Try a hunting query through the agent to confirm the auth + endpoint are working.

## 6. Troubleshooting

- **401 / token errors** → re-run `az login --tenant 0527ecb7-06fb-4769-b324-fd4a3bb865eb`.
- **Server not appearing** → check VS Code MCP logs; verify the `--directory` path and that `uv run defender-advanced-hunting-mcp` works from a terminal.
- **Wrong tenant** → `az account set --subscription <sub-in-ZAVA-CORP>` after login.
