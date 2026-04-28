# Schema Catalog — Usage Guide

The Schema Catalog service exposes a set of read-only tools that help an LLM reliably generate KQL for product schemas (e.g. Microsoft Defender XDR Advanced Hunting) without hallucinating table or column names.

## Configuration

Set the `SCHEMA_CATALOG_PATH` environment variable to point to a schema JSON file:

```bash
export SCHEMA_CATALOG_PATH=/path/to/DefenderSchema.json
```

If `SCHEMA_CATALOG_PATH` is not set, the tools are not registered and the service is silently skipped.

## Schema file format

The JSON file must follow this structure:

```json
{
  "PackName": "defender_xdr_advanced_hunting",
  "GeneratedAtUtc": "2026-04-28T09:54:45Z",
  "Aliases": {
    "EntraIdSignInEvents": ["logon", "signin"]
  },
  "Schema": {
    "GroupName": {
      "TableName": [
        {"Name": "ColumnName", "Type": "string", "Description": "..."}
      ]
    }
  }
}
```

- `PackName` — optional. If omitted, the pack name is derived from the filename.
- `Aliases` — optional per-table aliases that improve search recall (e.g. mapping "signin" to `EntraIdSignInEvents`).
- `GeneratedAtUtc` — optional timestamp for schema versioning.

## Available tools

| Tool | Description |
|---|---|
| `schema_list_packs` | List available schema packs and their metadata |
| `schema_list_tables` | List all table names in a pack |
| `schema_get_table` | Get the full column schema for one table |
| `schema_search` | Find the most relevant tables/columns for a natural-language prompt |

## Recommended agent workflow for Advanced Hunting KQL

The recommended pattern for an agent generating Defender XDR Advanced Hunting KQL:

1. **Discover packs**: call `schema_list_packs` to get the available pack name (e.g. `defender_xdr_advanced_hunting`).

2. **Search for relevant tables**: call `schema_search` with the user's request:
   ```
   schema_search(pack="defender_xdr_advanced_hunting", prompt="impossible travel for user sign-ins")
   ```
   The tool returns the top matching tables and their most relevant columns.

3. **Write KQL using only returned tables/columns.** Do not invent column names.

4. **Validate if needed**: if a column you need was not included in `schema_search` results, call `schema_get_table` to get the full column list for that specific table.

### Example system prompt addition

Add the following instruction to your system prompt when Advanced Hunting KQL generation is expected:

> Before writing any Advanced Hunting KQL query, call `schema_search` with the user's request to identify the correct tables and columns.
> Only use tables and columns returned by the schema tools — never invent schema elements.
> If a needed column is not in the `schema_search` results, call `schema_get_table` for the specific table to get its full column list.
