# KQL Schema Discovery — Advanced Hunting (AH) Mode

> **When to use this file**: When `KUSTO_AH_MODE=true` (the default), the MCP server is targeting
> Sentinel / Log Analytics / Microsoft Defender Advanced Hunting. Management commands (`.show …`)
> are not supported in those environments. Use the pure-KQL patterns below instead.
>
> For native ADX / Fabric Eventhouse clusters, see `discovery-queries.md`.

---

## Table Discovery

```kql
// List all tables and their approximate row counts
union withsource=TableName *
| summarize RowCount=count() by TableName
| order by RowCount desc

// Quick check — does a table exist and how large is it?
DeviceFileEvents
| count

// Sample rows to understand shape
DeviceFileEvents
| take 10
```

---

## Column Discovery

```kql
// Get all column names and types for a table  ← primary alternative to .show table T cslschema
DeviceFileEvents
| getschema

// Same but show only names and types
DeviceFileEvents
| getschema
| project ColumnName, ColumnType

// Quick column peek via a single row (shows actual values alongside names)
DeviceFileEvents
| take 1

// Column cardinality profiling (pick the columns you care about)
DeviceFileEvents
| summarize
    Rows         = count(),
    DeviceCount  = dcount(DeviceName),
    ActionTypes  = dcount(ActionType),
    Earliest     = min(Timestamp),
    Latest       = max(Timestamp)
```

---

## Value Distribution

```kql
// Top values for a categorical column
DeviceFileEvents
| summarize count() by ActionType
| order by count_ desc

// Time range covered by a table
DeviceFileEvents
| summarize Min=min(Timestamp), Max=max(Timestamp)

// Null / empty check for a column
DeviceFileEvents
| summarize
    Total      = count(),
    NonNull    = countif(isnotempty(InitiatingProcessCommandLine)),
    NullOrEmpty = countif(isempty(InitiatingProcessCommandLine))
```

---

## Cross-Table Discovery

```kql
// Discover which tables contain a column named "AccountName"
search in (*) * | getschema | where ColumnName == "AccountName"

// Search across all tables for a value (use sparingly — scans everything)
search "suspicious.exe"
| take 100
| project $table, Timestamp, $raw

// Find tables that have data in a time window
union withsource=TableName *
| where Timestamp between (datetime(2024-01-01) .. datetime(2024-01-02))
| summarize count() by TableName
| order by count_ desc
```

---

## Function Discovery

> Stored functions are not available in Sentinel / Log Analytics workspaces.
> Use KQL `let` statements as inline function equivalents within a single query.

```kql
// Inline function equivalent using let
let extractDomain = (url: string) { extract(@"https?://([^/]+)", 1, url) };
DeviceNetworkEvents
| extend Domain = extractDomain(RemoteUrl)
| summarize count() by Domain
| top 10 by count_ desc
```

---

## What Is NOT Available in AH Mode

The following discovery patterns require management commands and **will not work** against
Sentinel / Log Analytics. The MCP will return an AH-mode notice instead of an error.

| Capability | Unavailable command | Workaround |
|---|---|---|
| Table schema (CSL format) | `.show table T cslschema` | `T \| getschema` |
| List all tables | `.show tables` | `union withsource=T * \| summarize count() by T` |
| List functions | `.show functions` | Not available; use `let` |
| Materialized views | `.show materialized-views` | Not available in Sentinel |
| Policies (retention, caching) | `.show table T policy retention` | Not available |
| Ingestion failures | `.show ingestion failures` | Not available |
| Cluster capacity | `.show capacity` | Not available |
| Cluster diagnostics | `.show diagnostics` | Not available |
| Workload groups | `.show workload_groups` | Not available |
| Ingestion mappings | `.show table T ingestion mappings` | Not available |
| Graph models | `.show graph_models` | Not available |

---

## AH Mode Exploration Workflow

When encountering a new Sentinel / Log Analytics workspace:

```kql
// Step 1 — discover what tables exist and their size
union withsource=TableName *
| summarize RowCount=count() by TableName
| order by RowCount desc

// Step 2 — understand a table's schema
DeviceFileEvents | getschema

// Step 3 — sample rows to see actual data shape
DeviceFileEvents | take 10

// Step 4 — understand time coverage
DeviceFileEvents
| summarize Min=min(Timestamp), Max=max(Timestamp)

// Step 5 — profile key columns
DeviceFileEvents
| summarize count() by ActionType
| order by count_ desc
```
