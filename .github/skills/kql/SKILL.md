---
name: kql
description: "KQL expertise for writing correct, efficient Microsoft 365 Defender Advanced Hunting queries via the Defender Advanced Hunting MCP tools. Covers syntax gotchas, join patterns, dynamic types, datetime pitfalls, regex patterns, serialization, and result-size discipline. USE THIS SKILL whenever writing, debugging, or reviewing Advanced Hunting KQL queries — even simple ones — because the gotchas section prevents the most common errors that waste tool calls and cause expensive retry cascades. Trigger on: KQL, Advanced Hunting, Defender, hunting query, DeviceProcessEvents, DeviceNetworkEvents, EmailEvents, IdentityLogonEvents, AlertEvidence, log analysis, anomaly detection, summarize, where clause, join, extend, project, let statement, parse operator, extract function, any mention of pipe-forward query syntax."
---

# KQL for Defender Advanced Hunting

This skill helps agents write correct, efficient KQL against the Microsoft 365 Defender Advanced
Hunting schema, exposed through the Defender Advanced Hunting MCP server.

## 1. Available MCP tools

| Tool | Purpose |
|------|---------|
| `run_hunting_query` | Execute a KQL hunting query against Defender data |
| `validate_hunting_query` | Validate a query (uses `\| getschema`) without returning rows |
| `get_hunting_schema` | List all Advanced Hunting tables grouped by section |
| `get_table_schema` | Get column details for a specific table |
| `get_device_insights` | Enrich device IDs with general info, vulnerabilities, logons, and alert evidence |
| `get_user_insights` | Enrich user identifiers (UPN/SID) with identity, logons, alerts, and risk events |
| `analyze_hunting_results` | Deterministic heuristics for visualization and column stats |
| `summarize_hunting_results` | LLM-backed natural-language summary of results |
| `suggest_hunting_followups` | Pivot/follow-up KQL suggestions |
| `get_available_hunting_actions` | Map result columns to applicable Defender response actions |

### Workflow

1. **Discover schema first**: `get_hunting_schema()` lists tables; `get_table_schema(table_name)` shows columns.
2. **Validate before running**: `validate_hunting_query(query)` cheaply checks syntax and table/column references.
3. **Run**: `run_hunting_query(query, timespan="P7D")`. Default timespan is 30 days.
4. **Enrich / pivot**: feed entity IDs into `get_device_insights` / `get_user_insights`, or call
   `suggest_hunting_followups` for next-step queries.

### Common Defender tables

| Section | Tables |
|---------|--------|
| Devices | `DeviceProcessEvents`, `DeviceNetworkEvents`, `DeviceFileEvents`, `DeviceRegistryEvents`, `DeviceLogonEvents`, `DeviceImageLoadEvents`, `DeviceEvents`, `DeviceInfo`, `DeviceTvmSoftwareVulnerabilities`, `DeviceTvmSoftwareVulnerabilitiesKB` |
| Email | `EmailEvents`, `EmailAttachmentInfo`, `EmailUrlInfo`, `EmailPostDeliveryEvents` |
| Identity | `IdentityLogonEvents`, `IdentityQueryEvents`, `IdentityDirectoryEvents`, `IdentityInfo`, `AADSignInEventsBeta`, `AADUserRiskEvents` |
| Cloud apps | `CloudAppEvents` |
| Alerts | `AlertInfo`, `AlertEvidence` |

## 2. Dynamic Type Discipline

KQL's `dynamic` type is flexible but strict in certain contexts. A common mistake is using a dynamic
column in `summarize by`, `order by`, or `join on` without casting.

**The rule**: Any time you use a dynamic column in `by`, `on`, or `order by`, cast it explicitly.

```kql
// ❌ ERROR: "Summarize group key 'CveTags' is of a 'dynamic' type"
DeviceTvmSoftwareVulnerabilities
| summarize count() by CveTags

// ✅ FIX
DeviceTvmSoftwareVulnerabilities
| summarize count() by tostring(CveTags)
```

When you see "is of a 'dynamic' type", reach for `tostring()`, `tolong()`, or `todouble()`.

## 3. Join Patterns & Pitfalls

### Equality only
Join conditions support **only `==`**. No `<`, `>`, `!=`, or function calls in join predicates.

```kql
// ❌ ERROR: "Only equality is allowed in this context"
| join on geo_distance_2points(a.Lat, a.Lon, b.Lat, b.Lon) < 1000

// ✅ WORKAROUND — pre-bucket into spatial cells / bins, then join on the bucket
| extend cell = geo_point_to_s2cell(Lon, Lat, 8)
| join kind=inner (other | extend cell = geo_point_to_s2cell(Lon, Lat, 8)) on cell
```

### Left/right attribute matching
Both sides of a join `on` clause must reference **column entities only** — not expressions, not aggregates.

```kql
// ❌ ERROR: "for each left attribute, right attribute should be selected"
| join kind=inner other on $left.DeviceId

// ✅ FIX — specify both sides explicitly
| join kind=inner other on $left.DeviceId == $right.DeviceId
```

### Cardinality check before large joins
Always check cardinality before joining tables with >10K rows. Cross-join explosions are the
biggest source of `E_RUNAWAY_QUERY` errors.

```kql
DeviceProcessEvents | summarize dcount(DeviceId)   // is this 25K? too big for an unconstrained join
DeviceNetworkEvents | summarize dcount(DeviceId)
```

## 4. Regex in KQL

KQL handles regex natively — no need to fall back to other tools.

### The `extract_all` gotcha
Unlike Python's `re.findall()`, KQL's `extract_all` **requires capturing groups** in the regex:

```kql
// ❌ ERROR: "extractall(): argument 2 must be a valid regex with [1..16] matching groups"
| extend words = extract_all(@"[a-zA-Z]{3,}", Text)

// ✅ FIX — add parentheses around the pattern
| extend words = extract_all(@"([a-zA-Z]{3,})", Text)
```

### Regex toolkit
| Function | Use case | Example |
|----------|----------|---------|
| `extract(regex, group, source)` | Single match | `extract(@"User '([^']+)'", 1, Msg)` |
| `extract_all(regex, source)` | All matches (needs `()`) | `extract_all(@"(\w+)", Text)` |
| `parse` | Structured extraction | `parse Msg with * "User '" Sender "' sent" *` |
| `matches regex` | Boolean filter | `where Url matches regex @"^https?://"` |
| `replace_regex` | Find and replace | `replace_regex(Text, @"\s+", " ")` |

## 5. Serialization Requirements

Window functions need serialized (ordered) input.

```kql
// ❌ ERROR: "Function 'row_cumsum' cannot be invoked. The row set must be serialized."
DeviceProcessEvents
| extend running = row_cumsum(1)

// ✅ FIX — serialize first
DeviceProcessEvents
| order by Timestamp asc
| serialize
| extend running = row_cumsum(1)
```

## 6. Datetime pitfalls

- **Always supply `timespan`** (e.g., `"P7D"`) or `startTime`/`endTime` to `run_hunting_query`.
  Defender hunting is throttled aggressively for wide windows.
- KQL datetimes are UTC; use `ago(7d)` or `bin(Timestamp, 1h)` for relative ranges and bucketing.
- For timezone-shifted reporting, use `datetime_add` / `format_datetime` at projection time only —
  never at filter time (kills index pushdown).

```kql
DeviceProcessEvents
| where Timestamp > ago(7d)            // ✅ uses Timestamp index
| where InitiatingProcessFileName =~ "powershell.exe"
```

## 7. Result-size discipline

`run_hunting_query` truncates to 500 rows by default and adds `_truncation_info`. For exploratory
queries, summarize first and only return rows after you've narrowed down. Patterns:

```kql
// Top-N
DeviceProcessEvents
| where Timestamp > ago(1d)
| summarize Count=count() by InitiatingProcessFileName
| top 20 by Count desc

// Deduplication
DeviceLogonEvents
| where Timestamp > ago(7d)
| summarize arg_max(Timestamp, *) by DeviceId, AccountName

// Time binning
DeviceNetworkEvents
| where Timestamp > ago(7d)
| summarize Connections=count() by bin(Timestamp, 1h), RemotePort
| order by Timestamp asc
```

## 8. Common error recovery

| Error excerpt | Likely cause | Fix |
|---------------|--------------|-----|
| `Semantic error: '<X>' is not a recognized table` | Wrong table name or wrong tenant section | Run `get_hunting_schema()` and pick an exact name |
| `Resolved as scalar 'name' which is not a column` | Misspelled or missing column | Run `get_table_schema("Table")` and verify |
| `extractall(): argument 2 must be a valid regex with [1..16] matching groups` | No `()` in regex | Wrap the pattern in `()` |
| `Function 'row_cumsum' cannot be invoked. The row set must be serialized.` | Missing `order by` + `serialize` | Add `\| order by ... \| serialize` before the window function |
| `Only equality is allowed in this context` | Non-equality join predicate | Pre-bucket and join on the bucket |
| `Summarize group key '<col>' is of a 'dynamic' type` | Dynamic column in `by` | Wrap in `tostring()` |
| `E_RUNAWAY_QUERY` | Cross-join blow-up | Reduce cardinality with filters/summarize before the join |

## 9. Good query habits

- Filter on `Timestamp` first to leverage the index.
- Project only the columns you need before joins and summarizations.
- Prefer `summarize arg_max(Timestamp, *) by Key` over `top 1 by Timestamp` per group when you
  need the latest row per entity.
- Use `validate_hunting_query` for any LLM-generated query before paying for a full run.
- When you need to enrich a result set, prefer `get_device_insights` / `get_user_insights` over
  hand-rolled join chains — they batch the right pivots in parallel.
