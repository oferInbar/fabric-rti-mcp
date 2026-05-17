# AWS User Activity — Analytics & Dashboard Plan

## 1. Goal
Provide a single pane of glass for SOC analysts to visualize and alert on AWS user
activity sourced from CloudTrail. The dashboard must answer three questions fast:

1. **Who** is active in our AWS environment right now?
2. **What** are they doing, and is any of it suspicious?
3. **Where** are they coming from (geo / IP / user-agent), and has that changed?

## 2. Data Source (Microsoft Defender XDR / Advanced Hunting)

> **Verified against this tenant (probes re-run 2026-05-17 via the unified
> Advanced Hunting endpoint — same `runHuntingQuery` Graph call):**
> The unified endpoint reaches **both** Defender XDR tables and the Sentinel
> workspace ("Sentinel built-in tables (USX)") in a single query. Probes
> confirmed: `SigninLogs`, `AzureActivity`, `OfficeActivity`, `AuditLogs`,
> `BehaviorAnalytics`, `AADRiskyUsers`, `UserPeerAnalytics`, `AADUserRiskEvents`
> all return results normally.
>
> **Update (2026-05-17): `AWSCloudTrail` is now fully queryable.** The
> Sentinel AWS CloudTrail data connector is flowing — `Usage` shows
> **~1,004 MB ingested in the last 7 days** (previously 0 MB) and
> `AWSCloudTrail | take 1` returns rows with the full first-class schema
> (`UserIdentityArn`, `EventName`, `SourceIpAddress`, `ErrorCode`,
> `ManagementEvent`, etc.). The earlier HTTP 400 was caused by the empty
> table; that blocker is resolved.
>
> Also available: **`CloudAppEvents` with `Application == "Amazon Web
> Services"`** — ~207K events / 7d, populated by the Defender for Cloud
> Apps AWS connector. `AzureActivity` carries Defender-for-Cloud AWS
> onboarding/EKS activity (resource group `AWSONBOARDINGMDC` visible in
> probes).

- **Primary — AWS events:** **`AWSCloudTrail`** (Sentinel) — first-class
  CloudTrail columns, full management-event coverage, ~1 GB/7d in this
  workspace. Use directly for IAM/auth/anomaly rows. Key columns:
  `EventName`, `EventSource`, `UserIdentityType`, `UserIdentityArn`,
  `UserIdentityAccountId`, `UserIdentityUserName`,
  `UserIdentityPrincipalid`, `SessionIssuerArn`, `SessionIssuerUserName`,
  `SessionMfaAuthenticated`, `AWSRegion`, `SourceIpAddress`, `UserAgent`,
  `ErrorCode`, `ErrorMessage`, `RequestParameters`, `ResponseElements`,
  `RecipientAccountId`, `ManagementEvent`, `ReadOnly`.
- **Secondary — AWS events (UEBA-enriched):** **`CloudAppEvents`** filtered
  to `Application == "Amazon Web Services"` (and/or `AuditSource == "AWS"`).
  Use for the built-in UEBA hints and identity resolution that MDA adds on
  top of CloudTrail. Full CloudTrail payload preserved in `RawEventData`
  (dynamic). Normalized columns populated by MDA:
  - `ActionType` (≈ `eventName`), `ActivityType`, `ActivityObjects`,
    `ObjectName`, `ObjectType`, `ObjectId`
  - `AccountObjectId` (Entra ObjectId when MDA resolved the user),
    `AccountId`, `AccountDisplayName`, `AccountType`, `IsExternalUser`,
    `IsAdminOperation`, `IsImpersonated`
  - `IPAddress`, `IsAnonymousProxy`, `CountryCode`, `City`, `ISP`,
    `IPTags`, `IPCategory`, `UserAgent`, `UserAgentTags`
  - **Built-in UEBA hints:** `UncommonForUser` (statistically uncommon
    features for this user) and `LastSeenForUser` (days since each
    feature last seen) — use directly instead of building baselines.
- **Data-event coverage gap:** `AWSCloudTrail` currently carries
  management events only in this workspace. If S3 / Lambda data events
  are required, confirm the connector is configured to forward them (or
  add the **Amazon Web Services S3** connector variant).
- **Identity enrichment ("IAM identity table"):** **`IdentityInfo`** —
  canonical Defender XDR identity table (synced from Entra ID / on-prem AD).
  Join on `AccountObjectId` (preferred — MDA already populates it for
  SSO/SAML-federated AWS principals) or `AccountUpn` / `AccountName` as
  fallback. Useful columns: `AccountObjectId`, `AccountUpn`, `AccountName`,
  `AccountDisplayName`, `Department`, `JobTitle`, `Manager`, `City`,
  `Country`, `IsAccountEnabled`, `RiskLevel`, `Tags`.
  - For IAM users / roles where MDA cannot resolve the identity
    (`AccountObjectId` is empty), fall back to the IAM-user→UPN watchlist
    keyed by `userIdentity.userName`.
- **Companion tables (all confirmed Graph-reachable in this tenant):**
  - XDR: `AADSignInEventsBeta`, `EntraIdSignInEvents`,
    `AADSpnSignInEventsBeta`, `IdentityLogonEvents`, `AlertEvidence`,
    `AlertInfo`.
  - Sentinel-side via unified AH: `SigninLogs`, `AuditLogs`,
    `AzureActivity`, `OfficeActivity`, `AADRiskyUsers`, `AADUserRiskEvents`,
    `BehaviorAnalytics`, `UserPeerAnalytics`, `MicrosoftGraphActivityLogs`.
- **Key fields available inside `CloudAppEvents.RawEventData`** (extract
  with `tostring(parse_json(RawEventData).userIdentity.arn)` etc.):
  `userIdentity.arn`, `userIdentity.type`, `userIdentity.accountId`,
  `userIdentity.userName`, `userIdentity.principalId`,
  `userIdentity.sessionContext.sessionIssuer.userName`,
  `userIdentity.webIdFederationData.attributes.oid`,
  `eventName`, `eventSource`, `eventTime`, `sourceIPAddress`, `userAgent`,
  `awsRegion`, `errorCode`, `errorMessage`, `requestParameters`,
  `responseElements`, `mfaAuthenticated`, `recipientAccountId`.

## 3. Identity Correlation — AWS Principal ↔ Entra / Defender Account

This is the linchpin of the dashboard: every AWS event must be resolved (where
possible) to the corporate identity that owns it so we can layer first-party
signals (risk, UEBA, alerts) on top.

> **Observed federation pattern in this tenant (2026-05-17 probe of 7d
> `AWSCloudTrail`):** **No `AWSReservedSSO_*` ARNs observed** — IAM
> Identity Center → Entra federation (row #1 below) is **not** the path
> in use. Human activity is dominated by **direct `AssumedRole` where the
> session-name segment carries the UPN** (e.g.
> `assumed-role/Fullaccessrole/aaronb@zava-corp.com`). Marginal volume of
> `SAMLUser` (1 evt/7d) and `IdentityCenterUser` (7 evts/7d) exists as
> side paths. The vast majority of `AssumedRole` volume (~700K/7d) is
> **workload/integration roles** (CspmMonitorAws, MicrosoftSentinelRole,
> CloudHealth, EKS, MDC roles, autoscaling) — these are the noise the
> workload allowlist must suppress. `WebIdentityUser` (~6K/7d) carries
> UUID-shaped session names → also workload (OIDC), not human.
>
> **Implication:** the primary human-resolution heuristic for v1 is
> "extract the segment after the last `/` of `UserIdentityArn`; if it
> looks like a UPN (`contains '@'`), treat it as `candidateUpn`". SSO
> permission-set parsing is unnecessary in this environment. Confirm
> with a wider sample before locking the heuristic.

### 3.1 Resolution strategy (in priority order)

| # | AWS principal pattern | How to extract the user key | Join target |
|---|---|---|---|
| 1 | **IAM Identity Center / SSO federated to Entra** (most common): `userIdentity.type = AssumedRole`, ARN looks like `arn:aws:sts::<acct>:assumed-role/AWSReservedSSO_<PermissionSet>_xxx/<email-or-upn>` | Take the **session-name segment** after the last `/` in `userIdentity.arn`, or `userIdentity.principalId` after the `:` (format `<roleId>:<session>`) | `IdentityInfo.AccountUpn` |
| 2 | **SAML federation to Entra** (`userIdentity.type = SAMLUser` or `WebIdentityUser`) | `userIdentity.userName` carries the SAML NameID (typically UPN) | `IdentityInfo.AccountUpn` |
| 3 | **Direct Entra-issued role assumption** via OIDC (`userIdentity.type = WebIdentityUser`, provider `sts.windows.net`) | `userIdentity.webIdFederationData.attributes.oid` = Entra ObjectId | `IdentityInfo.AccountObjectId` |
| 4 | **Standalone IAM user** (`userIdentity.type = IAMUser`) | `userIdentity.userName` → lookup in a maintained **IAM-to-UPN watchlist** (custom table, kept in sync from IAM tags `owner=<upn>` or a CSV) | `IdentityInfo.AccountUpn` via lookup |
| 5 | **Assumed role by service / cross-account** (`userIdentity.type = AssumedRole` but session looks non-human, e.g., `i-0abc...`, `lambda`, build-system names) | Classify as **non-human**; do NOT attempt human join. Tag with `IdentityClass = "Workload"` and route to a separate watchlist | n/a |
| 6 | **Root** (`userIdentity.type = Root`) | Always alert; map to break-glass owner via account-ownership table | account-owner table |

### 3.2 KQL pattern (sketch)

```kql
let aws =
    CloudAppEvents
    | where Application == "Amazon Web Services"
    | extend ui = todynamic(RawEventData).userIdentity
    | extend arn = tostring(ui.arn),
             principalType = tostring(ui.type),
             userName = tostring(ui.userName),
             awsAccountId = tostring(ui.accountId)
    // Extract session name (the part after the last '/') — works for SSO + AssumeRole
    | extend sessionName = tostring(split(arn, "/")[-1])
    // Heuristic: candidate UPN for the join
    | extend candidateUpn = case(
        principalType == "AssumedRole" and arn has "AWSReservedSSO", tolower(sessionName),
        principalType in ("SAMLUser","WebIdentityUser"), tolower(userName),
        principalType == "IAMUser", tolower(userName),  // resolved via watchlist below
        ""
    )
    | extend candidateOid = tostring(ui.webIdFederationData.attributes.oid);
let iamWatchlist = externaldata(IamUserName:string, AccountUpn:string)
    [@"https://<your-blob>/iam_to_upn.csv"] with (format="csv", ignoreFirstRecord=true);
aws
| join kind=leftouter (iamWatchlist) on $left.userName == $right.IamUserName
| extend resolvedUpn = coalesce(candidateUpn, AccountUpn)
| join kind=leftouter (
        IdentityInfo
        | summarize arg_max(Timestamp, *) by AccountUpn
        | project AccountUpn, AccountObjectId, Department, JobTitle, IsAccountEnabled,
                  SensitivityLevel = Tags
    ) on $left.resolvedUpn == $right.AccountUpn
| extend IdentityClass = case(
        principalType == "Root", "Root",
        isempty(resolvedUpn) and principalType == "AssumedRole", "Workload",
        isempty(resolvedUpn), "Unknown",
        "Human")
```

### 3.3 Maintained lookups
- **IAM-user → UPN watchlist** — *deferred*. v1 ships **without** a
  watchlist; expect noisy "Unresolved principal" rows for IAM users.
  Phase 2 will add it (source of truth TBD: IAM tag `owner=<upn>` export,
  HRIS, or manual CSV).
- **AWS-account → owner-team table** — Phase 2.
- **Workload-role allowlist** — Phase 1 deliverable (high impact: the
  probe shows ~95% of `AssumedRole` volume is workload roles like
  `CspmMonitorAws`, `MicrosoftSentinelRole`, `CloudHealth_Role-*`,
  `AWSServiceRoleForAmazonEKS`, `MDCContainers*`). Seed the allowlist
  directly from the issuer breakdown in the §3 probe.
- **Noise-reduction baseline (v1 substitute for watchlist):** maintain
  rolling 30-day per-principal baselines (§8) and surface deviations.
  Acceptable to be noisy at this stage; tuning lives behind UI controls.

## 4. First-Party Signal Enrichment (Defender / Entra / UEBA)

For every resolved human identity, join in the following per event (or per
session window):

| Signal | Table / source | Field(s) used | Where it shows on dashboard |
|---|---|---|---|
| **Entra sign-in risk (real-time)** | `AADSignInEventsBeta` | `RiskLevelDuringSignIn`, `RiskState`, `RiskEventTypes`, `ConditionalAccessStatus` | Risk badge on user tile; "AWS activity from risky sign-in" panel |
| **Entra user risk (aggregate)** | `AADSignInEventsBeta` → `RiskLevelAggregated`; Entra ID Protection riskyUsers (via API/Log Analytics) | `RiskLevelAggregated` | User risk column in top-N table |
| **Defender for Identity / XDR alerts** | `AlertEvidence` + `AlertInfo` | `EntityType == "User"`, `AccountObjectId`, `Severity`, `Title` | "Open alerts on user" widget; row highlight |
| **Investigation Priority (UEBA)** | MDA — surfaced on `CloudAppEvents` for some events; full score on Entity page (Graph API `investigationPriority`) | `RawEventData.investigationPriority` if present; otherwise pulled via API into a watchlist | "Top users by UEBA priority" widget |
| **Anomalous activity (MDA)** | `CloudAppEvents` alerts of type `AnomalyDetected*` | `ActionType`, `RawEventData.alertType` | Anomaly timeline overlay |
| **MFA posture at corporate sign-in** | `AADSignInEventsBeta` | `MfaRequired`, `AuthenticationDetails` | Cross-check vs CloudTrail `mfaAuthenticated` |
| **Account enabled / disabled / leaver** | `IdentityInfo` | `IsAccountEnabled`, `DeletedDateTime` | Critical alert if AWS activity from disabled/leaver |
| **Sensitivity / privileged** | `IdentityInfo.Tags`, custom "privileged role" list | `Tags has "Privileged"` | Highlight privileged-user AWS activity |
| **Geo / impossible travel cross-cloud** | join AWS `IPAddress`/`City` with `AADSignInEventsBeta.IPAddress`/`Location` | both | "Same user, Entra IP A → AWS IP B in <1h" panel |
| **Threat-intel IP match** | `ThreatIntelIndicators` (current Sentinel TI table; confirmed in this tenant) — fall back to `ThreatIntelObjects` / `AZFWThreatIntel` for FW-side hits | `IPAddress` | TI badge on event row |

### 4.1 Composite Risk Score (per user, rolling 24h)
Sum of weighted indicators — recompute every 15 min:

```
risk = 50 * (RootActivity)
     + 40 * (AWS_action_from_disabled_or_leaver)
     + 30 * (Entra RiskLevelAggregated == "high")
     + 25 * (Open XDR alert, severity High/Critical)
     + 20 * (Impossible travel Entra↔AWS)
     + 20 * (Privilege-escalation eventName fired)
     + 15 * (TI IP match)
     + 10 * (UEBA investigationPriority >= 50)
     + 10 * (New country/ASN for user, 30d baseline)
     +  5 * (Reconnaissance burst)
```

Show as a sortable "User Risk" column. **Default thresholds: >= 50 →
High, >= 30 → Medium, >= 15 → Low.** All weights and thresholds are
exposed as Streamlit controls so users can fine-tune live; defaults
persist per-user in session state.

## 5. Dashboard Layout

### Row 1 — Health & Volume (single-value tiles)
| Tile | Metric |
|---|---|
| Total events (24h) | `count` |
| Distinct principals | `dc(userIdentity.arn)` |
| Failed API calls | `count where errorCode!=""` |
| Root account events | `count where userIdentity.type="Root"` |
| Console logins (24h) | `count where eventName="ConsoleLogin"` |
| MFA-less logins | `count where mfaAuthenticated="false"` |

### Row 2 — Authentication & Access
- **Login map** — successful/failed ConsoleLogin by GeoIP (choropleth).
- **Login outcome timeline** — stacked area: Success / Failure / MFA-fail.
- **Top 10 users by failed logins** (table, drill-down to events).
- **New geos per user (7d baseline)** — users logging in from a city/country/region
  never seen for them before.

### Row 3 — Identity & Privilege Changes (IAM)
- **Sensitive IAM events timeline** — `CreateUser`, `CreateAccessKey`,
  `CreateLoginProfile`, `UpdateLoginProfile`, `AttachUserPolicy`,
  `PutUserPolicy`, `CreatePolicyVersion`, `SetDefaultPolicyVersion`,
  `DeleteAccountPasswordPolicy`, `UpdateAccountPasswordPolicy`.
- **Privilege-escalation indicators** — policies granting `*:*` on `*`.
- **MFA changes** — `DeactivateMFADevice`, `DeleteVirtualMFADevice`,
  `EnableMFADevice` per user.
- **Access-key lifecycle** — created vs deleted vs rotated (7/30/90d).

### Row 4 — Behavioral Anomalies
- **Impossible travel** — same `userIdentity.arn`, two IPs in different countries
  within < 1h.
- **Concurrent sessions from different IPs** (table).
- **Unknown principals** — ARNs not in the identity table.
- **Dormant account reactivation** — first activity in > 30d.
- **Rare user-agent per user** (e.g., `Boto3`, `Pacu`, blank UA).
- **Reconnaissance burst** — > N `Describe*`/`List*`/`Get*` calls per user in 5 min.

### Row 5 — Cross-Account & Region
- **Cross-account access** — `recipientAccountId != accountId` over time.
- **Activity by region heatmap** — user × region.
- **Unused-region usage** — events in regions normally idle for the account.

### Row 6 — Top-N Drill-downs
- Top users by event count, top services touched, top source IPs,
  top errors (`AccessDenied`, `UnauthorizedOperation`, `Throttling`).

### Row 7 — Identity Correlation (NEW)
- **Top users by composite risk score** (sortable table: ARN, resolvedUpn,
  Department, riskScore, open alerts, Entra risk, UEBA priority).
- **Unresolved AWS principals** — count + list (workloads vs unknown humans).
- **AWS activity from leavers / disabled accounts** (`IsAccountEnabled == false`).
- **AWS activity from risky Entra sign-ins** (`RiskLevelDuringSignIn != "none"`
  within ±15 min of the AWS event).
- **Cross-cloud impossible travel** — Entra sign-in IP/geo vs AWS
  `sourceIPAddress` for the same user.

## 6. Alerts (severity → trigger)

| Severity | Rule |
|---|---|
| **Critical** | Root account API call (any) |
| **Critical** | MFA disabled for any IAM user |
| **Critical** | IAM policy created/updated granting `*:*` on `*` |
| **High** | Impossible travel for same ARN < 1h |
| **High** | > 10 failed ConsoleLogin from one IP in 5 min (spray) |
| **High** | > 5 failed ConsoleLogin for one user in 5 min (brute force) |
| **High** | Access key created for another user |
| **High** | `UpdateLoginProfile` targeting a different user |
| **Medium** | New MFA method registered |
| **Medium** | Console login from new country for user |
| **Medium** | Activity from dormant account (idle > 30d) |
| **Medium** | Unknown principal (ARN not in identity table) |
| **Critical** | AWS activity from an Entra **disabled / leaver** account |
| **Critical** | AWS activity correlated with Entra sign-in `RiskLevel = high` |
| **High** | Cross-cloud impossible travel (Entra sign-in vs AWS event, same UPN, <1h, different countries) |
| **High** | Composite user risk score >= 50 |
| **High** | AWS activity from user with open Defender XDR alert (Sev High/Critical) |
| **Medium** | Unresolved human principal (AssumedRole session not in workload allowlist and not matchable to a UPN) |
| **Low** | UEBA investigation priority spike for user with AWS activity |
| **Low** | Console login from new city/region for user |
| **Low** | Reconnaissance burst (Describe/List/Get spike) |
| **Low** | Rare user-agent for user |

Each alert payload must include: `userIdentity.arn`, `sourceIPAddress`,
`eventName`, `eventTime`, `awsRegion`, `errorCode`, link back to the
dashboard with filters pre-applied.

## 7. Filters / Global Inputs
- Time range (last 1h / 24h / 7d / custom)
- **AWS account scope: full AWS Organization by default**, with a
  multi-select filter to narrow to specific account IDs.
- Account ID (multi-select)
- User ARN (free text + multi-select)
- Region
- Event source (e.g., `iam.amazonaws.com`, `signin.amazonaws.com`)
- Show only failures (toggle)
- Show only privileged actions (toggle)

## 8. Baselines & Thresholds
Maintain rolling 30-day baselines per principal for:
- Common source countries / cities / ASNs
- Typical user-agents
- Typical eventName set
- Typical regions and services

Anomaly = deviation against this baseline. Refresh nightly.

## 9. Implementation Phases

**Host & auth (applies to all phases):** the dashboard is a **Streamlit
app** that calls the same `run_hunting_query` function the Defender
Advanced Hunting MCP server uses (`defender_ah_mcp.services.hunting`).
Auth flows through the MCP server's existing Graph credential chain
(configured via `DEFENDER_GRAPH_*` / `HUNTING_*` env vars, auto-loaded
from `~/.copilot/mcp-config.json` on startup). No separate Entra app,
no in-app MSAL flow — the Streamlit process inherits the MCP server's
identity, exactly mirroring how the MCP tools call Advanced Hunting.

1. **Phase 1 — Ingest & schema** *(done)*: confirmed MDA AWS connector →
   `CloudAppEvents`, confirmed `AWSCloudTrail` ingestion (~1 GB/7d),
   validated row schema, seeded the **workload-role allowlist** from a
   live 7d probe.
2. **Phase 2 — Core dashboard** *(done)*: shipped Rows 1 (Health & Volume),
   2 (Authentication & Access: outcome timeline, top failed logins,
   logins-by-region, new (user, IP) pairs vs 7d baseline), 3 (IAM /
   privilege change timeline + table), 5 (cross-account + region heatmap),
   6 (top errors / event sources / source IPs). Plotly is used for
   charts; the Graph hunting endpoint rejects `kind=leftanti` joins so
   the new-geo query uses a `union + summarize` pattern instead.
3. **Phase 3 — Geo map & entity deep-dive** *(in progress, this iteration)*:
   port the world-map + drilldown + per-user panel pattern from the
   sibling "AWS login overview" app. Adds: world map of sign-ins
   (city-aggregated, colored by risk), city → user drilldown, per-user
   deep-dive panel (identity, recent IPs, regions, top events). Risk
   inputs are heuristic only (impossible travel + new-country + new-IP
   vs 30d baseline, plus Defender `AlertEvidence` severity). Reuses
   the existing parallel `query_many` fan-out. **Out of scope for this
   phase:** the full `ResolveAwsPrincipal()` KQL function, IAM-user→UPN
   watchlist, and AWS-account → owner-team table — those stay in
   Phase 4.
4. **Phase 4 — Identity resolution helpers**: build the IAM-user→UPN
   watchlist, account-owner table; ship the resolution KQL as a reusable
   function `ResolveAwsPrincipal()` (parse session-name segment of
   `UserIdentityArn`; lowercase if it contains `@`).
5. **Phase 5 — Anomalies + identity row**: Row 4 (behavioral anomalies —
   impossible travel cross-cloud, dormant reactivation, recon bursts) and
   Row 7 (identity correlation: unresolved principals, leaver activity,
   risky Entra sign-ins, cross-cloud impossible travel) plus
   critical/high alerts. Depends on Phase 4.
6. **Phase 6 — Composite risk + UEBA**: pull MDA investigation priority,
   Entra risk; compute rolling user risk score.
7. **Phase 7 — Tuning**: suppress noisy workload roles, document
   exceptions, measure alert precision.

## 10. Decisions & Open Items

### Decided (2026-05-17)
- **AWS scope:** start with full AWS Organization, multi-select account
  filter in the UI (§7).
- **Dashboard host:** Streamlit app that imports the Defender Advanced
  Hunting MCP server's `run_hunting_query` directly and inherits its
  Graph credential / endpoint config via `~/.copilot/mcp-config.json`
  (see §9 header). No separate Entra app or MSAL flow.
- **Licensing:** assume all Defender / Entra licenses available
  (including Entra ID Protection P2 → sign-in & user risk signals are in
  scope).
- **IAM-user → UPN watchlist:** deferred. v1 relies on rolling baselines
  for noise reduction; noisy unresolved rows are acceptable at this stage.
- **Risk thresholds (§4.1):** ship 50 / 30 / 15 as defaults; expose
  weights and thresholds as Streamlit controls.
- **Federation model:** investigated from data — no IAM Identity Center
  (`AWSReservedSSO_*`) usage observed; primary human path is
  `AssumedRole` with UPN in the session-name segment. See §3 callout.
- **Threat-intel table:** use `ThreatIntelIndicators` (current Sentinel
  TI table, present in this tenant). Older `ThreatIntelligenceIndicator`
  is **not** present.
- **Confirmed: Defender for Cloud Apps AWS connector is enabled** —
  `CloudAppEvents` shows ~207K AWS events / 7d in this tenant.
- **Confirmed: Sentinel `AWSCloudTrail` connector is flowing** —
  ~1,004 MB / 7d, queries return rows normally via the unified Advanced
  Hunting endpoint. Earlier HTTP 400 blocker is resolved.

### Open / tracked as TODOs
- **Data-event coverage (S3, Lambda):** current `AWSCloudTrail` feed
  appears to be management events only. Decide whether to enable the
  S3 connector variant.
- **Alert delivery channel:** Teams, Slack, ticketing, or Sentinel
  incident? Needed before Phase 4 alerts ship.
- **Retention requirement** for the underlying events (compliance).

## 11. References
- Splunk Security Content — Suspicious AWS Login Activities,
  AWS IAM Privilege Escalation, AWS IAM Account Takeover, AWS User Monitoring
  (https://research.splunk.com/)
- AWS CloudTrail Lake Highlights Dashboard
  (https://docs.aws.amazon.com/awscloudtrail/latest/userguide/lake-dashboard-highlights.html)
- MITRE ATT&CK Cloud matrix — T1078.004, T1098, T1110, T1526, T1535, T1556.006,
  T1586.003, T1621.
