# Narrative Risk Guard (NRG) — Requirements

## 0. Goal
Build a lightweight, automated risk-control system for a concentrated, narrative/thesis-driven portfolio. The system must:
- Automatically ingest current account equity and positions (multi-broker).
- Aggregate exposure by “Thesis” (narrative bucket) rather than by ticker.
- Enforce two hard constraints:
  1) Account-level drawdown “mode” (NORMAL / HALF / MIN).
  2) Per-thesis worst-loss budget (Utilization <= 1).
- Produce a daily (and optionally intraday) dashboard and a clear “what to reduce” action list.

This is a risk engine, not a trading bot. It outputs instructions; execution is manual.

## 1. Core Concepts
### 1.1 Account Equity
“Equity” = total net liquidation value of the account (positions marked-to-market + cash − any margin debt).

### 1.2 Peak & Drawdown
- Peak = highest recorded Equity to date.
- Drawdown = (Equity − Peak) / Peak (negative number when below peak).

### 1.3 Risk Mode (Account-level)
User sets `X` (e.g., 0.12 for 12%).
- NORMAL if drawdown > −X
- HALF if −2X < drawdown <= −X
- MIN if drawdown <= −2X

Mode affects allowed risk scaling. Default:
- NORMAL: risk_scale = 1.0
- HALF: risk_scale = 0.5
- MIN: risk_scale = 0.2 (configurable)

### 1.4 Thesis Budget (Per-narrative)
For each Thesis:
- MV = sum of market value of positions mapped to that Thesis.
- Stress% = user-defined adverse scenario percent move for that Thesis.
- WorstLoss = MV * Stress%
- Budget% = user-defined fraction of total Equity the Thesis is allowed to lose in the stress scenario.
- Budget$ = Equity * Budget% * risk_scale
- Utilization = WorstLoss / Budget$

Constraint: Utilization <= 1.0

If Utilization > 1.0:
- TargetMV = Budget$ / Stress%
- Reduce$ = MV − TargetMV
Output: “Reduce this Thesis exposure by approximately Reduce$”.

## 2. System Outputs (What the user sees)
### 2.1 Daily Summary
- Timestamp (PT)
- Total Equity
- Peak
- Drawdown
- Mode (NORMAL/HALF/MIN)
- Top Thesis Utilization table (sorted desc)
- Action List:
  - Any thesis where Utilization > 1.0 with Reduce$ amount
  - If Mode != NORMAL, highlight “Account mode change” and implied scale-down

### 2.2 Dashboard (Google Sheets)
A single Google Sheet updated automatically with:
- Sheet: `Account`
  - DateTime, Equity, Peak, Drawdown, Mode, risk_scale
- Sheet: `Thesis`
  - Thesis, MV, Stress%, Budget%, WorstLoss, Budget$, Utilization, Action, Falsifier (text)
- Sheet: `Positions` (normalized)
  - Broker, Account, Symbol, Type, Qty, Price, MV, Thesis, Notes
- Sheet: `Snapshots`
  - Append-only daily snapshot for auditing (Account + Thesis summary)

### 2.3 Local Artifacts
- SQLite DB (or append-only CSV) storing:
  - equity history
  - mode history
  - thesis daily metrics
  - positions snapshots
- Log file with run status and any data-source errors.

### 2.4 Notifications (optional but recommended)
Trigger a notification if:
- Mode changes (NORMAL→HALF, HALF→MIN, etc.)
- Any Utilization crosses above 1.0
- Data ingest fails for any broker

Notification channels (choose at least one):
- Email (SMTP)
- Slack webhook
- Push via Pushover/Telegram (optional)

## 3. Data Ingestion Requirements
### 3.1 Multi-Broker Support
Must support at least:
- Schwab (API-based)
- Fidelity (file-based baseline; API/aggregator optional)

#### 3.1.1 Schwab Connector (API)
- OAuth-based token handling with refresh.
- Fetch:
  - account list
  - balances / equity
  - positions (symbol, qty, mark price if available)
- Must be robust to token expiration and API rate limits.
- Store tokens securely (env vars / secret manager), never in the repo.

#### 3.1.2 Fidelity Connector (Baseline: CSV import)
Because Fidelity direct personal API access is not assumed.
Baseline approach:
- User exports a “Positions” CSV daily (or weekly) from Fidelity website.
- File is placed into a watched directory (local) or a Google Drive folder synced locally.
- System parses latest file for:
  - equity (if present) OR compute as sum(MV)+cash if cash included
  - positions with qty and mark

Optional extensions:
- Integration via aggregator API if user later provides credentials/token.

### 3.2 Normalization
Normalize all brokers into a common schema:
- broker: string
- account_id: string
- symbol: string
- instrument_type: enum {STOCK, ETF, OPTION, CASH, OTHER}
- qty: float
- multiplier: float (options default 100)
- price: float (mark/last)
- mv: float (qty * price * multiplier)
- currency: string (assume USD for MVP)
- thesis: string (mapped)
- notes: string (optional)

### 3.3 Pricing
MVP pricing rule:
- Use broker-provided mark/last when available.
- If missing, optionally fill via market data provider (optional, not required for MVP).

## 4. Thesis Mapping Requirements
### 4.1 Mapping Philosophy
Positions map to Theses, not to sectors. A Thesis groups exposures that share the same driver and failure mode.

### 4.2 Minimal Mapping Implementation
Mapping file maintained by user:
- `mapping.csv` with columns:
  - symbol_pattern (exact symbol or wildcard/regex)
  - thesis
  - weight (default 1.0; allow split mapping by weight if needed)

Rules:
- Primary mapping: each position maps to exactly one thesis by default.
- Split mapping (rare): allow a symbol to map to multiple theses by providing multiple rules with weights summing to 1.0.

### 4.3 Thesis Configuration
`thesis.toml` defines per-thesis parameters:
- stress_pct: float (e.g., 0.35)
- budget_pct: float (e.g., 0.08)
- falsifier: string (human text)
- status: enum {ACTIVE, WATCH, BROKEN}
- time_window_end: optional date (for time-based invalidation reminders)

If status == BROKEN:
- Action should automatically become “EXIT” regardless of utilization.

## 5. Risk Engine Logic (Detailed)
### 5.1 Compute Equity
Equity is computed per broker account then aggregated:
- If broker provides net liquidation equity, trust it.
- Else fallback: sum of MV + cash (if cash known).

### 5.2 Compute Account Mode
- Peak updates with historical max equity.
- Drawdown computed from Peak.
- Mode and risk_scale computed from thresholds.

Config:
- `account.toml`
  - drawdown_x: 0.12
  - risk_scale_normal: 1.0
  - risk_scale_half: 0.5
  - risk_scale_min: 0.2

### 5.3 Compute Thesis Aggregates
For each thesis:
- MV = sum(mapped positions MV)
- WorstLoss = MV * stress_pct
- Budget$ = Equity * budget_pct * risk_scale
- Utilization = WorstLoss / Budget$
- If Utilization > 1:
  - TargetMV = Budget$ / stress_pct
  - Reduce$ = MV − TargetMV
  - Action = `REDUCE {Reduce$}` (rounded)

### 5.4 Options Handling (MVP vs Advanced)
MVP (acceptable):
- Long options: MV = premium value; WorstLoss = MV (treat as fully at risk)
- Short options: flag as “UNSUPPORTED_RISK” unless a margin-at-risk estimate is provided manually
Advanced (optional):
- Scenario-based: compute thesis stress scenario PnL via option greeks or reprice with Black-Scholes.
This can be deferred; MVP must at least not silently mis-handle short options.

## 6. Execution & Scheduling
### 6.1 Run Modes
- Daily run (required): after market close (e.g., 18:00 PT).
- Intraday run (optional): hourly during market hours.

### 6.2 Runtime Targets
- Complete within 60 seconds for typical portfolios.

### 6.3 Deployment Options
Choose one:
- Local cron on a machine that has access to the Fidelity CSV folder.
- A small always-on VM with a synced folder.
- GitHub Actions is acceptable only if all credentials and files are accessible in CI (usually harder for Fidelity CSV).

## 7. Google Sheets Integration
### 7.1 Write Access
Use Google Sheets API with OAuth or service account.
- The script writes to specific ranges/tabs.
- Must be idempotent: rewriting current-day summary should not duplicate rows.
- Append-only snapshot table should add one row per run (or per day).

### 7.2 Sheet Layout Contract
The agent must produce a stable tab schema so downstream formulas/charts can depend on it:
- `Account` header row fixed.
- `Thesis` header row fixed.
- `Positions` header row fixed.
- `Snapshots` header row fixed.

## 8. Security & Privacy
- No broker username/password stored.
- Tokens in environment variables or secret manager.
- Do not log sensitive tokens.
- Local DB may contain financial data; store in user-controlled storage only.

## 9. Error Handling
- If one broker fails ingestion, system still runs with partial data BUT:
  - Mark run status as DEGRADED
  - Notify user
  - Dashboard should show which broker failed
- If equity cannot be computed reliably, abort run and notify.

## 10. Acceptance Criteria
1) With valid Schwab API credentials, the system updates positions and equity automatically.
2) With a Fidelity positions CSV present, system ingests it without manual edits.
3) Account Mode changes exactly at configured drawdown thresholds.
4) Thesis Utilization matches formulas:
   - WorstLoss = MV * Stress%
   - Budget$ = Equity * Budget% * risk_scale
   - Utilization = WorstLoss / Budget$
5) For Utilization > 1, the system outputs Reduce$ that would bring Utilization back to 1.
6) Google Sheet updates successfully and consistently with correct headers and types.
7) System maintains a local history (DB/CSV) for at least 1 year of daily runs.

## 11. Deliverables
- `/src`
  - `main.py` (entrypoint)
  - `connectors/schwab.py`
  - `connectors/fidelity_csv.py`
  - `risk_engine.py`
  - `sheets_writer.py`
  - `storage.py` (SQLite/CSV persistence)
- `/config`
  - `account.toml`
  - `thesis.toml`
  - `mapping.csv`
- `/docs`
  - `SETUP.md` (how to obtain Schwab tokens, where to drop Fidelity CSV, how to set env vars)
- `requirements.txt` or `pyproject.toml`

## 12. Example Configs

### 12.1 account.toml
```toml
timezone = "America/Los_Angeles"
drawdown_x = 0.12

[risk_scale]
NORMAL = 1.0
HALF = 0.5
MIN = 0.2

[run_schedule]
daily_time_pt = "18:00"
```

### 12.2 thesis.toml
```toml
[theses.Neocloud_CRWV]
stress_pct = 0.35
budget_pct = 0.08
status = "ACTIVE"
falsifier = "Financing terms deteriorate materially OR delivery slips beyond X weeks OR key customer credit event"

[theses.Index_Core]
stress_pct = 0.20
budget_pct = 0.05
status = "ACTIVE"
falsifier = "N/A"
```

### 12.3 mapping.csv
```csv
symbol_pattern,thesis,weight
CRWV,Neocloud_CRWV,1.0
SPY,Index_Core,1.0
QQQ,Index_Core,1.0
```

13. Implementation Notes (Non-goals)
	•	Do not place trades automatically.
	•	Do not optimize portfolios or recommend new trades.
	•	The system’s job is to compute risk state and required reductions given user-defined parameters.

