# Social Analyzer — Roadmap

Five phases, from stabilizing the current pipeline to autonomous agent capabilities.

---

## Phase 1: Stabilize & Clean Up

Get the current architecture production-solid before adding anything.

### 1a. Commit to Postgres — Drop Preview Mode

**Effort:** ~1 day | **Risk:** Low

The evaluation blueprint has two code paths: real Postgres queries and `_demo_*()` fallbacks. Remove the dual-path design.

- Use SQLite for local dev, Postgres for prod — same SQLAlchemy schema, no branching.
- Remove `_demo_channels()`, `_demo_funnel()`, `_demo_scoring()` and all `if session is None` guards.
- Add Alembic for schema migrations instead of `init_db()` on boot.
- Ship a `.env.example` documenting required env vars.

### 1b. Replace Celery with RQ

**Effort:** ~2 days | **Risk:** Medium

Celery is overkill — sequential pipeline, concurrency capped at 2, no routing. RQ does the same job with ~10 lines of config.

- Replace `celery_app.py` with RQ worker config.
- Swap `apply_async()` calls for `queue.enqueue()`.
- Drop `celery`, `kombu`, `billiard` from requirements.

### ~~1c. Pipeline as DAG — Parallel Stages~~ DROPPED

Analysis depends on enrichment output for Patreon and Facebook (emails, social links fed into GPT-4o prompts). Only Instagram's analysis is enrichment-independent. Not worth the complexity for one platform — keep the pipeline linear.

---

## Phase 2: Operationalize for Non-Technical Users

Make the system safe and usable for BDRs and ops people who won't touch code.

### 2a. Scoring & Thresholds as Config

**Why:** Scoring weights, tier cutoffs, and pre-screen thresholds are buried in adapter classes. Changing them means a code deploy.

- Extract scoring weights, tier boundaries, and pre-screen rules into a YAML/JSON config (or a DB-backed settings page).
- Expose a UI for tuning weights and previewing how tier distribution would change.
- Version the config so you can track what changed between runs.

### 2b. Run Failure Recovery — Retry from Stage

**Why:** A failure at scoring currently means re-running discovery + enrichment, burning API credits on work already done.

- Persist per-stage output (profile list after each stage) on the Run model.
- Add a "Retry from stage X" action that picks up from the last successful stage.
- UI button on the run detail page next to the failed stage.

### 2c. Human-Readable Run Summaries

**Why:** The error log stores `{stage, message, profile_id}` — developer-facing. Non-technical users need plain English.

- Generate a natural-language run summary on completion: "Found 312 profiles, 80 passed pre-screen, 14 scored as high-priority. 3 skipped at enrichment due to missing email."
- Surface warnings, not just errors: "Yield at pre-screen was 8% — below your 30-day average of 22%."
- Show these on the run detail page and in notifications.

### 2d. Cost Guardrails

**Why:** Discovery hits InsightIQ, Apollo, MillionVerifier — all paid. Non-technical users need visibility and hard limits.

- Estimate cost before a run launches based on expected profile count and per-API pricing.
- Show the estimate on the discovery page before the user confirms.
- Configurable hard caps: max profiles per run, max spend per run, monthly budget ceiling.
- Abort the pipeline if a cap is hit mid-run (with a clear message, not a stack trace).

### 2e. Discovery Dedup & Filter Staleness

**Why:** Running the same filters repeatedly returns the same profiles. You discover, pre-screen, enrich, analyze, and score someone — only to find out at CRM sync they're a duplicate. All that API spend is wasted.

**Post-discovery dedup (quick win):**
- After the discovery adapter returns profiles, query the `Lead` table for existing matches on `(platform, platform_id)`.
- Strip known leads from the batch *before* pre-screen — stop burning credits on profiles you've already processed.
- Move `duplicates_skipped` tracking from CRM sync to this point.
- Show dedup count on the run detail page: "312 found, 48 already known, 264 new."

**Filter fingerprinting:**
- SHA-256 hash the normalized filters dict (pattern already exists in `apify.py` for Apollo queries).
- Store a `FilterHistory` record per run: `filter_hash | platform | run_id | total_found | new_found | novelty_rate | ran_at`.
- Simple lookup: "has this exact filter been run before, and what did it yield?"

**Staleness detection & UX:**
- Compute novelty rate per filter combo: `new_profiles / total_found`.
- When novelty rate drops below a threshold (e.g., 20%), flag the filter as stale.
- On the discovery page, before the user hits "Run": "Last run with these filters: 3 days ago, 12% novelty rate — consider adjusting."
- Suggest what to change: broader geo, different keywords, shifted follower range.

### 2f. Discovery Presets

**Why:** Filling out the same filter form repeatedly causes errors and wastes time.

- Let users save, name, and reuse filter combinations ("Travel micro-influencers, 10k-50k, US").
- Store presets in Postgres (or a simple JSON config to start).
- "Run from preset" button on the discovery page.

### 2g. Notifications

**Why:** Runs take minutes to hours. Nobody sits watching the SSE stream.

- Slack webhook on run completion or failure (channel configurable).
- Optional email digest: daily summary of runs, yield rates, top-scored creators.
- In-app notification badge on the sidebar (you already have the HTMX pattern for this).

### 2h. Evaluation Benchmarks

**Why:** The eval dashboard shows data but not whether it's good or bad.

- Track 30-day rolling averages for key metrics (yield rate, avg score, tier distribution).
- Flag runs that deviate significantly from baseline ("This run's pre-screen yield is 40% below your average").
- Show trend lines on the evaluation charts.

---

## Phase 3: Reliability & Observability

Prerequisites for autonomy — the system needs to see itself and handle failures gracefully before it can make decisions.

### 3a. Structured Logging

**Why:** Everything is `print()` statements. When a run fails at 2am or a non-technical user reports "it didn't work," there's nothing actionable to look at.

- Replace `print()` with Python `logging` — structured JSON logs with level, timestamp, run_id, stage, adapter.
- Per-run log stream: attach a handler that writes log entries to the Run model (or a `run_logs` table) so they're viewable in the UI.
- Log levels that matter: INFO for stage transitions, WARNING for degraded behavior (retries, fallbacks), ERROR for failures.
- Run detail page gets a "Logs" tab — filterable by level, searchable by keyword. Non-technical users see warnings and errors; devs can toggle to see everything.
- Bonus: ship logs to a central service (Datadog, Papertrail, or even just stdout in JSON for Heroku's log drain) for cross-run search.

### 3b. API Health Tracking & Circuit Breakers

**Why:** InsightIQ goes down, Apollo rate-limits you, Apify times out. Right now each adapter just throws an exception and the entire run fails. You can't trust non-technical users or an agent to run this unsupervised without graceful degradation.

**Per-service health tracking:**
- Track success rate, avg latency, and error count per external service (InsightIQ, Apollo, MillionVerifier, Apify, OpenAI, HubSpot) over a rolling window.
- Store in Redis — lightweight, ephemeral, no schema changes.
- Expose on the dashboard: a simple health panel showing green/yellow/red per service.

**Circuit breaker pattern:**
- If a service's error rate exceeds a threshold (e.g., >50% failures in the last 10 calls), trip the circuit — stop calling it.
- Tripped state: the adapter returns a clear skip result ("InsightIQ unavailable, skipping enrichment") instead of crashing the run.
- Half-open: after a cooldown period, allow one test call. If it succeeds, close the circuit and resume normal operation.
- Configurable per service: thresholds, cooldown periods, whether to skip or pause the run when tripped.

**Retry with backoff:**
- Wrap external API calls in a retry decorator with exponential backoff + jitter.
- Distinguish retryable errors (429 rate limit, 502/503 transient) from permanent failures (401 auth, 404 not found).
- Cap retries (3 attempts default) to avoid burning time on a dead service.

---

## Phase 4: Autonomous Pipeline — Rules Engine

Close the feedback loop. The pipeline observes its own results and adjusts, no LLM needed yet.

### 4a. Decision Points Between Stages

**Why:** The pipeline manager currently passes output blindly to the next stage. Adding logic between stages handles 80% of the autonomous value without any AI.

- Add a `between_stages` hook in the pipeline manager that runs after each stage completes.
- Implement as a rules engine (configurable conditions → actions):
  - If pre-screen yield < X%, loosen filters and re-run discovery.
  - If enrichment fails on > Y% of profiles, back off and retry with exponential delay.
  - If scoring finds a high-density niche cluster, flag it for a follow-up run.
- Log every decision to a `decision_log` on the Run model (what it checked, what it chose, why).

### 4b. Approval Gates

**Why:** Full autonomy with paid APIs and CRM writes is risky. You need configurable pause points.

- Add a `requires_approval` flag per stage in the pipeline config.
- When a gate is hit, the run pauses and notifies the user (Slack/email).
- User approves or rejects from the run detail page. Rejection lets them adjust params before resuming.
- Default: autonomous through scoring, gate before CRM sync.

### 4c. Run Memory & Context

**Why:** The rules engine needs to remember what it already tried to avoid loops ("I already loosened filters once and yield was still low").

- Add a `decision_log` field to the Run model — structured list of `{condition, action_taken, result, timestamp}`.
- Rules engine checks decision history before acting: "already retried discovery with looser filters → escalate to notification instead of retrying again."
- This becomes the training data for the LLM planner in Phase 5.

---

## Phase 5: Autonomous Agent — LLM Planner

Replace the rules engine with an LLM that reasons about pipeline strategy. The typed stage adapters become tools.

### 5a. Stage Adapters as Tool Schemas

**Why:** Your `StageAdapter` interface already has name, description, platform, inputs, outputs. It's one step from an LLM tool spec.

- Export each adapter as a tool definition (name, description, parameters, expected output).
- Build a tool registry the LLM can query: "what tools are available for Instagram enrichment?"
- The pipeline manager becomes an LLM orchestration loop: observe results → pick next tool → execute → repeat.

### 5b. Goal-Oriented Runs

**Why:** Instead of "run these 6 stages with these filters," the input becomes a goal.

- User provides a goal: "Find 20 high-quality travel creators under $500 total API cost."
- The LLM planner decomposes the goal into a strategy: which platform, what filters, how many to discover, when to stop.
- It iterates — if the first discovery batch doesn't yield enough high-scorers, it adjusts and tries again.
- Cost tracking from Phase 2 becomes a hard constraint the planner respects.

### 5c. Multi-Run Coordination

**Why:** A single run targets one platform with one filter set. An agent should manage a portfolio.

- The agent manages multiple concurrent runs across platforms.
- It learns which filter combos yield high-quality creators for which niches (using evaluation data from Postgres).
- It proactively suggests new discovery campaigns based on scoring trends and CRM sync success rates.
- Weekly "agent report" summarizing what it did, what worked, and what it recommends next.

### 5d. Human-in-the-Loop Escalation

**Why:** Even with an LLM planner, some decisions are too consequential for full autonomy.

- Configurable escalation policies: "always ask before spending > $100", "always ask before syncing > 50 contacts to CRM."
- The agent explains its reasoning when escalating: "I want to run a second Instagram discovery with broader geo filters because the first batch only yielded 4 high-priority creators. Estimated cost: $45."
- User can approve, reject, or modify the plan before the agent continues.

---

## Priority Order

| Priority | Item | Why first |
|----------|------|-----------|
| Now | Phase 1a–1b | Foundation — clean up before building on top |
| Next | Phase 2a–2e | Unblocks non-technical users + stops wasted API spend |
| Then | Phase 2f–2h | Quality of life, not blockers |
| Then | Phase 3a–3b | Observability + resilience — prerequisite for autonomy |
| Later | Phase 4a–4c | Autonomous rules — high value, moderate effort |
| Future | Phase 5a–5d | Agent capabilities — needs Phases 3 + 4 as prerequisites |
