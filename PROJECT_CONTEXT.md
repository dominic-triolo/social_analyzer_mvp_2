# TrovaTrip Lead Pipeline — Project Context

## What This Is

A Flask + Celery pipeline that discovers, screens, enriches, analyzes, scores, and syncs creator profiles to HubSpot for BDR outreach. Deployed on Railway. Branch: `cc_env`.

**Goal:** Produce 500+ enriched+scored leads per day, eventually autonomously.

**Current throughput:** ~3 min/profile with full AI analysis. With 2 Celery workers: ~960/day — already exceeds 500 target.

---

## Architecture

```
Flask (web) ──→ POST /api/runs ──→ Celery task: run_pipeline
                                         │
                                    6-stage pipeline
                                         │
                              ┌──────────┴──────────┐
                              │   Stage Adapter      │
                              │   Pattern             │
                              │                       │
                              │  Each stage has a     │
                              │  per-platform adapter │
                              │  implementing         │
                              │  StageAdapter.run()   │
                              └───────────────────────┘
```

**Stack:** Flask 3.0, Celery 5.3, Redis (broker + real-time state), PostgreSQL (persistent storage), SQLAlchemy 2.0, Gunicorn, Railway

**State:** Redis for real-time (Celery broker, live progress, 7-day TTL). Postgres for persistent records (runs, leads, lead_runs). Dual-write — pipeline writes to both.

---

## The 6-Stage Pipeline

```
DISCOVERY → PRE-SCREEN → ENRICHMENT → ANALYSIS → SCORING → CRM SYNC
```

| Stage | What it does | APIs |
|---|---|---|
| **Discovery** | Find profiles from platform APIs | InsightIQ (IG), Apify (Patreon/FB) |
| **Pre-screen** | Quick disqualification (inactive, NSFW, wrong niche) | InsightIQ, OpenAI (IG only) |
| **Enrichment** | Contact info, social links, media rehosting | Apify, Apollo (Patreon/FB) |
| **Analysis** | AI content analysis (GPT-4o vision, Whisper) | OpenAI |
| **Scoring** | 5-dimension evidence scoring + tier assignment | OpenAI |
| **CRM Sync** | HubSpot import + BDR round-robin assignment | HubSpot |

### Supported Platforms (3 active)
- **Instagram** — InsightIQ discovery + GPT-4o vision analysis
- **Patreon** — Apify scraper + 11-step enrichment pipeline
- **Facebook Groups** — Google Search scraping + same enrichment as Patreon

### Coming Soon (placeholders in UI)
Meetup, Podcasts, Substack, Reddit, YouTube, Newsletters, Blogs/SEO

---

## Project Structure

```
social_analyzer_mvp_2/
├── app/
│   ├── __init__.py              ← Flask app factory
│   ├── config.py                ← Env vars, BDR map, pipeline stage list
│   ├── extensions.py            ← Redis, R2, OpenAI clients (lazy init)
│   ├── routes/
│   │   ├── __init__.py          ← Blueprint registration
│   │   ├── dashboard.py         ← GET /, /api/stats
│   │   ├── discovery.py         ← GET /discovery
│   │   ├── webhook.py           ← POST /webhook/async
│   │   └── monitor.py           ← /runs, /runs/<id>, /api/runs, /api/pipeline-info
│   ├── pipeline/
│   │   ├── base.py              ← StageAdapter ABC + StageResult dataclass
│   │   ├── manager.py           ← Run orchestration, STAGE_REGISTRY, Celery task
│   │   ├── discovery.py         ← 3 discovery adapters
│   │   ├── prescreen.py         ← 3 prescreen adapters
│   │   ├── enrichment.py        ← 3 enrichment adapters
│   │   ├── analysis.py          ← 3 analysis adapters
│   │   ├── scoring.py           ← 3 scoring adapters + evidence scoring logic
│   │   └── crm.py               ← 3 CRM sync adapters
│   ├── services/
│   │   ├── insightiq.py         ← InsightIQ discovery + content fetch
│   │   ├── openai_client.py     ← GPT-4o vision, Whisper, creator profiles
│   │   ├── hubspot.py           ← Webhook + batch import
│   │   ├── apify.py             ← Apollo, MillionVerifier, SocialGraphBuilder
│   │   ├── r2.py                ← Cloudflare R2 media ops + analysis cache
│   │   └── db.py                ← Postgres persistence (persist_run, persist_lead_results)
│   └── models/
│       ├── run.py               ← Run model (Redis-backed, real-time)
│       ├── db_run.py            ← DbRun model (Postgres, persistent)
│       ├── lead.py              ← Lead model (Postgres, deduplicated creators)
│       └── lead_run.py          ← LeadRun model (Postgres, per-lead-per-run evidence)
│   ├── database.py              ← SQLAlchemy engine, SessionLocal, Base, init_db()
│   ├── routes/
│   │   └── evaluation.py        ← Evaluation dashboard + 3 API endpoints
├── celery_app.py                ← Celery config
├── wsgi.py                      ← WSGI entry
├── templates/                   ← Jinja2 (base, home, discovery, runs_list, run_detail, evaluation)
├── static/
├── category_examples.json       ← Scoring reference examples
├── Procfile                     ← web: gunicorn, worker: celery
└── requirements.txt
```

---

## Key Design Patterns

### Stage Adapter Pattern
Every pipeline stage has a per-platform adapter class inheriting from `StageAdapter`:
```python
class StageAdapter(ABC):
    platform: str           # 'instagram', 'patreon', 'facebook'
    stage: str              # 'discovery', 'pre_screen', etc.
    description: str        # Human-readable (used in UI pipeline preview)
    apis: List[str]         # External APIs used (shown as tags in UI)
    est_seconds_per_profile: float  # Performance estimate (not yet measured)

    @abstractmethod
    def run(self, profiles, run) -> StageResult: ...
```

The pipeline manager is platform-agnostic — it looks up adapters from `STAGE_REGISTRY`:
```python
STAGE_REGISTRY = {
    'discovery':   discovery_mod.ADAPTERS,    # {'instagram': InstagramDiscovery, ...}
    'pre_screen':  prescreen_mod.ADAPTERS,
    'enrichment':  enrichment_mod.ADAPTERS,
    'analysis':    analysis_mod.ADAPTERS,
    'scoring':     scoring_mod.ADAPTERS,
    'crm_sync':    crm_mod.ADAPTERS,
}
```

Total: **18 adapter classes** (6 stages x 3 platforms).

### Redis-Backed Run Model
The `Run` class stores all state in Redis:
- `run:{id}` → JSON blob (TTL 7 days)
- `runs:list` → sorted set by creation time
- Tracks: status, current_stage, stage_progress, error log, aggregate counters
- Supports real-time polling from the frontend (3s refresh)

### Pipeline Info API
`/api/pipeline-info` returns adapter metadata for the frontend pipeline preview diagram:
```json
{
  "instagram": {
    "discovery": {"description": "Search by followers, interests, and lookalike", "apis": ["InsightIQ"], "est": null},
    "pre_screen": {"description": "Post frequency check + GPT-4o content scan", "apis": ["InsightIQ", "OpenAI"], "est": null},
    ...
  },
  "patreon": { ... },
  "facebook": { ... }
}
```

---

## External Services

| Service | Module | What it does |
|---|---|---|
| **InsightIQ** | `services/insightiq.py` | IG/YT/TT/FB discovery + content fetch |
| **OpenAI** | `services/openai_client.py` | GPT-4o vision analysis, Whisper transcription, creator profiles |
| **HubSpot** | `services/hubspot.py` | Contact create/update via webhook + batch import |
| **Apify** | `services/apify.py` | Web scraping (Patreon, FB, Linktree, personal sites) |
| **Apollo** | `services/apify.py` | Professional email lookup (ApolloEnrichment class) |
| **MillionVerifier** | `services/apify.py` | Email validation |
| **Cloudflare R2** | `services/r2.py` | Media rehosting, analysis cache |

---

## Env Vars (Railway)

```
REDIS_URL
DATABASE_URL                    ← Railway Postgres addon (auto-injected). Optional — app works without it.
OPENAI_API_KEY
INSIGHTIQ_CLIENT_ID, INSIGHTIQ_SECRET, INSIGHTIQ_USERNAME, INSIGHTIQ_PASSWORD, INSIGHTIQ_API_URL
HUBSPOT_API_KEY, HUBSPOT_WEBHOOK_URL
R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET_NAME, R2_ENDPOINT_URL, R2_PUBLIC_URL
APIFY_API_TOKEN
APOLLO_API_KEY
MILLIONVERIFIER_API_KEY
```

---

## BDR Assignment

Round-robin across 6 BDRs, configured in `app/config.py`:
```python
BDR_OWNER_IDS = {
    'Miriam Plascencia': '83266567',
    'Majo Juarez':       '79029958',
    'Nicole Roma':       '83266570',
    'Salvatore Renteria':'81500975',
    'Sofia Gonzalez':    '79029956',
    'Tanya Pina':        '83266565',
}
```

---

## Frontend

Flask/Jinja templates with Tailwind CDN + custom CSS + Turbo (Hotwire) for SPA-like navigation.

**Design system:**
- Teal-on-cream color scheme
- DM Sans (body) + JetBrains Mono (code/labels)
- Shared component classes: `.input-field`, `.btn-primary`, `.btn-accent`, `.badge`, `.kpi-card`, `.platform-card`, `.chip`, `.tag-pill`
- SVG icon objects: `STAGE_SVG` (6 pipeline stages), `PLATFORM_SVG` (6 platforms)
- Auto-fill grid for platform cards (scales to 10+)
- Wrapping chip filters for runs list
- Turbo Drive for smooth page transitions (no full-page reloads)

**Views:**
1. **Home** (`/`) — Dashboard with KPI stats, recent runs, quick actions (Discovery, Runs, Evaluation)
2. **Discovery** (`/discovery`) — Platform selector + filters + pipeline preview diagram
3. **Runs List** (`/runs`) — Filterable list with platform/status chips
4. **Run Detail** (`/runs/<id>`) — 6-stage pipeline tracker with live progress
5. **Evaluation** (`/evaluation`) — Chart.js dashboard: channel performance, funnel drop-off, tier distribution

---

## What's Been Done (Phases 1-5 Complete)

1. **Phase 1 — Codebase Restructure**: Broke monolithic `app.py` (42KB) and `tasks.py` (214KB) into `app/` package with routes, pipeline, services, models. Legacy files deleted.

2. **Phase 2 — Run-Centric Pipeline**: Introduced Run model, pipeline manager, 18 stage adapter classes, `STAGE_REGISTRY`, Celery orchestration task.

3. **Phase 3 — Run-Centric UI**: Replaced old platform-specific monitors with unified run views. Added pipeline preview diagram, auto-fill platform grid, wrapping chip filters. Full frontend redesign with sidebar layout.

4. **Phase 5 — PostgreSQL Persistence + Evaluation**: Added Postgres alongside Redis for persistent storage. 3 tables: `runs` (run history), `leads` (deduplicated creators), `lead_runs` (per-lead-per-run evidence trail with scores, tiers, prescreen results, analysis evidence). Pipeline writes to both Redis (real-time) and Postgres (persistent) — all DB writes are try/except wrapped so pipeline never blocks. Added evaluation dashboard with Chart.js charts (channel performance, funnel drop-off, tier distribution) and 3 API endpoints. Falls back to demo data when DATABASE_URL is not set. Added Turbo (Hotwire) for SPA-like navigation.

---

## What's Next

### Phase 4 — Multi-Platform Adapters
Build adapter classes for Meetup, Podcasts, Substack (currently "Coming soon" in UI). Each needs 6 adapters (one per stage). Skipped in favor of Phase 5 — can be done anytime.

### Phase 5.5 — Performance Measurement
`est_seconds_per_profile` is `None` on all 18 adapters. Need real measurements to populate pipeline preview estimates.

### Deferred
- Autonomous scheduling (cron-triggered runs)
- Scoring weight learning loop
- Cost tracking per run (API call costs)

---

## Postgres Schema

3 tables, all managed by SQLAlchemy (auto-created on startup if DATABASE_URL is set):

**`runs`** — one row per pipeline execution (mirrors Redis Run)
- id (TEXT PK), platform, status, filters (JSONB), bdr_assignment, profiles_found/pre_screened/enriched/scored, contacts_synced, duplicates_skipped, tier_distribution (JSONB), error_count, created_at, finished_at

**`leads`** — one row per unique creator, UNIQUE(platform, platform_id)
- id (SERIAL PK), platform, platform_id, name, profile_url, bio, follower_count, email, website, social_urls (JSONB), hubspot_contact_id, first_seen_at, last_seen_at

**`lead_runs`** — one row per lead per run (evidence trail)
- id (SERIAL PK), lead_id (FK→leads), run_id (FK→runs), stage_reached, prescreen_result/reason, analysis_evidence (JSONB), lead_score, manual_score, section_scores (JSONB), priority_tier, score_reasoning, synced_to_crm, created_at

**Evaluation queries these tables answer:**
1. Which platform produces best leads? → `runs` grouped by platform
2. Where are leads dropping off? → `lead_runs` grouped by stage_reached
3. Are our scores accurate? → `lead_runs` joined with `leads` where hubspot_contact_id IS NOT NULL
