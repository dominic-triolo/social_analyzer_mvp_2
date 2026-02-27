"""
Local preview server — runs the app without Redis or external services.

Seeds demo data into SQLite so all routes, HTMX partials, and API endpoints
work locally against real DB code paths.

Usage: python preview.py
"""
import os
import sys
from datetime import datetime, timedelta
from unittest.mock import MagicMock

# ── Stub Redis before any app imports ────────────────────────────────────────
mock_redis = MagicMock()
mock_redis.get.return_value = None
mock_redis.llen.return_value = 0
mock_redis.hgetall.return_value = {}
mock_redis.lrange.return_value = []
mock_redis.keys.return_value = []
mock_redis.zrevrange.return_value = []
mock_redis.zadd.return_value = 0
mock_redis.setex.return_value = True

import app.extensions
app.extensions.redis_client = mock_redis
app.extensions.r2_client = None
app.extensions.openai_client = None

# ── Create app (this also runs init_db → creates SQLite tables) ──────────────
from app import create_app
flask_app = create_app()

# ── Seed demo data into SQLite ───────────────────────────────────────────────
from app.database import get_session
from app.models.db_run import DbRun
from app.models.lead import Lead
from app.models.lead_run import LeadRun

DEMO_RUNS = [
    {
        'id': 'demo-run-001-abcd-1234-efgh',
        'platform': 'instagram',
        'status': 'completed',
        'filters': {'bio_phrase': 'travel', 'max_results': 500},
        'bdr_assignment': 'Miriam Plascencia, Majo Juarez, Nicole Roma',
        'profiles_found': 500,
        'profiles_pre_screened': 480,
        'profiles_enriched': 460,
        'profiles_scored': 450,
        'contacts_synced': 445,
        'duplicates_skipped': 32,
        'tier_distribution': {
            'auto_enroll': 45,
            'high_priority_review': 120,
            'standard_priority_review': 180,
            'low_priority_review': 105,
        },
        'error_count': 2,
        'created_at': datetime.now() - timedelta(hours=3),
        'finished_at': datetime.now() - timedelta(hours=1, minutes=45),
    },
    {
        'id': 'demo-run-002-wxyz-5678-ijkl',
        'platform': 'patreon',
        'status': 'analyzing',
        'filters': {'search_keywords': ['hiking', 'outdoor'], 'max_results': 200},
        'bdr_assignment': 'Sofia Gonzalez, Tanya Pina',
        'profiles_found': 200,
        'profiles_pre_screened': 185,
        'profiles_enriched': 185,
        'profiles_scored': 0,
        'contacts_synced': 0,
        'duplicates_skipped': 0,
        'tier_distribution': {},
        'error_count': 0,
        'created_at': datetime.now() - timedelta(minutes=30),
        'finished_at': None,
    },
    {
        'id': 'demo-run-003-mnop-9012-qrst',
        'platform': 'facebook',
        'status': 'failed',
        'filters': {'keywords': ['yoga']},
        'bdr_assignment': '',
        'profiles_found': 0,
        'profiles_pre_screened': 0,
        'profiles_enriched': 0,
        'profiles_scored': 0,
        'contacts_synced': 0,
        'duplicates_skipped': 0,
        'tier_distribution': {},
        'error_count': 1,
        'created_at': datetime.now() - timedelta(days=1),
        'finished_at': datetime.now() - timedelta(days=1) + timedelta(minutes=3),
    },
]

session = get_session()
try:
    for d in DEMO_RUNS:
        if session.get(DbRun, d['id']) is None:
            session.add(DbRun(**d))
    session.flush()

    # Seed demo leads + lead_runs for evaluation dashboard
    if session.query(Lead).count() == 0:
        import random
        random.seed(42)

        TIERS = ['auto_enroll', 'high_priority_review', 'standard_priority_review', 'low_priority_review']
        TIER_WEIGHTS = [0.12, 0.27, 0.39, 0.22]
        STAGES = ['discovery', 'pre_screen', 'enrichment', 'analysis', 'scoring', 'crm_sync']
        PLATFORMS = ['instagram', 'patreon']
        RUN_IDS = [DEMO_RUNS[0]['id'], DEMO_RUNS[1]['id']]

        lead_id_counter = 0
        for run_idx, run_id in enumerate(RUN_IDS):
            platform = PLATFORMS[run_idx]
            count = 450 if run_idx == 0 else 185

            for i in range(count):
                lead_id_counter += 1
                handle = f"demo_{platform}_{lead_id_counter}"

                lead = Lead(
                    platform=platform,
                    platform_id=handle,
                    name=f"Creator {lead_id_counter}",
                    profile_url=f"https://{platform}.com/{handle}",
                    bio=f"Demo bio for {handle}",
                    follower_count=random.randint(1000, 500000),
                    email=f"{handle}@example.com",
                )
                session.add(lead)
                session.flush()

                # Decide how far this lead got in the pipeline
                stage_idx = random.choices(range(len(STAGES)), weights=[5, 8, 6, 5, 38, 38])[0]
                stage_reached = STAGES[stage_idx]

                # Scored leads get a tier + score
                tier = None
                score = None
                if stage_idx >= 4:  # scoring or crm_sync
                    tier = random.choices(TIERS, weights=TIER_WEIGHTS)[0]
                    score = round(random.uniform(0.2, 0.95), 3)

                prescreen_result = None
                if stage_idx >= 2:
                    prescreen_result = 'passed'
                elif stage_idx == 1:
                    prescreen_result = random.choice(['passed', 'disqualified'])

                lead_run = LeadRun(
                    lead_id=lead.id,
                    run_id=run_id,
                    stage_reached=stage_reached,
                    prescreen_result=prescreen_result,
                    lead_score=score,
                    priority_tier=tier,
                    synced_to_crm=(stage_reached == 'crm_sync'),
                )
                session.add(lead_run)

    session.commit()
    print(f"[Preview] Seeded {session.query(Lead).count()} leads, {session.query(LeadRun).count()} lead_runs")
except Exception as e:
    session.rollback()
    print(f"[Preview] Seed error (may already exist): {e}")
finally:
    session.close()

# ── Stub launch_run so the discovery form doesn't crash ──────────────────────
from app.models.run import Run
from app.pipeline import manager as mgr

def _fake_launch(**kwargs):
    """Return a Run-like object seeded from demo data."""
    run = Run(
        id=DEMO_RUNS[0]['id'],
        platform=DEMO_RUNS[0]['platform'],
        status=DEMO_RUNS[0]['status'],
        filters=DEMO_RUNS[0]['filters'],
        bdr_assignment=DEMO_RUNS[0]['bdr_assignment'],
    )
    return run

mgr.launch_run = _fake_launch


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    print(f"\n  Preview server: http://localhost:{port}")
    print(f"  Pages:")
    print(f"    /              — Dashboard")
    print(f"    /discovery     — Discovery form")
    print(f"    /runs          — Pipeline runs list")
    print(f"    /evaluation    — Evaluation dashboard")
    print(f"  Demo runs:")
    print(f"    /runs/demo-run-001-abcd-1234-efgh  — Completed (Instagram)")
    print(f"    /runs/demo-run-002-wxyz-5678-ijkl  — In-progress (Patreon)")
    print(f"    /runs/demo-run-003-mnop-9012-qrst  — Failed (Facebook)")
    print()
    flask_app.run(host='0.0.0.0', port=port, debug=True)
