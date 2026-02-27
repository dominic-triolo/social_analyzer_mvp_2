#!/usr/bin/env python3
"""
Seed test data for verifying UI features locally.

Creates realistic runs + leads covering key scenarios:
  1. Successful run with full funnel
  2. Failed run at enrichment
  3. Retried run (child of #2)
  4. Run with warnings (low yield, cost overrun)
  5. Zero-results run

Usage:
    python scripts/seed_test_data.py          # seed all scenarios
    python scripts/seed_test_data.py --clear  # wipe seeded data first

Requires: Redis running, DATABASE_URL set (or defaults to sqlite:///local.db).
"""
import sys
import os
import uuid
import argparse
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import create_app
from app.database import get_session, engine, Base
from app.models.db_run import DbRun
from app.models.lead import Lead
from app.models.lead_run import LeadRun
from app.models.run import Run


# ── Fake creators ────────────────────────────────────────────────────────────

CREATORS = [
    {'handle': 'wanderlust_jane',    'name': 'Jane Morrison',     'followers': 82000,  'category': 'Travel',    'score': 0.91, 'tier': 'auto_enroll'},
    {'handle': 'trail_blazer_mik',   'name': 'Mik Andersen',      'followers': 45000,  'category': 'Travel',    'score': 0.87, 'tier': 'auto_enroll'},
    {'handle': 'nomad.sophie',       'name': 'Sophie Laurent',     'followers': 120000, 'category': 'Travel',    'score': 0.84, 'tier': 'auto_enroll'},
    {'handle': 'adventurecalling',   'name': 'Carlos Reyes',       'followers': 34000,  'category': 'Travel',    'score': 0.82, 'tier': 'auto_enroll'},
    {'handle': 'explore_with_priya', 'name': 'Priya Sharma',       'followers': 67000,  'category': 'Travel',    'score': 0.79, 'tier': 'high_priority_review'},
    {'handle': 'thebackpackdiaries', 'name': 'Liam O\'Brien',      'followers': 29000,  'category': 'Travel',    'score': 0.76, 'tier': 'high_priority_review'},
    {'handle': 'travelwithtam',      'name': 'Tamara Johansson',   'followers': 51000,  'category': 'Travel',    'score': 0.73, 'tier': 'high_priority_review'},
    {'handle': 'sunsetseeker_',      'name': 'Aisha Mohammed',     'followers': 15000,  'category': 'Lifestyle', 'score': 0.68, 'tier': 'standard_priority_review'},
    {'handle': 'roam.and.rest',      'name': 'Emma Chen',          'followers': 22000,  'category': 'Wellness',  'score': 0.65, 'tier': 'standard_priority_review'},
    {'handle': 'passportpages',      'name': 'Derek Williams',     'followers': 8000,   'category': 'Travel',    'score': 0.52, 'tier': 'low_priority_review'},
    {'handle': 'coastal_vibes_co',   'name': 'Natalia Torres',     'followers': 95000,  'category': 'Travel',    'score': 0.89, 'tier': 'auto_enroll'},
    {'handle': 'hike.eat.repeat',    'name': 'Jonas Müller',       'followers': 41000,  'category': 'Fitness',   'score': 0.71, 'tier': 'standard_priority_review'},
]

# Prefix for seeded IDs so we can clear them
SEED_PREFIX = 'seed-'


def make_id():
    return SEED_PREFIX + str(uuid.uuid4())


def _save_run_to_redis(run_data: dict):
    """Save a run dict to Redis via the Run model."""
    run = Run.__new__(Run)
    for k, v in run_data.items():
        setattr(run, k, v)
    run.save()


def _make_leads(session, creators, platform, run_id, stage_reached='crm_sync', synced=True):
    """Insert Lead + LeadRun records for a list of creators."""
    for c in creators:
        # Upsert lead
        existing = session.query(Lead).filter_by(
            platform=platform, platform_id=c['handle']
        ).first()
        if existing:
            lead = existing
        else:
            lead = Lead(
                platform=platform,
                platform_id=c['handle'],
                name=c['name'],
                profile_url=f'https://instagram.com/{c["handle"]}',
                bio=f'{c["category"]} creator | {c["followers"]//1000}k followers',
                follower_count=c['followers'],
                email=f'{c["handle"]}@example.com',
            )
            session.add(lead)
            session.flush()

        lead_run = LeadRun(
            lead_id=lead.id,
            run_id=run_id,
            stage_reached=stage_reached,
            prescreen_result='passed',
            prescreen_reason='Active content, good engagement',
            lead_score=c['score'],
            manual_score=c['score'] - 0.03,
            section_scores={
                'niche_alignment': round(c['score'] + 0.05, 2),
                'authenticity': round(c['score'] - 0.02, 2),
                'engagement': round(c['score'] + 0.01, 2),
                'reach': round(c['score'] - 0.05, 2),
            },
            priority_tier=c['tier'],
            score_reasoning=f'Strong {c["category"].lower()} creator with {c["followers"]//1000}k engaged followers.',
            synced_to_crm=synced,
        )
        session.add(lead_run)


# ── Scenarios ────────────────────────────────────────────────────────────────

def seed_successful_run(session):
    """Scenario 1: Full funnel, everything works."""
    run_id = make_id()
    now = datetime.now(timezone.utc)
    creators = CREATORS[:10]

    tiers = {'auto_enroll': 0, 'high_priority_review': 0, 'standard_priority_review': 0, 'low_priority_review': 0}
    for c in creators:
        tiers[c['tier']] += 1

    summary = (
        f'Discovered 250 Instagram profiles (38 duplicates removed). '
        f'180 of 250 passed pre-screen (72% yield). '
        f'{len(creators)} synced to CRM — {tiers["auto_enroll"]} auto-enroll, '
        f'{tiers["high_priority_review"]} high priority. '
        f'4% overall conversion. ~$8.42 spent.'
    )

    run_data = {
        'id': run_id,
        'status': 'completed',
        'platform': 'instagram',
        'created_at': (now - timedelta(hours=2)).isoformat(),
        'updated_at': (now - timedelta(hours=1, minutes=45)).isoformat(),
        'current_stage': 'crm_sync',
        'stage_progress': {
            'discovery':   {'total': 250, 'completed': 250, 'failed': 0},
            'pre_screen':  {'total': 212, 'completed': 180, 'failed': 32},
            'enrichment':  {'total': 180, 'completed': 172, 'failed': 8},
            'analysis':    {'total': 172, 'completed': 172, 'failed': 0},
            'scoring':     {'total': 172, 'completed': 172, 'failed': 0},
            'crm_sync':    {'total': 10,  'completed': 10,  'failed': 0},
        },
        'filters': {'max_results': 250, 'category': 'Travel', 'min_followers': 5000, 'max_followers': 200000, 'country': 'US'},
        'profiles_found': 250,
        'duplicates_skipped': 38,
        'profiles_pre_screened': 180,
        'profiles_enriched': 172,
        'profiles_scored': 172,
        'contacts_synced': 10,
        'bdr_assignment': 'Miriam Plascencia, Majo Juarez',
        'errors': [
            {'stage': 'pre_screen', 'message': 'Below minimum engagement rate (0.8%)', 'profile_id': 'inactive_user_1', 'timestamp': (now - timedelta(hours=1, minutes=50)).isoformat()},
            {'stage': 'enrichment', 'message': 'InsightIQ: profile not found', 'profile_id': 'deleted_acct_3', 'timestamp': (now - timedelta(hours=1, minutes=48)).isoformat()},
        ],
        'tier_distribution': tiers,
        'summary': summary,
        'estimated_cost': 9.00,
        'actual_cost': 8.42,
        'stage_outputs': {},
    }

    # Redis
    _save_run_to_redis(run_data)

    # Postgres
    db_run = DbRun(
        id=run_id, platform='instagram', status='completed',
        current_stage='crm_sync',
        filters=run_data['filters'], bdr_assignment=run_data['bdr_assignment'],
        profiles_found=250, profiles_pre_screened=180, profiles_enriched=172,
        profiles_scored=172, contacts_synced=10, duplicates_skipped=38,
        tier_distribution=tiers, summary=summary,
        estimated_cost=9.00, actual_cost=8.42,
        created_at=now - timedelta(hours=2), finished_at=now - timedelta(hours=1, minutes=45),
    )
    session.add(db_run)
    session.flush()

    _make_leads(session, creators, 'instagram', run_id)
    print(f'  [1] Successful run: {run_id}')
    return run_id


def seed_failed_run(session):
    """Scenario 2: Fails at enrichment with partial progress."""
    run_id = make_id()
    now = datetime.now(timezone.utc)

    summary = (
        'Instagram run failed at enrichment stage after processing 45 of 120 profiles. '
        'Before failure: discovered 200 (15 duplicates removed), 120 passed pre-screen. '
        '~$3.80 spent before failure. Error: InsightIQ rate limit exceeded (429)'
    )

    run_data = {
        'id': run_id,
        'status': 'failed',
        'platform': 'instagram',
        'created_at': (now - timedelta(hours=5)).isoformat(),
        'updated_at': (now - timedelta(hours=4, minutes=30)).isoformat(),
        'current_stage': 'enrichment',
        'stage_progress': {
            'discovery':   {'total': 200, 'completed': 200, 'failed': 0},
            'pre_screen':  {'total': 185, 'completed': 120, 'failed': 65},
            'enrichment':  {'total': 120, 'completed': 45,  'failed': 1},
            'analysis':    {'total': 0,   'completed': 0,   'failed': 0},
            'scoring':     {'total': 0,   'completed': 0,   'failed': 0},
            'crm_sync':    {'total': 0,   'completed': 0,   'failed': 0},
        },
        'filters': {'max_results': 200, 'category': 'Travel', 'min_followers': 10000, 'country': 'US'},
        'profiles_found': 200,
        'duplicates_skipped': 15,
        'profiles_pre_screened': 120,
        'profiles_enriched': 45,
        'profiles_scored': 0,
        'contacts_synced': 0,
        'bdr_assignment': 'Nicole Roma',
        'errors': [
            {'stage': 'enrichment', 'message': 'InsightIQ rate limit exceeded (429)', 'profile_id': 'creator_46', 'timestamp': (now - timedelta(hours=4, minutes=30)).isoformat()},
        ],
        'tier_distribution': {'auto_enroll': 0, 'high_priority_review': 0, 'standard_priority_review': 0, 'low_priority_review': 0},
        'summary': summary,
        'estimated_cost': 7.50,
        'actual_cost': 3.80,
        'stage_outputs': {},
    }

    _save_run_to_redis(run_data)

    db_run = DbRun(
        id=run_id, platform='instagram', status='failed',
        current_stage='enrichment',
        filters=run_data['filters'], bdr_assignment='Nicole Roma',
        profiles_found=200, profiles_pre_screened=120, profiles_enriched=45,
        profiles_scored=0, contacts_synced=0, duplicates_skipped=15,
        tier_distribution=run_data['tier_distribution'], summary=summary,
        estimated_cost=7.50, actual_cost=3.80, error_count=1,
        created_at=now - timedelta(hours=5), finished_at=now - timedelta(hours=4, minutes=30),
    )
    session.add(db_run)
    session.flush()

    print(f'  [2] Failed run:     {run_id}')
    return run_id


def seed_retried_run(session, parent_run_id):
    """Scenario 3: Retried from enrichment (child of the failed run)."""
    run_id = make_id()
    now = datetime.now(timezone.utc)
    creators = CREATORS[:6]

    tiers = {'auto_enroll': 0, 'high_priority_review': 0, 'standard_priority_review': 0, 'low_priority_review': 0}
    for c in creators:
        tiers[c['tier']] += 1

    summary = (
        f'Discovered 200 Instagram profiles (15 duplicates removed). '
        f'120 of 200 passed pre-screen (60% yield). '
        f'{len(creators)} synced to CRM — {tiers["auto_enroll"]} auto-enroll, '
        f'{tiers["high_priority_review"]} high priority. '
        f'3% overall conversion. ~$6.10 spent.'
    )

    run_data = {
        'id': run_id,
        'status': 'completed',
        'platform': 'instagram',
        'created_at': (now - timedelta(hours=4)).isoformat(),
        'updated_at': (now - timedelta(hours=3, minutes=30)).isoformat(),
        'current_stage': 'crm_sync',
        'stage_progress': {
            'discovery':   {'total': 0,   'completed': 0,   'failed': 0},  # skipped (retried)
            'pre_screen':  {'total': 0,   'completed': 0,   'failed': 0},  # skipped (retried)
            'enrichment':  {'total': 120, 'completed': 118, 'failed': 2},
            'analysis':    {'total': 118, 'completed': 118, 'failed': 0},
            'scoring':     {'total': 118, 'completed': 118, 'failed': 0},
            'crm_sync':    {'total': 6,   'completed': 6,   'failed': 0},
        },
        'filters': {
            'max_results': 200, 'category': 'Travel', 'min_followers': 10000, 'country': 'US',
            '_retry_from': 'enrichment',
            '_parent_run_id': parent_run_id,
        },
        'profiles_found': 200,
        'duplicates_skipped': 15,
        'profiles_pre_screened': 120,
        'profiles_enriched': 118,
        'profiles_scored': 118,
        'contacts_synced': 6,
        'bdr_assignment': 'Nicole Roma',
        'errors': [],
        'tier_distribution': tiers,
        'summary': summary,
        'estimated_cost': 5.00,
        'actual_cost': 6.10,
        'stage_outputs': {},
    }

    _save_run_to_redis(run_data)

    db_run = DbRun(
        id=run_id, platform='instagram', status='completed',
        current_stage='crm_sync',
        filters=run_data['filters'], bdr_assignment='Nicole Roma',
        profiles_found=200, profiles_pre_screened=120, profiles_enriched=118,
        profiles_scored=118, contacts_synced=6, duplicates_skipped=15,
        tier_distribution=tiers, summary=summary,
        estimated_cost=5.00, actual_cost=6.10,
        created_at=now - timedelta(hours=4), finished_at=now - timedelta(hours=3, minutes=30),
    )
    session.add(db_run)
    session.flush()

    _make_leads(session, creators, 'instagram', run_id)
    print(f'  [3] Retried run:    {run_id} (parent: {parent_run_id[:14]}...)')
    return run_id


def seed_warning_run(session):
    """Scenario 4: Low yield + cost overrun + no auto-enrolls."""
    run_id = make_id()
    now = datetime.now(timezone.utc)
    creators = CREATORS[7:10]  # standard + low priority only

    tiers = {'auto_enroll': 0, 'high_priority_review': 0, 'standard_priority_review': 2, 'low_priority_review': 1}

    summary = (
        'Discovered 300 Instagram profiles (22 duplicates removed). '
        '42 of 300 passed pre-screen (14% yield). '
        '3 synced to CRM — 2 standard, 1 low priority. '
        '1% overall conversion. ~$18.90 spent. '
        'Warning: Pre-screen yield was low at 14%. '
        'No auto-enroll candidates found. '
        'Cost exceeded estimate by 51%.'
    )

    run_data = {
        'id': run_id,
        'status': 'completed',
        'platform': 'instagram',
        'created_at': (now - timedelta(hours=8)).isoformat(),
        'updated_at': (now - timedelta(hours=7)).isoformat(),
        'current_stage': 'crm_sync',
        'stage_progress': {
            'discovery':   {'total': 300, 'completed': 300, 'failed': 0},
            'pre_screen':  {'total': 278, 'completed': 42,  'failed': 236},
            'enrichment':  {'total': 42,  'completed': 38,  'failed': 4},
            'analysis':    {'total': 38,  'completed': 38,  'failed': 0},
            'scoring':     {'total': 38,  'completed': 38,  'failed': 0},
            'crm_sync':    {'total': 3,   'completed': 3,   'failed': 0},
        },
        'filters': {'max_results': 300, 'category': 'Fitness', 'min_followers': 5000, 'country': 'US'},
        'profiles_found': 300,
        'duplicates_skipped': 22,
        'profiles_pre_screened': 42,
        'profiles_enriched': 38,
        'profiles_scored': 38,
        'contacts_synced': 3,
        'bdr_assignment': 'Sofia Gonzalez',
        'errors': [
            {'stage': 'pre_screen', 'message': 'Below minimum engagement rate', 'profile_id': 'fitness_bro_99', 'timestamp': (now - timedelta(hours=7, minutes=40)).isoformat()},
            {'stage': 'enrichment', 'message': 'Apollo: email not found', 'profile_id': 'gym_life_22', 'timestamp': (now - timedelta(hours=7, minutes=20)).isoformat()},
        ],
        'tier_distribution': tiers,
        'summary': summary,
        'estimated_cost': 12.50,
        'actual_cost': 18.90,
        'stage_outputs': {},
    }

    _save_run_to_redis(run_data)

    db_run = DbRun(
        id=run_id, platform='instagram', status='completed',
        current_stage='crm_sync',
        filters=run_data['filters'], bdr_assignment='Sofia Gonzalez',
        profiles_found=300, profiles_pre_screened=42, profiles_enriched=38,
        profiles_scored=38, contacts_synced=3, duplicates_skipped=22,
        tier_distribution=tiers, summary=summary,
        estimated_cost=12.50, actual_cost=18.90,
        created_at=now - timedelta(hours=8), finished_at=now - timedelta(hours=7),
    )
    session.add(db_run)
    session.flush()

    _make_leads(session, creators, 'instagram', run_id, stage_reached='crm_sync')
    print(f'  [4] Warning run:    {run_id}')
    return run_id


def seed_zero_results_run(session):
    """Scenario 5: No profiles found."""
    run_id = make_id()
    now = datetime.now(timezone.utc)

    summary = 'No Instagram profiles found. Check filters and try again.'

    run_data = {
        'id': run_id,
        'status': 'completed',
        'platform': 'instagram',
        'created_at': (now - timedelta(hours=12)).isoformat(),
        'updated_at': (now - timedelta(hours=12)).isoformat(),
        'current_stage': 'discovery',
        'stage_progress': {
            'discovery':   {'total': 0, 'completed': 0, 'failed': 0},
            'pre_screen':  {'total': 0, 'completed': 0, 'failed': 0},
            'enrichment':  {'total': 0, 'completed': 0, 'failed': 0},
            'analysis':    {'total': 0, 'completed': 0, 'failed': 0},
            'scoring':     {'total': 0, 'completed': 0, 'failed': 0},
            'crm_sync':    {'total': 0, 'completed': 0, 'failed': 0},
        },
        'filters': {'max_results': 50, 'category': 'Underwater Basket Weaving', 'min_followers': 500000, 'country': 'AQ'},
        'profiles_found': 0,
        'duplicates_skipped': 0,
        'profiles_pre_screened': 0,
        'profiles_enriched': 0,
        'profiles_scored': 0,
        'contacts_synced': 0,
        'bdr_assignment': 'Tanya Pina',
        'errors': [],
        'tier_distribution': {'auto_enroll': 0, 'high_priority_review': 0, 'standard_priority_review': 0, 'low_priority_review': 0},
        'summary': summary,
        'estimated_cost': 1.80,
        'actual_cost': 0.15,
        'stage_outputs': {},
    }

    _save_run_to_redis(run_data)

    db_run = DbRun(
        id=run_id, platform='instagram', status='completed',
        current_stage='discovery',
        filters=run_data['filters'], bdr_assignment='Tanya Pina',
        profiles_found=0, profiles_pre_screened=0, profiles_enriched=0,
        profiles_scored=0, contacts_synced=0, duplicates_skipped=0,
        tier_distribution=run_data['tier_distribution'], summary=summary,
        estimated_cost=1.80, actual_cost=0.15,
        created_at=now - timedelta(hours=12), finished_at=now - timedelta(hours=12),
    )
    session.add(db_run)
    session.flush()

    print(f'  [5] Zero results:   {run_id}')
    return run_id


# ── Clear / Main ─────────────────────────────────────────────────────────────

def clear_seeded_data(session):
    """Remove all seeded runs, leads, and lead_runs."""
    # Find seeded run IDs
    seeded_runs = session.query(DbRun).filter(DbRun.id.like(f'{SEED_PREFIX}%')).all()
    run_ids = [r.id for r in seeded_runs]

    if not run_ids:
        print('No seeded data found.')
        return

    # Delete lead_runs for seeded runs
    deleted_lr = session.query(LeadRun).filter(LeadRun.run_id.in_(run_ids)).delete(synchronize_session=False)
    # Delete the runs
    deleted_runs = session.query(DbRun).filter(DbRun.id.in_(run_ids)).delete(synchronize_session=False)
    session.commit()

    # Clear from Redis
    for run_id in run_ids:
        Run.delete(run_id)

    print(f'Cleared {deleted_runs} runs, {deleted_lr} lead_runs from DB + Redis.')


def main():
    parser = argparse.ArgumentParser(description='Seed test data for UI verification')
    parser.add_argument('--clear', action='store_true', help='Clear seeded data before (or instead of) seeding')
    parser.add_argument('--clear-only', action='store_true', help='Only clear, do not re-seed')
    args = parser.parse_args()

    app = create_app()
    with app.app_context():
        # Ensure tables exist (for SQLite local dev)
        Base.metadata.create_all(engine)

        session = get_session()
        try:
            if args.clear or args.clear_only:
                clear_seeded_data(session)
                if args.clear_only:
                    return

            print('Seeding test data...')
            seed_successful_run(session)
            failed_id = seed_failed_run(session)
            seed_retried_run(session, failed_id)
            seed_warning_run(session)
            seed_zero_results_run(session)
            session.commit()
            print('\nDone! Visit http://localhost:5001/runs to verify.')

        except Exception as e:
            session.rollback()
            print(f'Error: {e}')
            raise
        finally:
            session.close()


if __name__ == '__main__':
    main()
