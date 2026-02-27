"""
Postgres persistence helpers — called from the pipeline manager.

All writes are wrapped in try/except so the pipeline never blocks on DB errors.
"""
import hashlib
import json
import logging
from datetime import datetime

from app.database import get_session
from app.models.db_run import DbRun
from app.models.lead import Lead
from app.models.lead_run import LeadRun
from app.models.filter_history import FilterHistory

logger = logging.getLogger('services.db')


def persist_run(run):
    """
    INSERT or UPDATE a run record in Postgres.

    Called twice:
      1. After run creation (INSERT)
      2. After run completes/fails (UPDATE)
    """
    session = get_session()
    try:
        db_run = session.get(DbRun, run.id)
        if db_run is None:
            db_run = DbRun(
                id=run.id,
                platform=run.platform,
                status=run.status,
                filters=run.filters,
                bdr_assignment=run.bdr_assignment,
                estimated_cost=run.estimated_cost or None,
                created_at=datetime.fromisoformat(run.created_at),
            )
            session.add(db_run)
        else:
            db_run.status = run.status
            db_run.current_stage = run.current_stage or ''
            db_run.profiles_found = run.profiles_found
            db_run.profiles_pre_screened = run.profiles_pre_screened
            db_run.profiles_enriched = run.profiles_enriched
            db_run.profiles_scored = run.profiles_scored
            db_run.contacts_synced = run.contacts_synced
            db_run.duplicates_skipped = run.duplicates_skipped
            db_run.tier_distribution = run.tier_distribution
            db_run.error_count = len(run.errors)
            db_run.summary = run.summary or None
            db_run.estimated_cost = run.estimated_cost or None
            db_run.actual_cost = run.actual_cost or None
            db_run.stage_outputs = run.stage_outputs or None
            if run.status in ('completed', 'failed'):
                db_run.finished_at = datetime.now()

        session.commit()
    except Exception:
        session.rollback()
        logger.error("Failed to persist run %s", run.id, exc_info=True)
    finally:
        session.close()


def persist_lead_results(run, profiles):
    """
    Bulk upsert leads + insert lead_run records after a run completes.

    Each profile dict flows through all stages and accumulates underscore-prefixed
    keys (_lead_analysis, _bio_evidence, etc). We extract what we need here.
    """
    session = get_session()
    try:
        for profile in profiles:
            platform_id = _extract_platform_id(profile, run.platform)
            if not platform_id:
                continue

            # Upsert lead
            lead = session.query(Lead).filter_by(
                platform=run.platform,
                platform_id=platform_id,
            ).first()

            if lead is None:
                lead = Lead(
                    platform=run.platform,
                    platform_id=platform_id,
                    name=profile.get('name', '') or profile.get('_first_name', ''),
                    profile_url=profile.get('url', '') or profile.get('profile_url', ''),
                    bio=profile.get('introduction', '') or profile.get('bio', ''),
                    follower_count=profile.get('follower_count', 0) or 0,
                    email=profile.get('email', ''),
                    website=profile.get('website', '') or profile.get('url', ''),
                    social_urls=profile.get('_social_urls', {}),
                )
                session.add(lead)
                session.flush()  # get lead.id
            else:
                # Update existing lead with latest data
                lead.name = profile.get('name', '') or profile.get('_first_name', '') or lead.name
                lead.bio = profile.get('introduction', '') or profile.get('bio', '') or lead.bio
                lead.follower_count = profile.get('follower_count', 0) or lead.follower_count
                lead.email = profile.get('email', '') or lead.email
                lead.last_seen_at = datetime.now()

            # Extract scoring data
            analysis = profile.get('_lead_analysis', {})
            prescreen_result = profile.get('_prescreen_result')
            prescreen_reason = profile.get('_prescreen_reason')

            # Determine stage_reached
            stage_reached = _determine_stage_reached(profile)

            # Build analysis evidence
            evidence = {}
            for key in ('_bio_evidence', '_caption_evidence', '_thumbnail_evidence', '_creator_profile'):
                val = profile.get(key)
                if val:
                    evidence[key.lstrip('_')] = val

            lead_run = LeadRun(
                lead_id=lead.id,
                run_id=run.id,
                stage_reached=stage_reached,
                prescreen_result=prescreen_result or ('passed' if stage_reached != 'pre_screen' and stage_reached != 'discovery' else None),
                prescreen_reason=prescreen_reason,
                analysis_evidence=evidence or None,
                lead_score=analysis.get('lead_score'),
                manual_score=analysis.get('manual_score'),
                section_scores=analysis.get('section_scores'),
                priority_tier=analysis.get('priority_tier'),
                score_reasoning=analysis.get('score_reasoning'),
                synced_to_crm=stage_reached == 'crm_sync',
            )
            session.add(lead_run)

        session.commit()
    except Exception:
        session.rollback()
        logger.error("Failed to persist lead results for run %s", run.id, exc_info=True)
    finally:
        session.close()


# ── Dedup helpers ────────────────────────────────────────────────────────────

def dedup_profiles(profiles, platform):
    """
    Remove profiles that already exist in the Lead table (cross-run dedup).

    Returns (new_profiles, duplicates_skipped_count).
    Dedup failure never blocks the pipeline.
    """
    try:
        session = get_session()
        try:
            new_profiles = []
            skipped = 0
            for profile in profiles:
                platform_id = _extract_platform_id(profile, platform)
                if not platform_id:
                    new_profiles.append(profile)
                    continue

                existing = session.query(Lead.id).filter_by(
                    platform=platform,
                    platform_id=platform_id,
                ).first()

                if existing:
                    skipped += 1
                else:
                    new_profiles.append(profile)

            logger.info("%d profiles in, %d new, %d already in DB", len(profiles), len(new_profiles), skipped)
            return new_profiles, skipped
        finally:
            session.close()
    except Exception:
        logger.error("Dedup failed — returning all profiles unfiltered", exc_info=True)
        return profiles, 0


# ── Filter fingerprinting ────────────────────────────────────────────────────

def make_filter_hash(platform, filters):
    """SHA-256 hash of normalized filters dict for staleness detection."""
    # Strip non-deterministic keys like bdr_names
    clean = {k: v for k, v in sorted(filters.items()) if k != 'bdr_names'}
    payload = json.dumps({'platform': platform, 'filters': clean}, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def record_filter_history(run, new_count, total_count):
    """Record filter fingerprint after discovery for staleness tracking."""
    try:
        session = get_session()
        try:
            fh = FilterHistory(
                filter_hash=make_filter_hash(run.platform, run.filters),
                platform=run.platform,
                run_id=run.id,
                total_found=total_count,
                new_found=new_count,
                novelty_rate=round(new_count / total_count, 3) if total_count > 0 else 0.0,
            )
            session.add(fh)
            session.commit()
        finally:
            session.close()
    except Exception:
        logger.error("Failed to record filter history for run %s", run.id, exc_info=True)


def get_filter_staleness(platform, filters):
    """
    Check if these filters have been used before and return staleness info.
    Returns dict or None if no history.
    """
    try:
        session = get_session()
        try:
            fhash = make_filter_hash(platform, filters)
            last = session.query(FilterHistory).filter_by(
                filter_hash=fhash,
            ).order_by(FilterHistory.ran_at.desc()).first()

            if not last:
                return None

            days_ago = (datetime.now() - (last.ran_at.replace(tzinfo=None) if last.ran_at else datetime.now())).days

            return {
                'last_run_days_ago': days_ago,
                'novelty_rate': round(last.novelty_rate * 100, 1),
                'total_found': last.total_found,
                'new_found': last.new_found,
            }
        finally:
            session.close()
    except Exception:
        return None


# ── Private helpers ──────────────────────────────────────────────────────────

def _extract_platform_id(profile, platform):
    """Get the unique identifier for a profile based on platform."""
    if platform == 'instagram':
        return profile.get('platform_username') or profile.get('username') or profile.get('handle')
    elif platform == 'patreon':
        return profile.get('slug') or profile.get('id') or profile.get('vanity')
    elif platform == 'facebook':
        return profile.get('group_id') or profile.get('id')
    return profile.get('id') or profile.get('platform_id')


def _determine_stage_reached(profile):
    """Determine the last pipeline stage a profile passed through."""
    if profile.get('_lead_analysis'):
        # Has scoring — check if synced
        return 'crm_sync' if profile.get('_synced_to_crm') else 'scoring'
    if profile.get('_creator_profile') or profile.get('_content_analyses'):
        return 'analysis'
    if profile.get('_social_data') or profile.get('enrichment_status') == 'success':
        return 'enrichment'
    if profile.get('_prescreen_result'):
        return 'pre_screen'
    return 'discovery'
