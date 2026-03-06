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
from app.models.enrollment_run import EnrollmentRun
from app.models.app_config import AppConfig

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
                run_type=getattr(run, 'run_type', 'discovery') or 'discovery',
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
            db_run.profiles_discovered = run.profiles_discovered
            db_run.profiles_found = run.profiles_found
            db_run.profiles_pre_screened = run.profiles_pre_screened
            db_run.profiles_enriched = run.profiles_enriched
            db_run.profiles_scored = run.profiles_scored
            db_run.contacts_synced = run.contacts_synced
            db_run.duplicates_skipped = run.duplicates_skipped
            db_run.hubspot_duplicates = run.hubspot_duplicates
            db_run.tier_distribution = run.tier_distribution
            db_run.error_count = len(run.errors)
            db_run.summary = run.summary or None
            db_run.estimated_cost = run.estimated_cost or None
            db_run.actual_cost = run.actual_cost or None
            db_run.stage_outputs = run.stage_outputs or None
            db_run.stage_timings = run.stage_timings or None
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
                logger.warning("Could not extract platform_id for profile keys: %s", list(profile.keys())[:10])
                continue

            # Upsert lead
            lead = session.query(Lead).filter_by(
                platform=run.platform,
                platform_id=platform_id,
            ).first()

            # Resolve fields — canonical keys first, then InsightIQ-style
            name = (profile.get('name', '')
                    or profile.get('first_and_last_name', '')
                    or profile.get('_first_name', ''))
            bio = (profile.get('introduction', '')
                   or profile.get('bio', '')
                   or profile.get('instagram_bio', ''))
            follower_count = (profile.get('follower_count', 0)
                              or _parse_int(profile.get('instagram_followers'))
                              or 0)
            email = profile.get('email', '')
            profile_url = (profile.get('url', '')
                           or profile.get('profile_url', '')
                           or profile.get('instagram_handle', ''))
            website = profile.get('website', '') or ''

            if lead is None:
                lead = Lead(
                    platform=run.platform,
                    platform_id=platform_id,
                    name=name,
                    profile_url=profile_url,
                    bio=bio,
                    follower_count=follower_count,
                    email=email,
                    website=website,
                    social_urls=profile.get('_social_urls', {}),
                )
                session.add(lead)
                session.flush()  # get lead.id
            else:
                # Update existing lead with latest data
                lead.name = name or lead.name
                lead.bio = bio or lead.bio
                lead.follower_count = follower_count or lead.follower_count
                lead.email = email or lead.email
                lead.last_seen_at = datetime.now()

            # Persist HubSpot contact ID if available
            hs_id = profile.get('_hubspot_contact_id')
            if hs_id:
                lead.hubspot_contact_id = hs_id

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


# ── App config store ─────────────────────────────────────────────────────────

def get_app_config(key: str):
    """Read a config value from Postgres. Returns dict/list or None."""
    session = get_session()
    try:
        row = session.query(AppConfig).filter_by(key=key).first()
        return row.value if row else None
    except Exception:
        logger.error("Failed to read app config key=%s", key, exc_info=True)
        return None
    finally:
        session.close()


def save_app_config(key: str, value) -> bool:
    """Upsert a config value in Postgres. Returns True on success."""
    session = get_session()
    try:
        row = session.query(AppConfig).filter_by(key=key).first()
        if row:
            row.value = value
        else:
            row = AppConfig(key=key, value=value)
            session.add(row)
        session.commit()
        return True
    except Exception:
        session.rollback()
        logger.error("Failed to save app config key=%s", key, exc_info=True)
        return False
    finally:
        session.close()


# ── Enrollment persistence ───────────────────────────────────────────────────

def persist_enrollment_run(summary: dict):
    """Insert an enrollment dispatch run record into Postgres."""
    session = get_session()
    try:
        run = EnrollmentRun(
            status=summary.get('status', 'error'),
            reason=summary.get('reason'),
            enrolled_count=summary.get('enrolled_count', 0),
            error_count=summary.get('error_count', 0),
            active_count=summary.get('active_count', 0),
            queued_count=summary.get('queued_count', 0),
            total_slots=summary.get('total_slots', 0),
            allocation=summary.get('allocation'),
            enrolled_details=summary.get('enrolled_details'),
            errors=summary.get('errors'),
            dry_run=summary.get('dry_run', False),
            run_date=summary.get('run_date'),
            finished_at=summary.get('finished_at'),
        )
        session.add(run)
        session.commit()
        return run.id
    except Exception:
        session.rollback()
        logger.error("Failed to persist enrollment run", exc_info=True)
        return None
    finally:
        session.close()


def get_enrollment_history(limit: int = 20) -> list:
    """Fetch recent enrollment runs from Postgres, newest first."""
    session = get_session()
    try:
        rows = (
            session.query(EnrollmentRun)
            .order_by(EnrollmentRun.started_at.desc())
            .limit(limit)
            .all()
        )
        results = []
        for r in rows:
            results.append({
                'id': r.id,
                'status': r.status,
                'reason': r.reason,
                'enrolled_count': r.enrolled_count,
                'error_count': r.error_count,
                'active_count': r.active_count,
                'queued_count': r.queued_count,
                'total_slots': r.total_slots,
                'allocation': r.allocation,
                'enrolled_details': r.enrolled_details,
                'errors': r.errors,
                'dry_run': r.dry_run,
                'run_date': r.run_date.isoformat() if r.run_date else None,
                'started_at': r.started_at.isoformat() if r.started_at else None,
                'finished_at': r.finished_at.isoformat() if r.finished_at else None,
            })
        return results
    except Exception:
        logger.error("Failed to fetch enrollment history", exc_info=True)
        return []
    finally:
        session.close()


# ── Private helpers ──────────────────────────────────────────────────────────

def _extract_platform_id(profile, platform):
    """Get the unique identifier for a profile based on platform.

    Handles both canonical keys (platform_username) and InsightIQ-style
    keys (instagram_handle — a full URL).
    """
    if platform == 'instagram':
        # Try canonical keys first, then InsightIQ-style URL
        pid = (profile.get('platform_username')
               or profile.get('username')
               or profile.get('handle'))
        if not pid:
            ig_handle = profile.get('instagram_handle', '')
            # instagram_handle may be a full URL like https://www.instagram.com/mollyyeh/
            if ig_handle:
                pid = ig_handle.rstrip('/').split('/')[-1]
        return pid
    elif platform == 'patreon':
        return profile.get('slug') or profile.get('id') or profile.get('vanity')
    elif platform == 'facebook':
        return profile.get('group_id') or profile.get('id')
    return profile.get('id') or profile.get('platform_id')


def _parse_int(value):
    """Safely parse an int from a string or number. Returns 0 on failure."""
    if value is None:
        return 0
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


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
