"""
Enrollment Dispatcher — moves HubSpot contacts from queued → active in sequences.

Distributes enrollments across inboxes under daily send limits.
Runs as an RQ job, triggered by external cron hitting POST /api/enrollment/dispatch.
"""
import json
import logging
import time
from datetime import datetime, date
from zoneinfo import ZoneInfo

from app.services.enrollment_config import load_enrollment_config
from app.extensions import redis_client as r
from app.services.enrollment_helpers import (
    is_business_day,
    build_committed_schedule,
    compute_total_available_slots,
    available_slots_for_inbox,
    allocate_slots_by_weight,
    best_inbox_for_enrollment,
    update_committed_for_enrollment,
)

logger = logging.getLogger('services.enrollment_dispatcher')

LOCK_KEY = 'enrollment:lock'
LOCK_TTL = 600  # 10 minutes
LAST_RUN_KEY = 'enrollment:last_run'
HISTORY_KEY = 'enrollment:run_history'
HISTORY_MAX = 50


def run_enrollment_dispatcher(force=False, dry_run=False):
    """Main entry point — called as an RQ job.

    1. Acquire Redis lock
    2. Check business day
    3. Fetch active + queued contacts from HubSpot
    4. Compute capacity and allocate slots
    5. Enroll contacts (update HubSpot properties)
    6. Persist results to Redis + Postgres

    Returns summary dict.
    """
    # Load config at runtime so DB/env changes take effect without redeploy
    cfg = load_enrollment_config()
    inboxes = cfg['inboxes']
    max_per_day = cfg['max_per_day']
    cadence = cfg['sequence_cadence']
    weights = cfg['outreach_weights']
    api_delay = cfg.get('api_delay', 0.1)
    hs = cfg['hubspot_properties']
    status_f = hs['status_field']
    inbox_f = hs['inbox_field']
    date_f = hs['date_field']
    segment_f = hs['segment_field']
    trigger_f = hs['trigger_field']
    score_f = hs['score_field']
    createdate_f = hs['createdate_field']

    tz = ZoneInfo(cfg['timezone'])
    now = datetime.now(tz)
    today = now.date()

    summary = {
        'status': 'error',
        'run_date': today.isoformat(),
        'started_at': now.isoformat(),
        'dry_run': dry_run,
        'enrolled_count': 0,
        'error_count': 0,
        'active_count': 0,
        'queued_count': 0,
        'total_slots': 0,
        'allocation': {},
        'enrolled_details': [],
        'errors': [],
    }

    # Acquire lock
    if not _acquire_lock():
        summary['status'] = 'skipped'
        summary['reason'] = 'concurrent_run'
        summary['finished_at'] = datetime.now(tz).isoformat()
        _save_results(summary)
        return summary

    try:
        # Check business day
        if not force and not is_business_day(today):
            summary['status'] = 'skipped'
            summary['reason'] = 'not_business_day'
            summary['finished_at'] = datetime.now(tz).isoformat()
            _save_results(summary)
            return summary

        # Check configuration
        if not inboxes:
            summary['status'] = 'skipped'
            summary['reason'] = 'no_inboxes_configured'
            summary['finished_at'] = datetime.now(tz).isoformat()
            _save_results(summary)
            return summary

        # Fetch contacts from HubSpot
        from app.services.hubspot import hubspot_search_contacts_all

        active_contacts_raw = hubspot_search_contacts_all(
            filters=[
                {'propertyName': status_f, 'operator': 'EQ', 'value': 'active'},
            ],
            properties=['email', status_f, inbox_f, date_f, segment_f],
        )

        queued_contacts_raw = hubspot_search_contacts_all(
            filters=[
                {'propertyName': status_f, 'operator': 'EQ', 'value': 'queued'},
            ],
            properties=['email', status_f, segment_f, score_f, createdate_f],
        )

        summary['active_count'] = len(active_contacts_raw)
        summary['queued_count'] = len(queued_contacts_raw)

        if not queued_contacts_raw:
            summary['status'] = 'skipped'
            summary['reason'] = 'no_queued_contacts'
            summary['finished_at'] = datetime.now(tz).isoformat()
            _save_results(summary)
            return summary

        # Build committed schedule from active contacts
        active_for_schedule = []
        for c in active_contacts_raw:
            props = c.get('properties', {})
            enroll_date_str = props.get(date_f, '')
            if enroll_date_str:
                try:
                    enroll_date = date.fromisoformat(enroll_date_str)
                except (ValueError, TypeError):
                    continue
                active_for_schedule.append({
                    'inbox': props.get(inbox_f, ''),
                    'enrollment_date': enroll_date,
                })

        committed = build_committed_schedule(active_for_schedule, cadence)

        # Compute capacity
        total_slots = compute_total_available_slots(
            inboxes, today, committed, cadence, max_per_day,
        )
        summary['total_slots'] = total_slots

        if total_slots == 0:
            summary['status'] = 'skipped'
            summary['reason'] = 'no_capacity'
            summary['finished_at'] = datetime.now(tz).isoformat()
            _save_results(summary)
            return summary

        # Sort queued contacts: lead score DESC, create date ASC (tiebreaker)
        def _sort_key(c):
            props = c.get('properties', {})
            try:
                score = float(props.get(score_f) or 0)
            except (ValueError, TypeError):
                score = 0.0
            createdate = props.get(createdate_f, '') or ''
            return (-score, createdate)

        queued_contacts_raw.sort(key=_sort_key)

        # Bucket contacts by segment (known vs unknown)
        queue_depths = {}
        queued_by_segment = {}
        queued_unknown = []
        for c in queued_contacts_raw:
            props = c.get('properties', {})
            segment = props.get(segment_f, '') or ''
            if segment in weights:
                queue_depths[segment] = queue_depths.get(segment, 0) + 1
                queued_by_segment.setdefault(segment, []).append(c)
            else:
                queued_unknown.append(c)

        # Allocate slots for known segments
        allocation = allocate_slots_by_weight(
            total_slots, weights, queue_depths
        )
        summary['allocation'] = allocation

        # Enroll contacts
        from app.services.hubspot import hubspot_update_contact

        enrolled_so_far = 0

        for segment, count in allocation.items():
            contacts_to_enroll = queued_by_segment.get(segment, [])[:count]

            for contact in contacts_to_enroll:
                enrolled_so_far = _enroll_contact(
                    contact, segment, today, committed, inboxes, cadence,
                    max_per_day, dry_run, api_delay, status_f, inbox_f,
                    date_f, trigger_f, hubspot_update_contact, summary,
                    enrolled_so_far,
                )

        # Fill remaining capacity with unknown-segment contacts (D2)
        remaining_slots = total_slots - summary['enrolled_count'] - summary['error_count']
        if remaining_slots > 0 and queued_unknown:
            overflow_contacts = queued_unknown[:remaining_slots]
            summary['allocation']['_unknown'] = len(overflow_contacts)
            for contact in overflow_contacts:
                enrolled_so_far = _enroll_contact(
                    contact, '_unknown', today, committed, inboxes, cadence,
                    max_per_day, dry_run, api_delay, status_f, inbox_f,
                    date_f, trigger_f, hubspot_update_contact, summary,
                    enrolled_so_far,
                )

        # Per-inbox remaining capacity (D5)
        inbox_capacity = {}
        for inbox_name in inboxes:
            inbox_capacity[inbox_name] = available_slots_for_inbox(
                inbox_name, today, committed, cadence, max_per_day,
            )
        summary['inbox_capacity'] = inbox_capacity

        summary['status'] = 'completed'
        summary['finished_at'] = datetime.now(tz).isoformat()
        _save_results(summary)
        return summary

    except Exception as e:
        logger.error("Enrollment dispatcher error: %s", e, exc_info=True)
        summary['status'] = 'error'
        summary['reason'] = str(e)
        summary['errors'].append(str(e))
        summary['finished_at'] = datetime.now(tz).isoformat()
        _save_results(summary)
        return summary

    finally:
        _release_lock()


def get_last_run():
    """Read the last enrollment run summary from Redis."""
    try:
        data = r.get(LAST_RUN_KEY)
        if data:
            return json.loads(data)
    except Exception as e:
        logger.error("Error reading last enrollment run: %s", e)
    return None


def get_run_history(limit=20):
    """Read recent enrollment run history.

    Tries Redis first (hot), falls back to Postgres for older runs.
    """
    # Try Redis first
    try:
        raw = r.lrange(HISTORY_KEY, 0, limit - 1)
        if raw:
            return [json.loads(item) for item in raw]
    except Exception as e:
        logger.error("Error reading enrollment history from Redis: %s", e)

    # Fallback to Postgres
    from app.services.db import get_enrollment_history
    return get_enrollment_history(limit)


# ── Private helpers ──────────────────────────────────────────────────────────

def _enroll_contact(contact, segment, today, committed, inboxes, cadence,
                    max_per_day, dry_run, api_delay, status_f, inbox_f,
                    date_f, trigger_f, hubspot_update_contact, summary,
                    enrolled_so_far):
    """Enroll a single contact. Returns updated enrolled_so_far count."""
    contact_id = contact.get('id')
    if not contact_id:
        return enrolled_so_far

    inbox = best_inbox_for_enrollment(
        today, committed, inboxes, cadence, max_per_day,
    )
    if not inbox:
        summary['errors'].append(f'No inbox capacity for contact {contact_id}')
        summary['error_count'] += 1
        return enrolled_so_far

    if dry_run:
        summary['enrolled_details'].append({
            'contact_id': contact_id,
            'inbox': inbox,
            'segment': segment,
            'dry_run': True,
        })
        summary['enrolled_count'] += 1
    else:
        # Rate limit between CRM calls (D4)
        if enrolled_so_far > 0:
            time.sleep(api_delay)

        owner_id = inboxes.get(inbox, '')
        success = hubspot_update_contact(contact_id, {
            status_f: 'active',
            inbox_f: inbox,
            date_f: today.isoformat(),
            'hubspot_owner_id': owner_id,
            trigger_f: 'true',              # D3: trigger Reply.io pickup
        })

        if success:
            summary['enrolled_details'].append({
                'contact_id': contact_id,
                'inbox': inbox,
                'segment': segment,
            })
            summary['enrolled_count'] += 1
        else:
            summary['errors'].append(f'Failed to update contact {contact_id}')
            summary['error_count'] += 1

    # Update committed schedule so next contact picks a balanced inbox
    update_committed_for_enrollment(inbox, today, committed, cadence)
    return enrolled_so_far + 1


def _acquire_lock():
    """Try to acquire a Redis lock. Returns True if acquired."""
    try:
        return r.set(LOCK_KEY, '1', nx=True, ex=LOCK_TTL)
    except Exception as e:
        logger.error("Error acquiring enrollment lock: %s", e)
        return False


def _release_lock():
    """Release the Redis lock."""
    try:
        r.delete(LOCK_KEY)
    except Exception as e:
        logger.error("Error releasing enrollment lock: %s", e)


def _save_results(summary):
    """Persist run summary to Redis and Postgres."""
    try:
        serialized = json.dumps(summary, default=str)
        r.set(LAST_RUN_KEY, serialized)
        r.lpush(HISTORY_KEY, serialized)
        r.ltrim(HISTORY_KEY, 0, HISTORY_MAX - 1)
    except Exception as e:
        logger.error("Error saving enrollment results to Redis: %s", e)

    try:
        from app.services.db import persist_enrollment_run
        # Convert date strings back for the DB layer
        db_summary = {**summary}
        if isinstance(db_summary.get('run_date'), str):
            db_summary['run_date'] = date.fromisoformat(db_summary['run_date'])
        if isinstance(db_summary.get('finished_at'), str):
            db_summary['finished_at'] = datetime.fromisoformat(db_summary['finished_at'])
        persist_enrollment_run(db_summary)
    except Exception as e:
        logger.error("Error saving enrollment results to Postgres: %s", e)
