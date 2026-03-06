"""
HubSpot webhook + batch import.
"""
import json
import logging
import os
import time
import requests
from typing import Dict, List
from datetime import datetime

from app.config import HUBSPOT_WEBHOOK_URL, HUBSPOT_API_KEY, HUBSPOT_API_URL
from app.extensions import redis_client as r

logger = logging.getLogger('services.hubspot')


def send_to_hubspot(
    contact_id: str, lead_score: float, section_scores: Dict,
    score_reasoning: str, creator_profile: Dict,
    content_analyses: List[Dict], lead_analysis: Dict = None,
    first_name: str = "there",
):
    """Send results to HubSpot with validation.

    HubSpot workflow: https://app.hubspot.com/workflows/4329123/platform/flow/1773801022/edit
    """
    content_summaries = [
        f"Content {idx} ({item['type']}): {item['summary']}"
        for idx, item in enumerate(content_analyses, 1)
    ]

    manual_score = lead_analysis.get('manual_score', 0.0) if lead_analysis else 0.0
    follower_boost = lead_analysis.get('follower_boost', 0.0) if lead_analysis else 0.0
    engagement_adjustment = lead_analysis.get('engagement_adjustment', 0.0) if lead_analysis else 0.0
    category_penalty = lead_analysis.get('category_penalty', 0.0) if lead_analysis else 0.0
    priority_tier = lead_analysis.get('priority_tier', '') if lead_analysis else ''
    expected_precision = lead_analysis.get('expected_precision', 0.0) if lead_analysis else 0.0

    def safe_str(value):
        if value is None:
            return ''
        if isinstance(value, list):
            return ', '.join(str(item) for item in value if item is not None)
        if isinstance(value, dict):
            return json.dumps(value)
        return str(value)

    community_building = creator_profile.get('community_building', '')
    if isinstance(community_building, list):
        community_text = ' '.join(str(item) for item in community_building if item).lower()
    else:
        community_text = str(community_building).lower()

    platforms = []
    for keyword, name in [('email', 'Email List'), ('patreon', 'Patreon'),
                          ('discord', 'Discord'), ('substack', 'Substack')]:
        if keyword in community_text and name not in platforms:
            platforms.append(name)

    # Validation
    enrichment_status = "success"
    error_details = []

    if not content_analyses:
        enrichment_status = "error"
        error_details.append("No content analyzed")
    if not score_reasoning or len(score_reasoning) < 10:
        enrichment_status = "error"
        error_details.append("Missing or invalid score reasoning")
    if lead_score == 0.0 and all(score == 0.0 for score in section_scores.values()):
        enrichment_status = "warning"
        error_details.append("All scores are 0.0")
    if not creator_profile.get('content_category'):
        enrichment_status = "warning" if enrichment_status == "success" else "error"
        error_details.append("Missing content category")

    error_keywords = ['error', 'failed', 'could not', 'unable to', 'missing data', 'no content', 'unavailable']
    if any(keyword in score_reasoning.lower() for keyword in error_keywords):
        enrichment_status = "warning" if enrichment_status == "success" else enrichment_status
        error_details.append("Error indicators found in reasoning")

    # Track stats in Redis
    try:
        result_type = 'enriched'
        if 'post frequency check' in score_reasoning.lower():
            result_type = 'post_frequency'
        elif 'pre-screen rejected' in score_reasoning.lower() or 'pre-screened' in score_reasoning.lower():
            result_type = 'pre_screened'
        elif enrichment_status == 'error':
            result_type = 'error'
        r.hincrby('trovastats:results', result_type, 1)
        if result_type == 'enriched' and lead_analysis:
            r.hincrby('trovastats:priority_tiers', lead_analysis.get('priority_tier', 'unknown'), 1)
    except Exception as e:
        logger.error("Error tracking stats in Redis: %s", e)

    payload = {
        "contact_id": contact_id,
        "first_name": first_name,
        "lead_score": lead_score,
        "manual_score": manual_score,
        "follower_boost_applied": follower_boost,
        "engagement_adjustment_applied": engagement_adjustment,
        "category_penalty_applied": category_penalty,
        "priority_tier": priority_tier,
        "expected_precision": expected_precision,
        "score_reasoning": score_reasoning,
        "score_niche_and_audience": section_scores.get('niche_and_audience_identity', 0.0),
        "score_host_likeability": section_scores.get('creator_authenticity_and_presence',
                                                      section_scores.get('host_likeability_and_content_style', 0.0)),
        "score_monetization": section_scores.get('monetization_and_business_mindset', 0.0),
        "score_community_infrastructure": section_scores.get('community_infrastructure', 0.0),
        "score_trip_fit": section_scores.get('engagement_and_connection',
                                             section_scores.get('trip_fit_and_travelability', 0.0)),
        "content_summary_structured": "\n\n".join(content_summaries),
        "profile_category": safe_str(creator_profile.get('content_category')),
        "primary_category": safe_str(creator_profile.get('primary_category', 'unknown')),
        "profile_content_types": safe_str(creator_profile.get('content_types')),
        "profile_engagement": safe_str(creator_profile.get('audience_engagement')),
        "profile_presence": safe_str(creator_profile.get('creator_presence')),
        "profile_monetization": safe_str(creator_profile.get('monetization')),
        "profile_community_building": safe_str(community_building),
        "has_community_platform": len(platforms) > 0,
        "community_platforms_detected": ", ".join(platforms) if platforms else "None",
        "analyzed_at": datetime.now().isoformat(),
        "enrichment_status": enrichment_status,
        "enrichment_error_details": "; ".join(error_details) if error_details else "",
        "items_analyzed": len(content_analyses),
    }

    if priority_tier == "auto_enroll":
        payload["bdr_"] = ""

    logger.info("Sending to HubSpot: %s", HUBSPOT_WEBHOOK_URL)
    logger.info("Enrichment status: %s", enrichment_status)
    if error_details:
        logger.warning("Error details: %s", '; '.join(error_details))

    from app.services.circuit_breaker import get_breaker
    cb = get_breaker('hubspot')
    response = cb.call(requests.post, HUBSPOT_WEBHOOK_URL, json=payload, timeout=10)
    logger.info("HubSpot response: %d", response.status_code)


def check_existing_contacts(emails: List[str]) -> set:
    """Check which emails already exist in HubSpot.

    Uses batch/read with idProperty=email — 100 per call at general rate limit.
    Returns a set of emails that already exist as contacts.
    """
    if not HUBSPOT_API_KEY or not emails:
        return set()

    existing = set()
    from app.services.circuit_breaker import get_breaker
    cb = get_breaker('hubspot')

    for i in range(0, len(emails), 100):
        batch = emails[i:i + 100]
        try:
            resp = cb.call(
                requests.post,
                f"{HUBSPOT_API_URL}/crm/v3/objects/contacts/batch/read",
                headers={
                    'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json={
                    'inputs': [{'id': email} for email in batch],
                    'idProperty': 'email',
                    'properties': ['email'],
                },
                timeout=15,
            )

            if resp.status_code == 200:
                for result in resp.json().get('results', []):
                    email = result.get('properties', {}).get('email', '').lower()
                    if email:
                        existing.add(email)
                logger.info("HubSpot dedup batch %d: %d/%d already exist",
                            (i // 100) + 1, len(existing), len(batch))
            elif resp.status_code == 207:
                # Partial success — some found, some not
                for result in resp.json().get('results', []):
                    email = result.get('properties', {}).get('email', '').lower()
                    if email:
                        existing.add(email)
            else:
                logger.warning("HubSpot dedup batch error: %d — %s",
                               resp.status_code, resp.text[:200])
        except Exception as e:
            logger.error("HubSpot dedup exception: %s", e)

        if i + 100 < len(emails):
            time.sleep(0.1)

    logger.info("HubSpot dedup: %d/%d emails already in CRM", len(existing), len(emails))
    return existing


def import_profiles_to_hubspot(profiles: List[Dict], job_id: str) -> Dict:
    if not HUBSPOT_API_KEY:
        raise ValueError("HUBSPOT_API_KEY must be set in environment")

    contacts = []
    for idx, profile in enumerate(profiles):
        properties = {k: v for k, v in profile.items() if v is not None and v != ''}
        contacts.append({
            'properties': properties,
            'objectWriteTraceId': f"{job_id}_{idx}",
        })

    created_count = 0
    skipped_count = 0
    id_map = {}  # objectWriteTraceId → HubSpot contact ID
    existing_trace_ids = set()  # trace IDs for contacts that already exist in HubSpot
    total_batches = (len(contacts) + 99) // 100

    logger.info("Importing %d contacts in %d batches", len(contacts), total_batches)

    for i in range(0, len(contacts), 100):
        batch = contacts[i:i + 100]
        batch_num = (i // 100) + 1

        try:
            from app.services.circuit_breaker import get_breaker
            cb = get_breaker('hubspot')
            resp = cb.call(
                requests.post,
                f"{HUBSPOT_API_URL}/crm/v3/objects/contacts/batch/create",
                headers={
                    'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json={'inputs': batch},
                timeout=30,
            )

            if resp.status_code in (201, 207):
                result = resp.json()
                # Capture IDs from created contacts
                for r in result.get('results', []):
                    trace_id = r.get('objectWriteTraceId')
                    hs_id = r.get('id')
                    if trace_id and hs_id:
                        id_map[trace_id] = hs_id

                # Capture IDs from duplicates ("Existing ID: 12345" in error message)
                import re
                for err in result.get('errors', []):
                    msg = err.get('message', '')
                    match = re.search(r'Existing ID:\s*(\d+)', msg)
                    trace_ids = err.get('context', {}).get('objectWriteTraceId', [])
                    if match and trace_ids:
                        id_map[trace_ids[0]] = match.group(1)
                        existing_trace_ids.add(trace_ids[0])

                batch_created = len(result.get('results', []))
                batch_skipped = len(result.get('errors', []))
                if resp.status_code == 201:
                    created_count += len(batch)
                    logger.info("Batch %d/%d: %d created", batch_num, total_batches, len(batch))
                else:
                    created_count += batch_created
                    skipped_count += batch_skipped
                    logger.warning("Batch %d/%d: %d created, %d duplicates/errors", batch_num, total_batches, batch_created, batch_skipped)
                    for err in result.get('errors', [])[:3]:
                        logger.warning("  Error: %s", err.get('message', 'Unknown'))
            else:
                logger.error("Batch %d error: %d — %s", batch_num, resp.status_code, resp.text[:200])
                skipped_count += len(batch)

        except Exception as e:
            logger.error("Exception on batch %d: %s", batch_num, e)
            skipped_count += len(batch)

        if i + 100 < len(contacts):
            time.sleep(0.5)

    logger.info("Import complete: %d created, %d skipped, %d existing, %d IDs captured",
                 created_count, skipped_count, len(existing_trace_ids), len(id_map))
    return {'created': created_count, 'skipped': skipped_count, 'id_map': id_map,
            'existing_trace_ids': existing_trace_ids}


def hubspot_batch_create(profiles: List[Dict], run) -> tuple:
    from app.services.apify import assign_bdr_round_robin
    from app.config import BDR_OWNER_IDS

    bdr_names = run.filters.get('bdr_names', list(BDR_OWNER_IDS.keys()))
    profiles = assign_bdr_round_robin(profiles, bdr_names)

    # Strip enrichment_status so batch create doesn't trigger the HubSpot
    # workflow prematurely — the stage 6 webhook sets it after scoring.
    for p in profiles:
        p.pop('enrichment_status', None)

    hs_dupe_count = 0
    try:
        result = import_profiles_to_hubspot(profiles, run.id)
        created = result.get('created', 0)
        skipped = result.get('skipped', 0)
        id_map = result.get('id_map', {})
        existing_trace_ids = result.get('existing_trace_ids', set())

        # Map HubSpot contact IDs back onto profiles via objectWriteTraceId
        for idx, p in enumerate(profiles):
            trace_id = f"{run.id}_{idx}"
            hs_id = id_map.get(trace_id)
            if hs_id:
                p['_hubspot_contact_id'] = hs_id

        # Drop profiles that already exist in HubSpot
        if existing_trace_ids:
            new_profiles = []
            for idx, p in enumerate(profiles):
                trace_id = f"{run.id}_{idx}"
                if trace_id not in existing_trace_ids:
                    new_profiles.append(p)
            hs_dupe_count = len(profiles) - len(new_profiles)
            profiles = new_profiles
            logger.info("Dropped %d HubSpot duplicates, %d profiles remain", hs_dupe_count, len(profiles))

        mapped = sum(1 for p in profiles if '_hubspot_contact_id' in p)
        logger.info("HubSpot batch create: %d created, %d skipped, %d dupes dropped, %d IDs mapped",
                     created, skipped, hs_dupe_count, mapped)
    except Exception as e:
        logger.error("HubSpot batch create failed: %s", e)
        created, skipped = 0, 0

    return profiles, created, hs_dupe_count


def hubspot_search_contacts_all(filters: List[Dict], properties: List[str],
                                 sorts: List[Dict] = None) -> List[Dict]:
    """Paginated search via POST /crm/v3/objects/contacts/search.

    Cursor-based pagination, 100 per page, max 10K safety limit.
    Returns list of contact dicts with requested properties.
    """
    if not HUBSPOT_API_KEY:
        logger.warning("hubspot_search_contacts_all: no API key set")
        return []

    from app.services.circuit_breaker import get_breaker
    cb = get_breaker('hubspot')

    all_results = []
    after = None
    max_total = 10_000

    while len(all_results) < max_total:
        body = {
            'filterGroups': [{'filters': filters}],
            'properties': properties,
            'limit': 100,
        }
        if sorts:
            body['sorts'] = sorts
        if after:
            body['after'] = after

        try:
            resp = cb.call(
                requests.post,
                f"{HUBSPOT_API_URL}/crm/v3/objects/contacts/search",
                headers={
                    'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json=body,
                timeout=15,
            )

            if resp.status_code != 200:
                logger.error("HubSpot search error: %d — %s",
                             resp.status_code, resp.text[:200])
                break

            data = resp.json()
            results = data.get('results', [])
            all_results.extend(results)

            paging = data.get('paging', {}).get('next', {})
            after = paging.get('after')
            if not after or not results:
                break

        except Exception as e:
            logger.error("HubSpot search exception: %s", e)
            break

        time.sleep(0.1)

    logger.info("HubSpot search: %d contacts found", len(all_results))
    return all_results


def hubspot_update_contact(contact_id: str, properties: Dict) -> bool:
    """Update a single contact via PATCH /crm/v3/objects/contacts/{id}.

    Returns True on success, False on failure.
    """
    if not HUBSPOT_API_KEY:
        logger.warning("hubspot_update_contact: no API key set")
        return False

    from app.services.circuit_breaker import get_breaker
    cb = get_breaker('hubspot')

    try:
        resp = cb.call(
            requests.patch,
            f"{HUBSPOT_API_URL}/crm/v3/objects/contacts/{contact_id}",
            headers={
                'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                'Content-Type': 'application/json',
            },
            json={'properties': properties},
            timeout=10,
        )

        if resp.status_code == 200:
            return True
        else:
            logger.error("HubSpot update contact %s error: %d — %s",
                         contact_id, resp.status_code, resp.text[:200])
            return False

    except Exception as e:
        logger.error("HubSpot update contact %s exception: %s", contact_id, e)
        return False


# ── List / Segment helpers (rewarm pipeline) ───────────────────────────


def hubspot_list_all() -> list:
    """Fetch ALL HubSpot lists, paginating through the search endpoint.

    Returns list of dicts: [{"id": list_id, "name": name, "size": size}, ...]
    Typically called once and cached in-memory by the route layer.
    """
    if not HUBSPOT_API_KEY:
        logger.warning("hubspot_list_all: no API key set")
        return []

    from app.services.circuit_breaker import get_breaker
    cb = get_breaker('hubspot')

    all_lists = []
    offset = 0
    page_size = 100

    try:
        while True:
            resp = cb.call(
                requests.post,
                f"{HUBSPOT_API_URL}/crm/v3/lists/search",
                headers={
                    'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json={
                    'query': '',
                    'processingTypes': ['MANUAL', 'SNAPSHOT', 'DYNAMIC'],
                    'objectTypeId': '0-1',
                    'count': page_size,
                    'offset': offset,
                },
                timeout=15,
            )

            if resp.status_code != 200:
                logger.error("hubspot_list_all error at offset %d: %d — %s",
                             offset, resp.status_code, resp.text[:200])
                break

            data = resp.json()
            for item in data.get('lists', []):
                all_lists.append({
                    'id': str(item.get('listId', '')),
                    'name': item.get('name', ''),
                    'size': int(item.get('additionalProperties', {}).get('hs_list_size', 0)),
                    'processing_type': item.get('processingType'),
                })

            if not data.get('hasMore'):
                break
            offset = data.get('offset', offset + page_size)

        logger.info("hubspot_list_all: fetched %d lists total", len(all_lists))
        return all_lists

    except Exception as e:
        logger.error("hubspot_list_all exception: %s", e)
        return all_lists  # return whatever we got so far


def hubspot_list_search(query: str) -> list:
    """Search HubSpot lists/segments by name.

    POST /crm/v3/lists/search
    Returns list of dicts: [{"id": list_id, "name": name, "size": size}, ...]
    """
    if not HUBSPOT_API_KEY:
        logger.warning("hubspot_list_search: no API key set")
        return []

    from app.services.circuit_breaker import get_breaker
    cb = get_breaker('hubspot')

    try:
        resp = cb.call(
            requests.post,
            f"{HUBSPOT_API_URL}/crm/v3/lists/search",
            headers={
                'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                'Content-Type': 'application/json',
            },
            json={
                'query': query,
                'processingTypes': ['MANUAL', 'SNAPSHOT', 'DYNAMIC'],
                'objectTypeId': '0-1',
                'count': 100,
            },
            timeout=15,
        )

        if resp.status_code != 200:
            logger.error("hubspot_list_search error: %d — %s",
                         resp.status_code, resp.text[:200])
            return []

        data = resp.json()
        results = []
        for item in data.get('lists', []):
            results.append({
                'id': str(item.get('listId', '')),
                'name': item.get('name', ''),
                'size': int(item.get('additionalProperties', {}).get('hs_list_size', 0)),
            })

        logger.info("hubspot_list_search(%s): %d lists found", query, len(results))
        return results

    except Exception as e:
        logger.error("hubspot_list_search exception: %s", e)
        return []


def hubspot_get_list_members(list_id: str, limit: int = 500) -> list:
    """Get contact IDs from a HubSpot list.

    GET /crm/v3/lists/{listId}/memberships — paginated via paging.next.after.
    Returns list of contact ID strings, capped at `limit`.
    """
    if not HUBSPOT_API_KEY:
        logger.warning("hubspot_get_list_members: no API key set")
        return []

    from app.services.circuit_breaker import get_breaker
    cb = get_breaker('hubspot')

    all_ids = []
    after = None

    while len(all_ids) < limit:
        try:
            params = {}
            if after:
                params['after'] = after

            resp = cb.call(
                requests.get,
                f"{HUBSPOT_API_URL}/crm/v3/lists/{list_id}/memberships",
                headers={
                    'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                },
                params=params,
                timeout=15,
            )

            if resp.status_code != 200:
                logger.error("hubspot_get_list_members error: %d — %s",
                             resp.status_code, resp.text[:200])
                break

            data = resp.json()
            members = data.get('results', [])
            for m in members:
                all_ids.append(str(m))
                if len(all_ids) >= limit:
                    break

            paging = data.get('paging', {}).get('next', {})
            after = paging.get('after')
            if not after or not members:
                break

        except Exception as e:
            logger.error("hubspot_get_list_members exception: %s", e)
            break

        time.sleep(0.1)

    logger.info("hubspot_get_list_members(%s): %d contact IDs", list_id, len(all_ids))
    return all_ids


_DEFAULT_CONTACT_PROPERTIES = [
    'email', 'firstname', 'lastname', 'instagram_handle',
    'instagram_followers', 'city', 'state', 'country',
]


def hubspot_batch_get_contacts(contact_ids: list, properties: list = None) -> list:
    """Batch-read contacts by ID with requested properties.

    POST /crm/v3/objects/contacts/batch/read — 100 per batch.
    Returns list of flattened property dicts with 'id' included.
    """
    if not HUBSPOT_API_KEY:
        logger.warning("hubspot_batch_get_contacts: no API key set")
        return []

    if properties is None:
        properties = list(_DEFAULT_CONTACT_PROPERTIES)

    from app.services.circuit_breaker import get_breaker
    cb = get_breaker('hubspot')

    all_contacts = []

    for i in range(0, len(contact_ids), 100):
        batch = contact_ids[i:i + 100]
        try:
            resp = cb.call(
                requests.post,
                f"{HUBSPOT_API_URL}/crm/v3/objects/contacts/batch/read",
                headers={
                    'Authorization': f'Bearer {HUBSPOT_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json={
                    'inputs': [{'id': cid} for cid in batch],
                    'properties': properties,
                },
                timeout=15,
            )

            if resp.status_code in (200, 207):
                for result in resp.json().get('results', []):
                    contact = {'id': result.get('id', '')}
                    props = result.get('properties', {})
                    for prop in properties:
                        contact[prop] = props.get(prop, '')
                    all_contacts.append(contact)
            else:
                logger.error("hubspot_batch_get_contacts batch %d error: %d — %s",
                             (i // 100) + 1, resp.status_code, resp.text[:200])

        except Exception as e:
            logger.error("hubspot_batch_get_contacts batch %d exception: %s",
                         (i // 100) + 1, e)

        if i + 100 < len(contact_ids):
            time.sleep(0.1)

    logger.info("hubspot_batch_get_contacts: %d contacts retrieved", len(all_contacts))
    return all_contacts


def sync_hubspot_lists_to_db() -> Dict:
    """Fetch all HubSpot lists and persist to hubspot_lists table.

    Truncate + reinsert in a single transaction.  Refuses to truncate if
    the API returns an empty result (protects against API failures wiping
    the cache).

    Updates AppConfig key ``hubspot_lists_synced_at`` with the current
    timestamp.

    Returns ``{"count": N, "synced_at": "ISO timestamp"}``.
    """
    from app.database import get_session
    from app.models.hubspot_list import HubSpotList
    from app.models.app_config import AppConfig

    lists = hubspot_list_all()
    if not lists:
        logger.warning("sync_hubspot_lists_to_db: API returned 0 lists — skipping truncate")
        return {'count': 0, 'synced_at': None}

    session = get_session()
    try:
        session.query(HubSpotList).delete()
        session.bulk_save_objects([
            HubSpotList(
                list_id=item['id'],
                name=item['name'],
                size=item['size'],
                processing_type=item.get('processing_type'),
            )
            for item in lists
        ])
        from datetime import timezone
        now = datetime.now(tz=timezone.utc).isoformat()
        cfg = session.get(AppConfig, 'hubspot_lists_synced_at')
        if cfg:
            cfg.value = now
        else:
            session.add(AppConfig(key='hubspot_lists_synced_at', value=now))
        session.commit()
        logger.info("sync_hubspot_lists_to_db: wrote %d lists", len(lists))
        return {'count': len(lists), 'synced_at': now}
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def hubspot_import_segment(list_id: str, platform: str = 'instagram') -> list:
    """Import a HubSpot list/segment as profile dicts for the rewarm pipeline.

    Combines: get members → batch get contacts → return profile dicts.
    """
    if not HUBSPOT_API_KEY:
        logger.warning("hubspot_import_segment: no API key set")
        return []

    contact_ids = hubspot_get_list_members(list_id)
    if not contact_ids:
        logger.info("hubspot_import_segment(%s): no members found", list_id)
        return []

    contacts = hubspot_batch_get_contacts(contact_ids)

    logger.info("hubspot_import_segment(%s): %d contacts imported from list %s",
                platform, len(contacts), list_id)
    return contacts
