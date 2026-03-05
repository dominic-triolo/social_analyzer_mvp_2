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
    """Import standardized profiles to HubSpot via batch contacts API."""
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

            if resp.status_code == 201:
                created_count += len(batch)
                logger.info("Batch %d/%d: %d created", batch_num, total_batches, len(batch))
            elif resp.status_code == 207:
                result = resp.json()
                batch_created = len(result.get('results', []))
                batch_errors = result.get('errors', [])
                batch_skipped = len(batch_errors)
                created_count += batch_created
                skipped_count += batch_skipped
                logger.warning("Batch %d/%d: %d created, %d duplicates/errors", batch_num, total_batches, batch_created, batch_skipped)
                for err in batch_errors[:3]:
                    logger.warning("  Error: %s", err.get('message', 'Unknown'))
            else:
                logger.error("Batch %d error: %d — %s", batch_num, resp.status_code, resp.text[:200])
                skipped_count += len(batch)

        except Exception as e:
            logger.error("Exception on batch %d: %s", batch_num, e)
            skipped_count += len(batch)

        if i + 100 < len(contacts):
            time.sleep(0.5)

    logger.info("Import complete: %d created, %d skipped", created_count, skipped_count)
    return {'created': created_count, 'skipped': skipped_count}


def hubspot_batch_create(profiles: List[Dict], run) -> tuple:
    """Batch-create HubSpot contacts from raw discovery profiles (Instagram).

    1. Assign BDR round-robin from run filters
    2. Call import_profiles_to_hubspot() for batch create with dedup via objectWriteTraceId

    All profiles pass through regardless of create/skip outcome — the expensive
    GPT-4.1 prescreen still runs on all of them. The stage 6 webhook
    (send_to_hubspot) updates these contacts with scores/enrichment later.

    Returns (profiles, created_count, skipped_count).
    """
    from app.services.apify import assign_bdr_round_robin
    from app.config import BDR_OWNER_IDS

    bdr_names = run.filters.get('bdr_names', list(BDR_OWNER_IDS.keys()))
    profiles = assign_bdr_round_robin(profiles, bdr_names)

    # Strip enrichment_status so batch create doesn't trigger the HubSpot
    # workflow prematurely — the stage 6 webhook sets it after scoring.
    for p in profiles:
        p.pop('enrichment_status', None)

    try:
        result = import_profiles_to_hubspot(profiles, run.id)
        created = result.get('created', 0)
        skipped = result.get('skipped', 0)
        logger.info("HubSpot batch create: %d created, %d skipped", created, skipped)
    except Exception as e:
        logger.error("HubSpot batch create failed: %s", e)
        created, skipped = 0, 0

    return profiles, created, skipped


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
