"""
Pipeline Stage 6: CRM SYNC — HubSpot import + BDR assignment.

Instagram: Send each contact to HubSpot via workflow webhook.
Patreon/Facebook: Batch import (standardize → BDR assign → import_profiles_to_hubspot).
"""
import logging
import time
from typing import Dict, List, Any

from app.config import BDR_OWNER_IDS
from app.services.hubspot import send_to_hubspot, import_profiles_to_hubspot
from app.services.apify import (
    assign_bdr_round_robin,
    standardize_instagram_profiles,
    standardize_patreon_profiles,
    standardize_facebook_profiles,
)
from app.pipeline.base import StageAdapter, StageResult

logger = logging.getLogger('pipeline.crm')


class InstagramCrmSync(StageAdapter):
    """IG CRM sync: send each scored contact to HubSpot via workflow webhook."""
    platform = 'instagram'
    stage = 'crm_sync'
    description = 'Send scored contacts to HubSpot workflow webhook'
    apis = ['HubSpot']

    def estimate_cost(self, count: int) -> float:
        return 0.0  # HubSpot API is free-tier

    def run(self, profiles, run) -> StageResult:
        if not profiles:
            return StageResult(profiles=[], processed=0)

        synced = 0
        skipped = 0
        errors = []

        for p in profiles:
            lead_analysis = p.get('_lead_analysis', {})
            creator_profile = p.get('_creator_profile', {})
            content_analyses = p.get('_content_analyses', [])
            section_scores = lead_analysis.get('section_scores', {})
            contact_id = p.get('email') or p.get('instagram_handle', '')

            try:
                send_to_hubspot(
                    contact_id=contact_id,
                    lead_score=lead_analysis.get('lead_score', 0.0),
                    section_scores=section_scores,
                    score_reasoning=lead_analysis.get('score_reasoning', ''),
                    creator_profile=creator_profile,
                    content_analyses=content_analyses,
                    lead_analysis=lead_analysis,
                    first_name=p.get('_first_name', 'there'),
                )
                p['_synced_to_crm'] = True
                synced += 1
                run.increment_stage_progress('crm_sync', 'completed')
            except Exception as e:
                logger.error("Webhook error for %s: %s", contact_id, e)
                errors.append(f"{contact_id}: {str(e)}")
                skipped += 1
                run.increment_stage_progress('crm_sync', 'failed')

            # Rate limit: stay well under 100 req/10s
            time.sleep(0.15)

        run.contacts_synced = synced
        run.duplicates_skipped = skipped
        run.save()

        logger.info("Webhook sync: %d sent, %d failed", synced, skipped)

        return StageResult(
            profiles=profiles,
            processed=len(profiles),
            failed=len(errors),
            skipped=skipped,
            errors=errors,
            meta={'synced': synced, 'skipped': skipped},
        )


# NOTE: Batch API alternative — use this if switching away from webhook.
# Faster (100 contacts/call) but bypasses HubSpot workflow automations.
#
# class InstagramCrmSyncBatchAPI(StageAdapter):
#     """IG CRM sync: batch-create contacts via HubSpot contacts API."""
#     platform = 'instagram'
#     stage = 'crm_sync'
#     description = 'Batch-create HubSpot contacts from scored discovery leads'
#     apis = ['HubSpot']
#
#     def estimate_cost(self, count: int) -> float:
#         return 0.0
#
#     def run(self, profiles, run) -> StageResult:
#         if not profiles:
#             return StageResult(profiles=[], processed=0)
#
#         bdr_names = run.filters.get('bdr_names', list(BDR_OWNER_IDS.keys()))
#         standardized = standardize_instagram_profiles(profiles)
#         standardized = assign_bdr_round_robin(standardized, bdr_names)
#         import_results = import_profiles_to_hubspot(standardized, run.id)
#
#         for p in profiles:
#             p['_synced_to_crm'] = True
#         run.contacts_synced = import_results.get('created', 0)
#         run.duplicates_skipped = import_results.get('skipped', 0)
#         run.save()
#
#         return StageResult(
#             profiles=profiles,
#             processed=len(profiles),
#             skipped=import_results.get('skipped', 0),
#             meta=import_results,
#         )


class PatreonCrmSync(StageAdapter):
    """Patreon CRM sync: standardize → BDR assign → batch HubSpot import."""
    platform = 'patreon'
    stage = 'crm_sync'
    description = 'Standardize → BDR round-robin → batch HubSpot import'
    apis = ['HubSpot']

    def estimate_cost(self, count: int) -> float:
        return 0.0

    def run(self, profiles, run) -> StageResult:
        if not profiles:
            return StageResult(profiles=[], processed=0)

        bdr_names = run.filters.get('bdr_names', list(BDR_OWNER_IDS.keys()))

        standardized = standardize_patreon_profiles(profiles)
        standardized = assign_bdr_round_robin(standardized, bdr_names)
        import_results = import_profiles_to_hubspot(standardized, run.id)

        for p in standardized:
            p['_synced_to_crm'] = True
        run.contacts_synced = import_results.get('created', 0)
        run.duplicates_skipped = import_results.get('skipped', 0)
        run.save()

        logger.info("Created: %d, Skipped: %d", import_results['created'], import_results['skipped'])

        return StageResult(
            profiles=standardized,
            processed=len(profiles),
            skipped=import_results.get('skipped', 0),
            meta=import_results,
        )


class FacebookCrmSync(StageAdapter):
    """Facebook CRM sync: standardize → BDR assign → batch HubSpot import."""
    platform = 'facebook'
    stage = 'crm_sync'
    description = 'Standardize → BDR round-robin → batch HubSpot import'
    apis = ['HubSpot']

    def estimate_cost(self, count: int) -> float:
        return 0.0

    def run(self, profiles, run) -> StageResult:
        if not profiles:
            return StageResult(profiles=[], processed=0)

        bdr_names = run.filters.get('bdr_names', list(BDR_OWNER_IDS.keys()))

        standardized = standardize_facebook_profiles(profiles)
        standardized = assign_bdr_round_robin(standardized, bdr_names)
        import_results = import_profiles_to_hubspot(standardized, run.id)

        for p in standardized:
            p['_synced_to_crm'] = True
        run.contacts_synced = import_results.get('created', 0)
        run.duplicates_skipped = import_results.get('skipped', 0)
        run.save()

        logger.info("Created: %d, Skipped: %d", import_results['created'], import_results['skipped'])

        return StageResult(
            profiles=standardized,
            processed=len(profiles),
            skipped=import_results.get('skipped', 0),
            meta=import_results,
        )


# ── Adapter registry ─────────────────────────────────────────────────────────

ADAPTERS: Dict[str, type] = {
    'instagram': InstagramCrmSync,
    'patreon': PatreonCrmSync,
    'facebook': FacebookCrmSync,
}
