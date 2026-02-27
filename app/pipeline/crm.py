"""
Pipeline Stage 6: CRM SYNC — HubSpot import + BDR assignment.

Instagram: Batch-create contacts from discovery leads.
Patreon/Facebook: Batch import (standardize → BDR assign → import_profiles_to_hubspot).
"""
import logging
from typing import Dict, List, Any

from app.config import BDR_OWNER_IDS
from app.services.hubspot import import_profiles_to_hubspot
from app.services.apify import (
    assign_bdr_round_robin,
    update_discovery_job_status,
    standardize_patreon_profiles,
    standardize_facebook_profiles,
)
from app.pipeline.base import StageAdapter, StageResult

logger = logging.getLogger('pipeline.crm')


class InstagramCrmSync(StageAdapter):
    """IG CRM sync: batch-create new contacts in HubSpot from discovery leads."""
    platform = 'instagram'
    stage = 'crm_sync'
    description = 'Batch-create HubSpot contacts from scored discovery leads'
    apis = ['HubSpot']

    def estimate_cost(self, count: int) -> float:
        return 0.0  # HubSpot API is free-tier

    def run(self, profiles, run) -> StageResult:
        # TODO: call import_profiles_to_hubspot() when ready
        synced = []
        errors = []

        for profile in profiles:
            lead_analysis = profile.get('_lead_analysis', {})
            profile_data = profile.get('_profile_data', {})
            name = profile_data.get('username') or profile.get('profile_url') or profile.get('url', 'unknown')
            score = lead_analysis.get('lead_score', 0)

            # Determine tier from score
            if score >= 0.8:
                tier = 'auto_enroll'
            elif score >= 0.5:
                tier = 'high_priority'
            else:
                tier = 'review'

            logger.info("Would create HubSpot contact: %s (score=%.3f, tier=%s)", name, score, tier)
            synced.append(profile)
            run.increment_stage_progress('crm_sync', 'completed')

        run.contacts_synced = len(synced)
        run.save()

        return StageResult(
            profiles=synced,
            processed=len(profiles),
            failed=len(errors),
            errors=errors,
            meta={'synced_count': len(synced), 'mode': 'stub'},
        )


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
