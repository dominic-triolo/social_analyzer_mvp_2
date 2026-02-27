"""
Pipeline Stage 6: CRM SYNC — HubSpot import + BDR assignment.

Instagram: Single contact update (send_to_hubspot per profile).
Patreon/Facebook: Batch import (standardize → BDR assign → import_profiles_to_hubspot).
"""
import logging
from typing import Dict, List, Any

from app.config import BDR_OWNER_IDS
from app.services.hubspot import send_to_hubspot, import_profiles_to_hubspot
from app.services.apify import (
    assign_bdr_round_robin,
    update_discovery_job_status,
    standardize_patreon_profiles,
    standardize_facebook_profiles,
)
from app.pipeline.base import StageAdapter, StageResult

logger = logging.getLogger('pipeline.crm')


class InstagramCrmSync(StageAdapter):
    """IG CRM sync: push each scored profile to HubSpot individually."""
    platform = 'instagram'
    stage = 'crm_sync'
    description = 'Push each profile to HubSpot with score + analysis'
    apis = ['HubSpot']

    def estimate_cost(self, count: int) -> float:
        return 0.0  # HubSpot API is free-tier

    def run(self, profiles, run) -> StageResult:
        synced = []
        errors = []
        duplicates = 0

        for profile in profiles:
            contact_id = profile.get('contact_id') or profile.get('id', '')
            lead_analysis = profile.get('_lead_analysis', {})

            if not contact_id:
                errors.append("No contact_id for profile")
                continue

            try:
                send_to_hubspot(
                    contact_id,
                    lead_analysis.get('lead_score', 0),
                    lead_analysis.get('section_scores', {}),
                    lead_analysis.get('score_reasoning', ''),
                    profile.get('_creator_profile', {}),
                    profile.get('_content_analyses', []),
                    lead_analysis,
                    first_name=profile.get('_first_name', 'there'),
                )

                synced.append(profile)
                run.increment_stage_progress('crm_sync', 'completed')
                logger.info("Synced %s: score=%.3f", contact_id, lead_analysis.get('lead_score', 0))

            except Exception as e:
                logger.error("Error syncing %s: %s", contact_id, e)
                errors.append(f"{contact_id}: {str(e)}")
                run.increment_stage_progress('crm_sync', 'failed')

        run.contacts_synced = len(synced)
        run.save()

        return StageResult(
            profiles=synced,
            processed=len(profiles),
            failed=len(errors),
            errors=errors,
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
