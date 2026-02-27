"""
Pipeline Stage 3: ENRICHMENT — Contact info, social data, media.

Instagram: Content already fetched in prescreen; this stage does R2 rehosting.
Patreon/Facebook: Full 11-step enrichment pipeline (social bios, Apollo, email validation).
"""
import logging
from typing import Dict, List, Any

from app.pipeline.base import StageAdapter, StageResult

logger = logging.getLogger('pipeline.enrichment')


# ── Adapters ──────────────────────────────────────────────────────────────────

class InstagramEnrichment(StageAdapter):
    """
    IG enrichment: content is already attached from prescreen.
    This stage just ensures profiles have the data needed for analysis.
    Future: add Apollo email lookup here.
    """
    platform = 'instagram'
    stage = 'enrichment'
    description = 'Content from InsightIQ (passthrough)'
    apis = []

    def estimate_cost(self, count: int) -> float:
        return 0.0  # Passthrough

    def run(self, profiles, run) -> StageResult:
        enriched = []
        errors = []

        for profile in profiles:
            # Content was attached by InstagramPrescreen as '_content_items'
            if not profile.get('_content_items'):
                errors.append(f"No content for {profile.get('url', 'unknown')}")
                continue
            enriched.append(profile)
            run.increment_stage_progress('enrichment', 'completed')

        logger.info("%d/%d enriched", len(enriched), len(profiles))

        return StageResult(
            profiles=enriched,
            processed=len(profiles),
            failed=len(errors),
            errors=errors,
        )


class PatreonEnrichment(StageAdapter):
    """Patreon enrichment: 11-step pipeline (Google bridge, social bios, Apollo, etc.)."""
    platform = 'patreon'
    stage = 'enrichment'
    description = 'Website crawl, email extraction, social links, Apollo lookup'
    apis = ['Apify', 'Apollo']

    def estimate_cost(self, count: int) -> float:
        return count * 0.05

    def run(self, profiles, run) -> StageResult:
        from app.services.apify import enrich_profiles_full_pipeline

        if not profiles:
            return StageResult(profiles=[], processed=0)

        logger.info("Enriching %d profiles", len(profiles))
        enriched = enrich_profiles_full_pipeline(profiles, run.id, platform='patreon')

        return StageResult(
            profiles=enriched,
            processed=len(profiles),
            meta={'enrichment_steps': 11},
            cost=len(profiles) * 0.05,
        )


class FacebookEnrichment(StageAdapter):
    """Facebook enrichment: same 11-step pipeline as Patreon."""
    platform = 'facebook'
    stage = 'enrichment'
    description = 'Website crawl, email extraction, social links, Apollo lookup'
    apis = ['Apify', 'Apollo']

    def estimate_cost(self, count: int) -> float:
        return count * 0.05

    def run(self, profiles, run) -> StageResult:
        from app.services.apify import enrich_profiles_full_pipeline

        if not profiles:
            return StageResult(profiles=[], processed=0)

        logger.info("Enriching %d profiles", len(profiles))
        enriched = enrich_profiles_full_pipeline(profiles, run.id, platform='facebook_groups')

        return StageResult(
            profiles=enriched,
            processed=len(profiles),
            meta={'enrichment_steps': 11},
            cost=len(profiles) * 0.05,
        )


# ── Adapter registry ─────────────────────────────────────────────────────────

ADAPTERS: Dict[str, type] = {
    'instagram': InstagramEnrichment,
    'patreon': PatreonEnrichment,
    'facebook': FacebookEnrichment,
}
