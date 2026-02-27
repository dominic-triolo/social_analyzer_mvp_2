"""
Pipeline Manager — Run orchestration using the stage adapter registry.

Launches a Run through the 6 pipeline stages:
  DISCOVERY → PRE-SCREEN → ENRICHMENT → ANALYSIS → SCORING → CRM SYNC

Each stage looks up the platform's adapter from the registry, calls adapter.run(),
and feeds the result to the next stage. Progress is tracked on the Run object.
"""
import json
import logging
import traceback
from typing import Dict, Type

from app.models.run import Run
from app.config import PIPELINE_STAGES, BDR_OWNER_IDS
from app.pipeline.base import StageAdapter, StageResult, get_adapter
from app.services.db import (
    persist_run, persist_lead_results,
    dedup_profiles, record_filter_history,
)
from app.services.notifications import notify_run_complete, notify_run_failed
from app.services.benchmarks import persist_metric_snapshot, get_baseline, compute_deviations
from app.pipeline.cost_config import get_default_budget, get_warning_threshold

logger = logging.getLogger('pipeline.manager')

# Import adapter registries from each stage module
from app.pipeline import discovery as discovery_mod
from app.pipeline import prescreen as prescreen_mod
from app.pipeline import enrichment as enrichment_mod
from app.pipeline import analysis as analysis_mod
from app.pipeline import scoring as scoring_mod
from app.pipeline import crm as crm_mod


# ── Lazy RQ queue (avoids import-time Redis connection in preview mode) ───

_queue = None

def _get_queue():
    global _queue
    if _queue is None:
        from app.extensions import redis_client
        from rq import Queue
        _queue = Queue(connection=redis_client)
    return _queue


# ── Stage registry ────────────────────────────────────────────────────────────
# Maps stage name → dict of platform → adapter class

import os
if os.getenv('MOCK_PIPELINE'):
    from app.pipeline.mock_adapters import MOCK_STAGE_REGISTRY
    STAGE_REGISTRY = MOCK_STAGE_REGISTRY
    logger.info("MOCK_PIPELINE active — using fake adapters")
else:
    STAGE_REGISTRY: Dict[str, Dict[str, Type[StageAdapter]]] = {
        'discovery':   discovery_mod.ADAPTERS,
        'pre_screen':  prescreen_mod.ADAPTERS,
        'enrichment':  enrichment_mod.ADAPTERS,
        'analysis':    analysis_mod.ADAPTERS,
        'scoring':     scoring_mod.ADAPTERS,
        'crm_sync':    crm_mod.ADAPTERS,
    }


# ── Public API ────────────────────────────────────────────────────────────────

def launch_run(platform: str, filters: dict, bdr_names: list = None) -> Run:
    """
    Create a new Run and enqueue the pipeline as a background RQ job.

    The run_pipeline function executes all 6 stages sequentially, using the
    appropriate adapter for each stage based on the platform.
    """
    if platform not in STAGE_REGISTRY['discovery']:
        raise ValueError(f"Unsupported platform: {platform}. "
                         f"Available: {list(STAGE_REGISTRY['discovery'].keys())}")

    # Ensure BDR names in filters
    if bdr_names:
        filters['bdr_names'] = bdr_names
    elif 'bdr_names' not in filters:
        filters['bdr_names'] = list(BDR_OWNER_IDS.keys())

    run = Run(
        platform=platform,
        filters=filters,
        bdr_assignment=', '.join(filters.get('bdr_names', [])),
    )

    # Pre-run cost estimate
    try:
        run.estimated_cost = _estimate_total_cost(platform, filters)
    except Exception:
        pass

    run.save()
    persist_run(run)

    # Launch async via RQ
    _get_queue().enqueue(run_pipeline, run.id, job_timeout=14400)

    return run


def get_run_status(run_id: str) -> dict:
    """Get the current status of a run."""
    run = Run.load(run_id)
    if not run:
        return None
    return run.to_dict()


# ── Pipeline runner (enqueued via RQ) ─────────────────────────────────────────

def run_pipeline(run_id: str, retry_from_stage: str = None):
    """
    Execute all 6 pipeline stages for a run.

    Each stage:
    1. Look up the adapter for run.platform
    2. Call adapter.run(profiles, run)
    3. Update run progress
    4. Pass result.profiles to next stage

    If a stage fails, the run is marked as failed and processing stops.
    If a stage returns 0 profiles, the pipeline stops early (nothing to process).

    If retry_from_stage is set, load checkpoint from stage_outputs and skip
    stages before the retry point.
    """
    run = Run.load(run_id)
    if not run:
        logger.error("Run %s not found", run_id)
        return

    logger.info("Starting run %s for platform=%s", run_id, run.platform)
    profiles = []

    # Handle retry from checkpoint
    skipping = False
    if retry_from_stage:
        skipping = True
        stage_idx = PIPELINE_STAGES.index(retry_from_stage) if retry_from_stage in PIPELINE_STAGES else 0
        if stage_idx > 0:
            prev_stage = PIPELINE_STAGES[stage_idx - 1]
            checkpoint = (run.stage_outputs or {}).get(prev_stage)
            if checkpoint:
                profiles = checkpoint
                logger.info("Retrying from '%s', loaded %d profiles from '%s' checkpoint", retry_from_stage, len(profiles), prev_stage)
            else:
                logger.info("No checkpoint for '%s', starting from scratch", prev_stage)
                skipping = False

    for stage_name in PIPELINE_STAGES:
        # Skip stages before retry point
        if skipping:
            if stage_name == retry_from_stage:
                skipping = False
            else:
                continue

        adapters = STAGE_REGISTRY.get(stage_name)
        if not adapters:
            logger.warning("No registry for stage '%s', skipping", stage_name)
            continue

        # Get adapter for this platform + stage
        try:
            adapter = get_adapter(adapters, run.platform)
        except ValueError as e:
            logger.warning("%s — skipping stage '%s'", e, stage_name)
            continue

        # Cost guardrail: check max_budget before each stage
        max_budget = run.filters.get('max_budget')
        if max_budget is None:
            max_budget = get_default_budget(run.platform)
        if max_budget:
            est = adapter.estimate_cost(len(profiles))
            projected = (run.actual_cost or 0) + est
            # Hard stop: projected cost exceeds budget
            if projected > max_budget:
                logger.warning("Budget exceeded: actual=%.2f + est=%.2f > max=%.2f", run.actual_cost, est, max_budget)
                run.fail(f"Budget limit ${max_budget:.2f} would be exceeded (spent ${run.actual_cost:.2f}, next stage ~${est:.2f})")
                run.summary = _generate_run_summary(run, failed=True)
                run.save()
                persist_run(run)
                notify_run_failed(run)
                return
            # Warning threshold: log warning but continue
            warning_ratio = get_warning_threshold(run.platform)
            if projected > max_budget * warning_ratio:
                pct = int(warning_ratio * 100)
                msg = f"Cost at {pct}%+ of budget (${projected:.2f} / ${max_budget:.2f})"
                logger.warning("%s", msg)
                run.add_error(stage_name, msg)

        # Update run status
        status_map = {
            'discovery': 'discovering',
            'pre_screen': 'pre_screening',
            'enrichment': 'enriching',
            'analysis': 'analyzing',
            'scoring': 'scoring',
            'crm_sync': 'syncing',
        }
        run.update_stage(stage_name, status=status_map.get(stage_name, stage_name))

        # Set stage totals
        run.stage_progress[stage_name]['total'] = len(profiles) if stage_name != 'discovery' else 0

        # Execute
        try:
            logger.info("Stage '%s' — %s — %d profiles in", stage_name, adapter.__class__.__name__, len(profiles))
            result: StageResult = adapter.run(profiles, run)

            # Update progress
            run.stage_progress[stage_name]['completed'] = result.processed - result.failed
            run.stage_progress[stage_name]['failed'] = result.failed

            # Log errors
            for error in result.errors:
                run.add_error(stage_name, error)

            # Accumulate actual cost
            if result.cost > 0:
                run.actual_cost = (run.actual_cost or 0) + result.cost

            # Update aggregate counters
            if stage_name == 'discovery':
                run.profiles_found = len(result.profiles)
            elif stage_name == 'pre_screen':
                run.profiles_pre_screened = len(result.profiles)
            elif stage_name == 'enrichment':
                run.profiles_enriched = len(result.profiles)
            elif stage_name == 'scoring':
                run.profiles_scored = len(result.profiles)

            run.save()

            profiles = result.profiles

            # Post-discovery: dedup against existing leads
            if stage_name == 'discovery' and profiles:
                total_before = len(profiles)
                profiles, dupes = dedup_profiles(profiles, run.platform)
                run.duplicates_skipped = dupes
                run.profiles_found = len(profiles)
                run.save()

                # Record filter fingerprint
                record_filter_history(run, len(profiles), total_before)

            # Checkpoint profiles after each stage for retry-from-stage
            try:
                # Only store serializable data (strip large binary fields)
                checkpoint_profiles = []
                for p in profiles:
                    cp = {k: v for k, v in p.items() if isinstance(v, (str, int, float, bool, list, dict, type(None)))}
                    checkpoint_profiles.append(cp)
                if not run.stage_outputs:
                    run.stage_outputs = {}
                run.stage_outputs[stage_name] = checkpoint_profiles
                run.save()
            except Exception:
                logger.warning("Failed to checkpoint stage '%s' — continuing", stage_name)

            logger.info("Stage '%s' done — %d profiles out (processed=%d, failed=%d, skipped=%d)",
                        stage_name, len(profiles), result.processed, result.failed, result.skipped)

            # Early exit if no profiles to process
            if not profiles and stage_name != 'crm_sync':
                logger.warning("No profiles after '%s' — stopping early", stage_name)
                run.summary = _generate_run_summary(run)
                run.complete()
                persist_run(run)
                persist_lead_results(run, profiles)
                persist_metric_snapshot(run)
                notify_run_complete(run)
                return

        except Exception as e:
            logger.error("Stage '%s' FAILED: %s", stage_name, e)
            traceback.print_exc()
            run.fail(f"Stage '{stage_name}' failed: {str(e)}")
            run.summary = _generate_run_summary(run, failed=True)
            run.save()
            persist_run(run)
            notify_run_failed(run)
            return

    # All stages complete
    run.summary = _generate_run_summary(run)
    run.complete()
    persist_run(run)
    persist_lead_results(run, profiles)
    persist_metric_snapshot(run)
    notify_run_complete(run)
    logger.info("Run %s completed — found=%s, scored=%s, synced=%s",
                run_id, run.profiles_found, run.profiles_scored, run.contacts_synced)


# ── Run summary generator ────────────────────────────────────────────────────

def _generate_run_summary(run, failed: bool = False) -> str:
    """Generate a human-readable summary of the run. Pure Python, no API calls.

    Produces a narrative funnel summary with contextual warnings.
    Works for both completed and failed runs.
    """
    found = run.profiles_found or 0
    dupes = run.duplicates_skipped or 0
    prescreened = run.profiles_pre_screened or 0
    enriched = run.profiles_enriched or 0
    scored = run.profiles_scored or 0
    synced = run.contacts_synced or 0
    tier = run.tier_distribution or {}
    auto = tier.get('auto_enroll', 0)
    high_pri = tier.get('high_priority_review', 0)
    estimated = run.estimated_cost or 0.0
    actual = run.actual_cost or 0.0
    platform = (run.platform or 'unknown').capitalize()

    # ── Zero-results short circuit ──
    if found == 0 and not failed:
        return f"No {platform} profiles found. Check filters and try again."

    # ── Failed run ──
    if failed:
        return _generate_failed_summary(
            run, platform=platform, found=found, dupes=dupes,
            prescreened=prescreened, enriched=enriched, scored=scored,
            actual=actual,
        )

    # ── Narrative funnel ──
    lines = []

    # Discovery line
    discovery = f"Discovered {found} {platform} profiles"
    if dupes:
        discovery += f" ({dupes} duplicates removed)"
    discovery += "."
    lines.append(discovery)

    # Pre-screen line
    if prescreened:
        if found > 0:
            yield_pct = round((prescreened / found) * 100)
            lines.append(f"{prescreened} of {found} passed pre-screen ({yield_pct}% yield).")
        else:
            lines.append(f"{prescreened} passed pre-screen.")

    # Enrichment (only mention if there were failures worth noting)
    if prescreened > 0 and enriched > 0 and enriched < prescreened:
        lines.append(f"{enriched} of {prescreened} enriched successfully.")

    # Scoring + CRM sync line — "42 synced to CRM — 8 auto-enroll, 18 high priority."
    crm_parts = []
    if synced:
        crm_parts.append(f"{synced} synced to CRM")
    tier_details = []
    if auto:
        tier_details.append(f"{auto} auto-enroll")
    if high_pri:
        tier_details.append(f"{high_pri} high priority")
    if crm_parts:
        line = crm_parts[0]
        if tier_details:
            line += ' — ' + ', '.join(tier_details)
        line += '.'
        lines.append(line)

    # Conversion rate
    if found > 0 and synced > 0:
        conv = round((synced / found) * 100)
        lines.append(f"{conv}% overall conversion.")

    # Cost
    if actual > 0:
        lines.append(f"~${actual:.2f} spent.")

    # ── Warnings ──
    warnings = _collect_warnings(
        run, failed=False, found=found, prescreened=prescreened,
        enriched=enriched, scored=scored, synced=synced,
        auto=auto, estimated=estimated, actual=actual,
    )

    if warnings:
        lines.append('Warning: ' + ' '.join(warnings))

    # Benchmark deviations
    try:
        baseline = get_baseline(run.platform)
        if baseline:
            devs = compute_deviations(run, baseline)
            for d in devs:
                lines.append(f"{d.label} {abs(d.pct_change):.0f}% {d.direction} 30-day average.")
    except Exception:
        pass

    return ' '.join(lines)


def _generate_failed_summary(
    run, *, platform, found, dupes, prescreened, enriched, scored, actual,
) -> str:
    """Build summary for a failed run, including partial progress and error context."""
    # Extract error reason from the last error entry
    last_error = ''
    if run.errors:
        last_err = run.errors[-1] if isinstance(run.errors, list) else run.errors
        if isinstance(last_err, dict):
            last_error = last_err.get('message', '')
        elif isinstance(last_err, str):
            last_error = last_err

    stage = run.current_stage or 'unknown'
    stage_label = stage.replace('_', ' ')

    # Determine partial progress within the failing stage from stage_progress
    progress = getattr(run, 'stage_progress', None) or {}
    stage_info = progress.get(stage, {})
    stage_completed = stage_info.get('completed', 0)
    stage_total = stage_info.get('total', 0)

    parts = []

    # Opening line — include partial progress when available
    if stage_total > 0 and stage_completed > 0:
        parts.append(
            f"{platform} run failed at {stage_label} stage after processing "
            f"{stage_completed} of {stage_total} profiles."
        )
    else:
        parts.append(f"{platform} run failed during {stage_label}.")

    # Funnel context — what completed before the failure
    progress_notes = []
    if found:
        note = f"discovered {found}"
        if dupes:
            note += f" ({dupes} duplicates removed)"
        progress_notes.append(note)
    if prescreened:
        progress_notes.append(f"{prescreened} passed pre-screen")
    if enriched:
        progress_notes.append(f"{enriched} enriched")
    if scored:
        progress_notes.append(f"{scored} scored")
    if progress_notes:
        parts.append("Before failure: " + ', '.join(progress_notes) + '.')

    # Cost incurred before failure
    if actual > 0:
        parts.append(f"~${actual:.2f} spent before failure.")

    # Error reason
    if last_error:
        parts.append(f"Error: {last_error}")

    return ' '.join(parts)


def _collect_warnings(
    run, *, failed, found, prescreened, enriched, scored, synced,
    auto, estimated, actual,
) -> list:
    """Collect contextual warning strings for the summary."""
    warnings = []

    # Low pre-screen yield
    if found > 0 and prescreened > 0:
        yield_pct = round((prescreened / found) * 100)
        if yield_pct < 30:
            warnings.append(f"Pre-screen yield was low at {yield_pct}%.")

    # High enrichment failure rate
    if prescreened > 0 and enriched > 0 and enriched < (prescreened * 0.8):
        fail_pct = round((1 - enriched / prescreened) * 100)
        warnings.append(f"{fail_pct}% of profiles failed enrichment.")

    # No auto-enrolls despite scoring
    if scored > 0 and auto == 0:
        warnings.append("No auto-enroll candidates found.")

    # Cost overrun
    if estimated > 0 and actual > estimated * 1.2:
        overrun_pct = round(((actual - estimated) / estimated) * 100)
        warnings.append(f"Cost exceeded estimate by {overrun_pct}%.")

    # Early pipeline exit (completed but didn't reach CRM sync)
    stage = run.current_stage or ''
    if not failed and synced == 0 and scored == 0 and stage and stage != 'crm_sync':
        stage_label = stage.replace('_', ' ')
        warnings.append(f"Pipeline stopped after {stage_label} — no profiles advanced further.")

    return warnings


# ── Cost estimation ──────────────────────────────────────────────────────────

def _estimate_total_cost(platform: str, filters: dict) -> float:
    """Estimate total pipeline cost based on max_results."""
    count = filters.get('max_results', 100)
    total = 0.0
    for stage_name in PIPELINE_STAGES:
        adapters = STAGE_REGISTRY.get(stage_name, {})
        adapter_cls = adapters.get(platform)
        if adapter_cls:
            total += adapter_cls().estimate_cost(count)
            # Profile count drops through funnel — rough estimate
            if stage_name == 'discovery':
                count = int(count * 0.7)  # ~30% drop at prescreen
            elif stage_name == 'pre_screen':
                count = int(count * 0.9)  # ~10% drop at enrichment
    return round(total, 2)
