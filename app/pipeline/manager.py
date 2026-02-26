"""
Pipeline Manager — Run orchestration using the stage adapter registry.

Launches a Run through the 6 pipeline stages:
  DISCOVERY → PRE-SCREEN → ENRICHMENT → ANALYSIS → SCORING → CRM SYNC

Each stage looks up the platform's adapter from the registry, calls adapter.run(),
and feeds the result to the next stage. Progress is tracked on the Run object.
"""
import json
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
    print("[Pipeline] ⚠ MOCK_PIPELINE active — using fake adapters")
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
        print(f"[Pipeline] Run {run_id} not found")
        return

    print(f"[Pipeline] Starting run {run_id} for platform={run.platform}")
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
                print(f"[Pipeline] Retrying from '{retry_from_stage}', loaded {len(profiles)} profiles from '{prev_stage}' checkpoint")
            else:
                print(f"[Pipeline] No checkpoint for '{prev_stage}', starting from scratch")
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
            print(f"[Pipeline] No registry for stage '{stage_name}', skipping")
            continue

        # Get adapter for this platform + stage
        try:
            adapter = get_adapter(adapters, run.platform)
        except ValueError as e:
            print(f"[Pipeline] {e} — skipping stage '{stage_name}'")
            continue

        # Cost guardrail: check max_budget before each stage
        max_budget = run.filters.get('max_budget')
        if max_budget and run.actual_cost > 0:
            est = adapter.estimate_cost(len(profiles))
            if run.actual_cost + est > max_budget:
                print(f"[Pipeline] Budget exceeded: actual={run.actual_cost:.2f} + est={est:.2f} > max={max_budget:.2f}")
                run.fail(f"Budget limit ${max_budget:.2f} would be exceeded (spent ${run.actual_cost:.2f}, next stage ~${est:.2f})")
                persist_run(run)
                notify_run_failed(run)
                return

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
            print(f"[Pipeline] Stage '{stage_name}' — {adapter.__class__.__name__} — {len(profiles)} profiles in")
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
                print(f"[Pipeline] Failed to checkpoint stage '{stage_name}' — continuing")

            print(f"[Pipeline] Stage '{stage_name}' done — {len(profiles)} profiles out "
                  f"(processed={result.processed}, failed={result.failed}, skipped={result.skipped})")

            # Early exit if no profiles to process
            if not profiles and stage_name != 'crm_sync':
                print(f"[Pipeline] No profiles after '{stage_name}' — stopping early")
                run.summary = _generate_run_summary(run)
                run.complete()
                persist_run(run)
                persist_lead_results(run, profiles)
                notify_run_complete(run)
                return

        except Exception as e:
            print(f"[Pipeline] Stage '{stage_name}' FAILED: {e}")
            traceback.print_exc()
            run.fail(f"Stage '{stage_name}' failed: {str(e)}")
            persist_run(run)
            notify_run_failed(run)
            return

    # All stages complete
    run.summary = _generate_run_summary(run)
    run.complete()
    persist_run(run)
    persist_lead_results(run, profiles)
    notify_run_complete(run)
    print(f"[Pipeline] Run {run_id} completed — "
          f"found={run.profiles_found}, scored={run.profiles_scored}, synced={run.contacts_synced}")


# ── Run summary generator ────────────────────────────────────────────────────

def _generate_run_summary(run) -> str:
    """Generate a human-readable summary of the run. Pure Python, no API calls."""
    parts = [f"{run.platform.capitalize()} run."]

    found = run.profiles_found or 0
    dupes = run.duplicates_skipped or 0
    prescreened = run.profiles_pre_screened or 0
    enriched = run.profiles_enriched or 0
    scored = run.profiles_scored or 0
    synced = run.contacts_synced or 0

    parts.append(f"Found {found} profiles")
    if dupes:
        parts.append(f"skipped {dupes} duplicates")

    if prescreened:
        parts.append(f"{prescreened} passed pre-screen")
    if scored:
        parts.append(f"{scored} scored")
    if synced:
        parts.append(f"{synced} synced to CRM")

    # Tier highlights
    tier = run.tier_distribution or {}
    auto = tier.get('auto_enroll', 0)
    if auto:
        parts.append(f"{auto} auto-enroll")

    # Conversion rate
    if found > 0 and synced > 0:
        conv = round((synced / found) * 100)
        parts.append(f"{conv}% conversion")

    # Cost
    if run.actual_cost and run.actual_cost > 0:
        parts.append(f"~${run.actual_cost:.2f} spent")

    return ". ".join(parts[:3]) + ". " + ", ".join(parts[3:]) + "." if len(parts) > 3 else ". ".join(parts) + "."


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
