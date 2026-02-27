"""
Pipeline stage contracts.

Every stage adapter implements StageAdapter.run() and returns a StageResult.
Platform-specific logic lives in concrete adapter classes; the pipeline manager
only sees the uniform interface.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Dict, List, Any, Type


@dataclass
class StageResult:
    """Uniform output from every pipeline stage."""
    profiles: List[Dict[str, Any]]
    processed: int = 0
    failed: int = 0
    skipped: int = 0
    errors: List[str] = field(default_factory=list)
    meta: Dict[str, Any] = field(default_factory=dict)
    cost: float = 0.0


class StageAdapter(ABC):
    """
    Base class for all pipeline stage adapters.

    Each platform implements one adapter per stage. The adapter receives
    the current list of profiles and the Run object, does its work,
    and returns a StageResult with the (possibly modified) profiles.
    """
    platform: str = ''
    stage: str = ''

    # Metadata — shown in the discovery "What happens" diagram
    description: str = ''                 # e.g. "InsightIQ API — followers & interests"
    apis: List[str] = []                  # e.g. ["InsightIQ", "OpenAI"]
    est_seconds_per_profile: float = None # None = unknown / not yet measured

    @abstractmethod
    def run(self, profiles: List[Dict[str, Any]], run: Any) -> StageResult:
        """
        Execute this stage on the given profiles.

        Args:
            profiles: List of profile dicts from the previous stage.
                      For discovery, this is an empty list.
            run:      The Run object — use run.filters, run.id, etc.
                      Adapter can call run.increment_stage_progress()
                      for real-time progress updates.

        Returns:
            StageResult with the profiles to pass to the next stage.
        """
        ...

    def estimate_cost(self, count: int) -> float:
        """Optional: estimate API cost for N profiles."""
        return 0.0


# ── Stage registry ────────────────────────────────────────────────────────────
# Each stage module populates its own ADAPTERS dict, e.g.:
#   ADAPTERS = {'instagram': InstagramDiscovery, 'patreon': PatreonDiscovery, ...}
#
# The manager imports all of them into STAGE_REGISTRY:
#   STAGE_REGISTRY = {
#       'discovery':   discovery.ADAPTERS,
#       'pre_screen':  prescreen.ADAPTERS,
#       ...
#   }


def get_adapter(stage_adapters: Dict[str, Type[StageAdapter]], platform: str) -> StageAdapter:
    """Look up and instantiate the adapter for a platform."""
    adapter_cls = stage_adapters.get(platform)
    if not adapter_cls:
        raise ValueError(f"No adapter registered for platform '{platform}' in this stage")
    return adapter_cls()


def get_pipeline_info(stage_registry: Dict[str, Dict[str, Type[StageAdapter]]]) -> Dict[str, Any]:
    """
    Serialize the full stage registry into a JSON-friendly dict.

    Returns: { "instagram": { "discovery": { "description": "...", "apis": [...], "est": null }, ... }, ... }
    """
    result = {}
    for stage_name, adapters in stage_registry.items():
        for platform, cls in adapters.items():
            if platform not in result:
                result[platform] = {}
            result[platform][stage_name] = {
                'description': cls.description or '',
                'apis': cls.apis if isinstance(cls.apis, list) else [],
                'est': cls.est_seconds_per_profile,
            }
    return result
