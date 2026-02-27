"""
Cost configuration loader — per-platform budgets, rates, and guardrails.

Follows the same pattern as scoring_config.yaml: YAML file with in-memory
cache and hardcoded fallback if the file is missing.
"""
import logging
import os
from typing import Optional

import yaml

logger = logging.getLogger('pipeline.cost')


_cost_config = None


def _default_config():
    """Hardcoded fallback if YAML is missing."""
    return {
        'version': 'default',
        'defaults': {
            'instagram': {'max_budget': 50.00, 'warning_threshold': 0.80},
            'patreon':   {'max_budget': 20.00, 'warning_threshold': 0.80},
            'facebook':  {'max_budget': 20.00, 'warning_threshold': 0.80},
        },
        'rates': {
            'instagram': {'discovery': 0.02, 'pre_screen': 0.05, 'analysis': 0.15, 'scoring': 0.02},
            'patreon':   {'discovery': 0.01, 'enrichment': 0.05, 'analysis': 0.10, 'scoring': 0.02},
            'facebook':  {'discovery': 0.01, 'enrichment': 0.05, 'analysis': 0.10, 'scoring': 0.02},
        },
        'guardrails': {
            'confirmation_threshold': 10.00,
            'absolute_max': 200.00,
        },
    }


def load_cost_config() -> dict:
    """Load cost config from YAML, with in-memory cache and hardcoded fallback."""
    global _cost_config
    if _cost_config is not None:
        return _cost_config

    config_path = os.path.join(os.path.dirname(__file__), 'cost_config.yaml')
    try:
        with open(config_path, 'r') as f:
            _cost_config = yaml.safe_load(f)
        logger.info("Config loaded from YAML (version=%s)", _cost_config.get('version', '?'))
    except Exception as e:
        logger.warning("YAML config not found (%s), using defaults", e)
        _cost_config = _default_config()

    return _cost_config


def get_rate(platform: str, stage: str) -> float:
    """Get the per-profile cost rate for a platform + stage."""
    cfg = load_cost_config()
    return cfg.get('rates', {}).get(platform, {}).get(stage, 0.0)


def get_default_budget(platform: str) -> float:
    """Get the default max_budget for a platform."""
    cfg = load_cost_config()
    return cfg.get('defaults', {}).get(platform, {}).get('max_budget', 50.00)


def get_warning_threshold(platform: str) -> float:
    """Get the warning threshold ratio (0-1) for a platform."""
    cfg = load_cost_config()
    return cfg.get('defaults', {}).get(platform, {}).get('warning_threshold', 0.80)


def get_confirmation_threshold() -> float:
    """Get the cost estimate above which a confirmation dialog is shown."""
    cfg = load_cost_config()
    return cfg.get('guardrails', {}).get('confirmation_threshold', 10.00)


def get_absolute_max() -> float:
    """Hard ceiling on budget regardless of user input."""
    cfg = load_cost_config()
    return cfg.get('guardrails', {}).get('absolute_max', 200.00)


def reset_cache():
    """Reset the in-memory cache (useful for testing)."""
    global _cost_config
    _cost_config = None
