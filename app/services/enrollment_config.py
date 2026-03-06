"""
Enrollment config loader.

Priority chain: DB (app_config table) > YAML file defaults.
The DB layer is what makes config editable via the dashboard and persistent
across Railway deploys.
"""
import logging
import json
import pathlib

logger = logging.getLogger('services.enrollment_config')

YAML_PATH = pathlib.Path(__file__).resolve().parent.parent.parent / 'config' / 'enrollment.yml'
DB_KEY = 'enrollment'

# Defaults if nothing else is configured
_DEFAULTS = {
    'inboxes': {},
    'max_per_day': 25,
    'sequence_cadence': 3,
    'sequence_steps': 5,
    'outreach_weights': {
        'schedule_call': 4,
        'rewarm_schedule_call': 2,
        'interest_check': 2,
        'self_service': 1,
    },
    'inbox_allowed_types': {},
    'api_delay': 0.1,
    'timezone': 'America/Los_Angeles',
    'hubspot_properties': {
        'status_field': 'reply_sequence_queue_status',
        'inbox_field': 'reply_io_sequence',
        'date_field': 'recent_reply_sequence_enrolled_date',
        'segment_field': 'outreach_segment',
        'trigger_field': 'enroll_in_reply_sequence',
        'score_field': 'combined_lead_score',
        'createdate_field': 'hs_createdate',
    },
}


def _load_yaml():
    """Load config/enrollment.yml. Returns dict or empty dict."""
    try:
        import yaml
    except ImportError:
        return {}
    if not YAML_PATH.exists():
        return {}
    try:
        with open(YAML_PATH) as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.error("Failed to load enrollment YAML: %s", e)
        return {}


def _load_db():
    """Load config from Postgres app_config table. Returns dict or None."""
    try:
        from app.services.db import get_app_config
        return get_app_config(DB_KEY)
    except Exception as e:
        logger.debug("Could not load enrollment config from DB: %s", e)
        return None


def load_enrollment_config() -> dict:
    """Load enrollment config with priority: DB > YAML file > defaults.

    Called at runtime (not import time) so DB changes take effect immediately.
    """
    # Start with defaults
    cfg = dict(_DEFAULTS)

    # Layer 1: YAML file
    yaml_cfg = _load_yaml()
    if yaml_cfg:
        for key in _DEFAULTS:
            if key in yaml_cfg:
                cfg[key] = yaml_cfg[key]

    # Layer 2: DB (overrides YAML if present)
    db_cfg = _load_db()
    if db_cfg:
        for key in _DEFAULTS:
            if key in db_cfg:
                cfg[key] = db_cfg[key]

    return cfg


def save_enrollment_config(cfg: dict) -> bool:
    """Save enrollment config to Postgres for persistence across deploys."""
    try:
        from app.services.db import save_app_config
        return save_app_config(DB_KEY, cfg)
    except Exception as e:
        logger.error("Failed to save enrollment config: %s", e)
        return False


def config_to_yaml(cfg: dict) -> str:
    """Serialize config dict to YAML string for the editor UI."""
    try:
        import yaml
        return yaml.dump(cfg, default_flow_style=False, sort_keys=False, allow_unicode=True)
    except ImportError:
        return json.dumps(cfg, indent=2)


def yaml_to_config(yaml_str: str) -> dict:
    """Parse YAML string from the editor UI into a config dict."""
    import yaml
    return yaml.safe_load(yaml_str) or {}
