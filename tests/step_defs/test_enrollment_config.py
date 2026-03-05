"""Step definitions for enrollment_config.feature — config priority chain + dashboard."""
from unittest.mock import patch
from pytest_bdd import scenarios, given, when, then, parsers
import pytest

from app.services.enrollment_config import load_enrollment_config, _DEFAULTS

scenarios('../features/enrollment_config.feature')


# ── Helpers ───────────────────────────────────────────────────────────────

def _make_config(n_inboxes, **overrides):
    """Build a config dict with n generic inboxes."""
    inboxes = {f'BDR{i+1}': str(1000 + i) for i in range(n_inboxes)}
    return {**_DEFAULTS, 'inboxes': inboxes, **overrides}


@pytest.fixture
def context():
    return {}


# ── Given steps ───────────────────────────────────────────────────────────

@given('no database config exists')
def no_db_config(context):
    context['db_config'] = None


@given(parsers.parse('the YAML file defines {n:d} inboxes'))
def yaml_defines_inboxes(n, context):
    context['yaml_config'] = _make_config(n)


@given(parsers.parse('the database config defines {n:d} inboxes'))
def db_defines_inboxes(n, context):
    context['db_config'] = _make_config(n)


@given('no YAML file exists')
def no_yaml(context):
    context['yaml_config'] = {}


@given(parsers.parse(
    'the YAML file defines weights schedule_call={sc:g} interest_check={ic:g} self_service={ss:g}'
))
def yaml_defines_weights(sc, ic, ss, context):
    weights = {'schedule_call': sc, 'interest_check': ic, 'self_service': ss}
    context['yaml_config'] = _make_config(2, outreach_weights=weights)


@given(parsers.parse('the YAML file defines max_per_day {n:d}'))
def yaml_defines_max(n, context):
    context['yaml_config'] = _make_config(2, max_per_day=n)


# ── When steps ────────────────────────────────────────────────────────────

@when('the config is loaded')
def config_loaded(context):
    db_val = context.get('db_config')
    yaml_val = context.get('yaml_config', {})
    with patch('app.services.enrollment_config._load_db', return_value=db_val), \
         patch('app.services.enrollment_config._load_yaml', return_value=yaml_val):
        context['loaded_config'] = load_enrollment_config()


@when(parsers.parse('the user saves config with {n:d} inboxes via the dashboard'))
def save_n_inboxes(n, context):
    context['db_config'] = _make_config(n)


@when('the user resets config to file defaults')
def reset_to_defaults(context):
    context['db_config'] = None


@when(parsers.parse('the user saves config with an added inbox "{name}" via the dashboard'))
def save_with_added_inbox(name, context):
    yaml_cfg = context.get('yaml_config', {})
    inboxes = dict(yaml_cfg.get('inboxes', {}))
    inboxes[name] = '9999999'
    context['db_config'] = {**yaml_cfg, 'inboxes': inboxes}


@when(parsers.parse('the user saves config without inbox "{name}" via the dashboard'))
def save_without_inbox(name, context):
    yaml_cfg = context.get('yaml_config', {})
    inboxes = {k: v for k, v in yaml_cfg.get('inboxes', {}).items() if k != name}
    context['db_config'] = {**yaml_cfg, 'inboxes': inboxes}


@when(parsers.parse(
    'the user saves config with weights schedule_call={sc:g} interest_check={ic:g} self_service={ss:g}'
))
def save_weights(sc, ic, ss, context):
    base = context.get('yaml_config', _make_config(2))
    weights = {'schedule_call': sc, 'interest_check': ic, 'self_service': ss}
    context['db_config'] = {**base, 'outreach_weights': weights}


@when(parsers.parse('the user saves config with max_per_day {n:d}'))
def save_max(n, context):
    base = context.get('yaml_config', _make_config(2))
    context['db_config'] = {**base, 'max_per_day': n}


# ── Then steps ────────────────────────────────────────────────────────────

@then(parsers.parse('the config has {n:d} inboxes'))
def config_has_n_inboxes(n, context):
    actual = len(context['loaded_config']['inboxes'])
    assert actual == n, f"Expected {n} inboxes, got {actual}: {context['loaded_config']['inboxes']}"


@then(parsers.parse('the config has max_per_day {n:d}'))
def config_has_max(n, context):
    assert context['loaded_config']['max_per_day'] == n


@then(parsers.parse('the config includes inbox "{name}"'))
def config_includes_inbox(name, context):
    assert name in context['loaded_config']['inboxes'], \
        f"Expected '{name}' in {list(context['loaded_config']['inboxes'].keys())}"


@then(parsers.parse('the config does not include inbox "{name}"'))
def config_excludes_inbox(name, context):
    assert name not in context['loaded_config']['inboxes'], \
        f"Expected '{name}' NOT in {list(context['loaded_config']['inboxes'].keys())}"


@then(parsers.parse('the config weight for "{segment}" is {expected:g}'))
def config_weight(segment, expected, context):
    actual = context['loaded_config']['outreach_weights'].get(segment)
    assert actual == expected, f"Expected weight {expected} for {segment}, got {actual}"
