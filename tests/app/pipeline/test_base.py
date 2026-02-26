"""Tests for app.pipeline.base — StageResult, StageAdapter, get_adapter, get_pipeline_info."""
import pytest
from typing import Dict, List, Any

from app.pipeline.base import StageResult, StageAdapter, get_adapter, get_pipeline_info


# ── Concrete adapter subclass for testing ────────────────────────────────────

class _DummyAdapter(StageAdapter):
    """Minimal concrete adapter for testing the abstract base class."""
    platform = 'instagram'
    stage = 'discovery'
    description = 'Dummy adapter for tests'
    apis = ['TestAPI']
    est_seconds_per_profile = 2.5

    def run(self, profiles: List[Dict[str, Any]], run: Any) -> StageResult:
        return StageResult(profiles=profiles, processed=len(profiles))


class _CostAdapter(StageAdapter):
    """Adapter that overrides estimate_cost."""
    platform = 'instagram'
    stage = 'scoring'
    description = 'Cost-aware adapter'
    apis = ['OpenAI']
    est_seconds_per_profile = 1.0

    def run(self, profiles, run):
        return StageResult(profiles=profiles, processed=len(profiles))

    def estimate_cost(self, count: int) -> float:
        return count * 0.02


class _PatreonAdapter(StageAdapter):
    """Second platform adapter for registry tests."""
    platform = 'patreon'
    stage = 'discovery'
    description = 'Patreon discovery'
    apis = ['PatreonAPI']
    est_seconds_per_profile = 3.0

    def run(self, profiles, run):
        return StageResult(profiles=profiles, processed=len(profiles))


class _MinimalAdapter(StageAdapter):
    """Adapter with bare minimum — no overrides on class attrs."""
    def run(self, profiles, run):
        return StageResult(profiles=profiles)


# ── StageResult ──────────────────────────────────────────────────────────────

class TestStageResult:
    """StageResult dataclass construction and defaults."""

    def test_minimal_construction(self):
        """Only profiles is required; everything else has defaults."""
        result = StageResult(profiles=[])
        assert result.profiles == []
        assert result.processed == 0
        assert result.failed == 0
        assert result.skipped == 0
        assert result.errors == []
        assert result.meta == {}
        assert result.cost == 0.0

    def test_construction_with_all_fields(self):
        """All fields can be set explicitly."""
        profiles = [{'username': 'alice'}]
        result = StageResult(
            profiles=profiles,
            processed=1,
            failed=0,
            skipped=2,
            errors=['rate limited'],
            meta={'api_calls': 5},
            cost=0.50,
        )
        assert result.profiles == profiles
        assert result.processed == 1
        assert result.skipped == 2
        assert result.errors == ['rate limited']
        assert result.meta == {'api_calls': 5}
        assert result.cost == 0.50

    def test_default_errors_list_is_independent(self):
        """Each instance gets its own errors list (no shared mutable default)."""
        r1 = StageResult(profiles=[])
        r2 = StageResult(profiles=[])
        r1.errors.append('oops')
        assert r2.errors == []

    def test_default_meta_dict_is_independent(self):
        """Each instance gets its own meta dict (no shared mutable default)."""
        r1 = StageResult(profiles=[])
        r2 = StageResult(profiles=[])
        r1.meta['key'] = 'value'
        assert r2.meta == {}

    def test_profiles_list_is_mutable(self):
        """Profiles can be modified after construction."""
        result = StageResult(profiles=[])
        result.profiles.append({'username': 'new'})
        assert len(result.profiles) == 1

    def test_cost_can_be_float(self):
        """Cost field accepts float values."""
        result = StageResult(profiles=[], cost=1.2345)
        assert result.cost == 1.2345


# ── StageAdapter ─────────────────────────────────────────────────────────────

class TestStageAdapter:
    """StageAdapter ABC: class attributes and abstract method contract."""

    def test_cannot_instantiate_abstract_class(self):
        """Instantiating StageAdapter directly raises TypeError."""
        with pytest.raises(TypeError):
            StageAdapter()

    def test_concrete_subclass_instantiates(self):
        """A subclass that implements run() can be created."""
        adapter = _DummyAdapter()
        assert adapter.platform == 'instagram'
        assert adapter.stage == 'discovery'

    def test_run_returns_stage_result(self):
        """Adapter.run() returns a StageResult."""
        adapter = _DummyAdapter()
        result = adapter.run([{'user': 'a'}], run=None)
        assert isinstance(result, StageResult)
        assert result.processed == 1

    def test_default_estimate_cost_returns_zero(self):
        """Base estimate_cost() returns 0.0 for any count."""
        adapter = _DummyAdapter()
        assert adapter.estimate_cost(0) == 0.0
        assert adapter.estimate_cost(100) == 0.0
        assert adapter.estimate_cost(999) == 0.0

    def test_overridden_estimate_cost(self):
        """Subclass can override estimate_cost()."""
        adapter = _CostAdapter()
        assert adapter.estimate_cost(100) == 2.0
        assert adapter.estimate_cost(50) == 1.0
        assert adapter.estimate_cost(0) == 0.0

    def test_class_attributes_have_defaults(self):
        """Base class attrs default to empty values."""
        adapter = _MinimalAdapter()
        assert adapter.platform == ''
        assert adapter.stage == ''
        assert adapter.description == ''
        assert adapter.apis == []
        assert adapter.est_seconds_per_profile is None

    def test_description_and_apis_on_subclass(self):
        """Subclass class attributes are accessible on instances."""
        adapter = _DummyAdapter()
        assert adapter.description == 'Dummy adapter for tests'
        assert adapter.apis == ['TestAPI']
        assert adapter.est_seconds_per_profile == 2.5


# ── get_adapter ──────────────────────────────────────────────────────────────

class TestGetAdapter:
    """get_adapter() looks up and instantiates platform adapters."""

    def test_returns_adapter_instance_for_valid_platform(self):
        """Returns an instantiated adapter for a registered platform."""
        adapters = {'instagram': _DummyAdapter}
        adapter = get_adapter(adapters, 'instagram')
        assert isinstance(adapter, _DummyAdapter)
        assert isinstance(adapter, StageAdapter)

    def test_raises_value_error_for_unknown_platform(self):
        """Raises ValueError when platform is not in the registry."""
        adapters = {'instagram': _DummyAdapter}
        with pytest.raises(ValueError, match="No adapter registered for platform 'tiktok'"):
            get_adapter(adapters, 'tiktok')

    def test_raises_value_error_for_empty_registry(self):
        """Raises ValueError when the adapters dict is empty."""
        with pytest.raises(ValueError, match="No adapter registered"):
            get_adapter({}, 'instagram')

    def test_returns_new_instance_each_call(self):
        """Each call returns a fresh instance, not a shared singleton."""
        adapters = {'instagram': _DummyAdapter}
        a1 = get_adapter(adapters, 'instagram')
        a2 = get_adapter(adapters, 'instagram')
        assert a1 is not a2

    def test_selects_correct_adapter_from_multi_platform_registry(self):
        """Picks the right adapter when multiple platforms are registered."""
        adapters = {
            'instagram': _DummyAdapter,
            'patreon': _PatreonAdapter,
        }
        ig = get_adapter(adapters, 'instagram')
        pa = get_adapter(adapters, 'patreon')
        assert isinstance(ig, _DummyAdapter)
        assert isinstance(pa, _PatreonAdapter)

    def test_error_message_includes_platform_name(self):
        """Error message mentions the missing platform."""
        with pytest.raises(ValueError, match="youtube"):
            get_adapter({'instagram': _DummyAdapter}, 'youtube')


# ── get_pipeline_info ────────────────────────────────────────────────────────

class TestGetPipelineInfo:
    """get_pipeline_info() serializes the stage registry for API responses."""

    def test_single_platform_single_stage(self):
        """One platform with one stage produces correct structure."""
        registry = {
            'discovery': {'instagram': _DummyAdapter},
        }
        info = get_pipeline_info(registry)
        assert 'instagram' in info
        assert 'discovery' in info['instagram']
        assert info['instagram']['discovery']['description'] == 'Dummy adapter for tests'
        assert info['instagram']['discovery']['apis'] == ['TestAPI']
        assert info['instagram']['discovery']['est'] == 2.5

    def test_multiple_platforms_multiple_stages(self):
        """Multiple platforms and stages are all represented."""
        registry = {
            'discovery': {
                'instagram': _DummyAdapter,
                'patreon': _PatreonAdapter,
            },
            'scoring': {
                'instagram': _CostAdapter,
            },
        }
        info = get_pipeline_info(registry)

        assert set(info.keys()) == {'instagram', 'patreon'}
        assert set(info['instagram'].keys()) == {'discovery', 'scoring'}
        assert set(info['patreon'].keys()) == {'discovery'}

    def test_empty_registry_returns_empty_dict(self):
        """Empty registry produces empty result."""
        assert get_pipeline_info({}) == {}

    def test_adapter_with_no_description_defaults_to_empty_string(self):
        """Adapter with empty description serializes as ''."""
        registry = {'discovery': {'test': _MinimalAdapter}}
        info = get_pipeline_info(registry)
        assert info['test']['discovery']['description'] == ''

    def test_adapter_with_no_apis_serializes_as_empty_list(self):
        """Adapter with empty apis list serializes as []."""
        registry = {'discovery': {'test': _MinimalAdapter}}
        info = get_pipeline_info(registry)
        assert info['test']['discovery']['apis'] == []

    def test_est_none_when_not_set(self):
        """est is None when est_seconds_per_profile is not set."""
        registry = {'discovery': {'test': _MinimalAdapter}}
        info = get_pipeline_info(registry)
        assert info['test']['discovery']['est'] is None

    def test_same_platform_across_stages_merges_correctly(self):
        """A platform appearing in multiple stages gets all entries under one key."""
        registry = {
            'discovery': {'instagram': _DummyAdapter},
            'scoring': {'instagram': _CostAdapter},
        }
        info = get_pipeline_info(registry)
        assert info['instagram']['discovery']['apis'] == ['TestAPI']
        assert info['instagram']['scoring']['apis'] == ['OpenAI']

    def test_output_is_json_serializable(self):
        """Result can be serialized to JSON without errors."""
        import json
        registry = {
            'discovery': {'instagram': _DummyAdapter, 'patreon': _PatreonAdapter},
            'scoring': {'instagram': _CostAdapter},
        }
        info = get_pipeline_info(registry)
        serialized = json.dumps(info)
        assert isinstance(serialized, str)
        roundtripped = json.loads(serialized)
        assert roundtripped == info
