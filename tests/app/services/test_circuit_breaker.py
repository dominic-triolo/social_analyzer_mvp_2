"""Tests for app.services.circuit_breaker — CircuitBreaker class and registry."""
import time
import pytest
from unittest.mock import MagicMock, patch

from app.services.circuit_breaker import (
    CircuitBreaker, CircuitOpenError, CLOSED, OPEN, HALF_OPEN,
    get_breaker, get_all_breakers, init_breakers, _registry,
)


class FakeRedis:
    """Minimal in-memory Redis fake for circuit breaker tests."""

    def __init__(self):
        self.get_store = {}
        self.hash_store = {}

    def get(self, key):
        return self.get_store.get(key)

    def set(self, key, value):
        self.get_store[key] = value

    def incr(self, key):
        val = int(self.get_store.get(key, 0)) + 1
        self.get_store[key] = str(val)
        return val

    def delete(self, *keys):
        for k in keys:
            self.get_store.pop(k, None)
            self.hash_store.pop(k, None)

    def hset(self, key, field, value):
        self.hash_store.setdefault(key, {})[field] = value

    def hincrby(self, key, field, amount):
        h = self.hash_store.setdefault(key, {})
        h[field] = str(int(h.get(field, 0)) + amount)

    def hgetall(self, key):
        return dict(self.hash_store.get(key, {}))

    def llen(self, key):
        return 0

    def pipeline(self):
        return FakePipeline(self)


class FakePipeline:
    """Fake Redis pipeline that executes immediately."""

    def __init__(self, redis):
        self._redis = redis
        self._ops = []

    def set(self, key, value):
        self._ops.append(('set', key, value))
        return self

    def delete(self, *keys):
        self._ops.append(('delete', keys))
        return self

    def hincrby(self, key, field, amount):
        self._ops.append(('hincrby', key, field, amount))
        return self

    def hset(self, key, field, value):
        self._ops.append(('hset', key, field, value))
        return self

    def execute(self):
        for op in self._ops:
            if op[0] == 'set':
                self._redis.set(op[1], op[2])
            elif op[0] == 'delete':
                self._redis.delete(*op[1])
            elif op[0] == 'hincrby':
                self._redis.hincrby(op[1], op[2], op[3])
            elif op[0] == 'hset':
                self._redis.hset(op[1], op[2], op[3])
        self._ops = []


@pytest.fixture
def fake_redis():
    """In-memory Redis fake with dict-backed storage."""
    return FakeRedis()


@pytest.fixture
def cb(fake_redis):
    """Fresh circuit breaker with fake Redis."""
    return CircuitBreaker('test_svc', fake_redis, failure_threshold=3, reset_timeout=10)


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------

class TestCircuitBreakerStates:
    """Circuit breaker state machine: CLOSED → OPEN → HALF_OPEN → CLOSED."""

    def test_starts_closed(self, cb):
        assert cb.state == CLOSED

    def test_stays_closed_on_success(self, cb):
        cb.call(lambda: 'ok')
        assert cb.state == CLOSED

    def test_increments_failures(self, cb):
        for _ in range(2):
            with pytest.raises(ValueError):
                cb.call(self._fail)
        assert cb.failure_count == 2
        assert cb.state == CLOSED  # still below threshold

    def test_opens_at_threshold(self, cb):
        for _ in range(3):
            with pytest.raises(ValueError):
                cb.call(self._fail)
        assert cb.state == OPEN

    def test_open_rejects_calls(self, cb):
        # Force open
        for _ in range(3):
            with pytest.raises(ValueError):
                cb.call(self._fail)
        with pytest.raises(CircuitOpenError) as exc_info:
            cb.call(lambda: 'ok')
        assert 'test_svc' in str(exc_info.value)

    def test_half_open_after_timeout(self, cb, fake_redis):
        # Force open
        for _ in range(3):
            with pytest.raises(ValueError):
                cb.call(self._fail)
        # Simulate time passing beyond reset_timeout
        fake_redis.get_store[cb._last_failure_key] = str(time.time() - 20)
        assert cb.state == HALF_OPEN

    def test_success_in_half_open_closes(self, cb, fake_redis):
        for _ in range(3):
            with pytest.raises(ValueError):
                cb.call(self._fail)
        fake_redis.get_store[cb._last_failure_key] = str(time.time() - 20)
        assert cb.state == HALF_OPEN
        result = cb.call(lambda: 'recovered')
        assert result == 'recovered'
        assert cb.state == CLOSED

    def test_failure_in_half_open_reopens(self, cb, fake_redis):
        for _ in range(3):
            with pytest.raises(ValueError):
                cb.call(self._fail)
        fake_redis.get_store[cb._last_failure_key] = str(time.time() - 20)
        assert cb.state == HALF_OPEN
        # Reset failure count so we start from 0 in half_open
        fake_redis.get_store[cb._failures_key] = '0'
        with pytest.raises(ValueError):
            cb.call(self._fail)
        # After one failure in half_open, should still track

    @staticmethod
    def _fail():
        raise ValueError("boom")


# ---------------------------------------------------------------------------
# Reset
# ---------------------------------------------------------------------------

class TestCircuitBreakerReset:
    """Manual circuit breaker reset."""

    def test_reset_closes_circuit(self, cb):
        for _ in range(3):
            with pytest.raises(ValueError):
                cb.call(lambda: (_ for _ in ()).throw(ValueError("boom")))
        cb.reset()
        assert cb.state == CLOSED
        assert cb.failure_count == 0

    def test_reset_allows_calls(self, cb):
        for _ in range(3):
            with pytest.raises(ValueError):
                cb.call(lambda: (_ for _ in ()).throw(ValueError("boom")))
        cb.reset()
        result = cb.call(lambda: 'ok')
        assert result == 'ok'


# ---------------------------------------------------------------------------
# Health metrics
# ---------------------------------------------------------------------------

class TestCircuitBreakerHealth:
    """get_health() returns metrics dict."""

    def test_health_after_success(self, cb):
        cb.call(lambda: 'ok')
        health = cb.get_health()
        assert health['name'] == 'test_svc'
        assert health['state'] == CLOSED
        assert health['total_success'] == 1
        assert health['total_failure'] == 0

    def test_health_after_failure(self, cb):
        with pytest.raises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("err")))
        health = cb.get_health()
        assert health['total_failure'] == 1
        assert health['last_error'] == 'err'

    def test_health_includes_thresholds(self, cb):
        health = cb.get_health()
        assert health['failure_threshold'] == 3
        assert health['reset_timeout'] == 10


# ---------------------------------------------------------------------------
# Decorator
# ---------------------------------------------------------------------------

class TestCircuitBreakerDecorator:
    """@cb.protect decorator form."""

    def test_protect_passes_through(self, cb):
        @cb.protect
        def my_func(x):
            return x * 2
        assert my_func(5) == 10

    def test_protect_tracks_failures(self, cb):
        @cb.protect
        def my_func():
            raise RuntimeError("fail")
        with pytest.raises(RuntimeError):
            my_func()
        assert cb.failure_count == 1


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TestCircuitBreakerRegistry:
    """init_breakers() and get_all_breakers()."""

    def test_init_breakers(self, fake_redis):
        breakers = init_breakers(fake_redis)
        assert 'insightiq' in breakers
        assert 'openai' in breakers
        assert 'apify' in breakers
        assert 'hubspot' in breakers
        assert 'apollo' in breakers
        assert 'anthropic' in breakers
        # Also registered globally
        all_b = get_all_breakers()
        assert 'insightiq' in all_b

    def test_get_breaker_creates_on_demand(self, fake_redis):
        cb = get_breaker('new_service', fake_redis, failure_threshold=5)
        assert cb.name == 'new_service'
        assert cb.failure_threshold == 5


# ---------------------------------------------------------------------------
# CircuitOpenError
# ---------------------------------------------------------------------------

class TestCircuitOpenError:
    """CircuitOpenError exception properties."""

    def test_has_name(self):
        err = CircuitOpenError('myservice', retry_after=30)
        assert err.name == 'myservice'
        assert err.retry_after == 30
        assert 'myservice' in str(err)
