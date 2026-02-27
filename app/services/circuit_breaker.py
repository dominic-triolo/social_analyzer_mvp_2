"""
Circuit breaker pattern with Redis-backed state and health tracking.

Each breaker tracks failures per service in Redis. States:
  - CLOSED  → normal operation, requests pass through
  - OPEN    → too many failures, requests short-circuit with CircuitOpenError
  - HALF_OPEN → after reset_timeout, allows one probe request

Health metrics are stored in Redis hashes for the /api/health endpoint.
"""
import logging
import time
from functools import wraps

logger = logging.getLogger('services.circuit_breaker')

# State constants
CLOSED = 'closed'
OPEN = 'open'
HALF_OPEN = 'half_open'


class CircuitOpenError(Exception):
    """Raised when calling through an open circuit breaker."""
    def __init__(self, name, retry_after=None):
        self.name = name
        self.retry_after = retry_after
        super().__init__(f"Circuit breaker '{name}' is OPEN — service unavailable")


class CircuitBreaker:
    """
    Redis-backed circuit breaker.

    Usage:
        cb = CircuitBreaker('insightiq', redis_client, failure_threshold=3, reset_timeout=300)
        result = cb.call(some_api_function, arg1, arg2)

    Or as a decorator:
        @cb.protect
        def my_api_call(...): ...
    """

    # Redis key prefix
    PREFIX = 'cb'

    def __init__(self, name, redis_client, failure_threshold=3, reset_timeout=300):
        self.name = name
        self.redis = redis_client
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout  # seconds before OPEN → HALF_OPEN

    # ── Redis keys ────────────────────────────────────────────────────

    @property
    def _state_key(self):
        return f'{self.PREFIX}:{self.name}:state'

    @property
    def _failures_key(self):
        return f'{self.PREFIX}:{self.name}:failures'

    @property
    def _last_failure_key(self):
        return f'{self.PREFIX}:{self.name}:last_failure'

    @property
    def _health_key(self):
        return f'{self.PREFIX}:{self.name}:health'

    # ── State management ──────────────────────────────────────────────

    @property
    def state(self):
        try:
            s = self.redis.get(self._state_key)
            if s is None:
                return CLOSED
            # Check if OPEN has expired → transition to HALF_OPEN
            if s == OPEN:
                last = self.redis.get(self._last_failure_key)
                if last and (time.time() - float(last)) > self.reset_timeout:
                    self._set_state(HALF_OPEN)
                    return HALF_OPEN
            return s
        except Exception:
            return CLOSED  # fail-open: if Redis is down, allow requests

    def _set_state(self, new_state):
        try:
            self.redis.set(self._state_key, new_state)
        except Exception:
            pass

    @property
    def failure_count(self):
        try:
            val = self.redis.get(self._failures_key)
            return int(val) if val else 0
        except Exception:
            return 0

    # ── Health metrics ────────────────────────────────────────────────

    def _record_success(self):
        try:
            pipe = self.redis.pipeline()
            pipe.hincrby(self._health_key, 'success', 1)
            pipe.hset(self._health_key, 'last_success', str(time.time()))
            pipe.execute()
        except Exception:
            pass

    def _record_failure(self, error_msg=''):
        try:
            pipe = self.redis.pipeline()
            pipe.hincrby(self._health_key, 'failure', 1)
            pipe.hset(self._health_key, 'last_failure', str(time.time()))
            if error_msg:
                pipe.hset(self._health_key, 'last_error', str(error_msg)[:200])
            pipe.execute()
        except Exception:
            pass

    def get_health(self):
        """Return health metrics dict for this service."""
        try:
            data = self.redis.hgetall(self._health_key)
            return {
                'name': self.name,
                'state': self.state,
                'failure_count': self.failure_count,
                'failure_threshold': self.failure_threshold,
                'reset_timeout': self.reset_timeout,
                'total_success': int(data.get('success', 0)),
                'total_failure': int(data.get('failure', 0)),
                'last_success': float(data['last_success']) if data.get('last_success') else None,
                'last_failure': float(data['last_failure']) if data.get('last_failure') else None,
                'last_error': data.get('last_error', ''),
            }
        except Exception:
            return {
                'name': self.name,
                'state': 'unknown',
                'failure_count': 0,
                'failure_threshold': self.failure_threshold,
                'reset_timeout': self.reset_timeout,
                'total_success': 0,
                'total_failure': 0,
                'last_success': None,
                'last_failure': None,
                'last_error': '',
            }

    # ── Core call logic ───────────────────────────────────────────────

    def call(self, func, *args, **kwargs):
        """Execute func through the circuit breaker."""
        current = self.state

        if current == OPEN:
            last = self.redis.get(self._last_failure_key)
            retry_after = None
            if last:
                retry_after = max(0, self.reset_timeout - (time.time() - float(last)))
            raise CircuitOpenError(self.name, retry_after=retry_after)

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure(e)
            raise

    def _on_success(self):
        """Reset failure count, close circuit."""
        try:
            pipe = self.redis.pipeline()
            pipe.set(self._state_key, CLOSED)
            pipe.set(self._failures_key, 0)
            pipe.execute()
        except Exception:
            pass
        self._record_success()

    def _on_failure(self, error):
        """Increment failures, open circuit if threshold reached."""
        try:
            new_count = self.redis.incr(self._failures_key)
            self.redis.set(self._last_failure_key, str(time.time()))
            if new_count >= self.failure_threshold:
                self._set_state(OPEN)
                logger.warning(
                    "Circuit '%s' OPENED after %d failures (threshold=%d): %s",
                    self.name, new_count, self.failure_threshold, error,
                )
            else:
                logger.info(
                    "Circuit '%s' failure %d/%d: %s",
                    self.name, new_count, self.failure_threshold, error,
                )
        except Exception:
            pass
        self._record_failure(str(error))

    def reset(self):
        """Manually reset the circuit breaker to closed state."""
        try:
            pipe = self.redis.pipeline()
            pipe.set(self._state_key, CLOSED)
            pipe.set(self._failures_key, 0)
            pipe.delete(self._last_failure_key)
            pipe.execute()
            logger.info("Circuit '%s' manually reset to CLOSED", self.name)
        except Exception as e:
            logger.error("Failed to reset circuit '%s': %s", self.name, e)

    def protect(self, func):
        """Decorator form of the circuit breaker."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            return self.call(func, *args, **kwargs)
        return wrapper


# ── Global registry ───────────────────────────────────────────────────────

_registry = {}


def get_breaker(name, redis_client=None, **kwargs):
    """Get or create a named circuit breaker (singleton per name)."""
    if name not in _registry:
        if redis_client is None:
            from app.extensions import redis_client as rc
            redis_client = rc
        _registry[name] = CircuitBreaker(name, redis_client, **kwargs)
    return _registry[name]


def get_all_breakers():
    """Return all registered circuit breakers."""
    return dict(_registry)


def init_breakers(redis_client):
    """Initialize standard circuit breakers for all external services."""
    breakers = {
        'insightiq': CircuitBreaker('insightiq', redis_client, failure_threshold=3, reset_timeout=300),
        'openai': CircuitBreaker('openai', redis_client, failure_threshold=5, reset_timeout=60),
        'apify': CircuitBreaker('apify', redis_client, failure_threshold=3, reset_timeout=300),
        'hubspot': CircuitBreaker('hubspot', redis_client, failure_threshold=3, reset_timeout=180),
        'apollo': CircuitBreaker('apollo', redis_client, failure_threshold=5, reset_timeout=120),
        'anthropic': CircuitBreaker('anthropic', redis_client, failure_threshold=5, reset_timeout=60),
    }
    _registry.update(breakers)
    return breakers
