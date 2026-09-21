"""
TokenRelay v2 — Circuit Breaker for fault-tolerant message processing.

Provides:
- CircuitBreaker: automatic failure detection with exponential backoff,
  half-open recovery, and comprehensive metrics tracking.
"""

import time
import math
from enum import Enum, auto
from typing import Optional, Dict, Any, Callable, List, Tuple
from collections import deque


# ─────────────────────────────────────────────
# States
# ─────────────────────────────────────────────

class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = auto()     # Normal operation, requests pass through
    OPEN = auto()       # Circuit tripped, requests are rejected
    HALF_OPEN = auto()  # Testing if the service recovered


STATE_NAMES = {
    CircuitState.CLOSED: "CLOSED",
    CircuitState.OPEN: "OPEN",
    CircuitState.HALF_OPEN: "HALF_OPEN",
}


# ─────────────────────────────────────────────
# CircuitBreaker
# ─────────────────────────────────────────────

class CircuitBreaker:
    """Circuit breaker pattern implementation for fault-tolerant operations.

    States:
    - CLOSED: Normal operation. Requests pass through and are monitored.
    - OPEN: Circuit tripped after threshold failures. Requests are rejected
      immediately until the cooldown expires.
    - HALF_OPEN: After cooldown, allows one test request through. If it
      succeeds, the circuit closes. If it fails, it re-opens.

    Features:
    - Configurable failure threshold
    - Exponential backoff for retry intervals
    - Half-open state for automatic recovery detection
    - Comprehensive metrics tracking

    Example:
        cb = CircuitBreaker(failure_threshold=5, recovery_timeout=30)

        def call_service():
            return some_risky_call()

        result = cb.call(call_service)
        if cb.state == CircuitState.CLOSED:
            # Service is healthy
            pass
    """

    def __init__(self, failure_threshold: int = 5,
                 recovery_timeout: float = 60.0,
                 backoff_multiplier: float = 2.0,
                 max_backoff: float = 300.0,
                 min_backoff: float = 0.01,
                 half_open_max_calls: int = 1,
                 expected_exception: type = Exception,
                 name: str = "default"):
        """Initialize the CircuitBreaker.

        Args:
            failure_threshold: Number of consecutive failures before
                tripping to OPEN state.
            recovery_timeout: Seconds to wait in OPEN before transitioning
                to HALF_OPEN.
            backoff_multiplier: Multiplier for exponential backoff on
                each re-trip after HALF_OPEN failure.
            max_backoff: Maximum backoff time in seconds.
            min_backoff: Minimum backoff time in seconds.
            half_open_max_calls: Number of test calls allowed in HALF_OPEN.
            expected_exception: Exception type to catch and count as failure.
            name: Identifier for this circuit breaker instance.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.backoff_multiplier = backoff_multiplier
        self.max_backoff = max_backoff
        self.half_open_max_calls = half_open_max_calls
        self.expected_exception = expected_exception
        self.name = name
        self._min_backoff = min_backoff

        # ── State ──
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._consecutive_failures = 0
        self._last_failure_time: Optional[float] = None
        self._last_state_change: float = time.time()
        self._backoff_time = recovery_timeout

        # ── Half-open tracking ──
        self._half_open_calls_made = 0
        self._half_open_last_result: Optional[bool] = None

        # ── Metrics ──
        self._metrics = {
            "total_calls": 0,
            "total_successes": 0,
            "total_failures": 0,
            "total_rejected": 0,
            "state_transitions": [],
            "total_calls_blocked": 0,
        }

        # ── Rolling window for recent history ──
        self._history_window: deque = deque(maxlen=100)  # Last 100 call results

    # ── State Properties ───────────────────────────────────────

    @property
    def state(self) -> CircuitState:
        """Current circuit state."""
        self._check_transition()
        return self._state

    @property
    def state_name(self) -> str:
        """String name of the current state."""
        return STATE_NAMES[self.state]

    @property
    def is_closed(self) -> bool:
        return self.state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        return self.state == CircuitState.OPEN

    @property
    def is_half_open(self) -> bool:
        return self.state == CircuitState.HALF_OPEN

    @property
    def failure_count(self) -> int:
        """Total failure count (cumulative)."""
        return self._failure_count

    @property
    def consecutive_failures(self) -> int:
        """Current consecutive failure count."""
        return self._consecutive_failures

    @property
    def recovery_timeout(self) -> float:
        """Current recovery timeout (may be increased by backoff)."""
        return self._backoff_time

    @recovery_timeout.setter
    def recovery_timeout(self, value: float):
        """Set the base recovery timeout and backoff."""
        self._recovery_timeout = value
        self._backoff_time = value

    # ── Core Call Method ─────────────────────────────────────

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function through the circuit breaker.

        Args:
            func: The callable to execute.
            *args: Positional arguments passed to func.
            **kwargs: Keyword arguments passed to func.

        Returns:
            The return value of func().

        Raises:
            Exception: The original exception from func(), or
                CircuitBreakerError if the circuit is OPEN.
        """
        self._metrics["total_calls"] += 1
        self._check_transition()

        # If OPEN, reject with CircuitBreakerError
        if self.state == CircuitState.OPEN:
            self._metrics["total_rejected"] += 1
            self._metrics["total_calls_blocked"] += 1
            last_failure = (
                f"{self._last_failure_time:.1f}"
                if self._last_failure_time is not None
                else "N/A"
            )
            raise CircuitBreakerError(
                f"Circuit breaker '{self.name}' is OPEN. "
                f"Retry after {self._backoff_time:.1f}s "
                f"(last failure: {last_failure})"
            )

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise

    def call_with_fallback(self, func: Callable, fallback: Callable,
                           *args, **kwargs) -> Any:
        """Execute a function with a fallback if the circuit is OPEN.

        Args:
            func: The primary callable.
            fallback: Called when circuit is OPEN or func raises.
            *args, **kwargs: Arguments passed to both callables.

        Returns:
            Result of func() on success, or fallback() on failure/open.
        """
        try:
            return self.call(func, *args, **kwargs)
        except CircuitBreakerError:
            return fallback(*args, **kwargs)
        except self.expected_exception:
            return fallback(*args, **kwargs)

    # ── State Transitions ──────────────────────────────────────

    def _check_transition(self):
        """Check if the circuit should transition between states.

        This is called on every state access and on every call().
        """
        if self._state == CircuitState.OPEN:
            # Check if recovery timeout has elapsed
            if self._last_failure_time is not None:
                elapsed = time.time() - self._last_failure_time
                if elapsed >= self._backoff_time:
                    self._transition_to(CircuitState.HALF_OPEN)
        elif self._state == CircuitState.CLOSED:
            # Check if enough consecutive failures to trip
            if (self._consecutive_failures >= self.failure_threshold
                    and self._consecutive_failures > 0):
                self._transition_to(CircuitState.OPEN)
        # HALF_OPEN stays until a test call succeeds or fails

    def _transition_to(self, new_state: CircuitState):
        """Transition to a new state and record the event."""
        old_state = self._state
        self._state = new_state
        self._last_state_change = time.time()

        self._metrics["state_transitions"].append({
            "from": STATE_NAMES[old_state],
            "to": STATE_NAMES[new_state],
            "timestamp": time.time(),
            "consecutive_failures": self._consecutive_failures,
        })

    def _on_success(self):
        """Record a successful call."""
        self._success_count += 1
        self._metrics["total_successes"] += 1
        self._consecutive_failures = 0
        self._half_open_calls_made = 0
        self._half_open_last_result = True
        self._history_window.append(("success", time.time()))

        # If in HALF_OPEN and success, close the circuit
        if self.state == CircuitState.HALF_OPEN:
            self._transition_to(CircuitState.CLOSED)
            self._backoff_time = self._recovery_timeout  # Reset backoff

    def _on_failure(self):
        """Record a failed call."""
        self._failure_count += 1
        self._consecutive_failures += 1
        self._last_failure_time = time.time()
        self._half_open_calls_made += 1
        self._half_open_last_result = False
        self._history_window.append(("failure", time.time()))

        self._metrics["total_failures"] += 1

        # In HALF_OPEN, any failure re-opens the circuit with backoff
        if self._state == CircuitState.HALF_OPEN:
            self._backoff_time = min(
                max(self._backoff_time * self.backoff_multiplier, self._min_backoff),
                self.max_backoff
            )
            self._transition_to(CircuitState.OPEN)
        # In CLOSED, check if threshold is reached
        elif self._state == CircuitState.CLOSED:
            if self._consecutive_failures >= self.failure_threshold:
                self._transition_to(CircuitState.OPEN)

    # ── Manual Control ─────────────────────────────────────────

    def reset(self):
        """Manually reset the circuit breaker to CLOSED state."""
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = None
        self._last_state_change = time.time()
        self._backoff_time = self._recovery_timeout
        self._half_open_calls_made = 0
        self._half_open_last_result = None
        self._history_window.clear()

    def force_open(self):
        """Force the circuit to OPEN state immediately."""
        self._transition_to(CircuitState.OPEN)
        self._last_failure_time = time.time()

    def force_half_open(self):
        """Force the circuit to HALF_OPEN state for testing."""
        self._transition_to(CircuitState.HALF_OPEN)
        self._half_open_calls_made = 0

    # ── Time & Backoff ─────────────────────────────────────────

    def time_until_retry(self) -> float:
        """Get seconds until the circuit transitions from OPEN to HALF_OPEN.

        Returns:
            Seconds remaining, or 0 if not in OPEN state.
        """
        if self.state != CircuitState.OPEN:
            return 0.0
        if self._last_failure_time is None:
            return 0.0
        elapsed = time.time() - self._last_failure_time
        return max(0, self._backoff_time - elapsed)

    def wait_for_recovery(self):
        """Block until the circuit transitions to HALF_OPEN.

        Useful for testing or waiting for automatic recovery.
        """
        remaining = self.time_until_retry()
        if remaining > 0:
            time.sleep(remaining)

    # ── Metrics & Health ───────────────────────────────────────

    @property
    def total_successes(self) -> int:
        return self._metrics["total_successes"]

    @property
    def total_failures(self) -> int:
        return self._metrics["total_failures"]

    @property
    def total_rejected(self) -> int:
        return self._metrics["total_rejected"]

    @property
    def total_calls_blocked(self) -> int:
        return self._metrics["total_calls_blocked"]

    @property
    def success_rate(self) -> float:
        """Calculate success rate across all calls."""
        total = self._metrics["total_calls"]
        if total == 0:
            return 0.0
        return self._metrics["total_successes"] / total

    @property
    def failure_rate(self) -> float:
        """Calculate failure rate across all calls."""
        total = self._metrics["total_calls"]
        if total == 0:
            return 0.0
        return self._metrics["total_failures"] / total

    @property
    def failure_rate_window(self) -> float:
        """Failure rate within the recent rolling window."""
        if not self._history_window:
            return 0.0
        failures = sum(1 for status, _ in self._history_window
                       if status == "failure")
        return failures / len(self._history_window)

    def get_metrics(self) -> Dict[str, Any]:
        """Return comprehensive circuit breaker metrics."""
        return {
            "name": self.name,
            "state": self.state_name,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": round(self._backoff_time, 2),
            "backoff_multiplier": self.backoff_multiplier,
            "max_backoff": self.max_backoff,
            "current_backoff": round(self.time_until_retry(), 2),
            "failure_count": self._failure_count,
            "consecutive_failures": self._consecutive_failures,
            "success_count": self._success_count,
            "total_calls": self._metrics["total_calls"],
            "total_successes": self._metrics["total_successes"],
            "total_failures": self._metrics["total_failures"],
            "total_rejected": self._metrics["total_rejected"],
            "total_calls_blocked": self._metrics["total_calls_blocked"],
            "success_rate": round(self.success_rate * 100, 2),
            "failure_rate": round(self.failure_rate * 100, 2),
            "failure_rate_window": round(self.failure_rate_window * 100, 2),
            "last_failure_time": self._last_failure_time,
            "last_state_change": self._last_state_change,
            "time_until_retry": round(self.time_until_retry(), 2),
            "history_window_size": len(self._history_window),
            "half_open_calls_made": self._half_open_calls_made,
            "state_transition_count": len(self._metrics["state_transitions"]),
            "recent_transitions": self._metrics["state_transitions"][-5:],
        }

    def get_health(self) -> Dict[str, Any]:
        """Get circuit breaker health status.

        Returns:
            Dict with state, health indicator, and key metrics.
        """
        state = self.state
        if state == CircuitState.CLOSED:
            health = "healthy"
        elif state == CircuitState.HALF_OPEN:
            health = "recovering"
        else:
            health = "degraded"

        return {
            "health": health,
            "state": self.state_name,
            "consecutive_failures": self._consecutive_failures,
            "success_rate": round(self.success_rate * 100, 2),
            "failure_rate_window": round(self.failure_rate_window * 100, 2),
            "time_until_retry": round(self.time_until_retry(), 2),
        }

    def get_history(self, last_n: int = 20) -> List[Tuple[str, float]]:
        """Get the recent call history.

        Args:
            last_n: Number of recent entries to return.

        Returns:
            List of (status, timestamp) tuples, most recent last.
        """
        return list(self._history_window)[-last_n:]

    # ── Statistics ─────────────────────────────────────────────

    def get_failure_times(self) -> List[float]:
        """Get timestamps of all failures in the rolling window."""
        return [ts for status, ts in self._history_window
                if status == "failure"]

    def get_uptime_ratio(self) -> float:
        """Get ratio of time spent in CLOSED state.

        Calculated from state transition history.
        """
        transitions = self._metrics["state_transitions"]
        if not transitions:
            return 1.0  # Always closed, or never started
        closed_time = 0.0
        total_time = 0.0
        for i, trans in enumerate(transitions):
            ts = trans["timestamp"]
            if trans["to"] == "CLOSED":
                if i + 1 < len(transitions):
                    closed_time += transitions[i + 1]["timestamp"] - ts
                else:
                    closed_time += time.time() - ts
            total_time += ts
        if total_time <= 0:
            return 0.0
        return closed_time / total_time if total_time > 0 else 0.0


# ─────────────────────────────────────────────
# CircuitBreakerError
# ─────────────────────────────────────────────

class CircuitBreakerError(Exception):
    """Raised when a call is blocked because the circuit is OPEN."""
    pass


# ─────────────────────────────────────────────
# Decorator & Convenience
# ─────────────────────────────────────────────

def circuit_breaker(failure_threshold: int = 5,
                    recovery_timeout: float = 60.0,
                    name: str = "default"):
    """Decorator to wrap a function with a circuit breaker.

    Example:
        @circuit_breaker(failure_threshold=3, recovery_timeout=10)
        def risky_operation():
            return call_external_service()
    """
    def decorator(func: Callable) -> Callable:
        cb = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            name=f"{name}_{func.__name__}",
        )

        def wrapper(*args, **kwargs):
            return cb.call(func, *args, **kwargs)

        wrapper.circuit_breaker = cb  # Expose the breaker for inspection
        return wrapper

    return decorator


def create_breaker(name: str = "default", **kwargs) -> CircuitBreaker:
    """Factory function to create a CircuitBreaker."""
    return CircuitBreaker(name=name, **kwargs)
