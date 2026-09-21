"""
Tests for TokenRelay v2 CircuitBreaker.

Tests cover:
- State transitions (CLOSED, OPEN, HALF_OPEN)
- Exponential backoff
- Failure threshold
- Half-open recovery detection
- Metrics tracking
- Decorator and convenience functions
"""

import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from circuit_breaker import (
    CircuitBreaker, CircuitState, CircuitBreakerError,
    create_breaker, circuit_breaker,
)


class TestCircuitBreakerBasics(unittest.TestCase):
    """Test basic circuit breaker functionality."""

    def test_initial_state(self):
        """Circuit breaker starts in CLOSED state."""
        cb = CircuitBreaker()
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertTrue(cb.is_closed)
        self.assertFalse(cb.is_open)
        self.assertFalse(cb.is_half_open)

    def test_state_name(self):
        """state_name returns correct string."""
        cb = CircuitBreaker()
        self.assertEqual(cb.state_name, "CLOSED")
        cb._state = CircuitState.OPEN
        self.assertEqual(cb.state_name, "OPEN")
        cb._state = CircuitState.HALF_OPEN
        self.assertEqual(cb.state_name, "HALF_OPEN")

    def test_success(self):
        """Successful calls work normally in CLOSED state."""
        cb = CircuitBreaker(failure_threshold=3)
        result = cb.call(lambda: 42)
        self.assertEqual(result, 42)

    def test_failure_counting(self):
        """Failures increment counters."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)
        def fail():
            raise ValueError("fail")
        with self.assertRaises(ValueError):
            cb.call(fail)
        self.assertEqual(cb.consecutive_failures, 1)
        self.assertEqual(cb.failure_count, 1)


class TestFailureThreshold(unittest.TestCase):
    """Test failure threshold and tripping to OPEN."""

    def test_threshold_trips_to_open(self):
        """After enough failures, circuit trips to OPEN."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)
        def fail():
            raise ValueError("fail")
        for _ in range(3):
            with self.assertRaises(ValueError):
                cb.call(fail)
        # Circuit should now be OPEN
        self.assertTrue(cb.is_open)

    def test_open_blocks_calls(self):
        """OPEN circuit blocks calls with CircuitBreakerError."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1)
        def fail():
            raise ValueError("fail")
        with self.assertRaises(ValueError):
            cb.call(fail)
        with self.assertRaises(ValueError):
            cb.call(fail)
        # Now OPEN
        with self.assertRaises(CircuitBreakerError):
            cb.call(lambda: 42)

    def test_custom_exception_type(self):
        """Only specified exception types count as failures."""
        cb = CircuitBreaker(failure_threshold=3,
                            recovery_timeout=1,
                            expected_exception=ValueError)
        def raise_value_error():
            raise ValueError("fail")
        def raise_type_error():
            raise TypeError("not counted")

        # ValueError failures count
        for _ in range(3):
            with self.assertRaises(ValueError):
                cb.call(raise_value_error)
        self.assertTrue(cb.is_open)

        # After circuit is OPEN, call_with_fallback handles it gracefully
        result = cb.call_with_fallback(
            raise_type_error, lambda: "fallback"
        )
        self.assertEqual(result, "fallback")

    def test_threshold_reset_on_success(self):
        """Success resets consecutive failure count."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)
        def fail():
            raise ValueError("fail")
        with self.assertRaises(ValueError):
            cb.call(fail)
        with self.assertRaises(ValueError):
            cb.call(fail)
        # Success resets counter
        result = cb.call(lambda: 42)
        self.assertEqual(result, 42)
        self.assertEqual(cb.consecutive_failures, 0)


class TestExponentialBackoff(unittest.TestCase):
    """Test exponential backoff behavior."""

    def test_backoff_increases(self):
        """Backoff time increases after repeated failures."""
        cb = CircuitBreaker(failure_threshold=2,
                            recovery_timeout=1,
                            backoff_multiplier=2,
                            max_backoff=10)
        def fail():
            raise ValueError("fail")

        # First trip
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)

        timeout1 = cb.recovery_timeout
        self.assertEqual(timeout1, 1.0)

        # Wait for HALF_OPEN then fail again
        time.sleep(1.1)
        with self.assertRaises(ValueError):
            cb.call(fail)

        # After second trip, backoff should have increased
        timeout2 = cb.recovery_timeout
        self.assertEqual(timeout2, 2.0)  # 1.0 * 2

    def test_max_backoff_respected(self):
        """Backoff does not exceed max_backoff."""
        cb = CircuitBreaker(failure_threshold=2,
                            recovery_timeout=1,
                            backoff_multiplier=5,
                            max_backoff=10)
        def fail():
            raise ValueError("fail")

        # Trip 1
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)
        time.sleep(1.1)
        # Trip 2
        with self.assertRaises(ValueError):
            cb.call(fail)
        # After trip 2, backoff is 5s; circuit is OPEN
        # 3rd call is rejected
        with self.assertRaises(CircuitBreakerError):
            cb.call(fail)

        self.assertLessEqual(cb.recovery_timeout, 10.0)

    def test_backoff_reset_on_close(self):
        """Backoff resets when circuit closes after recovery."""
        cb = CircuitBreaker(failure_threshold=2,
                            recovery_timeout=1,
                            backoff_multiplier=3)
        def fail():
            raise ValueError("fail")

        # Trip
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)

        time.sleep(1.1)
        # HALF_OPEN - succeed
        result = cb.call(lambda: 42)
        self.assertEqual(result, 42)
        self.assertEqual(cb.recovery_timeout, 1.0)  # Reset


class TestHalfOpenRecovery(unittest.TestCase):
    """Test half-open state and automatic recovery."""

    def test_transition_to_half_open(self):
        """After recovery_timeout, circuit transitions to HALF_OPEN."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.5)
        def fail():
            raise ValueError("fail")

        # Trip to OPEN
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)
        self.assertTrue(cb.is_open)

        # Wait for recovery
        time.sleep(0.6)
        # State should be HALF_OPEN
        self.assertTrue(cb.is_half_open)

    def test_success_in_half_open_closes(self):
        """Success in HALF_OPEN closes the circuit."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.5)
        def fail():
            raise ValueError("fail")

        # Trip
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)

        time.sleep(0.6)
        # HALF_OPEN - test succeeds
        result = cb.call(lambda: 42)
        self.assertEqual(result, 42)
        self.assertTrue(cb.is_closed)

    def test_failure_in_half_open_reopens(self):
        """Failure in HALF_OPEN reopens the circuit."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.5)
        def fail():
            raise ValueError("fail")

        # Trip
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)

        time.sleep(0.6)
        # HALF_OPEN - test fails
        with self.assertRaises(ValueError):
            cb.call(fail)
        self.assertTrue(cb.is_open)

    def test_half_open_max_calls(self):
        """half_open_max_calls limits test calls in HALF_OPEN."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.5,
                            half_open_max_calls=1)
        def fail():
            raise ValueError("fail")

        # Trip
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)

        time.sleep(0.6)
        # First test call fails → OPEN again
        with self.assertRaises(ValueError):
            cb.call(fail)
        self.assertTrue(cb.is_open)


class TestTimeUntilRetry(unittest.TestCase):
    """Test time_until_retry calculation."""

    def test_zero_when_not_open(self):
        """time_until_retry is 0 when not in OPEN state."""
        cb = CircuitBreaker()
        self.assertEqual(cb.time_until_retry(), 0.0)

    def test_decreases_over_time(self):
        """time_until_retry decreases as time passes."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=3)
        def fail():
            raise ValueError("fail")

        # Trip
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)

        remaining = cb.time_until_retry()
        self.assertGreater(remaining, 0)
        self.assertLessEqual(remaining, 3.0)

        time.sleep(0.5)
        remaining2 = cb.time_until_retry()
        self.assertLess(remaining2, remaining)


class TestManualControl(unittest.TestCase):
    """Test manual state control."""

    def test_reset(self):
        """reset returns circuit to CLOSED."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1)
        def fail():
            raise ValueError("fail")
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)
        self.assertTrue(cb.is_open)

        cb.reset()
        self.assertTrue(cb.is_closed)
        self.assertEqual(cb.consecutive_failures, 0)

    def test_force_open(self):
        """force_open immediately opens the circuit."""
        cb = CircuitBreaker()
        cb.force_open()
        self.assertTrue(cb.is_open)
        self.assertIsNotNone(cb._last_failure_time)

    def test_force_half_open(self):
        """force_half_open transitions to HALF_OPEN."""
        cb = CircuitBreaker()
        cb.force_half_open()
        self.assertTrue(cb.is_half_open)


class TestCallWithFallback(unittest.TestCase):
    """Test call_with_fallback method."""

    def test_fallback_when_open(self):
        """Fallback called when circuit is OPEN."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.5)
        def fail():
            raise ValueError("fail")
        # Trip the circuit
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)

        result = cb.call_with_fallback(
            lambda: 42, lambda: "fallback"
        )
        self.assertEqual(result, "fallback")

    def test_fallback_on_exception(self):
        """Fallback called when function raises expected exception."""
        cb = CircuitBreaker()
        result = cb.call_with_fallback(
            lambda: (_ for _ in ()).throw(ValueError("boom")),
            lambda: "fallback"
        )
        self.assertEqual(result, "fallback")

    def test_primary_on_success(self):
        """Primary function called when successful."""
        cb = CircuitBreaker()
        result = cb.call_with_fallback(
            lambda: 42, lambda: "fallback"
        )
        self.assertEqual(result, 42)


class TestMetrics(unittest.TestCase):
    """Test circuit breaker metrics."""

    def test_metrics_after_calls(self):
        """Metrics are tracked correctly."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1)

        # Success
        cb.call(lambda: 42)
        self.assertEqual(cb.total_successes, 1)

        # Failure
        def fail():
            raise ValueError("fail")
        with self.assertRaises(ValueError):
            cb.call(fail)
        self.assertEqual(cb.total_failures, 1)

        metrics = cb.get_metrics()
        self.assertEqual(metrics["total_calls"], 2)
        self.assertEqual(metrics["total_successes"], 1)
        self.assertEqual(metrics["total_failures"], 1)

    def test_success_rate(self):
        """Success rate calculation is correct."""
        cb = CircuitBreaker(failure_threshold=10, recovery_timeout=1)
        cb.call(lambda: 42)
        cb.call(lambda: 42)
        def fail():
            raise ValueError("fail")
        with self.assertRaises(ValueError):
            cb.call(fail)
        self.assertAlmostEqual(cb.success_rate, 2/3, places=2)

    def test_rejected_count(self):
        """Rejected calls are counted."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.5)
        def fail():
            raise ValueError("fail")
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)

        # Circuit now OPEN - calls rejected
        with self.assertRaises(CircuitBreakerError):
            cb.call(lambda: 42)
        with self.assertRaises(CircuitBreakerError):
            cb.call(lambda: 42)
        self.assertGreater(cb.total_rejected, 0)
        self.assertGreater(cb.total_calls_blocked, 0)

    def test_state_transitions_recorded(self):
        """State transitions are recorded in metrics."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.5)
        def fail():
            raise ValueError("fail")
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)

        transitions = cb.get_metrics()["recent_transitions"]
        self.assertTrue(len(transitions) > 0)
        self.assertIn("CLOSED", transitions[0]["from"])
        self.assertIn("OPEN", transitions[-1]["to"])

    def test_get_health(self):
        """get_health returns correct status."""
        cb = CircuitBreaker(failure_threshold=10, recovery_timeout=1)
        health = cb.get_health()
        self.assertEqual(health["health"], "healthy")
        self.assertEqual(health["state"], "CLOSED")

        def fail():
            raise ValueError("fail")
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)

        health = cb.get_health()
        self.assertIn(health["health"], ["degraded", "healthy"])
        self.assertIn(health["state"], ["OPEN", "CLOSED"])

    def test_get_history(self):
        """get_history returns recent call results."""
        cb = CircuitBreaker(failure_threshold=10, recovery_timeout=1)
        cb.call(lambda: 42)
        def fail():
            raise ValueError("fail")
        with self.assertRaises(ValueError):
            cb.call(fail)
        history = cb.get_history(last_n=5)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0][0], "success")
        self.assertEqual(history[1][0], "failure")

    def test_get_failure_times(self):
        """get_failure_times returns failure timestamps."""
        cb = CircuitBreaker(failure_threshold=10, recovery_timeout=1)
        def fail():
            raise ValueError("fail")
        with self.assertRaises(ValueError):
            cb.call(fail)
        with self.assertRaises(ValueError):
            cb.call(fail)
        fail_times = cb.get_failure_times()
        self.assertEqual(len(fail_times), 2)


class TestDecorator(unittest.TestCase):
    """Test the circuit_breaker decorator."""

    def test_decorator_wraps_function(self):
        """Decorator wraps function with circuit breaker."""
        @circuit_breaker(failure_threshold=2, recovery_timeout=1, name="test")
        def my_function():
            return 42

        result = my_function()
        self.assertEqual(result, 42)
        # Exposes the circuit breaker
        self.assertTrue(hasattr(my_function, "circuit_breaker"))
        self.assertIsInstance(my_function.circuit_breaker, CircuitBreaker)

    def test_decorator_trips_on_failures(self):
        """Decorator circuit breaker trips on failures."""
        @circuit_breaker(failure_threshold=2, recovery_timeout=0.5)
        def flaky():
            raise ValueError("boom")

        with self.assertRaises(ValueError):
            flaky()
        with self.assertRaises(ValueError):
            flaky()
        # Circuit should be OPEN
        with self.assertRaises(CircuitBreakerError):
            flaky()


class TestCreateBreaker(unittest.TestCase):
    """Test create_breaker factory function."""

    def test_factory(self):
        """create_breaker produces a working CircuitBreaker."""
        cb = create_breaker(name="test", failure_threshold=3)
        self.assertEqual(cb.name, "test")
        self.assertEqual(cb.failure_threshold, 3)
        cb.call(lambda: 42)
        self.assertEqual(cb.total_successes, 1)

    def test_factory_default(self):
        """create_breaker with defaults works."""
        cb = create_breaker()
        self.assertEqual(cb.failure_threshold, 5)
        self.assertEqual(cb.recovery_timeout, 60.0)


class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions."""

    def test_zero_threshold(self):
        """Zero failure threshold trips immediately on first failure."""
        cb = CircuitBreaker(failure_threshold=0, recovery_timeout=1)
        def fail():
            raise ValueError("fail")
        with self.assertRaises(ValueError):
            cb.call(fail)
        # Already OPEN
        self.assertTrue(cb.is_open)

    def test_very_high_threshold(self):
        """Very high threshold means circuit never trips."""
        cb = CircuitBreaker(failure_threshold=1000, recovery_timeout=1)
        def fail():
            raise ValueError("fail")
        for _ in range(5):
            with self.assertRaises(ValueError):
                cb.call(fail)
        # Should still be CLOSED
        self.assertTrue(cb.is_closed)

    def test_no_failures(self):
        """Circuit with no failures has 0 failure rate."""
        cb = CircuitBreaker()
        for _ in range(10):
            cb.call(lambda: 42)
        self.assertEqual(cb.failure_rate, 0.0)
        self.assertEqual(cb.success_rate, 1.0)

    def test_all_rejected(self):
        """All calls rejected when circuit stays OPEN."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=300)
        def fail():
            raise ValueError("fail")
        with self.assertRaises(ValueError):
            cb.call(fail)
        # Now OPEN
        self.assertEqual(cb.total_rejected, 0)  # First failure wasn't rejected
        with self.assertRaises(CircuitBreakerError):
            cb.call(lambda: 42)
        self.assertGreater(cb.total_rejected, 0)

    def test_call_with_args_kwargs(self):
        """call() passes args and kwargs correctly."""
        cb = CircuitBreaker(failure_threshold=10, recovery_timeout=1)
        result = cb.call(lambda x, y, z=3: x + y + z, 1, 2, z=4)
        self.assertEqual(result, 7)

    def test_up_to_date_metrics(self):
        """get_metrics always returns current state."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.5)
        def fail():
            raise ValueError("fail")
        with self.assertRaises(ValueError):
            cb.call(fail)
        with self.assertRaises(ValueError):
            cb.call(fail)
        # After a brief wait, check metrics are consistent
        time.sleep(0.7)
        metrics = cb.get_metrics()
        self.assertEqual(metrics["state"], "HALF_OPEN")
        # Metrics should include the failures
        self.assertEqual(metrics["total_failures"], 2)

    def test_recovery_after_multiple_trips(self):
        """Circuit recovers correctly after multiple OPEN→HALF_OPEN→OPEN cycles."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.5,
                            backoff_multiplier=2)
        def fail():
            raise ValueError("fail")

        # First trip
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(fail)
        time.sleep(0.6)
        # HALF_OPEN fails
        with self.assertRaises(ValueError):
            cb.call(fail)
        # Second trip
        time.sleep(cb.recovery_timeout)
        with self.assertRaises(ValueError):
            cb.call(fail)
        time.sleep(cb.recovery_timeout)
        # HALF_OPEN succeeds
        result = cb.call(lambda: 42)
        self.assertEqual(result, 42)
        self.assertTrue(cb.is_closed)


if __name__ == "__main__":
    unittest.main(verbosity=2)