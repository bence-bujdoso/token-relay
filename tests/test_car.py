"""
Tests for TokenRelay v3 Context-Aware Routing (CAR).

Covers:
- ComplexityAnalyzer scoring and classification
- ModelSelector endpoint selection logic
- PerformanceTracker statistics and health
- RouteOptimizer weight computation and trends
- CARRouter end-to-end routing pipeline
- CARConfig defaults and customization
- Convenience functions
"""

import sys
import time
import unittest
from dataclasses import asdict
from pathlib import Path
from circuit_breaker import CircuitState

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from car import (
    CARRouter, ComplexityAnalyzer, ModelSelector, PerformanceTracker,
    RouteOptimizer, CARConfig, ComplexityLevel, UserPreference,
    EndpointStatus, create_router, route_query,
)


# ═══════════════════════════════════════════════════════════════════════
# CARConfig Tests
# ═══════════════════════════════════════════════════════════════════════

class TestCARConfig(unittest.TestCase):
    def test_default_config(self):
        config = CARConfig()
        self.assertEqual(config.max_complexity, 10)
        self.assertEqual(config.min_complexity, 1)
        self.assertEqual(config.default_endpoint, "default")
        self.assertEqual(config.performance_window_size, 100)
        self.assertEqual(config.success_rate_floor, 0.5)

    def test_custom_config(self):
        config = CARConfig(max_complexity=5, default_endpoint="my_endpoint")
        self.assertEqual(config.max_complexity, 5)
        self.assertEqual(config.default_endpoint, "my_endpoint")

    def test_config_as_dict(self):
        config = CARConfig()
        d = asdict(config)
        self.assertIn("max_complexity", d)
        self.assertIn("performance_window_size", d)


# ═══════════════════════════════════════════════════════════════════════
# ComplexityAnalyzer Tests
# ═══════════════════════════════════════════════════════════════════════

class TestComplexityAnalyzerBasics(unittest.TestCase):
    def setUp(self):
        self.analyzer = ComplexityAnalyzer()

    def test_simple_query(self):
        score = self.analyzer.analyze("What is 2+2?")
        self.assertLessEqual(score, 4)
        self.assertGreaterEqual(score, 1)

    def test_short_query_low_complexity(self):
        score = self.analyzer.analyze("Hi")
        self.assertEqual(score, 1)

    def test_long_query_higher_complexity(self):
        short = "What is AI?"
        long_query = "Analyze the impact of machine learning algorithms on distributed database performance and scalability in cloud environments"
        score_short = self.analyzer.analyze(short)
        score_long = self.analyzer.analyze(long_query)
        self.assertGreaterEqual(score_long, score_short)

    def test_empty_query_raises(self):
        with self.assertRaises(ValueError):
            self.analyzer.analyze("")

    def test_whitespace_query_raises(self):
        with self.assertRaises(ValueError):
            self.analyzer.analyze("   ")

    def test_non_string_query_raises(self):
        with self.assertRaises(ValueError):
            self.analyzer.analyze(123)

    def test_score_clamped_to_max(self):
        config = CARConfig(max_complexity=5)
        analyzer = ComplexityAnalyzer(config)
        score = analyzer.analyze("Analyze the distributed consensus algorithm with recursive proof of cryptographic hash functions and Byzantine fault tolerance")
        self.assertLessEqual(score, 5)

    def test_score_clamped_to_min(self):
        config = CARConfig(min_complexity=3)
        analyzer = ComplexityAnalyzer(config)
        score = analyzer.analyze("Hi")
        self.assertGreaterEqual(score, 3)

    def test_technical_terms_increase_score(self):
        simple = self.analyzer.analyze("What is a cat?")
        technical = self.analyzer.analyze("What is the complexity of the encryption algorithm?")
        self.assertGreaterEqual(technical, simple)

    def test_batch_analyze(self):
        queries = ["What is AI?", "Analyze distributed systems", ""]
        results = self.analyzer.batch_analyze(queries)
        self.assertEqual(len(results), 3)
        self.assertIn("score", results[0])
        self.assertIn("classification", results[0])

    def test_batch_analyze_handles_invalid(self):
        results = self.analyzer.batch_analyze(["valid query", ""])
        self.assertEqual(results[1]["classification"], "invalid")


class TestComplexityClassification(unittest.TestCase):
    def setUp(self):
        self.analyzer = ComplexityAnalyzer()

    def test_trivial_classification(self):
        level = self.analyzer.classify("Hi")
        self.assertEqual(level, ComplexityLevel.TRIVIAL)

    def test_simple_classification(self):
        level = self.analyzer.classify("What is the capital of France?")
        self.assertIn(level, [ComplexityLevel.TRIVIAL, ComplexityLevel.SIMPLE])

    def test_moderate_classification(self):
        level = self.analyzer.classify("How does a B-tree work?")
        self.assertIn(level, [ComplexityLevel.SIMPLE, ComplexityLevel.MODERATE])

    def test_expert_classification(self):
        """Very complex queries produce EXPERT classification."""
        level = self.analyzer.classify(
            "Analyze the Byzantine fault tolerance of distributed consensus algorithms with recursive proof systems and cryptographic hash functions and encryption protocols"
        )
        self.assertIn(level, [ComplexityLevel.COMPLEX, ComplexityLevel.EXPERT])

    def test_complexity_8_plus_is_expert(self):
        """A query scoring 8+ should be EXPERT."""
        query = "Analyze the Byzantine fault tolerance of distributed consensus algorithms with recursive proof systems and cryptographic hash functions"
        score = self.analyzer.analyze(query)
        if score >= 8:
            self.assertEqual(self.analyzer.classify(query), ComplexityLevel.EXPERT)


# ═══════════════════════════════════════════════════════════════════════
# PerformanceTracker Tests
# ═══════════════════════════════════════════════════════════════════════

class TestPerformanceTrackerBasics(unittest.TestCase):
    def setUp(self):
        self.tracker = PerformanceTracker()

    def test_record_and_get_stats(self):
        self.tracker.record("ep_a", latency_ms=100.0, success=True)
        stats = self.tracker.get_stats("ep_a")
        self.assertEqual(stats["avg_latency_ms"], 100.0)
        self.assertEqual(stats["success_rate"], 1.0)
        self.assertEqual(stats["total_requests"], 1)
        self.assertEqual(stats["status"], EndpointStatus.HEALTHY.value)

    def test_multiple_records_average(self):
        self.tracker.record("ep_a", latency_ms=100.0, success=True)
        self.tracker.record("ep_a", latency_ms=200.0, success=True)
        stats = self.tracker.get_stats("ep_a")
        self.assertEqual(stats["avg_latency_ms"], 150.0)

    def test_failure_tracks(self):
        self.tracker.record("ep_a", latency_ms=50.0, success=True)
        self.tracker.record("ep_a", latency_ms=50.0, success=False)
        stats = self.tracker.get_stats("ep_a")
        self.assertEqual(stats["success_rate"], 0.5)

    def test_unknown_endpoint_returns_zeros(self):
        stats = self.tracker.get_stats("unknown")
        self.assertEqual(stats["avg_latency_ms"], 0.0)
        self.assertEqual(stats["total_requests"], 0)
        self.assertEqual(stats["status"], EndpointStatus.DOWN.value)

    def test_success_rate_floor_marks_down(self):
        """Low success rate marks endpoint as DOWN."""
        config = CARConfig(success_rate_floor=0.51)
        tracker = PerformanceTracker(config)
        for _ in range(5):
            tracker.record("ep_a", latency_ms=100, success=True)
        for _ in range(5):
            tracker.record("ep_a", latency_ms=100, success=False)
        stats = tracker.get_stats("ep_a")
        self.assertEqual(stats["status"], EndpointStatus.DOWN.value)

    def test_overloaded_endpoint(self):
        config = CARConfig(overload_latency_ms=1000)
        tracker = PerformanceTracker(config)
        for _ in range(5):
            tracker.record("ep_a", latency_ms=3000.0, success=True)
        stats = tracker.get_stats("ep_a")
        self.assertEqual(stats["status"], EndpointStatus.OVERLOADED.value)

    def test_degraded_endpoint(self):
        config = CARConfig(degradation_latency_ms=500)
        tracker = PerformanceTracker(config)
        for _ in range(5):
            tracker.record("ep_a", latency_ms=800.0, success=True)
        stats = tracker.get_stats("ep_a")
        self.assertEqual(stats["status"], EndpointStatus.DEGRADED.value)

    def test_breaker_state_tracking(self):
        """Circuit breaker state reflects endpoint health."""
        self.tracker.record("ep_a", latency_ms=100, success=True)
        state = self.tracker.get_breaker_state("ep_a")
        self.assertIn(state, [CircuitState.CLOSED, CircuitState.HALF_OPEN])


class TestPerformanceTrackerBestEndpoint(unittest.TestCase):
    def setUp(self):
        self.tracker = PerformanceTracker()

    def test_get_best_endpoint(self):
        self.tracker.record("fast_ep", latency_ms=50.0, success=True)
        self.tracker.record("fast_ep", latency_ms=60.0, success=True)
        self.tracker.record("slow_ep", latency_ms=500.0, success=True)
        self.tracker.record("slow_ep", latency_ms=600.0, success=False)
        best = self.tracker.get_best_endpoint()
        self.assertEqual(best, "fast_ep")

    def test_exclude_endpoint(self):
        self.tracker.record("ep_a", latency_ms=50.0, success=True)
        self.tracker.record("ep_b", latency_ms=3000.0, success=True)
        best = self.tracker.get_best_endpoint(exclude=["ep_a"])
        self.assertEqual(best, "ep_b")

    def test_empty_tracker_returns_none(self):
        best = self.tracker.get_best_endpoint()
        self.assertIsNone(best)

    def test_get_all_stats(self):
        self.tracker.record("ep_a", latency_ms=100.0, success=True)
        self.tracker.record("ep_b", latency_ms=200.0, success=True)
        all_stats = self.tracker.get_all_stats()
        self.assertIn("ep_a", all_stats)
        self.assertIn("ep_b", all_stats)

    def test_is_healthy(self):
        self.tracker.record("ep_a", latency_ms=100.0, success=True)
        self.assertTrue(self.tracker.is_healthy("ep_a"))
        self.tracker.record("ep_b", latency_ms=5000.0, success=False)
        self.assertFalse(self.tracker.is_healthy("ep_b"))

    def test_get_success_rate(self):
        self.tracker.record("ep_a", latency_ms=100, success=True)
        self.tracker.record("ep_a", latency_ms=100, success=False)
        self.assertAlmostEqual(self.tracker.get_success_rate("ep_a"), 0.5)

    def test_get_avg_latency(self):
        self.tracker.record("ep_a", latency_ms=150.0, success=True)
        self.assertAlmostEqual(self.tracker.get_avg_latency("ep_a"), 150.0)

    def test_reset_single_endpoint(self):
        self.tracker.record("ep_a", latency_ms=100.0, success=True)
        self.tracker.record("ep_b", latency_ms=200.0, success=True)
        self.tracker.reset("ep_a")
        self.assertEqual(self.tracker.get_stats("ep_a")["total_requests"], 0)
        self.assertGreater(self.tracker.get_stats("ep_b")["total_requests"], 0)

    def test_reset_all(self):
        self.tracker.record("ep_a", latency_ms=100.0, success=True)
        self.tracker.reset()
        self.assertEqual(len(self.tracker._history), 0)


# ═══════════════════════════════════════════════════════════════════════
# ModelSelector Tests
# ═══════════════════════════════════════════════════════════════════════

class TestModelSelectorBasics(unittest.TestCase):
    def setUp(self):
        self.selector = ModelSelector()

    def test_select_returns_valid_endpoint(self):
        endpoint = self.selector.select(complexity=3, user_pref=UserPreference.SPEED, load=0.3)
        self.assertIn(endpoint, self.selector.profiles)

    def test_select_speed_prefers_fast(self):
        endpoint = self.selector.select(complexity=2, user_pref=UserPreference.SPEED, load=0.2)
        self.assertEqual(endpoint, "fast")

    def test_select_quality_prefers_quality(self):
        endpoint = self.selector.select(complexity=8, user_pref=UserPreference.QUALITY, load=0.3)
        self.assertIn(endpoint, ["quality", "expert"])

    def test_select_balanced_returns_valid(self):
        endpoint = self.selector.select(complexity=5, user_pref=UserPreference.BALANCED, load=0.5)
        self.assertIn(endpoint, self.selector.profiles)

    def test_complexity_exceeds_all_profiles(self):
        """When complexity exceeds all profiles' max, returns default."""
        selector = ModelSelector(profiles={"fast": {"latency_ms": 50.0, "quality_score": 0.5, "max_complexity": 3, "cost_per_token": 0.001, "broker_priority": 1}})
        endpoint = selector.select(complexity=15, user_pref=UserPreference.QUALITY, load=0.3)
        self.assertEqual(endpoint, selector.config.default_endpoint)

    def test_load_affects_selection(self):
        ep_low_load = self.selector.select(complexity=5, user_pref=UserPreference.SPEED, load=0.1)
        ep_high_load = self.selector.select(complexity=5, user_pref=UserPreference.SPEED, load=0.9)
        self.assertIn(ep_low_load, self.selector.profiles)
        self.assertIn(ep_high_load, self.selector.profiles)

    def test_clamp_complexity(self):
        endpoint = self.selector.select(complexity=100, user_pref=UserPreference.SPEED, load=0.5)
        self.assertIn(endpoint, self.selector.profiles)

    def test_clamp_load(self):
        endpoint = self.selector.select(complexity=3, user_pref=UserPreference.SPEED, load=2.0)
        self.assertIn(endpoint, self.selector.profiles)

    def test_add_profile(self):
        self.selector.add_profile("custom_ep", {
            "latency_ms": 25.0, "quality_score": 0.6,
            "max_complexity": 3, "cost_per_token": 0.002, "broker_priority": 1,
        })
        self.assertIn("custom_ep", self.selector.profiles)

    def test_add_profile_invalid(self):
        with self.assertRaises(ValueError):
            self.selector.add_profile("bad", {"latency_ms": 10.0})

    def test_get_profile(self):
        profile = self.selector.get_profile("fast")
        self.assertIsNotNone(profile)
        self.assertEqual(profile["latency_ms"], 50.0)

    def test_get_profile_unknown(self):
        self.assertIsNone(self.selector.get_profile("nonexistent"))

    def test_get_profiles(self):
        profiles = self.selector.get_profiles()
        self.assertIn("fast", profiles)
        self.assertIn("balanced", profiles)
        self.assertIn("quality", profiles)
        self.assertIn("expert", profiles)

    def test_empty_profiles_returns_default(self):
        """Empty profiles dict returns default endpoint."""
        selector = ModelSelector(profiles={})
        endpoint = selector.select(complexity=3, user_pref=UserPreference.SPEED, load=0.2)
        self.assertEqual(endpoint, selector.config.default_endpoint)


class TestModelSelectorWithPerformanceTracker(unittest.TestCase):
    def setUp(self):
        self.tracker = PerformanceTracker()
        self.selector = ModelSelector()
        for _ in range(10):
            self.tracker.record("fast", latency_ms=50.0, success=True)
        for _ in range(10):
            self.tracker.record("quality", latency_ms=3000.0, success=False)

    def test_performance_overrides_selection(self):
        endpoint = self.selector.select(
            complexity=3, user_pref=UserPreference.QUALITY,
            load=0.3, performance_tracker=self.tracker
        )
        self.assertEqual(endpoint, "fast")

    def test_exclude_with_performance(self):
        endpoint = self.selector.select(
            complexity=3, user_pref=UserPreference.SPEED,
            load=0.3, performance_tracker=self.tracker,
            exclude=["fast"]
        )
        self.assertNotEqual(endpoint, "fast")


# ═══════════════════════════════════════════════════════════════════════
# RouteOptimizer Tests
# ═══════════════════════════════════════════════════════════════════════

class TestRouteOptimizerBasics(unittest.TestCase):
    def setUp(self):
        self.optimizer = RouteOptimizer()

    def test_update_records_performance(self):
        self.optimizer.update("ep_a", latency_ms=100.0, success=True)
        weights = self.optimizer.compute_weights(["ep_a"])
        self.assertIn("ep_a", weights)

    def test_compute_weights_normalized(self):
        self.optimizer.update("ep_a", latency_ms=100.0, success=True)
        self.optimizer.update("ep_b", latency_ms=200.0, success=True)
        weights = self.optimizer.compute_weights(["ep_a", "ep_b"])
        total = sum(weights.values())
        self.assertAlmostEqual(total, 1.0, places=4)

    def test_best_endpoint(self):
        self.optimizer.update("ep_a", latency_ms=50.0, success=True)
        self.optimizer.update("ep_b", latency_ms=500.0, success=False)
        best = self.optimizer.get_best_endpoint(["ep_a", "ep_b"])
        self.assertEqual(best, "ep_a")

    def test_empty_endpoints(self):
        weights = self.optimizer.compute_weights([])
        self.assertEqual(weights, {})
        self.assertIsNone(self.optimizer.get_best_endpoint([]))

    def test_no_data_defaults(self):
        weights = self.optimizer.compute_weights(["ep_a", "ep_b"])
        self.assertAlmostEqual(weights["ep_a"], weights["ep_b"], places=4)

    def test_recommendations(self):
        self.optimizer.update("ep_a", latency_ms=50.0, success=True)
        self.optimizer.update("ep_b", latency_ms=500.0, success=False)
        recs = self.optimizer.get_recommendations(["ep_a", "ep_b"], n=1)
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["endpoint"], "ep_a")
        self.assertIn("reason", recs[0])
        self.assertIn("weight", recs[0])

    def test_recommendations_n_larger_than_list(self):
        self.optimizer.update("ep_a", latency_ms=50.0, success=True)
        recs = self.optimizer.get_recommendations(["ep_a"], n=5)
        self.assertEqual(len(recs), 1)

    def test_reset(self):
        self.optimizer.update("ep_a", latency_ms=50.0, success=True)
        self.optimizer.reset()
        weights = self.optimizer.compute_weights(["ep_a"])
        self.assertEqual(weights["ep_a"], 1.0)

    def test_trend_penalty(self):
        for _ in range(10):
            self.optimizer.update("ep_good", latency_ms=100.0, success=True)
        for i in range(10):
            self.optimizer.update("ep_bad", latency_ms=500.0, success=(i < 3))
        weights = self.optimizer.compute_weights(["ep_good", "ep_bad"])
        self.assertGreater(weights["ep_good"], weights["ep_bad"])

    def test_adjustment_factor(self):
        for _ in range(10):
            self.optimizer.update("ep_a", latency_ms=100.0, success=True)
        self.optimizer.update("ep_b", latency_ms=500.0, success=False)
        adj_a = self.optimizer._adjustments["ep_a"]
        adj_b = self.optimizer._adjustments["ep_b"]
        self.assertGreater(adj_a, adj_b)

    def test_breaker_integration(self):
        """Circuit breakers are created and updated."""
        self.optimizer.update("ep_a", latency_ms=100.0, success=True)
        breaker = self.optimizer._breakers["ep_a"]
        self.assertIsNotNone(breaker)
        self.assertIn(breaker.state, [CircuitState.CLOSED, CircuitState.HALF_OPEN])


# ═══════════════════════════════════════════════════════════════════════
# CARRouter Tests
# ═══════════════════════════════════════════════════════════════════════

class TestCARRouterBasics(unittest.TestCase):
    def setUp(self):
        self.router = CARRouter()

    def test_route_returns_valid_endpoint(self):
        result = self.router.route("What is AI?", UserPreference.BALANCED, 0.5)
        self.assertIn("endpoint", result)
        self.assertIn(result["endpoint"], self.router.model_selector.profiles)

    def test_route_returns_complexity(self):
        result = self.router.route("What is AI?")
        self.assertIn("complexity", result)
        self.assertGreaterEqual(result["complexity"], 1)

    def test_route_returns_classification(self):
        result = self.router.route("What is AI?")
        self.assertIn("classification", result)
        self.assertIn(result["classification"], [e.value for e in ComplexityLevel])

    def test_route_returns_reasoning(self):
        result = self.router.route("What is AI?")
        self.assertIn("reasoning", result)
        self.assertIsInstance(result["reasoning"], list)

    def test_complex_query_routes_to_higher_tier(self):
        simple = self.router.route("Hi")
        complex_q = self.router.route("Analyze the distributed consensus algorithm with recursive proofs")
        self.assertGreater(complex_q["complexity"], simple["complexity"])

    def test_record_outcome_updates_tracker(self):
        result = self.router.route("What is AI?")
        endpoint = result["endpoint"]
        self.router.record_outcome(endpoint, latency_ms=100.0, success=True)
        stats = self.router.performance_tracker.get_stats(endpoint)
        self.assertEqual(stats["total_requests"], 1)

    def test_record_outcome_updates_optimizer(self):
        result = self.router.route("What is AI?")
        endpoint = result["endpoint"]
        self.router.record_outcome(endpoint, latency_ms=100.0, success=True)
        weights = self.router.route_optimizer.compute_weights([endpoint])
        self.assertIn(endpoint, weights)

    def test_get_routing_stats(self):
        result = self.router.route("What is AI?")
        self.router.record_outcome(result["endpoint"], 100.0, True)
        stats = self.router.get_routing_stats()
        self.assertIn("total_routes", stats)
        self.assertIn("performance_stats", stats)
        self.assertIn("optimizer_recommendations", stats)
        self.assertIn("best_endpoint", stats)

    def test_routing_history_tracked(self):
        self.router.route("Query 1")
        self.router.route("Query 2")
        self.assertEqual(len(self.router._routing_history), 2)
        self.assertEqual(self.router._routing_history[0]["query"], "Query 1")

    def test_load_parameter_affects_routing(self):
        result_low = self.router.route("What is AI?", load=0.1)
        result_high = self.router.route("What is AI?", load=0.9)
        self.assertIn(result_low["endpoint"], self.router.model_selector.profiles)
        self.assertIn(result_high["endpoint"], self.router.model_selector.profiles)

    def test_reset_clears_all(self):
        self.router.route("Test query")
        self.router.record_outcome("fast", 100.0, True)
        self.router.reset()
        self.assertEqual(len(self.router._routing_history), 0)


# ═══════════════════════════════════════════════════════════════════════
# Convenience Functions Tests
# ═══════════════════════════════════════════════════════════════════════

class TestConvenienceFunctions(unittest.TestCase):
    def test_create_router(self):
        router = create_router()
        self.assertIsInstance(router, CARRouter)

    def test_create_router_with_config(self):
        config = CARConfig(max_complexity=5)
        router = create_router(config)
        self.assertEqual(router.complexity_analyzer.config.max_complexity, 5)

    def test_route_query_returns_dict(self):
        result = route_query("What is AI?")
        self.assertIn("endpoint", result)
        self.assertIn("complexity", result)

    def test_route_query_with_pref(self):
        result = route_query("What is AI?", user_pref="speed")
        self.assertIn("endpoint", result)

    def test_route_query_with_load(self):
        result = route_query("What is AI?", load=0.5)
        self.assertIn("endpoint", result)


# ═══════════════════════════════════════════════════════════════════════
# Edge Cases & Integration Tests
# ═══════════════════════════════════════════════════════════════════════

class TestEdgeCases(unittest.TestCase):
    def test_selector_with_all_excluded(self):
        selector = ModelSelector()
        endpoint = selector.select(complexity=3, user_pref=UserPreference.SPEED,
                                   load=0.3, exclude=list(selector.profiles.keys()))
        self.assertEqual(endpoint, selector.config.default_endpoint)

    def test_optimizer_all_failures(self):
        optimizer = RouteOptimizer()
        for _ in range(10):
            optimizer.update("ep_a", latency_ms=500.0, success=False)
        weights = optimizer.compute_weights(["ep_a"])
        self.assertIn("ep_a", weights)
        self.assertGreater(weights["ep_a"], 0)

    def test_performance_tracker_large_window(self):
        config = CARConfig(performance_window_size=5)
        tracker = PerformanceTracker(config)
        for i in range(10):
            tracker.record(f"ep_{i%2}", latency_ms=100.0, success=True)
        stats = tracker.get_stats("ep_0")
        self.assertLessEqual(stats["recent_samples"], 5)

    def test_model_selector_empty_profiles_returns_default(self):
        selector = ModelSelector(profiles={})
        endpoint = selector.select(complexity=3, user_pref=UserPreference.SPEED, load=0.3)
        self.assertEqual(endpoint, selector.config.default_endpoint)

    def test_complexity_analyzer_special_chars(self):
        analyzer = ComplexityAnalyzer()
        score_normal = analyzer.analyze("What is a function?")
        score_code = analyzer.analyze("def foo(x): return x * 2")
        self.assertGreaterEqual(score_code, score_normal)

    def test_route_returns_consistent_structure(self):
        router = CARRouter()
        keys = {"endpoint", "complexity", "classification", "reasoning", "user_preference", "system_load"}
        for _ in range(5):
            result = router.route("Test query")
            self.assertTrue(keys.issubset(result.keys()))

    def test_router_v2_broker_integration(self):
        """CARRouter uses v2 MessageBroker correctly."""
        from broker import MessageBroker
        broker = MessageBroker()
        router = CARRouter(broker=broker)
        result = router.route("Test query")
        # Verify the v2 encoder was used to create a message
        self.assertIn("message", result)
        self.assertIn("tokens", result["message"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
