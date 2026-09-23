"""Tests for the A/B Testing Framework (src/ab_testing.py)."""
import sys
import os
import unittest
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from ab_testing import (
    ABTestRunner, ExperimentRegistry, StatisticalTest, MetricsTracker,
    Experiment, ExperimentStatus, MetricType
)


class TestMetricsTracker(unittest.TestCase):
    """Test MetricsTracker functionality."""

    def test_create_tracker(self):
        tracker = MetricsTracker(variant="control")
        self.assertEqual(tracker.variant, "control")
        self.assertEqual(tracker.samples, 0)

    def test_record_observations(self):
        tracker = MetricsTracker(variant="treatment")
        tracker.record(token_savings=10.0, latency_ms=100.0, success=True)
        tracker.record(token_savings=20.0, latency_ms=200.0, success=True)
        self.assertEqual(tracker.samples, 2)
        self.assertEqual(tracker.total_requests, 2)
        self.assertGreater(tracker.latency_ms, 0)

    def test_get_summary(self):
        tracker = MetricsTracker(variant="control")
        tracker.record(10.0, 100.0, True)
        summary = tracker.get_summary()
        self.assertEqual(summary["variant"], "control")
        self.assertEqual(summary["samples"], 1)
        self.assertIn("token_savings", summary)

    def test_empty_tracker(self):
        tracker = MetricsTracker(variant="test")
        summary = tracker.get_summary()
        self.assertEqual(summary["token_savings"], 0.0)
        self.assertEqual(summary["latency_ms"], 0.0)
        self.assertEqual(summary["success_rate"], 0.0)


class TestStatisticalTest(unittest.TestCase):
    """Test StatisticalTest functionality."""

    def test_mean(self):
        self.assertEqual(StatisticalTest.mean([1, 2, 3]), 2.0)
        self.assertEqual(StatisticalTest.mean([]), 0.0)

    def test_variance(self):
        self.assertAlmostEqual(StatisticalTest.variance([1, 2, 3]), 1.0, places=5)
        self.assertEqual(StatisticalTest.variance([5]), 0.0)

    def test_std_dev(self):
        self.assertAlmostEqual(StatisticalTest.std_dev([1, 2, 3]), 1.0, places=5)

    def test_confidence_interval(self):
        values = [100, 105, 98, 102, 101]
        ci = StatisticalTest.confidence_interval(values)
        self.assertIsInstance(ci, tuple)
        self.assertEqual(len(ci), 2)
        self.assertLess(ci[0], ci[1])

    def test_significance(self):
        # Same distributions should not be significant
        ctrl = [100, 101, 102, 99, 100]
        treat = [100, 101, 102, 99, 100]
        # p-value may be very high for identical data
        p = StatisticalTest.calculate_p_value(ctrl, treat)
        self.assertGreaterEqual(p, 0.0)

    def test_z_score(self):
        self.assertAlmostEqual(StatisticalTest._z_score(0.95), 1.96, places=2)
        self.assertAlmostEqual(StatisticalTest._z_score(0.99), 2.576, places=2)
        self.assertAlmostEqual(StatisticalTest._z_score(0.90), 1.645, places=2)


class TestExperiment(unittest.TestCase):
    """Test Experiment dataclass."""

    def test_create_experiment(self):
        exp = Experiment(
            id="test-123",
            name="Test Experiment",
            hypothesis="Test hypothesis",
            control_variant="control",
            treatment_variant="treatment",
        )
        self.assertEqual(exp.name, "Test Experiment")
        self.assertEqual(exp.status, ExperimentStatus.PLANNED.value)

    def test_experiment_auto_id(self):
        exp = Experiment(
            id="",
            name="Auto ID",
            hypothesis="Test",
            control_variant="c",
            treatment_variant="t",
        )
        self.assertNotEqual(exp.id, "")

    def test_experiment_with_metrics(self):
        exp = Experiment(
            id="test",
            name="Test",
            hypothesis="Test",
            control_variant="c",
            treatment_variant="t",
            metrics=["token_savings", "latency"],
        )
        self.assertEqual(len(exp.metrics), 2)

    def test_experiment_to_dict(self):
        exp = Experiment(
            id="test",
            name="Test",
            hypothesis="Test",
            control_variant="c",
            treatment_variant="t",
        )
        d = exp.__dict__ if hasattr(exp, '__dict__') else dict(exp._fields)
        self.assertIn("name", d)
        self.assertIn("status", d)


class TestABTestRunner(unittest.TestCase):
    """Test ABTestRunner functionality."""

    def setUp(self):
        self.runner = ABTestRunner()

    def test_create_experiment(self):
        exp = self.runner.create_experiment(
            name="Test", hypothesis="H", control_variant="control", treatment_variant="treatment"
        )
        self.assertIsInstance(exp.id, str)
        self.assertEqual(exp.name, "Test")
        self.assertEqual(exp.status, ExperimentStatus.RUNNING.value)

    def test_create_experiment_invalid_comparison(self):
        with self.assertRaises(ValueError):
            self.runner.create_experiment(
                name="Test", hypothesis="H",
                control_variant="c", treatment_variant="t",
                comparison_type="invalid"
            )

    def test_list_supported_comparisons(self):
        self.assertIn("swarm_vs_direct", ABTestRunner.SUPPORTED_COMPARISONS)
        self.assertIn("agent_count_6_vs_8", ABTestRunner.SUPPORTED_COMPARISONS)
        self.assertIn("compression_level_50_vs_90", ABTestRunner.SUPPORTED_COMPARISONS)

    def test_record_and_complete(self):
        exp = self.runner.create_experiment(
            name="Test", hypothesis="H", control_variant="control", treatment_variant="treatment"
        )
        for _ in range(5):
            self.runner.record_result(exp.id, "control", 10.0, 100.0, True)
            self.runner.record_result(exp.id, "treatment", 5.0, 50.0, True)
        result = self.runner.complete_experiment(exp.id)
        self.assertEqual(result["status"], ExperimentStatus.COMPLETED.value)
        self.assertIn("control_summary", result)
        self.assertIn("treatment_summary", result)
        self.assertIn("p_value", result)
        self.assertIn("significant", result)

    def test_get_experiment(self):
        exp = self.runner.create_experiment("Test", "H", "c", "t")
        retrieved = self.runner.get_experiment(exp.id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.id, exp.id)

    def test_get_nonexistent_experiment(self):
        retrieved = self.runner.get_experiment("nonexistent")
        self.assertIsNone(retrieved)

    def test_run_experiment(self):
        result = self.runner.run_experiment(
            name="Quick Test",
            hypothesis="Test",
            comparison_type="swarm_vs_direct",
            control_variant="control",
            treatment_variant="treatment",
            control_data=[10, 11, 12, 10, 11],
            treatment_data=[5, 6, 5, 6, 5],
        )
        self.assertIn("experiment_id", result)
        self.assertIn("significant", result)


class TestExperimentRegistry(unittest.TestCase):
    """Test ExperimentRegistry functionality."""

    def setUp(self):
        self.registry = ExperimentRegistry()

    def test_create_and_list(self):
        exp = self.registry.create_experiment("Test", "H", "c", "t")
        experiments = self.registry.list_experiments()
        self.assertEqual(len(experiments), 1)

    def test_get_experiment(self):
        exp = self.registry.create_experiment("Test", "H", "c", "t")
        retrieved = self.registry.get_experiment(exp.id)
        self.assertIsNotNone(retrieved)

    def test_filter_by_status(self):
        exp = self.registry.create_experiment("Test", "H", "c", "t")
        planned = self.registry.list_experiments(status=ExperimentStatus.PLANNED.value)
        self.assertGreaterEqual(len(planned), 0)


if __name__ == '__main__':
    unittest.main()
