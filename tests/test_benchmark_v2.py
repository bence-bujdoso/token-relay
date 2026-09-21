"""
Comprehensive tests for TokenRelay v2 Benchmark System.

Tests cover:
- BenchmarkV2 class initialization and configuration
- Protocol benchmarking (TokenRelay, Traditional Text, JSON Passthrough)
- Scalability testing across message counts
- Memory profiling via resource module
- Latency percentile calculations (p50, p95, p99)
- Comparative analysis between protocols
- JSON output format validation
- Real LLM integration (subprocess-based)
"""

import sys
import os
import json
import time
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from benchmark_v2 import (
    BenchmarkV2, Protocol, BenchmarkResult,
    LatencyStats, MemorySnapshot
)


class TestLatencyStats(unittest.TestCase):
    """Test LatencyStats computation."""

    def test_from_values_empty(self):
        stats = LatencyStats.from_values([])
        self.assertEqual(stats.p50, 0)
        self.assertEqual(stats.p95, 0)
        self.assertEqual(stats.p99, 0)

    def test_from_values_computed(self):
        values = [1.0, 2.0, 3.0, 4.0, 5.0]
        stats = LatencyStats.from_values(values)
        self.assertEqual(stats.p50, 3.0)
        self.assertEqual(stats.mean, 3.0)
        self.assertEqual(stats.min, 1.0)
        self.assertEqual(stats.max, 5.0)
        self.assertEqual(len(stats.all_values), 5)

    def test_p99_higher_than_p50(self):
        values = [1.0, 2.0, 5.0, 10.0, 50.0]
        stats = LatencyStats.from_values(values)
        self.assertGreaterEqual(stats.p99, stats.p50)

    def test_to_dict(self):
        values = [1.0, 2.0, 3.0]
        stats = LatencyStats.from_values(values)
        d = stats.to_dict()
        self.assertIn("p50", d)
        self.assertIn("p95", d)
        self.assertIn("p99", d)
        self.assertIn("all_values", d)


class TestMemorySnapshot(unittest.TestCase):
    """Test MemorySnapshot capture."""

    def test_capture_returns_valid(self):
        snap = MemorySnapshot.capture()
        self.assertIsInstance(snap.rss_kb, int)
        self.assertGreaterEqual(snap.rss_kb, 0)

    def test_to_dict(self):
        snap = MemorySnapshot.capture()
        d = snap.to_dict()
        self.assertIn("rss_kb", d)
        self.assertIn("peak_rss_kb", d)


class TestBenchmarkV2Init(unittest.TestCase):
    """Test BenchmarkV2 initialization."""

    def setUp(self):
        self.bench = BenchmarkV2(use_real_llm=False)

    def test_default_config(self):
        self.assertEqual(self.bench.min_messages, 10)
        self.assertEqual(self.bench.max_messages, 100000)
        self.assertEqual(self.bench.scale_steps, 7)
        self.assertFalse(self.bench.use_real_llm)

    def test_custom_config(self):
        bench = BenchmarkV2(
            use_real_llm=True,
            min_messages=5,
            max_messages=5000,
            scale_steps=5,
            llm_command="echo test"
        )
        self.assertEqual(bench.min_messages, 5)
        self.assertEqual(bench.max_messages, 5000)
        self.assertEqual(bench.scale_steps, 5)
        self.assertTrue(bench.use_real_llm)

    def test_components_initialized(self):
        self.assertIsNotNone(self.bench.registry)
        self.assertIsNotNone(self.bench.encoder)
        self.assertIsNotNone(self.bench.decoder)
        self.assertIsNotNone(self.bench.bridge)


class TestProtocolBenchmarking(unittest.TestCase):
    """Test protocol benchmarking functionality."""

    def setUp(self):
        self.bench = BenchmarkV2(use_real_llm=False, min_messages=10, max_messages=100)

    def test_token_relay_benchmark(self):
        result = self.bench.benchmark_protocol(Protocol.TOKEN_RELAY, message_count=10)
        self.assertEqual(result.protocol, "token_relay")
        self.assertEqual(result.message_count, 10)
        self.assertGreater(result.total_time_sec, 0)
        self.assertGreater(result.throughput_per_sec, 0)
        self.assertIn(result.protocol, ["token_relay"])
        self.assertIsInstance(result.latency_stats, dict)
        self.assertIn("p50", result.latency_stats)

    def test_traditional_text_benchmark(self):
        result = self.bench.benchmark_protocol(Protocol.TRADITIONAL_TEXT, message_count=50)
        self.assertEqual(result.protocol, "traditional_text")
        self.assertEqual(result.message_count, 50)
        self.assertGreaterEqual(result.total_time_sec, 0)

    def test_json_passthrough_benchmark(self):
        result = self.bench.benchmark_protocol(Protocol.JSON_PASSTHROUGH, message_count=50)
        self.assertEqual(result.protocol, "json_passthrough")
        self.assertEqual(result.message_count, 50)
        self.assertGreaterEqual(result.total_time_sec, 0)

    def test_token_relay_savings(self):
        """TokenRelay should show token savings vs traditional."""
        tr_result = self.bench.benchmark_protocol(Protocol.TOKEN_RELAY, message_count=100)
        tt_result = self.bench.benchmark_protocol(Protocol.TRADITIONAL_TEXT, message_count=100)
        # TokenRelay should have lower LLM tokens
        self.assertLessEqual(
            tr_result.llm_tokens_tokenrelay, tt_result.llm_tokens_traditional,
            msg="TokenRelay should consume fewer LLM tokens"
        )

    def test_latency_percentiles_present(self):
        result = self.bench.benchmark_protocol(Protocol.TOKEN_RELAY, message_count=10)
        self.assertIn("p50", result.latency_stats)
        self.assertIn("p95", result.latency_stats)
        self.assertIn("p99", result.latency_stats)

    def test_memory_snapshot_in_result(self):
        result = self.bench.benchmark_protocol(Protocol.TOKEN_RELAY, message_count=10)
        self.assertIn("rss_kb", result.memory_before)
        self.assertIn("rss_kb", result.memory_after)
        self.assertIsInstance(result.memory_delta_kb, int)

    def test_throughput_calculated(self):
        result = self.bench.benchmark_protocol(Protocol.TOKEN_RELAY, message_count=100)
        self.assertGreaterEqual(result.throughput_per_sec, 0)
        self.assertIn("throughput_per_sec", str(result))  # Verify field exists


class TestScalability(unittest.TestCase):
    """Test scalability testing functionality."""

    def setUp(self):
        self.bench = BenchmarkV2(use_real_llm=False, min_messages=10, max_messages=500)

    def test_run_scalability_token_relay(self):
        results = self.bench.run_scalability_test(Protocol.TOKEN_RELAY)
        self.assertGreater(len(results), 0)
        for r in results:
            self.assertEqual(r.protocol, "token_relay")

    def test_run_scalability_multiple_counts(self):
        results = self.bench.run_scalability_test(Protocol.TOKEN_RELAY, scale_steps=3)
        counts = [r.message_count for r in results]
        # Should have at least 3 different counts, monotonically increasing
        self.assertGreaterEqual(len(counts), 3)
        for i in range(1, len(counts)):
            self.assertGreaterEqual(counts[i], counts[i-1])

    def test_scalability_throughput(self):
        results = self.bench.run_scalability_test(Protocol.TOKEN_RELAY, scale_steps=3)
        for r in results:
            self.assertGreater(r.throughput_per_sec, 0)

    def test_scalability_traditional(self):
        results = self.bench.run_scalability_test(Protocol.TRADITIONAL_TEXT, scale_steps=3)
        self.assertGreater(len(results), 0)
        for r in results:
            self.assertEqual(r.protocol, "traditional_text")


class TestComparativeBenchmark(unittest.TestCase):
    """Test comparative benchmark analysis."""

    def setUp(self):
        self.bench = BenchmarkV2(use_real_llm=False, min_messages=10, max_messages=100)

    def test_comparative_runs_all_protocols(self):
        comparison = self.bench.run_comparative_benchmark(message_count=50, iterations=2)
        self.assertIn("token_relay", comparison["protocols"])
        self.assertIn("traditional_text", comparison["protocols"])
        self.assertIn("json_passthrough", comparison["protocols"])

    def test_comparative_has_analysis(self):
        comparison = self.bench.run_comparative_benchmark(message_count=50, iterations=2)
        analysis = comparison["analysis"]
        self.assertIn("token_relay_savings_vs_traditional_pct", analysis)
        self.assertIn("throughput_comparison", analysis)
        self.assertIn("latency_comparison", analysis)

    def test_comparative_savings_positive(self):
        comparison = self.bench.run_comparative_benchmark(message_count=50, iterations=2)
        savings = comparison["analysis"]["token_relay_savings_vs_traditional_pct"]
        self.assertGreaterEqual(savings, 0)

    def test_comparative_results_structure(self):
        comparison = self.bench.run_comparative_benchmark(message_count=20, iterations=1)
        for proto_name, proto_data in comparison["protocols"].items():
            self.assertIn("scenario_name", proto_data)
            self.assertIn("message_count", proto_data)
            self.assertIn("total_time_sec", proto_data)
            self.assertIn("latency_stats", proto_data)
            self.assertIn("throughput_per_sec", proto_data)
            self.assertIn("savings_pct", proto_data)


class TestFullBenchmarkSuite(unittest.TestCase):
    """Test the complete benchmark suite."""

    def setUp(self):
        self.bench = BenchmarkV2(use_real_llm=False, min_messages=10, max_messages=200)

    def test_run_full_suite_returns_dict(self):
        results = self.bench.run_full_benchmark_suite(iterations=1)
        self.assertIn("benchmark_version", results)
        self.assertIn("timestamp", results)
        self.assertIn("configuration", results)
        self.assertIn("scalability_results", results)
        self.assertIn("comparative_results", results)
        self.assertIn("summary", results)

    def test_full_suite_has_multiple_results(self):
        results = self.bench.run_full_benchmark_suite(iterations=1)
        self.assertGreater(len(results["scalability_results"]), 0)

    def test_full_suite_summary(self):
        results = self.bench.run_full_benchmark_suite(iterations=1)
        summary = results["summary"]
        self.assertIn("avg_savings_pct", summary)
        self.assertIn("total_scenarios", summary)

    def test_configuration_preserved(self):
        results = self.bench.run_full_benchmark_suite(iterations=2)
        config = results["configuration"]
        self.assertEqual(config["iterations"], 2)
        self.assertEqual(config["use_real_llm"], False)


class TestJSONOutput(unittest.TestCase):
    """Test JSON output format."""

    def setUp(self):
        self.bench = BenchmarkV2(use_real_llm=False, min_messages=10, max_messages=50)

    def test_output_json_valid(self):
        suite = self.bench.run_full_benchmark_suite(iterations=1)
        json_str = self.bench.output_json(suite)
        parsed = json.loads(json_str)
        self.assertEqual(parsed["benchmark_version"], "2.0.0")

    def test_json_is_serializable(self):
        suite = self.bench.run_full_benchmark_suite(iterations=1)
        json_str = self.bench.output_json(suite)
        # Should not raise any exception
        parsed = json.loads(json_str)
        self.assertIsInstance(parsed, dict)

    def test_json_contains_latency_stats(self):
        suite = self.bench.run_full_benchmark_suite(iterations=1)
        json_str = self.bench.output_json(suite)
        parsed = json.loads(json_str)
        if parsed["scalability_results"]:
            first = parsed["scalability_results"][0]
            self.assertIn("latency_stats", first)
            self.assertIn("p50", first["latency_stats"])
            self.assertIn("p95", first["latency_stats"])
            self.assertIn("p99", first["latency_stats"])


class TestRealLLMIntegration(unittest.TestCase):
    """Test real LLM integration via subprocess."""

    def test_call_llm_subprocess(self):
        """Test that LLM subprocess calls work."""
        bench = BenchmarkV2(use_real_llm=True, llm_command="echo 'hello'")
        result = bench._call_llm("test input")
        # echo returns the input (with newline)
        self.assertIn("hello", result.lower() if result else True)

    def test_call_llm_fallback_on_error(self):
        """Test fallback when LLM command fails."""
        bench = BenchmarkV2(use_real_llm=True, llm_command="nonexistent_command_xyz_12345")
        result = bench._call_llm("test input")
        # Should fallback to returning the input
        self.assertEqual(result, "test input")

    def test_real_llm_flag(self):
        """Test that use_real_llm flag controls behavior."""
        bench = BenchmarkV2(use_real_llm=True, llm_command="echo test")
        self.assertTrue(bench.use_real_llm)

        bench2 = BenchmarkV2(use_real_llm=False)
        self.assertFalse(bench2.use_real_llm)


class TestEstimateLLMTokens(unittest.TestCase):
    """Test LLM token estimation."""

    def setUp(self):
        self.bench = BenchmarkV2(use_real_llm=False)

    def test_estimate_basic(self):
        tokens = self.bench._estimate_llm_tokens("hello world")
        self.assertGreater(tokens, 0)

    def test_estimate_proportional(self):
        long_text = "a" * 100
        short_text = "a" * 10
        long_tokens = self.bench._estimate_llm_tokens(long_text)
        short_tokens = self.bench._estimate_llm_tokens(short_text)
        self.assertGreater(long_tokens, short_tokens)

    def test_estimate_minimum(self):
        tokens = self.bench._estimate_llm_tokens("hi")
        self.assertEqual(tokens, 1)  # max(2//4, 1) = 1


class TestBenchmarkResult(unittest.TestCase):
    """Test BenchmarkResult dataclass."""

    def test_result_construction(self):
        result = BenchmarkResult(
            scenario_name="test",
            protocol="token_relay",
            message_count=10,
            total_time_sec=0.5,
            avg_latency_ms=50.0,
            latency_stats={"p50": 45.0, "p95": 60.0, "p99": 70.0, "mean": 50.0, "min": 40.0, "max": 60.0},
            memory_before={"rss_kb": 1000},
            memory_after={"rss_kb": 1100},
            memory_delta_kb=100,
            throughput_per_sec=200,
            llm_tokens_traditional=500,
            llm_tokens_tokenrelay=100,
            tokens_saved=400,
            savings_pct=80.0,
            payload_bytes_traditional=10000,
            payload_bytes_tokenrelay=2000,
            payload_ratio_pct=20.0,
        )
        self.assertEqual(result.scenario_name, "test")
        self.assertEqual(result.savings_pct, 80.0)

    def test_result_to_dict(self):
        result = BenchmarkResult(
            scenario_name="test",
            protocol="token_relay",
            message_count=10,
            total_time_sec=0.5,
            avg_latency_ms=50.0,
            latency_stats={"p50": 45.0},
            memory_before={},
            memory_after={},
            memory_delta_kb=0,
            throughput_per_sec=200,
            llm_tokens_traditional=500,
            llm_tokens_tokenrelay=100,
            tokens_saved=400,
            savings_pct=80.0,
            payload_bytes_traditional=10000,
            payload_bytes_tokenrelay=2000,
            payload_ratio_pct=20.0,
        )
        d = result.to_dict()
        self.assertEqual(d["scenario_name"], "test")
        self.assertIn("details", d)


class TestGenerateMessages(unittest.TestCase):
    """Test message generation for benchmarking."""

    def setUp(self):
        self.bench = BenchmarkV2(use_real_llm=False, min_messages=10, max_messages=100)

    def test_generate_traditional_text(self):
        messages = self.bench._generate_traditional_text(5, text_len=100)
        self.assertEqual(len(messages), 5)
        for msg in messages:
            self.assertIsInstance(msg, str)
            self.assertGreater(len(msg), 0)

    def test_generate_token_messages(self):
        messages = self.bench._generate_token_messages(5)
        self.assertEqual(len(messages), 5)
        for msg in messages:
            self.assertIsInstance(msg, dict)
            self.assertIn("tokens", msg)
            self.assertIn("payload", msg)
            self.assertIn("timestamp", msg)


if __name__ == "__main__":
    unittest.main(verbosity=2)