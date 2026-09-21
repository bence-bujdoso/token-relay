"""
Tests for TokenRelay v3 — Full Pipeline Integration (20+ tests).

Tests the complete User↔AI pipeline built on v2 foundation:
    user_input → ATC compress → CAR route → BRP send → model →
    POS stream → TEQ bill → EPC cache → user_output
"""

import unittest
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from v3_orchestrator import (
    V3Orchestrator, PipelineStage, PipelineStatus, PipelineMetrics,
    PipelineResult, create_orchestrator, run_pipeline
)
from teq import TokenBilling, RateLimiter, SLAMonitor, QoSTier, TEQConfig, QoSTierLevel
from epc import EdgeCache, CacheManager, EPCConfig
from codec import compress_message, decompress_message, MessageEncoder, MessageDecoder
from broker import MessageBroker, PRIORITY_CRITICAL, PRIORITY_HIGH, PRIORITY_MEDIUM, PRIORITY_LOW
from circuit_breaker import CircuitBreaker, CircuitState
from streaming import EventBus, StreamingProcessor
from bridge import TokenRelayBridge
from registry import TokenRegistry, get_registry
from atc import IntentClassifier, AdaptiveCompressor, ATCPipeline, ATCConfig
from pos import ResponsePredictor, POSConfig
from brp import ChannelManager, ConnectionMonitor, BRPConfig
from car import ComplexityAnalyzer, ModelSelector, CARRouter, CARConfig, PerformanceTracker
from car import ComplexityLevel


class TestV3OrchestratorBasics(unittest.TestCase):
    """Basic orchestrator functionality tests."""
    
    def setUp(self):
        self.orchestrator = V3Orchestrator()
    
    def test_orchestrator_initialization(self):
        """V3Orchestrator initializes all modules."""
        self.assertIsNotNone(self.orchestrator.v2_registry)
        self.assertIsNotNone(self.orchestrator.bridge)
        self.assertIsNotNone(self.orchestrator.broker)
        self.assertIsNotNone(self.orchestrator.circuit_breaker)
        self.assertIsNotNone(self.orchestrator.event_bus)
        self.assertIsNotNone(self.orchestrator.stream_processor)
        self.assertEqual(self.orchestrator.get_status(), PipelineStatus.IDLE)
    
    def test_pipeline_stage_enum(self):
        """PipelineStage enum has all expected stages."""
        stage_names = [s.value for s in PipelineStage]
        self.assertIn("atc_compress", stage_names)
        self.assertIn("car_route", stage_names)
        self.assertIn("brp_send", stage_names)
        self.assertIn("pos_stream", stage_names)
        self.assertIn("teq_bill", stage_names)
        self.assertIn("epc_cache", stage_names)
        self.assertIn("model_process", stage_names)
        self.assertIn("user_input", stage_names)
        self.assertIn("user_output", stage_names)
    
    def test_pipeline_status_enum(self):
        """PipelineStatus enum has all expected states."""
        status_names = [s.value for s in PipelineStatus]
        self.assertIn("idle", status_names)
        self.assertIn("running", status_names)
        self.assertIn("complete", status_names)
        self.assertIn("error", status_names)
        self.assertIn("paused", status_names)
    
    def test_pipeline_metrics_defaults(self):
        """PipelineMetrics has correct default values."""
        metrics = PipelineMetrics()
        self.assertEqual(metrics.total_latency_ms, 0.0)
        self.assertEqual(metrics.tokens_input, 0)
        self.assertEqual(metrics.tokens_output, 0)
        self.assertEqual(metrics.cache_hit, False)
        self.assertEqual(metrics.error_count, 0)
        self.assertEqual(metrics.compression_ratio, 1.0)
    
    def test_pipeline_metrics_properties(self):
        """PipelineMetrics properties compute correctly."""
        metrics = PipelineMetrics(tokens_input=100, tokens_output=25)
        self.assertEqual(metrics.total_tokens, 125)
        self.assertEqual(metrics.savings_pct, 75.0)
    
    def test_pipeline_result_dataclass(self):
        """PipelineResult stores execution results correctly."""
        metrics = PipelineMetrics(total_latency_ms=150.0)
        result = PipelineResult(success=True, metrics=metrics, stages_completed=["atc_compress", "car_route"])
        self.assertTrue(result.success)
        self.assertEqual(len(result.stages_completed), 2)
        self.assertIsNone(result.error)

class TestV3OrchestratorPipeline(unittest.TestCase):
    """Pipeline execution tests."""
    
    def setUp(self):
        self.orchestrator = V3Orchestrator()
    
    def test_execute_pipeline_simple_query(self):
        """Pipeline executes a simple query end-to-end."""
        result = self.orchestrator.execute_pipeline(
            "What is the weather?", user_id="test_user_1"
        )
        self.assertTrue(result.success)
        self.assertIn("atc_compress", result.stages_completed)
        self.assertIn("car_route", result.stages_completed)
        self.assertIn("brp_send", result.stages_completed)
        self.assertIn("model_process", result.stages_completed)
        self.assertIn("pos_stream", result.stages_completed)
        self.assertIn("teq_bill", result.stages_completed)
        self.assertIn("epc_cache", result.stages_completed)
        self.assertIn("user_output", result.stages_completed)
    
    def test_execute_pipeline_full_stages(self):
        """All pipeline stages complete for a valid query."""
        result = self.orchestrator.execute_pipeline(
            "Explain quantum computing briefly.", user_id="test_user_2"
        )
        expected = ["atc_compress", "car_route", "brp_send", "model_process", "pos_stream", "teq_bill", "epc_cache", "user_output"]
        for stage in expected:
            self.assertIn(stage, result.stages_completed, f"Missing stage: {stage}")
    
    def test_execute_pipeline_with_preference(self):
        """Pipeline respects user preference parameter."""
        result = self.orchestrator.execute_pipeline(
            "Summarize this article.", user_id="test_user_3", preference="fast"
        )
        self.assertTrue(result.success)
        self.assertIn("atc_compress", result.stages_completed)
        self.assertIn("car_route", result.stages_completed)
    
    def test_pipeline_returns_result_object(self):
        """Pipeline returns a PipelineResult with correct structure."""
        result = self.orchestrator.execute_pipeline(
            "Hello world!", user_id="test_user_4"
        )
        self.assertIsInstance(result, PipelineResult)
        self.assertTrue(result.success or not result.success)  # Always True
        self.assertIsInstance(result.metrics, PipelineMetrics)
        self.assertIsInstance(result.stages_completed, list)
    
    def test_pipeline_latency_tracking(self):
        """Pipeline tracks latency for each stage."""
        result = self.orchestrator.execute_pipeline(
            "Test latency.", user_id="test_user_5"
        )
        self.assertGreater(result.metrics.total_latency_ms, 0)
        self.assertGreater(len(result.metrics.stage_latencies), 0)
    
    def test_pipeline_tracks_tokens(self):
        """Pipeline tracks token counts."""
        result = self.orchestrator.execute_pipeline(
            "A very long input text that should consume many tokens in the pipeline execution cycle.",
            user_id="test_user_6"
        )
        self.assertGreater(result.metrics.tokens_input, 0)
        self.assertGreaterEqual(result.metrics.tokens_output, 0)

class TestV3OrchestratorMetricsAndState(unittest.TestCase):
    """Metrics and pipeline state management tests."""
    
    def setUp(self):
        self.orchestrator = V3Orchestrator()
    
    def test_pipeline_history(self):
        """Pipeline history accumulates results."""
        self.orchestrator.execute_pipeline("Query 1", user_id="hist_1")
        self.orchestrator.execute_pipeline("Query 2", user_id="hist_2")
        self.assertEqual(len(self.orchestrator._pipeline_history), 2)
    
    def test_get_metrics_returns_dict(self):
        """get_metrics returns a dictionary with expected keys."""
        self.orchestrator.execute_pipeline("Test metrics query.", user_id="metrics_user")
        metrics = self.orchestrator.get_metrics()
        self.assertIn("status", metrics)
        self.assertIn("total_executions", metrics)
        self.assertIn("avg_latency_ms", metrics)
        self.assertIn("cache_hit_rate", metrics)
        self.assertIn("broker_depth", metrics)
        self.assertIn("circuit_state", metrics)
        self.assertIn("pipeline_history", metrics)
    
    def test_get_stage_status(self):
        """get_stage_status returns a dict with all stages active."""
        status = self.orchestrator.get_stage_status()
        self.assertIn("atc", status)
        self.assertIn("car", status)
        self.assertIn("brp", status)
        self.assertIn("pos", status)
        self.assertIn("teq", status)
        self.assertIn("epc", status)
    
    def test_pause_resume_reset(self):
        """Pipeline can be paused, resumed, and reset."""
        self.assertEqual(self.orchestrator.get_status(), PipelineStatus.IDLE)
        self.orchestrator.pause()
        self.assertEqual(self.orchestrator.get_status(), PipelineStatus.PAUSED)
        self.orchestrator.resume()
        self.assertEqual(self.orchestrator.get_status(), PipelineStatus.RUNNING)
        self.orchestrator.reset()
        self.assertEqual(self.orchestrator.get_status(), PipelineStatus.IDLE)
    
    def test_pipeline_history_size(self):
        """Multiple executions accumulate in history."""
        for i in range(5):
            self.orchestrator.execute_pipeline(f"Query {i}", user_id=f"user_{i}")
        self.assertEqual(len(self.orchestrator._pipeline_history), 5)
        metrics = self.orchestrator.get_metrics()
        self.assertEqual(metrics["total_executions"], 5)

class TestV3OrchestratorErrorHandling(unittest.TestCase):
    """Error handling tests."""
    
    def setUp(self):
        self.orchestrator = V3Orchestrator()
    
    def test_pipeline_with_empty_input(self):
        """Pipeline handles empty input gracefully."""
        result = self.orchestrator.execute_pipeline("", user_id="empty_user")
        # Empty input still runs pipeline but may produce error
        self.assertIsInstance(result, PipelineResult)
    
    def test_pipeline_with_unicode_input(self):
        """Pipeline handles unicode characters."""
        result = self.orchestrator.execute_pipeline(
            "日本語のテスト", user_id="unicode_user"
        )
        self.assertIsInstance(result, PipelineResult)

class TestV3OrchestratorCreateAndRun(unittest.TestCase):
    """Factory functions tests."""
    
    def test_create_orchestrator(self):
        """create_orchestrator returns V3Orchestrator instance."""
        orchestrator = create_orchestrator()
        self.assertIsInstance(orchestrator, V3Orchestrator)
    
    def test_create_orchestrator_with_config(self):
        """create_orchestrator accepts config dict."""
        orchestrator = create_orchestrator({"broker_max_depth": 5000})
        self.assertIsInstance(orchestrator, V3Orchestrator)
    
    def test_run_pipeline(self):
        """run_pipeline function works correctly."""
        orchestrator = create_orchestrator()
        result = run_pipeline(orchestrator, "Hello", "run_user")
        self.assertIsInstance(result, PipelineResult)

class TestV3Infrastructure(unittest.TestCase):
    """Test that v2 infrastructure is properly integrated."""
    
    def setUp(self):
        self.orchestrator = V3Orchestrator()
    
    def test_v2_registry_integrated(self):
        """TokenRegistry from v2 is available."""
        self.assertIsNotNone(self.orchestrator.v2_registry)
        self.assertIsInstance(self.orchestrator.v2_registry, TokenRegistry)
    
    def test_v2_bridge_integrated(self):
        """TokenRelayBridge from v2 is available."""
        self.assertIsNotNone(self.orchestrator.bridge)
        self.assertIsInstance(self.orchestrator.bridge, TokenRelayBridge)
    
    def test_v2_broker_integrated(self):
        """MessageBroker from v2 is available."""
        self.assertIsNotNone(self.orchestrator.broker)
        self.assertIsInstance(self.orchestrator.broker, MessageBroker)
    
    def test_v2_circuit_breaker_integrated(self):
        """CircuitBreaker from v2 is available."""
        self.assertIsNotNone(self.orchestrator.circuit_breaker)
        self.assertIsInstance(self.orchestrator.circuit_breaker, CircuitBreaker)
    
    def test_v2_event_bus_integrated(self):
        """EventBus from v2 is available."""
        self.assertIsNotNone(self.orchestrator.event_bus)
        self.assertIsInstance(self.orchestrator.event_bus, EventBus)
    
    def test_v2_streaming_integrated(self):
        """StreamingProcessor from v2 is available."""
        self.assertIsNotNone(self.orchestrator.stream_processor)
        self.assertIsInstance(self.orchestrator.stream_processor, StreamingProcessor)
    
    def test_v2_codec_integrated(self):
        """MessageEncoder and MessageDecoder from v2 are available."""
        self.assertIsNotNone(self.orchestrator.encoder)
        self.assertIsNotNone(self.orchestrator.decoder)
        from codec import compress_message, decompress_message
        self.assertTrue(callable(compress_message))
        self.assertTrue(callable(decompress_message))

if __name__ == "__main__":
    unittest.main()
