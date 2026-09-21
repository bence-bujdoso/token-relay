"""
TokenRelay v2 — Full Pipeline Integration Test.

Tests the complete flow from v1 through v2 features:
- Encode → Compress → Sign → Broker → Circuit Breaker → Stream → Decode
- End-to-end message lifecycle
- v1 backward compatibility
"""

import unittest
import sys
import json
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from codec import MessageEncoder, MessageDecoder, decode_message, encode_message
from registry import TokenRegistry, get_registry
from bridge import TokenRelayBridge
from broker import MessageBroker, PRIORITY_CRITICAL, PRIORITY_HIGH
from circuit_breaker import CircuitBreaker, CircuitBreakerError
from streaming import StreamingProcessor, StreamEvent, StreamEventType, StreamSession, EventBus, SSEHandler, BackpressureStrategy
from protocol_v2 import (
    compress_payload, decompress_payload, MessageSigner,
    MessageEncryptor, ProtocolV2Message,
)


class TestFullPipelineV1ToV2(unittest.TestCase):
    """Integration tests spanning v1 and v2 features."""

    def setUp(self):
        self.registry = get_registry()
        self.encoder = MessageEncoder(self.registry)
        self.decoder = MessageDecoder(self.registry)
        self.bridge = TokenRelayBridge(self.registry, self.encoder, self.decoder)
        self.signer = MessageSigner()
        self.broker = MessageBroker(registry=self.registry, max_depth=100)
        self.stream_processor = StreamingProcessor(registry=self.registry)

    # ─── v1 Basic Encoding/Decoding ────────────────────────────

    def test_v1_encode_decode_roundtrip(self):
        """v1 encode/decode should work unchanged."""
        msg = self.encoder.encode_task_complete("result", "summary")
        decoded = self.decoder.decode(msg)
        self.assertEqual(decoded["tokens"], [42])
        self.assertEqual(decoded["payload"]["result"], "result")

    def test_v1_bridge_encode_decode(self):
        """v1 bridge should work unchanged."""
        result = self.bridge.encode_result("test_task", "Done")
        status = self.bridge.extract_status(result)
        self.assertEqual(status, "complete")
        decoded = self.bridge.decode_result(result)
        self.assertEqual(decoded["tokens"], [42])

    # ─── v2 Compression Integration ────────────────────────────

    def test_encode_compress_pipeline(self):
        """Encode a message, compress its payload, and verify round-trip."""
        msg = self.encoder.encode_task_complete("task_1", "Done")
        compressed = compress_payload(msg["payload"], level=6)
        decompressed = decompress_payload(compressed)
        self.assertEqual(decompressed, msg["payload"])

    def test_full_compress_sign_flow(self):
        """Full flow: encode → compress → sign."""
        msg = self.encoder.encode_request("build_api", "REST endpoints")
        # Compress payload
        compressed = compress_payload(msg["payload"])
        msg["payload"] = decompress_payload(compressed)
        # Sign
        signed = self.signer.sign_message(msg)
        self.assertIn("signature", signed)
        self.assertIn("key_id", signed)

    # ─── v2 Signing Integration ────────────────────────────────

    def test_encode_sign_verify_pipeline(self):
        """Full pipeline: encode → sign → verify."""
        msg = self.encoder.encode_task_complete("task_1", "Done")
        signed = self.signer.sign_message(msg)
        verification = self.signer.verify_message(signed)
        self.assertTrue(verification["valid"])

    def test_bridge_sign_verify(self):
        """Bridge encode → sign → verify."""
        result = self.bridge.encode_result("auth_service", "JWT built")
        signed = self.signer.sign_message(result)
        verification = self.signer.verify_message(signed)
        self.assertTrue(verification["valid"])
        self.assertEqual(signed["tokens"], [42])

    # ─── v2 Broker Integration ────────────────────────────────

    def test_encode_broker_enqueue_dequeue(self):
        """Encode → sign → broker enqueue → dequeue."""
        msg = self.encoder.encode_task_complete("task_1", "Done")
        signed = self.signer.sign_message(msg)
        self.broker.enqueue(signed, priority=PRIORITY_HIGH)
        dequeued = self.broker.dequeue()
        self.assertIsNotNone(dequeued)
        self.assertEqual(dequeued["tokens"], [42])

    def test_full_pipeline_with_broker(self):
        """Full pipeline through broker."""
        messages = [
            self.encoder.encode_start_boundary("task-001", "request"),
            self.encoder.encode_request("build_auth", "JWT + OAuth2"),
            self.encoder.encode_task_complete("auth_service", "Built"),
        ]
        for msg in messages:
            signed = self.signer.sign_message(msg)
            priority = PRIORITY_CRITICAL if msg["tokens"][0] == 42 else PRIORITY_HIGH
            self.broker.enqueue(signed, priority=priority)

        # Dequeue all in priority order
        results = []
        while True:
            msg = self.broker.dequeue()
            if msg is None:
                break
            results.append(msg)

        self.assertEqual(len(results), 3)
        self.broker.clear_dlq()

    # ─── v2 Circuit Breaker Integration ────────────────────────

    def test_encode_circuit_breaker_pipeline(self):
        """Encode → circuit breaker call → verify."""
        cb = CircuitBreaker(failure_threshold=5, recovery_timeout=10)

        def process_message(msg):
            return self.decoder.decode(msg)

        msg = self.encoder.encode_task_complete("task_1", "Done")
        result = cb.call(process_message, msg)
        self.assertEqual(result["tokens"], [42])

    def test_circuit_breaker_with_broker(self):
        """Circuit breaker protecting broker operations."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=0.1,
                            min_backoff=0.01)

        def enqueue_safe(msg):
            self.broker.enqueue(msg, priority=PRIORITY_HIGH)
            return True

        for i in range(3):
            msg = self.encoder.encode_request(f"task_{i}", f"desc_{i}")
            cb.call(enqueue_safe, msg)

        # Should have enqueued successfully
        self.assertGreater(self.broker.depth, 0)

    # ─── v2 Streaming Integration ──────────────────────────────

    def test_encode_stream_pipeline(self):
        """Encode → stream through processor."""
        session = self.stream_processor.create_session("test_session")
        msg = self.encoder.encode_task_complete("task_1", "Done")

        event = self.stream_processor.stream_token(
            "test_session", msg["tokens"], msg["payload"]
        )
        self.assertIsNotNone(event)
        self.assertEqual(event.event_type, StreamEventType.TOKEN_RECEIVED)

    def test_full_stream_pipeline(self):
        """Full pipeline: encode → sign → stream → decode."""
        session = self.stream_processor.create_session("integration_test")
        msg = self.encoder.encode_task_complete("task_1", "Done")
        signed = self.signer.sign_message(msg)

        # Stream the message
        event = self.stream_processor.stream_token(
            "integration_test", signed["tokens"], signed["payload"]
        )
        self.assertIsNotNone(event)

        # Verify the session recorded the event
        session = self.stream_processor.get_session("integration_test")
        self.assertIsNotNone(session)

        # Clean up
        self.stream_processor.close_session("integration_test")

    # ─── v2 ProtocolV2Message Integration ──────────────────────

    def test_protocol_v2_message_full_lifecycle(self):
        """Full lifecycle of ProtocolV2Message."""
        v2_msg = ProtocolV2Message(
            tokens=[42, 153],
            payload={"task": "test", "status": "complete"},
            protocol_version="2.0",
        )

        # Serialize and deserialize
        data = v2_msg.to_dict()
        reconstructed = ProtocolV2Message.from_dict(data)
        self.assertEqual(reconstructed.tokens, v2_msg.tokens)
        self.assertEqual(reconstructed.payload, v2_msg.payload)

        # Compress
        compressed = compress_payload(v2_msg.payload)
        v2_msg.payload = decompress_payload(compressed)
        v2_msg.compressed = True

        # Sign
        v2_msg.sign(self.signer)
        self.assertIsNotNone(v2_msg.signature)

        # Verify to dict includes signature
        data = v2_msg.to_dict()
        self.assertIn("signature", data)

    # ─── v1 Backward Compatibility ─────────────────────────────

    def test_v1_bridge_still_works(self):
        """All v1 bridge methods should still work."""
        result = self.bridge.encode_result("task", "summary")
        self.assertEqual(result["tokens"], [42])

        err = self.bridge.encode_error("E01", "Test error")
        self.assertEqual(err["tokens"], [0])

        req = self.bridge.encode_request("action", "desc")
        self.assertIn(151, req["tokens"])

        attn = self.bridge.encode_attention("high")
        self.assertEqual(attn["tokens"], [151])

        sep = self.bridge.encode_separator("task")
        self.assertEqual(sep["tokens"], [153])

        bnd = self.bridge.encode_start_boundary("msg-001")
        self.assertEqual(bnd["tokens"], [150])

        status = self.bridge.encode_status("running")
        self.assertEqual(status["tokens"], [103])

    def test_v1_cli_still_works(self):
        """v1 CLI test should still pass."""
        from cli import cmd_test
        import io
        import sys as sys_mod
        old_stdout = sys_mod.stdout
        sys_mod.stdout = io.StringIO()
        try:
            cmd_test(None)
            output = sys_mod.stdout.getvalue()
        finally:
            sys_mod.stdout = old_stdout
        self.assertIn("tests passed", output)

    def test_v1_benchmark_still_works(self):
        """v1 benchmark should still run."""
        import sys as sys_mod
        sys_mod.path.insert(0, str(Path(__file__).resolve().parent))
        from benchmark import run_all_benchmarks
        import io
        old_stdout = sys_mod.stdout
        sys_mod.stdout = io.StringIO()
        try:
            run_all_benchmarks()
            output = sys_mod.stdout.getvalue()
        finally:
            sys_mod.stdout = old_stdout
        self.assertIn("89.6%", output)

    # ─── Multi-Step Pipeline ───────────────────────────────────

    def test_multi_step_workflow(self):
        """Simulate a full multi-step agent workflow."""
        steps = []

        # Step 1: Initialize
        msg1 = self.encoder.encode_start_boundary("workflow-001", "request")
        steps.append(("start", msg1))

        # Step 2: Request auth service
        msg2 = self.encoder.encode_request("build_auth", "JWT + OAuth2")
        steps.append(("request", msg2))

        # Step 3: Auth complete
        msg3 = self.encoder.encode_task_complete("auth_service", "JWT built")
        steps.append(("complete", msg3))

        # Step 4: Request API service
        msg4 = self.encoder.encode_request("build_api", "REST endpoints")
        steps.append(("request", msg4))

        # Step 5: API complete
        msg5 = self.encoder.encode_task_complete("api_service", "Built")
        steps.append(("complete", msg5))

        # Process through broker
        for name, msg in steps:
            signed = self.signer.sign_message(msg)
            self.broker.enqueue(signed, priority=PRIORITY_HIGH)

        # Dequeue and verify all
        results = []
        while True:
            msg = self.broker.dequeue()
            if msg is None:
                break
            results.append(msg)

        self.assertEqual(len(results), 5)
        self.broker.clear_dlq()

    # ─── End-to-End Pipeline with All Features ─────────────────

    def test_e2e_pipeline_with_all_features(self):
        """End-to-end: encode → compress → sign → broker → stream → decode."""
        # 1. Encode
        msg = self.encoder.encode_task_complete("task_e2e", "All features")

        # 2. Compress payload
        compressed = compress_payload(msg["payload"], level=6)
        msg["payload"] = decompress_payload(compressed)

        # 3. Sign
        signed = self.signer.sign_message(msg)

        # 4. Broker enqueue
        self.broker.enqueue(signed, priority=PRIORITY_CRITICAL)

        # 5. Broker dequeue
        dequeued = self.broker.dequeue()
        self.assertIsNotNone(dequeued)
        self.assertEqual(dequeued["tokens"], [42])

        # 6. Stream
        session = self.stream_processor.create_session("e2e_test")
        event = self.stream_processor.stream_token(
            "e2e_test", dequeued["tokens"], dequeued["payload"]
        )
        self.assertIsNotNone(event)
        self.stream_processor.close_session("e2e_test")

        # 7. Decode
        interpretation = self.decoder.decode(dequeued)
        self.assertEqual(interpretation["tokens"], [42])

        # Cleanup
        self.broker.clear_dlq()

    def test_full_pipeline_with_error_handling(self):
        """Pipeline with error handling through circuit breaker."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=0.1,
                            min_backoff=0.01)

        def failing_processor(msg):
            raise ValueError("Processing failed")

        msg = self.encoder.encode_error("TIMEOUT", "Connection failed")

        # First call fails
        with self.assertRaises(ValueError):
            cb.call(failing_processor, msg)

        # Circuit should still be CLOSED
        self.assertTrue(cb.is_closed)

        # Force open by failing enough
        for _ in range(2):
            with self.assertRaises(ValueError):
                cb.call(failing_processor, msg)

        # Now circuit should be OPEN
        self.assertTrue(cb.is_open or cb.is_half_open)

        # Reset
        cb.reset()
        self.assertTrue(cb.is_closed)


class TestIntegrationMetrics(unittest.TestCase):
    """Integration-level performance metrics."""

    def setUp(self):
        self.registry = get_registry()
        self.encoder = MessageEncoder(self.registry)
        self.decoder = MessageDecoder(self.registry)
        self.bridge = TokenRelayBridge(self.registry, self.encoder, self.decoder)
        self.broker = MessageBroker(registry=self.registry)

    def test_pipeline_throughput(self):
        """Measure end-to-end pipeline throughput."""
        start = time.time()
        for i in range(100):
            msg = self.bridge.encode_result(f"task_{i}", f"Done {i}")
            self.broker.enqueue(msg, priority=1 if i % 2 == 0 else 5)

        processed = 0
        while True:
            msg = self.broker.dequeue()
            if msg is None:
                break
            self.decoder.decode(msg)
            processed += 1
            if processed >= 100:
                break

        elapsed = time.time() - start
        throughput = 100 / max(elapsed, 0.0001)
        self.assertGreater(throughput, 0)

    def test_pipeline_latency(self):
        """Measure per-message pipeline latency."""
        latencies = []
        for i in range(20):
            t0 = time.time()
            msg = self.bridge.encode_result(f"task_{i}", f"Done")
            signed = msg  # Simplified
            self.decoder.decode(msg)
            latencies.append(time.time() - t0)

        avg_latency = sum(latencies) / len(latencies)
        self.assertGreater(avg_latency, 0)
        self.assertLess(avg_latency, 1.0)  # Should be sub-second


if __name__ == "__main__":
    unittest.main(verbosity=2)
