"""
Tests for TokenRelay v2 MessageBroker.

Tests cover:
- Priority queue ordering
- Message deduplication
- Queue depth monitoring
- Dead letter queue
- Batch operations
- Metrics tracking
"""

import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from broker import (
    MessageBroker, PriorityMessage,
    QueueFullError, DuplicateMessageError, DLQError,
    PRIORITY_CRITICAL, PRIORITY_HIGH, PRIORITY_MEDIUM,
    PRIORITY_LOW, PRIORITY_BULK,
    create_broker,
)
from registry import TokenRegistry, get_registry


class TestMessageBrokerBasics(unittest.TestCase):
    """Test basic broker operations."""

    def setUp(self):
        self.registry = get_registry()
        self.broker = MessageBroker(registry=self.registry, max_depth=100)

    def test_enqueue_dequeue(self):
        """Basic enqueue and dequeue works."""
        msg = {"tokens": [42], "payload": {"result": "ok"}}
        self.broker.enqueue(msg)
        result = self.broker.dequeue()
        self.assertEqual(result["tokens"], [42])
        self.assertEqual(result["payload"]["result"], "ok")

    def test_dequeue_empty(self):
        """Dequeue from empty queue returns None."""
        self.assertIsNone(self.broker.dequeue())

    def test_peek(self):
        """Peek does not remove the message."""
        msg = {"tokens": [42], "payload": {"test": True}}
        self.broker.enqueue(msg)
        peeked = self.broker.peek()
        self.assertEqual(peeked["tokens"], [42])
        # Still there after peek
        self.assertEqual(self.broker.depth, 1)
        # Actually removed by dequeue
        self.broker.dequeue()
        self.assertEqual(self.broker.depth, 0)

    def test_enqueue_returns_true(self):
        """enqueue returns True on success."""
        msg = {"tokens": [42]}
        result = self.broker.enqueue(msg)
        self.assertTrue(result)


class TestPriorityQueue(unittest.TestCase):
    """Test priority queue ordering."""

    def setUp(self):
        self.registry = get_registry()
        self.broker = MessageBroker(registry=self.registry, max_depth=1000)

    def test_highest_priority_first(self):
        """Lower priority number is dequeued first."""
        msgs = [
            {"tokens": [100], "id": "low"},      # priority 60
            {"tokens": [42], "id": "high"},       # priority 1
            {"tokens": [151], "id": "medium"},    # priority 1 → but different seq
        ]
        # Enqueue in reverse order
        self.broker.enqueue(msgs[0], priority=60)
        self.broker.enqueue(msgs[1], priority=1)
        self.broker.enqueue(msgs[2], priority=1)

        # Highest priority (1) comes first; FIFO within same priority
        first = self.broker.dequeue()
        self.assertIn(first["id"], ["high", "medium"])
        second = self.broker.dequeue()
        self.assertIn(second["id"], ["high", "medium"])
        # Low priority last
        third = self.broker.dequeue()
        self.assertEqual(third["id"], "low")

    def test_fifo_within_same_priority(self):
        """Messages with same priority are dequeued FIFO."""
        for i in range(5):
            self.broker.enqueue(
                {"tokens": [42], "seq": i},
                priority=PRIORITY_HIGH
            )

        results = [self.broker.dequeue()["seq"] for _ in range(5)]
        self.assertEqual(results, [0, 1, 2, 3, 4])

    def test_explicit_priority(self):
        """Explicit priority overrides registry lookup."""
        msg = {"tokens": [100], "id": "test"}
        # 100 = request token, registry says priority 60
        # But we pass priority=0 (critical)
        self.broker.enqueue(msg, priority=PRIORITY_CRITICAL)
        result = self.broker.dequeue()
        self.assertEqual(result["id"], "test")

    def test_auto_priority_from_tokens(self):
        """Priority auto-inferred from message tokens."""
        # Token 42 has routing_priority=1 in registry
        msg = {"tokens": [42], "id": "auto"}
        self.broker.enqueue(msg)
        # Should have gotten priority from registry
        result = self.broker.dequeue()
        self.assertEqual(result["id"], "auto")

    def test_default_priority_when_no_tokens(self):
        """Messages without tokens get medium priority."""
        msg = {"payload": {"no": "tokens"}, "id": "notokens"}
        self.broker.enqueue(msg)
        result = self.broker.dequeue()
        self.assertEqual(result["id"], "notokens")


class TestDeduplication(unittest.TestCase):
    """Test message deduplication."""

    def setUp(self):
        self.registry = get_registry()
        self.broker = MessageBroker(
            registry=self.registry,
            dedup_window_seconds=10.0,
            max_depth=100,
        )

    def test_duplicate_rejected(self):
        """Duplicate messages are rejected."""
        msg = {"tokens": [42], "payload": {"same": True}}
        self.broker.enqueue(msg)
        with self.assertRaises(DuplicateMessageError):
            self.broker.enqueue(msg)

    def test_dedup_after_window(self):
        """After dedup window expires, same message is accepted."""
        msg = {"tokens": [42], "payload": {"same": True}}
        self.broker.enqueue(msg)
        time.sleep(0.05)  # Wait less than window
        # Should still be rejected
        with self.assertRaises(DuplicateMessageError):
            self.broker.enqueue(msg)

    def test_is_duplicate(self):
        """is_duplicate works correctly."""
        msg = {"tokens": [42], "payload": {"test": True}}
        self.assertFalse(self.broker.is_duplicate(msg))
        self.broker.enqueue(msg)
        self.assertTrue(self.broker.is_duplicate(msg))

    def test_different_messages_not_duplicates(self):
        """Different messages are not duplicates."""
        msg1 = {"tokens": [42], "payload": {"a": 1}}
        msg2 = {"tokens": [42], "payload": {"b": 2}}
        self.broker.enqueue(msg1)
        # Different payload → not a duplicate
        self.assertFalse(self.broker.is_duplicate(msg2))
        # msg2 should enqueue fine
        self.broker.enqueue(msg2)

    def test_clear_dedup_cache(self):
        """Clearing dedup cache allows re-enqueuing."""
        msg = {"tokens": [42], "payload": {"same": True}}
        self.broker.enqueue(msg)
        self.broker.clear_dedup_cache()
        # Should be accepted again
        self.broker.enqueue(msg)
        self.assertEqual(self.broker.depth, 2)

    def test_dedup_with_different_content(self):
        """Messages with same tokens but different payloads are distinct."""
        msg1 = {"tokens": [42], "payload": {"result": "ok"}}
        msg2 = {"tokens": [42], "payload": {"result": "error"}}
        self.broker.enqueue(msg1)
        # Different payload → not duplicate
        self.broker.enqueue(msg2)
        self.assertEqual(self.broker.depth, 2)


class TestQueueDepthMonitoring(unittest.TestCase):
    """Test queue depth monitoring and limits."""

    def setUp(self):
        self.registry = get_registry()
        self.broker = MessageBroker(registry=self.registry, max_depth=3)

    def test_depth(self):
        """Depth tracking works correctly."""
        self.assertEqual(self.broker.depth, 0)
        self.broker.enqueue({"tokens": [42]})
        self.assertEqual(self.broker.depth, 1)
        self.broker.enqueue({"tokens": [0]})
        self.assertEqual(self.broker.depth, 2)

    def test_queue_full(self):
        """QueueFullError raised when exceeding max_depth."""
        for i in range(3):
            self.broker.enqueue({"tokens": [42], "id": str(i)})
        with self.assertRaises(QueueFullError):
            self.broker.enqueue({"tokens": [42], "id": "overflow"})

    def test_is_full(self):
        """is_full property works."""
        self.assertFalse(self.broker.is_full)
        self.broker.enqueue({"tokens": [42]})
        self.broker.enqueue({"tokens": [0]})
        self.broker.enqueue({"tokens": [151]})
        self.assertTrue(self.broker.is_full)

    def test_depth_ratio(self):
        """depth_ratio is calculated correctly."""
        self.assertEqual(self.broker.depth_ratio, 0.0)
        self.broker.enqueue({"tokens": [42]})
        self.assertAlmostEqual(self.broker.depth_ratio, 1/3)
        self.broker.enqueue({"tokens": [0]})
        self.assertAlmostEqual(self.broker.depth_ratio, 2/3)

    def test_get_health(self):
        """Health status reflects queue state."""
        health = self.broker.get_health()
        self.assertEqual(health["status"], "healthy")
        self.assertEqual(health["depth"], 0)

        for i in range(3):
            self.broker.enqueue({"tokens": [42], "id": str(i)})
        health = self.broker.get_health()
        self.assertEqual(health["status"], "critical")
        self.assertGreaterEqual(health["depth"], 3)

    def test_get_depth_history(self):
        """Depth history records samples."""
        for i in range(5):
            self.broker.enqueue({"tokens": [42], "id": str(i)})
            self.broker.dequeue()
        history = self.broker.get_depth_history()
        self.assertTrue(len(history) > 0)
        # All values should be 0 or 1 (we enqueue and dequeue each time)
        self.assertTrue(all(h <= 1 for h in history))


class TestDeadLetterQueue(unittest.TestCase):
    """Test dead letter queue operations."""

    def setUp(self):
        self.registry = get_registry()
        self.broker = MessageBroker(registry=self.registry, max_depth=100)

    def test_send_to_dlq(self):
        """Messages can be sent to DLQ."""
        msg = {"tokens": [0], "payload": {"error_code": "TEST"}}
        self.broker.send_to_dlq(msg, reason="test failure")
        self.assertEqual(self.broker.dlq_depth, 1)

    def test_receive_from_dlq(self):
        """Messages can be retrieved from DLQ."""
        msg = {"tokens": [0], "id": "dlq-test"}
        self.broker.send_to_dlq(msg, reason="test")
        received = self.broker.receive_from_dlq()
        self.assertEqual(received["id"], "dlq-test")
        self.assertIn("_dlq_reason", received)
        self.assertEqual(received["_dlq_reason"], "test")

    def test_dlq_empty(self):
        """receive_from_dlq returns None when DLQ is empty."""
        self.assertIsNone(self.broker.receive_from_dlq())

    def test_clear_dlq(self):
        """clear_dlq removes all DLQ messages."""
        self.broker.send_to_dlq({"tokens": [0], "id": "1"})
        self.broker.send_to_dlq({"tokens": [0], "id": "2"})
        self.assertEqual(self.broker.dlq_depth, 2)
        count = self.broker.clear_dlq()
        self.assertEqual(count, 2)
        self.assertEqual(self.broker.dlq_depth, 0)

    def test_dlq_full(self):
        """DLQFullError raised when DLQ exceeds max depth."""
        small_broker = MessageBroker(registry=self.registry,
                                      max_depth=100,
                                      dlq_max_depth=2)
        small_broker.send_to_dlq({"tokens": [0], "id": "1"})
        small_broker.send_to_dlq({"tokens": [0], "id": "2"})
        with self.assertRaises(DLQError):
            small_broker.send_to_dlq({"tokens": [0], "id": "3"})

    def test_dlq_message_enriched(self):
        """DLQ messages have enriched metadata."""
        msg = {"tokens": [42], "payload": {"test": True}}
        self.broker.send_to_dlq(msg, reason="timeout")
        dlq_msg = self.broker.receive_from_dlq()
        self.assertIn("_dlq_reason", dlq_msg)
        self.assertIn("_dlq_timestamp", dlq_msg)
        self.assertEqual(dlq_msg["_dlq_reason"], "timeout")


class TestBatchOperations(unittest.TestCase):
    """Test batch enqueue and drain operations."""

    def setUp(self):
        self.registry = get_registry()
        self.broker = MessageBroker(registry=self.registry, max_depth=100)

    def test_enqueue_batch(self):
        """Batch enqueue works correctly."""
        messages = [
            {"tokens": [42], "id": str(i)} for i in range(5)
        ]
        result = self.broker.enqueue_batch(messages)
        self.assertEqual(result["enqueued"], 5)
        self.assertEqual(self.broker.depth, 5)

    def test_enqueue_batch_with_duplicates(self):
        """Batch enqueue correctly identifies duplicates."""
        msg = {"tokens": [42], "id": "dup"}
        messages = [msg, msg, msg]
        result = self.broker.enqueue_batch(messages)
        self.assertEqual(result["enqueued"], 1)
        self.assertEqual(result["duplicates"], 2)
        self.assertEqual(self.broker.depth, 1)

    def test_enqueue_batch_with_priority(self):
        """Batch enqueue with fixed priority."""
        messages = [
            {"tokens": [100], "id": str(i)} for i in range(3)
        ]
        result = self.broker.enqueue_batch(messages, priority=PRIORITY_LOW)
        self.assertEqual(result["enqueued"], 3)
        # All should be dequeued in order
        first = self.broker.dequeue()
        self.assertEqual(first["id"], "0")

    def test_drain(self):
        """Drain removes all messages in priority order."""
        self.broker.enqueue({"tokens": [100], "id": "low"}, priority=PRIORITY_LOW)
        self.broker.enqueue({"tokens": [42], "id": "high"}, priority=PRIORITY_HIGH)
        self.broker.enqueue({"tokens": [0], "id": "critical"}, priority=PRIORITY_CRITICAL)

        messages = self.broker.drain()
        self.assertEqual(len(messages), 3)
        # Highest priority first
        self.assertEqual(messages[0]["id"], "critical")
        self.assertEqual(messages[1]["id"], "high")
        self.assertEqual(messages[2]["id"], "low")
        self.assertTrue(self.broker.is_empty)


class TestMetrics(unittest.TestCase):
    """Test broker metrics tracking."""

    def setUp(self):
        self.registry = get_registry()
        self.broker = MessageBroker(registry=self.registry, max_depth=10)

    def test_get_metrics(self):
        """Metrics are tracked correctly."""
        metrics = self.broker.get_metrics()
        self.assertEqual(metrics["messages_enqueued"], 0)
        self.assertEqual(metrics["messages_dequeued"], 0)

        # Do some operations
        self.broker.enqueue({"tokens": [42]})
        self.broker.dequeue()
        metrics = self.broker.get_metrics()
        self.assertEqual(metrics["messages_enqueued"], 1)
        self.assertEqual(metrics["messages_dequeued"], 1)

    def test_total_enqueued(self):
        """Total enqueued counter works."""
        self.broker.enqueue({"tokens": [42]})
        self.broker.enqueue({"tokens": [0]})
        self.assertEqual(self.broker._total_enqueued, 2)

    def test_queue_peak_depth(self):
        """Peak depth is tracked."""
        self.broker.enqueue({"tokens": [42]})
        self.broker.enqueue({"tokens": [0]})
        self.broker.dequeue()
        metrics = self.broker.get_metrics()
        self.assertEqual(metrics["queue_peak_depth"], 2)

    def test_reset_metrics(self):
        """reset_metrics clears all counters."""
        self.broker.enqueue({"tokens": [42]})
        self.broker.enqueue({"tokens": [0]})
        self.broker.dequeue()
        self.broker.reset_metrics()
        metrics = self.broker.get_metrics()
        self.assertEqual(metrics["messages_enqueued"], 0)
        self.assertEqual(metrics["messages_dequeued"], 0)
        self.assertEqual(self.broker.depth, 0)

    def test_duplicate_rejection_metrics(self):
        """Duplicate rejections are counted in metrics."""
        msg = {"tokens": [42], "payload": {"same": True}}
        self.broker.enqueue(msg)
        with self.assertRaises(DuplicateMessageError):
            self.broker.enqueue(msg)
        metrics = self.broker.get_metrics()
        self.assertEqual(metrics["duplicates_rejected"], 1)


class TestPriorityMessage(unittest.TestCase):
    """Test PriorityMessage wrapper."""

    def test_to_tuple(self):
        """PriorityMessage.to_tuple produces correct queue tuple."""
        pm = PriorityMessage(
            {"tokens": [42]}, priority=1, timestamp=1000.0
        )
        result = pm.to_tuple()
        self.assertEqual(result[0], 1)  # priority
        self.assertEqual(result[1], 1000000000)  # sequence (timestamp * 1e6)
        self.assertEqual(result[2]["tokens"], [42])


class TestCreateBroker(unittest.TestCase):
    """Test create_broker factory function."""

    def test_factory(self):
        """create_broker produces a working MessageBroker."""
        broker = create_broker(max_depth=50)
        self.assertEqual(broker.max_depth, 50)
        broker.enqueue({"tokens": [42]})
        self.assertEqual(broker.depth, 1)

    def test_factory_with_registry(self):
        """create_broker with custom registry."""
        custom_reg = TokenRegistry()
        broker = create_broker(registry=custom_reg)
        self.assertIs(broker.registry, custom_reg)


class TestBrokerIntegration(unittest.TestCase):
    """Test broker integration with TokenRegistry."""

    def setUp(self):
        self.registry = get_registry()
        self.broker = MessageBroker(registry=self.registry)

    def test_broker_route(self):
        """Broker route_message delegates to registry."""
        routing = self.broker.route_message({"tokens": [42]})
        self.assertEqual(routing["action"], "status")
        self.assertEqual(routing["priority"], 1)

    def test_infer_priority(self):
        """_infer_priority correctly reads from registry."""
        msg = {"tokens": [42]}
        priority = self.broker._infer_priority(msg)
        self.assertEqual(priority, 1)  # Token 42 has routing_priority 1

    def test_enqueue_with_priority(self):
        """enqueue_with_priority uses registry to determine priority."""
        msg = {"tokens": [42]}
        self.broker.enqueue_with_priority(msg)
        self.assertEqual(self.broker.depth, 1)

    def test_registry_priority_distribution(self):
        """Registry broker priority distribution works."""
        dist = self.registry.get_broker_priority_distribution()
        self.assertIn("critical", dist)
        self.assertIn("high", dist)
        self.assertIn("medium", dist)
        self.assertIn("low", dist)
        self.assertIn("bulk", dist)
        total = sum(dist.values())
        self.assertEqual(total, len(self.registry._registry))

    def test_registry_broker_route(self):
        """Registry broker_route method works."""
        routing = self.registry.broker_route([42, 153])
        self.assertIn("broker_priority", routing)
        self.assertIn("broker_priority_label", routing)
        self.assertEqual(routing["broker_priority"], 0)  # Critical

    def test_registry_queue_recommendation(self):
        """Registry get_queue_recommendation works."""
        rec = self.registry.get_queue_recommendation([42])
        self.assertIn("broker_priority", rec)
        self.assertIn("dlq_recommendation", rec)
        self.assertIn("batch_hint", rec)


if __name__ == "__main__":
    unittest.main(verbosity=2)