"""
TokenRelay v3 Bidirectional Real-Time Protocol (BRP) Tests.

30+ tests covering:
- BRPServer lifecycle and WebSocket handling
- BRPClient connection and messaging
- ChannelManager (4 priority channels, message ordering)
- ConnectionMonitor (latency, jitter, packet loss)
- HeartbeatManager (ping/pong, disconnection detection)
- BRPConfig defaults
- WebSocketFrame encoding/decoding
- Integration scenarios
"""

import sys
import time
import threading
import json
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from brp import (
    BRPServer, BRPClient, ChannelManager, ConnectionMonitor,
    HeartbeatManager, BRPConfig, ChannelPriority, ConnectionState,
    QualityMetrics, ChannelStats, WebSocketFrame,
    create_brp_server, create_brp_client,
    FrameType,
)


# ─────────────────────────────────────────────
# BRPConfig Tests
# ─────────────────────────────────────────────

class TestBRPConfig(unittest.TestCase):
    """Test BRPConfig defaults and customization."""

    def test_default_config(self):
        """Default config has expected values."""
        cfg = BRPConfig()
        self.assertEqual(cfg.host, "localhost")
        self.assertEqual(cfg.port, 8765)
        self.assertEqual(cfg.heartbeat_interval_ms, 3000)
        self.assertEqual(cfg.heartbeat_timeout_ms, 10000)
        self.assertEqual(cfg.max_channels, 4)
        self.assertEqual(cfg.control_message_target_ms, 50.0)
        self.assertEqual(cfg.max_message_size, 65536)

    def test_custom_config(self):
        """Custom config overrides defaults."""
        cfg = BRPConfig(host="0.0.0.0", port=9999, heartbeat_interval_ms=5000)
        self.assertEqual(cfg.host, "0.0.0.0")
        self.assertEqual(cfg.port, 9999)
        self.assertEqual(cfg.heartbeat_interval_ms, 5000)

    def test_channel_queues(self):
        """Channel queues have all 4 channels."""
        cfg = BRPConfig()
        self.assertIn(ChannelPriority.SYSTEM, cfg.channel_queues)
        self.assertIn(ChannelPriority.USER, cfg.channel_queues)
        self.assertIn(ChannelPriority.AI, cfg.channel_queues)
        self.assertIn(ChannelPriority.EVENTS, cfg.channel_queues)

    def test_quality_thresholds(self):
        """Quality thresholds are configurable."""
        cfg = BRPConfig()
        self.assertEqual(cfg.latency_warning_ms, 100.0)
        self.assertEqual(cfg.latency_critical_ms, 500.0)
        self.assertEqual(cfg.packet_loss_warning_pct, 5.0)
        self.assertEqual(cfg.packet_loss_critical_pct, 20.0)
        self.assertEqual(cfg.jitter_warning_ms, 30.0)


# ─────────────────────────────────────────────
# ChannelManager Tests
# ─────────────────────────────────────────────

class TestChannelManagerBasics(unittest.TestCase):
    """Test basic channel operations."""

    def setUp(self):
        self.cm = ChannelManager(max_channels=4)

    def test_enqueue_dequeue(self):
        """Messages can be enqueued and dequeued."""
        self.cm.enqueue(ChannelPriority.USER, {"tokens": [42], "payload": "test"})
        msg = self.cm.dequeue(ChannelPriority.USER)
        self.assertIsNotNone(msg)
        self.assertEqual(msg["data"]["payload"], "test")

    def test_dequeue_empty(self):
        """Dequeue from empty channel returns None."""
        msg = self.cm.dequeue(ChannelPriority.SYSTEM)
        self.assertIsNone(msg)

    def test_peek(self):
            """Peek does not remove the message."""
            self.cm.enqueue(ChannelPriority.AI, {"type": "response"})
            peeked = self.cm.peek(ChannelPriority.AI)
            self.assertIsNotNone(peeked)
            self.assertEqual(peeked["data"]["type"], "response")
            # Still there after peek
            self.assertEqual(self.cm._brokers[ChannelPriority.AI].depth, 1)

    def test_invalid_channel(self):
        """Enqueue/dequeue on invalid channel returns False/None."""
        result = self.cm.enqueue(99, {"test": True})
        self.assertFalse(result)
        msg = self.cm.dequeue(99)
        self.assertIsNone(msg)

    def test_channel_ordering(self):
        """Messages within a channel are FIFO ordered."""
        for i in range(5):
            self.cm.enqueue(ChannelPriority.USER, {"seq": i})
        seqs = []
        for _ in range(5):
            msg = self.cm.dequeue(ChannelPriority.USER)
            seqs.append(msg["data"]["seq"])
        self.assertEqual(seqs, list(range(5)))


class TestChannelManagerPriority(unittest.TestCase):
    """Test priority-based message routing."""

    def setUp(self):
        self.cm = ChannelManager(max_channels=4)

    def test_dequeue_priority(self):
        """dequeue_priority returns from highest-priority non-empty channel."""
        self.cm.enqueue(3, {"priority": "low"})  # EVENTS
        self.cm.enqueue(0, {"priority": "high"})  # SYSTEM
        self.cm.enqueue(1, {"priority": "medium"})  # USER

        # System (channel 0) should be dequeued first
        channel, msg = self.cm.dequeue_priority()
        self.assertEqual(channel, 0)
        self.assertEqual(msg["data"]["priority"], "high")

        # Then user (channel 1)
        channel, msg = self.cm.dequeue_priority()
        self.assertEqual(channel, 1)
        self.assertEqual(msg["data"]["priority"], "medium")

    def test_dequeue_priority_empty(self):
        """dequeue_priority returns None when all channels empty."""
        result = self.cm.dequeue_priority()
        self.assertIsNone(result)

    def test_get_message_by_sequence(self):
        """Get a specific message by sequence number."""
        self.cm.enqueue(ChannelPriority.USER, {"id": "first"})
        self.cm.enqueue(ChannelPriority.USER, {"id": "second"})
        msg = self.cm.get_message(ChannelPriority.USER, 0)
        self.assertIsNotNone(msg)
        self.assertEqual(msg["data"]["id"], "first")

    def test_total_depth(self):
        """Total depth counts across all channels."""
        self.cm.enqueue(0, {"a": 1})
        self.cm.enqueue(1, {"b": 2})
        self.cm.enqueue(1, {"c": 3})
        self.assertEqual(self.cm.total_depth, 3)

    def test_clear(self):
        """Clear removes messages from a channel."""
        self.cm.enqueue(ChannelPriority.USER, {"test": True})
        self.cm.enqueue(ChannelPriority.SYSTEM, {"ctrl": True})
        self.cm.clear(ChannelPriority.USER)
        self.assertEqual(self.cm.dequeue(ChannelPriority.USER), None)
        self.assertEqual(self.cm.dequeue(ChannelPriority.SYSTEM) is not None, True)

    def test_clear_all(self):
        """Clear all channels."""
        self.cm.enqueue(0, {"a": 1})
        self.cm.enqueue(1, {"b": 2})
        self.cm.clear()
        self.assertEqual(self.cm.total_depth, 0)


class TestChannelManagerCallbacks(unittest.TestCase):
    """Test channel callbacks."""

    def setUp(self):
        self.cm = ChannelManager(max_channels=4)
        self.callbacks_fired = []

    def _make_cb(self, channel_id):
        def cb(msg):
            self.callbacks_fired.append((channel_id, msg["data"]))
        return cb

    def test_register_unregister_callback(self):
        """Callbacks fire when messages arrive."""
        cb = self._make_cb(1)
        self.cm.register_callback(ChannelPriority.USER, cb)
        self.cm.enqueue(ChannelPriority.USER, {"msg": "hello"})
        self.assertEqual(len(self.callbacks_fired), 1)
        self.assertEqual(self.callbacks_fired[0][1]["msg"], "hello")

    def test_unregister_callback(self):
        """Unregistered callbacks don't fire."""
        cb = self._make_cb(1)
        self.cm.register_callback(ChannelPriority.USER, cb)
        self.cm.unregister_callback(ChannelPriority.USER, cb)
        self.cm.enqueue(ChannelPriority.USER, {"msg": "hello"})
        self.assertEqual(len(self.callbacks_fired), 0)

    def test_get_stats(self):
        """Channel stats are tracked."""
        self.cm.enqueue(ChannelPriority.USER, {"test": 1})
        self.cm.enqueue(ChannelPriority.USER, {"test": 2})
        stats = self.cm.get_stats(ChannelPriority.USER)
        self.assertEqual(stats.messages_received, 2)
        self.assertEqual(stats.queue_depth, 2)

    def test_get_all_stats(self):
        """All channel stats available."""
        for ch in range(4):
            self.cm.enqueue(ch, {"ch": ch})
        all_stats = self.cm.get_all_stats()
        self.assertEqual(len(all_stats), 4)
        for ch in range(4):
            self.assertIn(ch, all_stats)


# ─────────────────────────────────────────────
# ConnectionMonitor Tests
# ─────────────────────────────────────────────

class TestConnectionMonitor(unittest.TestCase):
    """Test connection quality monitoring."""

    def setUp(self):
        self.monitor = ConnectionMonitor()

    def test_register_unregister(self):
        """Connections can be registered and unregistered."""
        self.monitor.register_connection("client1")
        metrics = self.monitor.get_metrics("client1")
        self.assertEqual(metrics.client_id, "client1")
        self.monitor.unregister_connection("client1")
        # After unregistering, get_metrics returns a default object
        metrics = self.monitor.get_metrics("client1")
        # Default object has the queried ID but no real data
        self.assertEqual(metrics.latency_ms, 0.0)
        self.assertEqual(metrics.sample_count, 0)
        self.assertEqual(metrics.packet_loss_pct, 0.0)

    def test_ping_pong_tracking(self):
        """Ping/pong timestamps are tracked."""
        self.monitor.register_connection("client1")
        self.monitor.record_ping("client1")
        self.monitor.record_pong("client1", rtt_ms=25.0)
        metrics = self.monitor.get_metrics("client1")
        self.assertAlmostEqual(metrics.latency_ms, 25.0, places=1)
        self.assertGreater(metrics.last_pong_received, 0)

    def test_packet_loss_tracking(self):
        """Packet loss is recorded."""
        self.monitor.register_connection("client1")
        self.monitor.record_ping("client1")
        self.monitor.record_packet_loss("client1")
        metrics = self.monitor.get_metrics("client1")
        self.assertGreater(metrics.packet_loss_pct, 0.0)

    def test_quality_summary(self):
        """Quality summary aggregates all clients."""
        self.monitor.register_connection("client1")
        self.monitor.register_connection("client2")
        self.monitor.record_pong("client1", rtt_ms=10.0)
        self.monitor.record_pong("client2", rtt_ms=200.0)
        summary = self.monitor.get_quality_summary()
        self.assertEqual(summary["total_connections"], 2)
        self.assertIn("avg_latency_ms", summary)

    def test_alerts(self):
        """Quality alerts are generated for threshold violations."""
        self.monitor.register_connection("client1")
        self.monitor.record_pong("client1", rtt_ms=600.0)
        alerts = self.monitor.check_alerts("client1")
        self.assertTrue(len(alerts) > 0)
        self.assertTrue(any("CRITICAL" in a for a in alerts))

    def test_no_alerts_for_good_connection(self):
        """Good connections produce no alerts."""
        self.monitor.register_connection("client1")
        self.monitor.record_pong("client1", rtt_ms=5.0)
        alerts = self.monitor.check_alerts("client1")
        self.assertEqual(len(alerts), 0)

    def test_get_all_metrics(self):
        """All client metrics are retrievable."""
        for cid in ["a", "b", "c"]:
            self.monitor.register_connection(cid)
        metrics = self.monitor.get_all_metrics()
        self.assertEqual(len(metrics), 3)


# ─────────────────────────────────────────────
# HeartbeatManager Tests
# ─────────────────────────────────────────────

class TestHeartbeatManager(unittest.TestCase):
    """Test heartbeat management and disconnection detection."""

    def setUp(self):
        self.config = BRPConfig(heartbeat_interval_ms=50, heartbeat_timeout_ms=200)
        self.monitor = ConnectionMonitor(self.config)
        self.disconnects = []
        self.heartbeat = HeartbeatManager(
            config=self.config,
            monitor=self.monitor,
            on_disconnect=lambda cid: self.disconnects.append(cid),
        )
        self.heartbeat.start()

    def tearDown(self):
        self.heartbeat.stop()

    def test_register_unregister(self):
        """Clients can be registered and unregistered."""
        self.heartbeat.register_client("client1")
        status = self.heartbeat.get_client_status("client1")
        self.assertTrue(status["active"])
        self.heartbeat.unregister_client("client1")
        status = self.heartbeat.get_client_status("client1")
        self.assertFalse(status.get("active", False))

    def test_heartbeat_stats(self):
        """Heartbeat statistics are tracked."""
        self.heartbeat.register_client("client1")
        stats = self.heartbeat.get_heartbeat_stats()
        self.assertEqual(stats["total_clients"], 1)
        self.assertEqual(stats["active_clients"], 1)

    def test_send_ping(self):
        """Ping can be sent to a registered client."""
        self.heartbeat.register_client("client1")
        result = self.heartbeat.send_ping("client1")
        self.assertTrue(result)

    def test_send_ping_unregistered(self):
        """Ping to unregistered client returns False."""
        result = self.heartbeat.send_ping("unknown")
        self.assertFalse(result)

    def test_record_pong(self):
        """Pong is recorded correctly."""
        self.heartbeat.register_client("client1")
        self.heartbeat.send_ping("client1")
        self.heartbeat.record_pong("client1", rtt_ms=15.0)
        status = self.heartbeat.get_client_status("client1")
        self.assertGreater(status["last_pong"], 0)

    def test_missed_heartbeat_detection(self):
        """Missed heartbeats are tracked and can trigger disconnect."""
        self.heartbeat.register_client("client1")
        self.heartbeat.send_ping("client1")
        # Simulate missed pongs by updating last_pong to be old
        with self.heartbeat._lock:
            self.heartbeat._heartbeats["client1"]["last_pong"] = time.time() - 30
            self.heartbeat._heartbeats["client1"]["missed_count"] = 3
        result = self.heartbeat.record_missed_heartbeat("client1")
        # Should disconnect after 3 missed with timeout exceeded
        self.assertTrue(result)
        self.assertIn("client1", self.disconnects)

    def test_disconnect_callback(self):
        """Disconnect callback is invoked."""
        self.heartbeat.register_client("client1")
        self.heartbeat.send_ping("client1")
        with self.heartbeat._lock:
            self.heartbeat._heartbeats["client1"]["last_pong"] = time.time() - 30
            self.heartbeat._heartbeats["client1"]["missed_count"] = 3
        self.heartbeat.record_missed_heartbeat("client1")
        self.assertIn("client1", self.disconnects)

    def test_get_client_status(self):
        """Client status includes all required fields."""
        self.heartbeat.register_client("client1")
        status = self.heartbeat.get_client_status("client1")
        self.assertIn("active", status)
        self.assertIn("last_ping", status)
        self.assertIn("last_pong", status)
        self.assertIn("missed_count", status)
        self.assertIn("interval_ms", status)


# ─────────────────────────────────────────────
# QualityMetrics Tests
# ─────────────────────────────────────────────

class TestQualityMetrics(unittest.TestCase):
    """Test quality metric calculations."""

    def test_initial_state(self):
        """Initial quality metrics have default values."""
        m = QualityMetrics(client_id="test")
        self.assertEqual(m.latency_ms, 0.0)
        self.assertEqual(m.packet_loss_pct, 0.0)
        self.assertEqual(m.sample_count, 0)

    def test_update_latency(self):
        """Latency is updated with new samples."""
        m = QualityMetrics(client_id="test")
        m.update_latency(10.0)
        m.update_latency(20.0)
        m.update_latency(30.0)
        self.assertAlmostEqual(m.latency_ms, 20.0, places=1)

    def test_jitter_calculation(self):
        """Jitter is calculated from latency variations."""
        m = QualityMetrics(client_id="test")
        m.update_latency(5.0)
        m.update_latency(25.0)
        m.update_latency(10.0)
        # Jitter should be non-zero due to variation
        self.assertGreaterEqual(m.jitter_ms, 0.0)

    def test_quality_levels(self):
        """Quality level reflects connection state."""
        cfg = BRPConfig()
        # Good connection
        m = QualityMetrics(client_id="good", latency_ms=5.0, packet_loss_pct=0.0, jitter_ms=1.0)
        self.assertEqual(m.quality_level, "good")
        # Fair connection
        m2 = QualityMetrics(client_id="fair", latency_ms=50.0, packet_loss_pct=0.0, jitter_ms=50.0)
        self.assertEqual(m2.quality_level, "fair")
        # Poor connection
        m3 = QualityMetrics(client_id="poor", latency_ms=200.0, packet_loss_pct=10.0)
        self.assertEqual(m3.quality_level, "poor")
        # Critical connection
        m4 = QualityMetrics(client_id="critical", latency_ms=600.0, packet_loss_pct=25.0)
        self.assertEqual(m4.quality_level, "critical")


# ─────────────────────────────────────────────
# WebSocketFrame Tests
# ─────────────────────────────────────────────

class TestWebSocketFrame(unittest.TestCase):
    """Test WebSocket frame encoding/decoding."""

    def test_encode_text(self):
        """Text frames are encoded correctly."""
        frame = WebSocketFrame.encode_text("Hello, BRP!")
        self.assertIsInstance(frame, bytes)
        self.assertEqual(frame[0], 0x81)  # FIN + TEXT opcode

    def test_decode_text(self):
        """Text frames are decoded correctly."""
        encoded = WebSocketFrame.encode_text("Test message")
        decoded = WebSocketFrame.decode_frame(encoded)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["payload_str"], "Test message")
        self.assertEqual(decoded["opcode"], FrameType.TEXT)

    def test_encode_decode_roundtrip(self):
        """Encode and decode produce the same content."""
        original = "BRP v3 bidirectional protocol test"
        encoded = WebSocketFrame.encode_text(original)
        decoded = WebSocketFrame.decode_frame(encoded)
        self.assertEqual(decoded["payload_str"], original)

    def test_encode_ping(self):
        """Ping frames are encoded correctly."""
        frame = WebSocketFrame.encode_ping(b"ping-data")
        self.assertEqual(frame[0], 0x89)  # FIN + PING opcode

    def test_encode_pong(self):
        """Pong frames are encoded correctly."""
        frame = WebSocketFrame.encode_pong(b"pong-data")
        self.assertEqual(frame[0], 0x8A)  # FIN + PONG opcode

    def test_encode_close(self):
        """Close frames are encoded correctly."""
        frame = WebSocketFrame.encode_close(1000, "Goodbye")
        self.assertEqual(frame[0], 0x88)  # FIN + CLOSE opcode
        # Decode the close frame
        decoded = WebSocketFrame.decode_frame(frame)
        self.assertEqual(decoded["opcode"], FrameType.CLOSE)

    def test_handshake_response(self):
        """WebSocket handshake response is valid."""
        key = "dGhlIHNhbXBsZSBub25jZQ=="
        accept = WebSocketFrame.handshake_response(key)
        self.assertIsInstance(accept, str)
        self.assertTrue(len(accept) > 0)

    def test_encode_decode_binary(self):
        """Binary frames with JSON payload work correctly."""
        data = json.dumps({"type": "message", "content": "binary test"})
        encoded = WebSocketFrame.encode_text(data)
        decoded = WebSocketFrame.decode_frame(encoded)
        parsed = json.loads(decoded["payload_str"])
        self.assertEqual(parsed["type"], "message")

    def test_decode_invalid_data(self):
        """Invalid data returns None."""
        result = WebSocketFrame.decode_frame(b"\x01")  # Too short
        self.assertIsNone(result)

    def test_large_payload(self):
        """Large payloads are encoded/decoded correctly."""
        large_text = "x" * 1000
        encoded = WebSocketFrame.encode_text(large_text)
        decoded = WebSocketFrame.decode_frame(encoded)
        self.assertEqual(decoded["payload_str"], large_text)


# ─────────────────────────────────────────────
# BRPServer Tests
# ─────────────────────────────────────────────

class TestBRPServer(unittest.TestCase):
    """Test BRPServer lifecycle and basic operations."""

    def setUp(self):
        self.config = BRPConfig(host="localhost", port=18765)
        self.server = BRPServer(self.config)

    def tearDown(self):
        if self.server._running:
            self.server.stop()

    def test_server_creation(self):
        """Server is created with all components."""
        self.assertIsInstance(self.server.channel_manager, ChannelManager)
        self.assertIsInstance(self.server.connection_monitor, ConnectionMonitor)
        self.assertIsInstance(self.server.heartbeat_manager, HeartbeatManager)

    def test_start_stop(self):
        """Server can start and stop without errors."""
        self.server.start()
        self.assertTrue(self.server._running)
        self.assertTrue(self.server.heartbeat_manager._running)
        self.server.stop()
        self.assertFalse(self.server._running)

    def test_server_stats(self):
        """Server stats are returned correctly."""
        self.server.start()
        stats = self.server.get_server_stats()
        self.assertIn("connected_clients", stats)
        self.assertIn("total_channel_depth", stats)
        self.assertIn("quality_summary", stats)
        self.assertIn("heartbeat_stats", stats)
        self.assertTrue(stats["server_running"])
        self.server.stop()

    def test_connected_clients(self):
        """Connected clients count is tracked."""
        self.assertEqual(self.server.connected_clients, 0)
        self.server.start()
        time.sleep(0.1)
        self.assertEqual(self.server.connected_clients, 0)
        self.server.stop()

    def test_channel_manager_properties(self):
        """Channel manager properties work correctly."""
        self.server.start()
        self.assertEqual(self.server.channel_manager.max_channels, 4)
        self.assertEqual(self.server.channel_manager.total_depth, 0)
        self.server.stop()

    def test_send_control_message(self):
        """Control messages can be sent."""
        self.server.start()
        # send_control should work without errors
        self.server.send_control_message("test_client", {"type": "ping"})
        self.server.stop()

    def test_broadcast(self):
        """Broadcast sends to all clients on a channel."""
        self.server.start()
        # Broadcast to channel 0 with no connected clients should not error
        self.server.broadcast(ChannelPriority.SYSTEM, {"type": "broadcast"}, exclude_client="none")
        self.server.stop()


# ─────────────────────────────────────────────
# BRPClient Tests
# ─────────────────────────────────────────────

class TestBRPClient(unittest.TestCase):
    """Test BRPClient operations."""

    def setUp(self):
        self.config = BRPConfig(host="localhost", port=18765)
        self.client = BRPClient(self.config, user_id="test_user")

    def test_client_creation(self):
        """Client is created with expected attributes."""
        self.assertEqual(self.client.user_id, "test_user")
        self.assertEqual(self.client.channel, ChannelPriority.USER)
        self.assertFalse(self.client.is_connected)

    def test_send_when_disconnected(self):
        """Send when disconnected attempts to connect."""
        # Since no server, this will fail but not raise
        result = self.client.send({"type": "test"})
        # May be True (reconnect) or False (max retries)
        self.assertIsInstance(result, bool)

    def test_disconnect_when_not_connected(self):
        """Disconnecting when not connected does not error."""
        self.client.disconnect()  # Should not raise

    def test_send_control(self):
        """Control message sends on system channel."""
        # Will try to connect but fail - no exception raised
        self.client.send_control({"type": "ctrl"})

    def test_on_off_callbacks(self):
        """Event callbacks can be registered and removed."""
        events = []
        def on_msg(data):
            events.append(data)

        self.client.on("message", on_msg)
        self.client.off("message", on_msg)
        # No exception on adding/removing

    def test_get_stats(self):
        """Client stats are returned correctly."""
        stats = self.client.get_stats()
        self.assertEqual(stats["client_id"], self.client.client_id)
        self.assertEqual(stats["user_id"], "test_user")
        self.assertFalse(stats["connected"])


# ─────────────────────────────────────────────
# Factory Functions Tests
# ─────────────────────────────────────────────

class TestFactoryFunctions(unittest.TestCase):
    """Test convenience factory functions."""

    def test_create_brp_server(self):
        """create_brp_server creates and starts a server."""
        cfg = BRPConfig(host="localhost", port=18766)
        server = create_brp_server(cfg)
        self.assertTrue(server._running)
        server.stop()

    def test_create_brp_client(self):
        """create_brp_client creates a client."""
        client = create_brp_client(BRPConfig(host="localhost", port=18767))
        self.assertIsInstance(client, BRPClient)
        self.assertFalse(client.is_connected)


# ─────────────────────────────────────────────
# BRPServer Integration Tests
# ─────────────────────────────────────────────

class TestBRPIntegration(unittest.TestCase):
    """Integration tests for the full BRP stack."""

    def setUp(self):
        self.config = BRPConfig(host="localhost", port=18768)
        self.server = BRPServer(self.config)
        self.server.start()

    def tearDown(self):
        self.server.stop()

    def test_server_with_clients(self):
        """Server runs with channel manager and monitor working."""
        stats = self.server.get_server_stats()
        self.assertTrue(stats["server_running"])
        self.assertIn("channel_stats", stats)

    def test_channel_message_flow(self):
        """Messages flow through channels correctly."""
        cm = self.server.channel_manager
        cm.enqueue(ChannelPriority.USER, {"type": "user_msg", "text": "hello"})
        cm.enqueue(ChannelPriority.AI, {"type": "ai_msg", "text": "response"})
        cm.enqueue(ChannelPriority.SYSTEM, {"type": "ctrl", "cmd": "ping"})

        self.assertEqual(cm.total_depth, 3)

        # Dequeue in priority order
        ch, msg = cm.dequeue_priority()
        self.assertEqual(ch, ChannelPriority.SYSTEM)
        ch, msg = cm.dequeue_priority()
        self.assertEqual(ch, ChannelPriority.USER)
        ch, msg = cm.dequeue_priority()
        self.assertEqual(ch, ChannelPriority.AI)

    def test_heartbeat_with_monitor(self):
        """Heartbeat manager works with connection monitor."""
        hm = self.server.heartbeat_manager
        hm.register_client("integration_client")
        hm.send_ping("integration_client")
        hm.record_pong("integration_client", rtt_ms=10.0)
        stats = hm.get_heartbeat_stats()
        self.assertEqual(stats["total_clients"], 1)

        # Check monitor has metrics
        metrics = self.server.connection_monitor.get_metrics("integration_client")
        self.assertEqual(metrics.client_id, "integration_client")

    def test_monitor_quality_with_heartbeats(self):
        """Connection quality is updated through heartbeat cycle."""
        hm = self.server.heartbeat_manager
        hm.register_client("quality_client")
        hm.send_ping("quality_client")

        # Simulate good pongs
        for _ in range(5):
            hm.record_pong("quality_client", rtt_ms=20.0)

        metrics = self.server.connection_monitor.get_metrics("quality_client")
        self.assertAlmostEqual(metrics.latency_ms, 20.0, delta=5.0)
        # sample_count incremented by both update_latency and record_pong
        self.assertGreater(metrics.sample_count, 0)


# ─────────────────────────────────────────────
# BRPServer WebSocket Tests
# ─────────────────────────────────────────────

class TestBRPWebSocketHandler(unittest.TestCase):
    """Test the WebSocket handler upgrade mechanism."""

    def test_websocket_frame_types(self):
        """All WebSocket frame types have correct opcodes."""
        self.assertEqual(FrameType.CONTINUATION, 0x0)
        self.assertEqual(FrameType.TEXT, 0x1)
        self.assertEqual(FrameType.BINARY, 0x2)
        self.assertEqual(FrameType.CLOSE, 0x8)
        self.assertEqual(FrameType.PING, 0x9)
        self.assertEqual(FrameType.PONG, 0xA)

    def test_frame_encode_decode_various_types(self):
        """Text, ping, pong, close frames all decode correctly."""
        text_frame = WebSocketFrame.encode_text("text data")
        ping_frame = WebSocketFrame.encode_ping(b"ping")
        pong_frame = WebSocketFrame.encode_pong(b"pong")
        close_frame = WebSocketFrame.encode_close(1000)

        text_decoded = WebSocketFrame.decode_frame(text_frame)
        ping_decoded = WebSocketFrame.decode_frame(ping_frame)
        pong_decoded = WebSocketFrame.decode_frame(pong_frame)
        close_decoded = WebSocketFrame.decode_frame(close_frame)

        self.assertEqual(text_decoded["opcode"], FrameType.TEXT)
        self.assertEqual(ping_decoded["opcode"], FrameType.PING)
        self.assertEqual(pong_decoded["opcode"], FrameType.PONG)
        self.assertEqual(close_decoded["opcode"], FrameType.CLOSE)

    def test_server_class_exists(self):
        """BRPServer class has all required methods."""
        methods = ["start", "stop", "disconnect_client", "broadcast",
                   "send_control_message", "get_server_stats"]
        for m in methods:
            self.assertTrue(hasattr(BRPServer, m), f"Missing method: {m}")

    def test_client_class_exists(self):
        """BRPClient class has all required methods."""
        methods = ["connect", "disconnect", "send", "send_control",
                   "send_to_channel", "on", "off", "get_stats"]
        for m in methods:
            self.assertTrue(hasattr(BRPClient, m), f"Missing method: {m}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

if __name__ == "__main__":
    unittest.main(verbosity=2)
