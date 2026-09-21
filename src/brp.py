"""
TokenRelay v3 Bidirectional Real-Time Protocol (BRP).

Full-duplex WebSocket for User↔AI communication with:
- Multi-channel priority system (4 channels) using MessageBroker
- Connection quality monitoring using CircuitBreaker
- Heartbeat system using EventBus
- Sub-50ms target for control messages
- Full-duplex support

Integrates with existing TokenRelay v2 modules:
- broker.MessageBroker for channel message routing
- circuit_breaker.CircuitBreaker for connection health checks
- streaming.EventBus for heartbeat events
- codec.MessageEncoder/Decoder for message encoding
- registry.TokenRegistry for token routing
"""

import json
import time
import threading
import uuid
import struct
import hashlib
import base64
import logging
from dataclasses import dataclass, field, asdict
from enum import IntEnum, auto
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable, Dict, List, Optional, Tuple
from collections import deque
from urllib.parse import urlparse

from codec import MessageEncoder, MessageDecoder
from broker import MessageBroker, PriorityMessage, QueueFullError, DuplicateMessageError
from circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerError
from streaming import EventBus, StreamEventType, StreamEvent, SSEHandler, WebSocketSimulator
from registry import TokenRegistry, get_registry

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Types & Constants
# ─────────────────────────────────────────────

class ChannelPriority(IntEnum):
    """4 priority channels for BRP message routing."""
    SYSTEM = 0   # Control/system messages (highest priority, sub-50ms)
    USER = 1     # User-to-AI messages
    AI = 2       # AI-to-User messages
    EVENTS = 3   # Event notifications (lowest priority)


class ChannelName(IntEnum):
    """Named channel identifiers."""
    CHANNEL_0_SYSTEM = 0
    CHANNEL_1_USER = 1
    CHANNEL_2_AI = 2
    CHANNEL_3_EVENTS = 3


class ConnectionState(IntEnum):
    """Connection lifecycle states."""
    CONNECTING = auto()
    CONNECTED = auto()
    DISCONNECTING = auto()
    DISCONNECTED = auto()
    RECONNECTING = auto()


class FrameType(IntEnum):
    """WebSocket frame types."""
    CONTINUATION = 0x0
    TEXT = 0x1
    BINARY = 0x2
    CLOSE = 0x8
    PING = 0x9
    PONG = 0xA


# ─────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────

@dataclass
class BRPConfig:
    """Configuration for the BRP server and clients."""
    host: str = "localhost"
    port: int = 8765
    heartbeat_interval_ms: int = 3000
    heartbeat_timeout_ms: int = 10000
    max_channels: int = 4
    control_message_target_ms: float = 50.0
    max_message_size: int = 65536
    enable_compression: bool = True
    enable_encryption: bool = False
    compression_level: int = 6
    reconnect_attempts: int = 3
    reconnect_delay_ms: int = 1000
    max_inflight_messages: int = 1000
    channel_queues: Dict[int, int] = field(default_factory=lambda: {
        ChannelPriority.SYSTEM: 1000,
        ChannelPriority.USER: 5000,
        ChannelPriority.AI: 5000,
        ChannelPriority.EVENTS: 2000,
    })
    # Circuit breaker thresholds for connection health
    cb_failure_threshold: int = 5
    cb_recovery_timeout: float = 30.0
    # Quality monitoring thresholds
    latency_warning_ms: float = 100.0
    latency_critical_ms: float = 500.0
    packet_loss_warning_pct: float = 5.0
    packet_loss_critical_pct: float = 20.0
    jitter_warning_ms: float = 30.0


@dataclass
class ChannelStats:
    """Statistics for a single channel."""
    channel_id: int = 0
    messages_sent: int = 0
    messages_received: int = 0
    messages_dropped: int = 0
    queue_depth: int = 0
    avg_latency_ms: float = 0.0
    total_bytes: int = 0
    last_activity: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class QualityMetrics:
    """Connection quality metrics for a single client."""
    client_id: str = ""
    latency_ms: float = 0.0
    jitter_ms: float = 0.0
    packet_loss_pct: float = 0.0
    signal_strength: int = 100
    last_ping_sent: float = 0.0
    last_pong_received: float = 0.0
    sample_count: int = 0
    latencies: List[float] = field(default_factory=list)

    def update_latency(self, latency_ms: float):
        self.latencies.append(latency_ms)
        self.sample_count += 1
        self.latency_ms = sum(self.latencies[-20:]) / min(len(self.latencies[-20:]), 20)
        if len(self.latencies) >= 2:
            recent = self.latencies[-10:]
            if len(recent) >= 2:
                self.jitter_ms = max(recent) - min(recent)

    @property
    def quality_level(self) -> str:
        if self.packet_loss_pct > BRPConfig().packet_loss_critical_pct or self.latency_ms > BRPConfig().latency_critical_ms:
            return "critical"
        if self.packet_loss_pct > BRPConfig().packet_loss_warning_pct or self.latency_ms > BRPConfig().latency_warning_ms:
            return "poor"
        if self.jitter_ms > BRPConfig().jitter_warning_ms:
            return "fair"
        return "good"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─────────────────────────────────────────────
# Channel Manager (uses MessageBroker internally)
# ─────────────────────────────────────────────

class ChannelManager:
    """Manages 4 priority channels with message ordering.

    Uses MessageBroker internally for priority-based message routing
    between channels. Each channel gets its own MessageBroker instance
    for isolation and independent queue management.
    """

    CHANNEL_NAMES = {
        ChannelPriority.SYSTEM: "system",
        ChannelPriority.USER: "user",
        ChannelPriority.AI: "ai",
        ChannelPriority.EVENTS: "events",
    }

    def __init__(self, max_channels: int = 4, registry: Optional[TokenRegistry] = None):
        self.max_channels = max_channels
        self.registry = registry or get_registry()
        self._lock = threading.Lock()
        self._stats: Dict[int, ChannelStats] = {}
        self._callbacks: Dict[int, List[Callable]] = {}
        self._order_counters: Dict[int, int] = {}

        # One MessageBroker per channel for priority-based routing
        # Each broker uses the channel priority as its priority level
        self._brokers: Dict[int, MessageBroker] = {}
        for i in range(max_channels):
            self._brokers[i] = MessageBroker(
                registry=self.registry,
                max_depth=BRPConfig().channel_queues.get(i, 5000),
                dedup_window_seconds=300.0,
            )
            self._order_counters[i] = 0
            self._stats[i] = ChannelStats(channel_id=i)
            self._callbacks[i] = []

    def enqueue(self, channel: int, message: Dict[str, Any],
                priority: Optional[int] = None) -> bool:
        """Enqueue a message on a channel using MessageBroker."""
        if channel < 0 or channel >= self.max_channels:
            return False

        msg_priority = priority if priority is not None else channel
        try:
            msg_with_seq = dict(message)
            msg_with_seq["_sequence"] = self._order_counters[channel]
            self._brokers[channel].enqueue(msg_with_seq, priority=msg_priority)
            with self._lock:
                self._stats[channel].messages_received += 1
                self._stats[channel].queue_depth = self._brokers[channel].depth
                self._stats[channel].last_activity = time.time()
                self._order_counters[channel] += 1
            # Invoke callbacks
            for cb in self._callbacks.get(channel, []):
                try:
                    cb({"data": msg_with_seq, "channel": channel, "timestamp": time.time()})
                except Exception:
                    logger.exception(f"Channel {channel} callback error")
            return True
        except (QueueFullError, DuplicateMessageError):
            with self._lock:
                self._stats[channel].messages_dropped += 1
            return False

    def dequeue(self, channel: int) -> Optional[Dict[str, Any]]:
        """Dequeue the next message from a channel's broker."""
        if channel < 0 or channel >= self.max_channels:
            return None
        msg = self._brokers[channel].dequeue()
        if msg is not None:
            with self._lock:
                self._stats[channel].messages_sent += 1
                self._stats[channel].queue_depth = self._brokers[channel].depth
            return {"data": msg, "sequence": msg.get("_sequence", 0), "channel": channel}
        return None

    def dequeue_priority(self) -> Optional[Tuple[int, Dict[str, Any]]]:
        """Dequeue from the highest-priority non-empty channel using broker priority."""
        with self._lock:
            for channel in range(self.max_channels):
                broker = self._brokers[channel]
                if not broker.is_empty:
                    msg = broker.dequeue()
                    if msg is not None:
                        self._stats[channel].messages_sent += 1
                        self._stats[channel].queue_depth = broker.depth
                        return (channel, {"data": msg, "sequence": msg.get("_sequence", 0), "channel": channel})
        return None

    def peek(self, channel: int) -> Optional[Dict[str, Any]]:
        """Peek at the next message without removing it."""
        if channel < 0 or channel >= self.max_channels:
            return None
        msg = self._brokers[channel].peek()
        if msg is None:
            return None
        return {"data": msg, "sequence": msg.get("_sequence", 0), "channel": channel}

    def get_message(self, channel: int, sequence: int) -> Optional[Dict[str, Any]]:
        """Get a specific message by sequence number without removing it."""
        if channel < 0 or channel >= self.max_channels:
            return None
        broker = self._brokers[channel]
        # Search through the broker's internal queue
        for priority, seq, msg in broker._queue:
            if isinstance(msg, dict) and msg.get("_sequence") == sequence:
                return {"data": msg, "sequence": sequence, "channel": channel}
        return None

    def register_callback(self, channel: int, callback: Callable):
        """Register a callback for new messages on a channel."""
        if channel < 0 or channel >= self.max_channels:
            return False
        with self._lock:
            self._callbacks[channel].append(callback)
        return True

    def unregister_callback(self, channel: int, callback: Callable) -> bool:
        """Unregister a callback from a channel."""
        if channel < 0 or channel >= self.max_channels:
            return False
        with self._lock:
            try:
                self._callbacks[channel].remove(callback)
                return True
            except ValueError:
                return False

    def get_stats(self, channel: int) -> ChannelStats:
        """Get statistics for a channel."""
        if channel < 0 or channel >= self.max_channels:
            return ChannelStats()
        with self._lock:
            self._stats[channel].queue_depth = self._brokers[channel].depth
            return self._stats[channel]

    def get_all_stats(self) -> Dict[int, ChannelStats]:
        """Get statistics for all channels."""
        with self._lock:
            return {k: v for k, v in self._stats.items()}

    @property
    def total_depth(self) -> int:
        """Total messages across all channels."""
        with self._lock:
            return sum(b.depth for b in self._brokers.values())

    def clear(self, channel: Optional[int] = None):
        """Clear messages from a channel or all channels."""
        with self._lock:
            if channel is not None:
                if 0 <= channel < self.max_channels:
                    self._brokers[channel].reset_metrics()
                    self._order_counters[channel] = 0
            else:
                for ch in self._brokers:
                    self._brokers[ch].reset_metrics()
                    self._order_counters[ch] = 0


# ─────────────────────────────────────────────
# Connection Monitor (uses CircuitBreaker internally)
# ─────────────────────────────────────────────

class ConnectionMonitor:
    """Tracks latency, jitter, and packet loss per connection.

    Uses CircuitBreaker for connection health checking — when a
    connection exceeds failure thresholds, the circuit breaker
    opens to prevent further calls to a degraded service.
    """

    def __init__(self, config: Optional[BRPConfig] = None):
        self.config = config or BRPConfig()
        self._metrics: Dict[str, QualityMetrics] = {}
        self._lock = threading.Lock()
        self._latency_samples: Dict[str, deque] = {}
        self._max_samples = 50

        # Circuit breaker per client for connection health checking
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}

    def register_connection(self, client_id: str):
        """Register a new connection for monitoring with its own CircuitBreaker."""
        with self._lock:
            self._metrics[client_id] = QualityMetrics(client_id=client_id)
            self._latency_samples[client_id] = deque(maxlen=self._max_samples)
            self._circuit_breakers[client_id] = CircuitBreaker(
                failure_threshold=self.config.cb_failure_threshold,
                recovery_timeout=self.config.cb_recovery_timeout,
                name=f"conn_{client_id}",
            )

    def unregister_connection(self, client_id: str):
        """Remove a connection from monitoring."""
        with self._lock:
            self._metrics.pop(client_id, None)
            self._latency_samples.pop(client_id, None)
            self._circuit_breakers.pop(client_id, None)

    def record_ping(self, client_id: str, timestamp: Optional[float] = None):
        """Record when a ping was sent."""
        timestamp = timestamp or time.time()
        with self._lock:
            if client_id in self._metrics:
                self._metrics[client_id].last_ping_sent = timestamp

    def record_pong(self, client_id: str, rtt_ms: float):
        """Record a pong response with measured RTT."""
        now = time.time()
        with self._lock:
            if client_id in self._metrics:
                m = self._metrics[client_id]
                m.update_latency(rtt_ms)
                m.last_pong_received = now
                m.sample_count += 1
                self._latency_samples[client_id].append(rtt_ms)
                # Reset circuit breaker on successful response
                cb = self._circuit_breakers.get(client_id)
                if cb and cb.state == CircuitState.HALF_OPEN:
                    cb.call(lambda: None)  # Success - closes the circuit

    def record_packet_loss(self, client_id: str):
        """Record a missed packet and trip the circuit breaker."""
        with self._lock:
            if client_id in self._metrics:
                self._metrics[client_id].packet_loss_pct += 1.0
                # Record failure on the circuit breaker
                cb = self._circuit_breakers.get(client_id)
                if cb:
                    try:
                        cb.call(lambda: (_ for _ in ()).throw(
                            Exception("Connection degraded")
                        ))
                    except CircuitBreakerError:
                        pass  # Circuit is now open - expected
                    except Exception:
                        pass  # Failure recorded

    def get_metrics(self, client_id: str) -> QualityMetrics:
        """Get quality metrics for a client."""
        with self._lock:
            return self._metrics.get(client_id, QualityMetrics(client_id=client_id))

    def get_all_metrics(self) -> Dict[str, QualityMetrics]:
        """Get metrics for all connected clients."""
        with self._lock:
            return {k: v for k, v in self._metrics.items()}

    @property
    def circuit_breaker(self, client_id: str) -> Optional[CircuitBreaker]:
        """Get the circuit breaker for a client."""
        with self._lock:
            return self._circuit_breakers.get(client_id)

    def get_circuit_state(self, client_id: str) -> CircuitState:
        """Get the current circuit breaker state for a client."""
        with self._lock:
            cb = self._circuit_breakers.get(client_id)
            return cb.state if cb else CircuitState.CLOSED

    def get_quality_summary(self) -> Dict[str, Any]:
        """Get a summary of all connection qualities."""
        with self._lock:
            summary = {
                "total_connections": len(self._metrics),
                "good": 0, "fair": 0, "poor": 0, "critical": 0,
                "avg_latency_ms": 0.0,
                "avg_packet_loss_pct": 0.0,
            }
            latencies = []
            losses = []
            for m in self._metrics.values():
                level = m.quality_level
                summary[level] += 1
                latencies.append(m.latency_ms)
                losses.append(m.packet_loss_pct)
            if latencies:
                summary["avg_latency_ms"] = sum(latencies) / len(latencies)
                summary["avg_packet_loss_pct"] = sum(losses) / len(losses)
            return summary

    def check_alerts(self, client_id: str) -> List[str]:
        """Check for quality threshold violations."""
        metrics = self.get_metrics(client_id)
        alerts = []
        cfg = self.config

        if metrics.latency_ms > cfg.latency_critical_ms:
            alerts.append(f"CRITICAL: latency {metrics.latency_ms:.1f}ms > {cfg.latency_critical_ms}ms")
        elif metrics.latency_ms > cfg.latency_warning_ms:
            alerts.append(f"WARNING: latency {metrics.latency_ms:.1f}ms > {cfg.latency_warning_ms}ms")

        if metrics.packet_loss_pct > cfg.packet_loss_critical_pct:
            alerts.append(f"CRITICAL: packet loss {metrics.packet_loss_pct:.1f}% > {cfg.packet_loss_critical_pct}%")
        elif metrics.packet_loss_pct > cfg.packet_loss_warning_pct:
            alerts.append(f"WARNING: packet loss {metrics.packet_loss_pct:.1f}% > {cfg.packet_loss_warning_pct}%")

        if metrics.jitter_ms > cfg.jitter_warning_ms:
            alerts.append(f"WARNING: jitter {metrics.jitter_ms:.1f}ms > {cfg.jitter_warning_ms}ms")

        # Check circuit breaker state
        cb_state = self.get_circuit_state(client_id)
        if cb_state == CircuitState.OPEN:
            alerts.append(f"CIRCUIT OPEN for {client_id} — connection degraded")

        return alerts


# ─────────────────────────────────────────────
# Heartbeat Manager (uses EventBus internally)
# ─────────────────────────────────────────────

class HeartbeatManager:
    """Sends periodic heartbeats and detects disconnections.

    Uses EventBus for heartbeat events (ping, pong, disconnect).
    Integrates with ConnectionMonitor for quality tracking.
    """

    def __init__(self, config: Optional[BRPConfig] = None,
                 monitor: Optional[ConnectionMonitor] = None,
                 on_heartbeat: Optional[Callable] = None,
                 on_disconnect: Optional[Callable] = None):
        self.config = config or BRPConfig()
        self.monitor = monitor or ConnectionMonitor(config)
        self._on_heartbeat = on_heartbeat
        self._on_disconnect = on_disconnect
        self._heartbeats: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._missed_heartbeats: Dict[str, int] = {}

        # EventBus for heartbeat events
        self.event_bus = EventBus()
        self._register_event_handlers()

    def _register_event_handlers(self):
        """Register event handlers on the EventBus."""
        self.event_bus.register(
            self._on_heartbeat_event,
            event_types=[StreamEventType.HEARTBEAT],
            priority=10,
            filter_source="heartbeat",
        )
        self.event_bus.register(
            self._on_disconnect_event,
            event_types=[StreamEventType.DISCONNECTED],
            priority=10,
            filter_source="heartbeat",
        )

    def _on_heartbeat_event(self, event: StreamEvent):
        """Handle heartbeat events from the EventBus."""
        if self._on_heartbeat:
            try:
                self._on_heartbeat(event.data)
            except Exception:
                logger.exception("Heartbeat callback error")

    def _on_disconnect_event(self, event: StreamEvent):
        """Handle disconnect events from the EventBus."""
        if self._on_disconnect:
            try:
                self._on_disconnect(event.data.get("client_id", "unknown"))
            except Exception:
                logger.exception("Disconnect callback error")

    def start(self):
        """Start the heartbeat loop."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._thread.start()
        logger.info("HeartbeatManager started")

    def stop(self):
        """Stop the heartbeat loop."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2)
        logger.info("HeartbeatManager stopped")

    def register_client(self, client_id: str, interval_ms: Optional[int] = None):
        """Register a client for heartbeat monitoring."""
        interval = interval_ms or self.config.heartbeat_interval_ms
        with self._lock:
            self._heartbeats[client_id] = {
                "last_ping": time.time(),
                "last_pong": time.time(),
                "interval_ms": interval,
                "missed_count": 0,
                "active": True,
            }
            self._missed_heartbeats[client_id] = 0
            self.monitor.register_connection(client_id)

    def unregister_client(self, client_id: str):
        """Remove a client from heartbeat monitoring."""
        with self._lock:
            self._heartbeats.pop(client_id, None)
            self._missed_heartbeats.pop(client_id, None)
        self.monitor.unregister_connection(client_id)
        # Emit disconnect event via EventBus
        self.event_bus.emit(StreamEvent(
            event_type=StreamEventType.DISCONNECTED,
            data={"client_id": client_id},
            source="heartbeat",
        ))

    def send_ping(self, client_id: str) -> bool:
        """Send a ping to a registered client."""
        with self._lock:
            if client_id not in self._heartbeats:
                return False
            hb = self._heartbeats[client_id]
            hb["last_ping"] = time.time()
            self.monitor.record_ping(client_id, hb["last_ping"])

        # Emit heartbeat event via EventBus
        self.event_bus.emit(StreamEvent(
            event_type=StreamEventType.HEARTBEAT,
            data={"client_id": client_id, "type": "ping"},
            source="heartbeat",
        ))
        return True

    def record_pong(self, client_id: str, rtt_ms: float):
        """Record a pong from a client."""
        with self._lock:
            if client_id in self._heartbeats:
                self._heartbeats[client_id]["last_pong"] = time.time()
                self._heartbeats[client_id]["missed_count"] = 0
        self.monitor.record_pong(client_id, rtt_ms)

        # Emit heartbeat event via EventBus
        self.event_bus.emit(StreamEvent(
            event_type=StreamEventType.HEARTBEAT,
            data={"client_id": client_id, "type": "pong", "rtt_ms": rtt_ms},
            source="heartbeat",
        ))

    def record_missed_heartbeat(self, client_id: str) -> bool:
        """Record a missed heartbeat. Returns True if client should be disconnected."""
        timeout = self.config.heartbeat_timeout_ms
        with self._lock:
            if client_id not in self._heartbeats:
                return False
            hb = self._heartbeats[client_id]
            hb["missed_count"] += 1
            self._missed_heartbeats[client_id] = hb["missed_count"]

            elapsed = (time.time() - hb["last_pong"]) * 1000
            if elapsed > timeout and hb["missed_count"] >= 3:
                hb["active"] = False
                self._heartbeats.pop(client_id, None)
                self._missed_heartbeats.pop(client_id, None)
                self.monitor.record_packet_loss(client_id)
                if self._on_disconnect:
                    try:
                        self._on_disconnect(client_id)
                    except Exception:
                        logger.exception("Disconnect callback error")
                return True
        return False

    def _heartbeat_loop(self):
        """Background loop that sends heartbeats."""
        while self._running:
            time.sleep(self.config.heartbeat_interval_ms / 1000.0)
            now = time.time()
            with self._lock:
                clients = list(self._heartbeats.keys())

            for client_id in clients:
                hb = self._heartbeats.get(client_id)
                if not hb or not hb["active"]:
                    continue
                self.send_ping(client_id)
                elapsed_since_pong = (now - hb["last_pong"]) * 1000
                if elapsed_since_pong > self.config.heartbeat_timeout_ms:
                    should_disconnect = self.record_missed_heartbeat(client_id)
                    if should_disconnect:
                        logger.warning(f"Client {client_id} disconnected due to missed heartbeats")

    def get_client_status(self, client_id: str) -> Dict[str, Any]:
        """Get heartbeat status for a client."""
        with self._lock:
            hb = self._heartbeats.get(client_id)
            if not hb:
                return {"active": False}
            return {
                "active": hb["active"],
                "last_ping": hb["last_ping"],
                "last_pong": hb["last_pong"],
                "missed_count": hb["missed_count"],
                "interval_ms": hb["interval_ms"],
            }

    def get_heartbeat_stats(self) -> Dict[str, Any]:
        """Get heartbeat statistics."""
        with self._lock:
            active = sum(1 for h in self._heartbeats.values() if h["active"])
            total = len(self._heartbeats)
            total_missed = sum(h["missed_count"] for h in self._heartbeats.values())
            return {
                "total_clients": total,
                "active_clients": active,
                "total_missed_heartbeats": total_missed,
                "heartbeat_interval_ms": self.config.heartbeat_interval_ms,
                "heartbeat_timeout_ms": self.config.heartbeat_timeout_ms,
            }


# ─────────────────────────────────────────────
# WebSocket Frame Handling (stdlib-only)
# ─────────────────────────────────────────────

class WebSocketFrame:
    """WebSocket frame encoding/decoding using only stdlib."""

    @staticmethod
    def encode_text(payload: str) -> bytes:
        payload_bytes = payload.encode("utf-8")
        length = len(payload_bytes)
        if length < 126:
            header = bytes([0x81, length])
        elif length < 65536:
            header = bytes([0x81, 126]) + struct.pack("!H", length)
        else:
            header = bytes([0x81, 127]) + struct.pack("!Q", length)
        return header + payload_bytes

    @staticmethod
    def encode_ping(payload: bytes = b"") -> bytes:
        length = len(payload)
        if length < 126:
            header = bytes([0x89, length])
        else:
            header = bytes([0x89, 126]) + struct.pack("!H", length)
        return header + payload

    @staticmethod
    def encode_pong(payload: bytes = b"") -> bytes:
        length = len(payload)
        if length < 126:
            header = bytes([0x8A, length])
        else:
            header = bytes([0x8A, 126]) + struct.pack("!H", length)
        return header + payload

    @staticmethod
    def encode_close(code: int = 1000, reason: str = "") -> bytes:
        reason_bytes = reason.encode("utf-8")
        if len(reason_bytes) > 123:
            reason_bytes = reason_bytes[:123]
        frame = struct.pack("!H", code) + reason_bytes
        length = len(frame)
        if length < 126:
            header = bytes([0x88, length])
        else:
            header = bytes([0x88, 126]) + struct.pack("!H", length)
        return header + frame

    @staticmethod
    def decode_frame(data: bytes) -> Optional[Dict[str, Any]]:
        if len(data) < 2:
            return None
        byte1 = data[0]
        byte2 = data[1]
        fin = (byte1 >> 7) & 1
        opcode = byte1 & 0x0F
        masked = (byte2 >> 7) & 1
        payload_len = byte2 & 0x7F
        offset = 2

        if payload_len == 126:
            if len(data) < 4:
                return None
            payload_len = struct.unpack("!H", data[2:4])[0]
            offset = 4
        elif payload_len == 127:
            if len(data) < 10:
                return None
            payload_len = struct.unpack("!Q", data[2:10])[0]
            offset = 10

        if masked:
            if len(data) < offset + 4:
                return None
            mask_key = data[offset:offset + 4]
            offset += 4

        if len(data) < offset + payload_len:
            return None

        payload = data[offset:offset + payload_len]
        if masked:
            payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))

        return {
            "fin": fin,
            "opcode": opcode,
            "payload": payload,
            "payload_str": payload.decode("utf-8", errors="replace"),
            "payload_len": payload_len,
        }

    @staticmethod
    def handshake_response(key: str) -> str:
        GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
        return accept


# ─────────────────────────────────────────────
# BRP HTTP Handler (WebSocket upgrade)
# ─────────────────────────────────────────────

class BRPWebSocketHandler(BaseHTTPRequestHandler):
    """HTTP handler that upgrades to WebSocket for BRP connections."""

    protocol_version = "HTTP/1.1"

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/brp/ws":
            self._handle_websocket_upgrade()
        elif parsed.path == "/brp/health":
            self._handle_health()
        else:
            self.send_response(404)
            self.end_headers()

    def _handle_websocket_upgrade(self):
        upgrade = self.headers.get("Upgrade", "")
        if upgrade.lower() != "websocket":
            self.send_response(400)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Expected WebSocket upgrade")
            return

        websocket_key = self.headers.get("Sec-WebSocket-Key", "")
        if not websocket_key:
            self.send_response(400)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"Missing Sec-WebSocket-Key")
            return

        accept = WebSocketFrame.handshake_response(websocket_key)
        self.send_response(101)
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()
        self.server._handle_websocket_connection(self)

    def _handle_health(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        health = {
            "status": "ok",
            "service": "TokenRelay v3 BRP",
            "clients": len(getattr(self.server, '_brp', None).connected_clients if hasattr(self.server, '_brp') else {}),
        }
        self.wfile.write(json.dumps(health).encode())

    def log_message(self, format, *args):
        pass


# ─────────────────────────────────────────────
# BRP Server
# ─────────────────────────────────────────────

class BRPServer:
    """WebSocket server for BRP bidirectional real-time protocol.

    Uses MessageBroker for channel routing, CircuitBreaker for
    connection health, and EventBus for heartbeat events.
    """

    def __init__(self, config: Optional[BRPConfig] = None):
        self.config = config or BRPConfig()
        self.channel_manager = ChannelManager(
            max_channels=self.config.max_channels,
            registry=get_registry(),
        )
        self.connection_monitor = ConnectionMonitor(config=self.config)
        self.heartbeat_manager = HeartbeatManager(
            config=self.config,
            monitor=self.connection_monitor,
            on_disconnect=self._on_client_disconnect,
        )
        self.encoder = MessageEncoder()
        self.decoder = MessageDecoder()
        self._clients: Dict[str, Dict[str, Any]] = {}
        self._client_channels: Dict[str, int] = {}
        self._lock = threading.Lock()
        self._running = False
        self._server: Optional[HTTPServer] = None
        self._httpd_thread: Optional[threading.Thread] = None

    def start(self):
        """Start the BRP server."""
        if self._running:
            return
        self._running = True
        handler_class = _create_brp_handler(self)
        self._server = HTTPServer((self.config.host, self.config.port), handler_class)
        self.heartbeat_manager.start()
        self._httpd_thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._httpd_thread.start()
        logger.info(f"BRP Server started on {self.config.host}:{self.config.port}")

    def stop(self):
        """Stop the BRP server."""
        self._running = False
        self.heartbeat_manager.stop()
        if self._server:
            self._server.shutdown()
            self._server.server_close()
        logger.info("BRP Server stopped")

    def _handle_websocket_connection(self, handler):
        """Handle a new WebSocket connection."""
        client_id = str(uuid.uuid4())[:8]

        with self._lock:
            self._clients[client_id] = {
                "handler": handler,
                "connected_at": time.time(),
                "last_activity": time.time(),
                "user_id": None,
                "authenticated": False,
            }
            self._client_channels[client_id] = ChannelPriority.USER

        self.heartbeat_manager.register_client(client_id)
        self.connection_monitor.register_connection(client_id)

        self._send_system_message(client_id, {
            "event": "connected",
            "client_id": client_id,
            "server_time": time.time(),
        })
        logger.info(f"Client {client_id} connected")
        self._read_loop(client_id, handler)

    def _read_loop(self, client_id: str, handler):
        """Read messages from a WebSocket client."""
        buffer = b""
        rfile = handler.rfile
        wfile = handler.wfile

        while self._running:
            try:
                header = rfile.read(2)
                if not header:
                    break
                buffer += header
                payload_len = header[1] & 0x7F
                if payload_len == 126:
                    buffer += rfile.read(2)
                    payload_len = struct.unpack("!H", buffer[2:4])[0]
                elif payload_len == 127:
                    buffer += rfile.read(8)
                    payload_len = struct.unpack("!Q", buffer[2:10])[0]

                masked = (header[1] >> 7) & 1
                mask_key = b""
                if masked:
                    mask_key = rfile.read(4)
                    buffer += mask_key

                payload = rfile.read(payload_len)
                buffer += payload

                if len(buffer) >= 2:
                    frame_data = buffer[:2 + payload_len + (4 if masked else 0)]
                    buffer = buffer[2 + payload_len + (4 if masked else 0):]
                    decoded = WebSocketFrame.decode_frame(frame_data)
                    if decoded:
                        self._handle_frame(client_id, decoded, wfile)
            except (ConnectionResetError, BrokenPipeError):
                break
            except Exception as e:
                logger.debug(f"Error reading from {client_id}: {e}")
                break
        self.disconnect_client(client_id)

    def _handle_frame(self, client_id: str, frame: Dict, wfile):
        """Handle an incoming WebSocket frame."""
        opcode = frame["opcode"]
        payload_str = frame.get("payload_str", "")

        if opcode == FrameType.TEXT:
            self._handle_text_message(client_id, payload_str)
        elif opcode == FrameType.PING:
            pong = WebSocketFrame.encode_pong(payload_str.encode())
            try:
                wfile.write(pong)
                wfile.flush()
            except Exception:
                pass
            rtt = self._measure_rtt(client_id)
            self.heartbeat_manager.record_pong(client_id, rtt)
        elif opcode == FrameType.PONG:
            rtt = self._measure_rtt(client_id)
            self.heartbeat_manager.record_pong(client_id, rtt)
        elif opcode == FrameType.CLOSE:
            self.disconnect_client(client_id)
        elif opcode == FrameType.BINARY:
            try:
                msg = json.loads(payload_str)
                self._route_message(client_id, msg)
            except json.JSONDecodeError:
                pass

    def _handle_text_message(self, client_id: str, payload: str):
        """Handle a text WebSocket message."""
        try:
            msg = json.loads(payload)
            self._route_message(client_id, msg)
        except json.JSONDecodeError:
            self._send_to_channel(client_id, ChannelPriority.USER, {"type": "text", "content": payload})

    def _route_message(self, client_id: str, msg: Dict):
        """Route a message to the appropriate channel."""
        msg_type = msg.get("type", "message")
        target_channel = msg.get("channel", self._client_channels.get(client_id, ChannelPriority.USER))
        if target_channel not in range(self.config.max_channels):
            target_channel = ChannelPriority.USER
        self._send_to_channel(client_id, target_channel, msg)

    def _send_to_channel(self, client_id: int, channel: int, msg: Dict):
        """Send a message to a channel using ChannelManager."""
        self.channel_manager.enqueue(channel, msg, priority=channel)

    def _send_system_message(self, client_id: str, msg: Dict):
        """Send a system message to a specific client."""
        try:
            with self._lock:
                handler = self._clients.get(client_id, {}).get("handler")
            if handler:
                encoded = WebSocketFrame.encode_text(json.dumps(msg))
                handler.wfile.write(encoded)
                handler.wfile.flush()
        except Exception:
            pass

    def broadcast(self, channel: int, msg: Dict, exclude_client: Optional[str] = None):
        """Broadcast a message to all clients subscribed to a channel."""
        with self._lock:
            clients = [
                cid for cid, ch in self._client_channels.items()
                if ch == channel and cid != exclude_client
            ]
        for client_id in clients:
            self._send_system_message(client_id, msg)

    def send_control_message(self, client_id: str, msg: Dict):
        """Send a control message on the system channel (sub-50ms target)."""
        self._send_system_message(client_id, msg)
        self.connection_monitor.get_metrics(client_id)

    def _on_client_disconnect(self, client_id: str):
        """Callback when a client disconnects."""
        with self._lock:
            self._clients.pop(client_id, None)
            self._client_channels.pop(client_id, None)
        logger.info(f"Client {client_id} disconnected")

    def disconnect_client(self, client_id: str):
        """Disconnect a client."""
        self.heartbeat_manager.unregister_client(client_id)
        with self._lock:
            if client_id in self._clients:
                self._clients[client_id]["active"] = False
                del self._clients[client_id]
                self._client_channels.pop(client_id, None)
        logger.info(f"Client {client_id} disconnected")

    def _measure_rtt(self, client_id: str) -> float:
        """Measure round-trip time for a client (simulated)."""
        import random
        return random.uniform(5, 50)

    @property
    def connected_clients(self) -> int:
        """Get the number of connected clients."""
        with self._lock:
            return len(self._clients)

    def get_server_stats(self) -> Dict[str, Any]:
        """Get comprehensive server statistics."""
        return {
            "connected_clients": self.connected_clients,
            "total_channel_depth": self.channel_manager.total_depth,
            "channel_stats": {
                str(k): v.to_dict() for k, v in self.channel_manager.get_all_stats().items()
            },
            "quality_summary": self.connection_monitor.get_quality_summary(),
            "heartbeat_stats": self.heartbeat_manager.get_heartbeat_stats(),
            "server_running": self._running,
            "config": {
                "host": self.config.host,
                "port": self.config.port,
                "heartbeat_interval_ms": self.config.heartbeat_interval_ms,
                "control_message_target_ms": self.config.control_message_target_ms,
            },
        }


def _create_brp_handler(server: BRPServer) -> type:
    """Create a BRP handler class bound to the server instance."""
    class BoundHandler(BRPWebSocketHandler):
        def log_message(self, format, *args):
            pass
    BoundHandler.server = server
    server._brp = server
    return BoundHandler


# ─────────────────────────────────────────────
# BRP Client
# ─────────────────────────────────────────────

class BRPClient:
    """BRP client for connecting to a BRPServer.

    Provides full-duplex communication over WebSocket with
    multi-channel support, automatic heartbeat, and reconnection.
    """

    def __init__(self, config: Optional[BRPConfig] = None,
                 user_id: Optional[str] = None):
        self.config = config or BRPConfig()
        self.user_id = user_id or str(uuid.uuid4())[:8]
        self.client_id = str(uuid.uuid4())[:8]
        self.channel = ChannelPriority.USER
        self._connected = False
        self._lock = threading.Lock()
        self._receive_buffer = deque()
        self._callbacks: Dict[str, List[Callable]] = {}
        self._socket = None
        self._reader_thread: Optional[threading.Thread] = None
        self._running = False
        self._reconnect_count = 0
        self._last_pong_time = time.time()
        self._pending_pings: Dict[str, float] = {}

    def connect(self) -> bool:
        """Connect to the BRP server."""
        try:
            import socket
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.settimeout(5)
            host = self.config.host
            port = self.config.port
            key = base64.b64encode(uuid.uuid4().bytes).decode().rstrip("=")
            request = (
                f"GET /brp/ws HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                f"Upgrade: websocket\r\n"
                f"Connection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {key}\r\n"
                f"Sec-WebSocket-Version: 13\r\n"
                f"User-Agent: TokenRelay-BRP/1.0\r\n\r\n"
            )
            self._socket.connect((host, port))
            self._socket.sendall(request.encode())
            response = b""
            while b"\r\n\r\n" not in response:
                chunk = self._socket.recv(4096)
                if not chunk:
                    break
                response += chunk
            if b"101" not in response:
                self._socket.close()
                self._socket = None
                self._connected = False
                return False
            self._connected = True
            self._running = True
            self._reconnect_count = 0
            self._reader_thread = threading.Thread(target=self._receive_loop, daemon=True)
            self._reader_thread.start()
            self._start_heartbeat()
            logger.info(f"BRPClient {self.client_id} connected")
            return True
        except (ConnectionRefusedError, socket.timeout, OSError) as e:
            logger.warning(f"Connection failed: {e}")
            self._attempt_reconnect()
            return False

    def _attempt_reconnect(self):
        """Attempt to reconnect to the server."""
        if self._reconnect_count >= self.config.reconnect_attempts:
            logger.error(f"Max reconnection attempts reached")
            return
        self._reconnect_count += 1
        delay = self.config.reconnect_delay_ms / 1000.0
        logger.info(f"Reconnecting in {delay:.1f}s")
        time.sleep(delay)
        self.connect()

    def _receive_loop(self):
        """Background loop to receive WebSocket frames."""
        while self._running and self._socket:
            try:
                header = self._socket.recv(2)
                if not header:
                    break
                payload_len = header[1] & 0x7F
                if payload_len == 126:
                    header += self._socket.recv(2)
                    payload_len = struct.unpack("!H", header[2:4])[0]
                elif payload_len == 127:
                    header += self._socket.recv(8)
                    payload_len = struct.unpack("!Q", header[2:10])[0]
                masked = (header[1] >> 7) & 1
                mask_key = b""
                if masked:
                    mask_key = self._socket.recv(4)
                payload = self._socket.recv(payload_len)
                if masked:
                    payload = bytes(b ^ mask_key[i % 4] for i, b in enumerate(payload))
                decoded = WebSocketFrame.decode_frame(header + payload)
                if decoded:
                    self._handle_incoming_frame(decoded)
            except socket.timeout:
                continue
            except (ConnectionResetError, BrokenPipeError):
                break
            except Exception as e:
                logger.debug(f"Receive error: {e}")
                break
        self._connected = False

    def _handle_incoming_frame(self, frame: Dict):
        """Handle an incoming WebSocket frame."""
        opcode = frame["opcode"]
        payload_str = frame.get("payload_str", "")
        if opcode == FrameType.TEXT:
            try:
                msg = json.loads(payload_str)
                self._invoke_callbacks("message", msg)
            except json.JSONDecodeError:
                self._invoke_callbacks("text", payload_str)
        elif opcode == FrameType.PING:
            pong = WebSocketFrame.encode_pong(frame.get("payload", b""))
            self._send_raw(pong)
            self._last_pong_time = time.time()
        elif opcode == FrameType.PONG:
            rtt = (time.time() - self._pending_pings.get(
                frame.get("payload", b"").decode(), time.time()
            )) * 1000
            self._last_pong_time = time.time()
            self._invoke_callbacks("pong", {"rtt_ms": rtt})
        elif opcode == FrameType.CLOSE:
            self._connected = False
        elif opcode == FrameType.BINARY:
            try:
                msg = json.loads(payload_str)
                self._invoke_callbacks("binary_message", msg)
            except json.JSONDecodeError:
                self._invoke_callbacks("binary", payload_str)

    def _send_raw(self, data: bytes) -> bool:
        """Send raw bytes over the socket."""
        if not self._socket or not self._connected:
            return False
        try:
            self._socket.sendall(data)
            return True
        except (BrokenPipeError, ConnectionResetError, OSError):
            self._connected = False
            return False

    def send(self, message: Dict[str, Any], channel: Optional[int] = None) -> bool:
        """Send a message over the BRP protocol."""
        if not self._connected:
            if not self.connect():
                return False
        channel = channel if channel is not None else self.channel
        msg = {
            **message,
            "client_id": self.client_id,
            "user_id": self.user_id,
            "channel": channel,
            "timestamp": time.time(),
        }
        payload = json.dumps(msg)
        frame = WebSocketFrame.encode_text(payload)
        return self._send_raw(frame)

    def send_control(self, message: Dict[str, Any]) -> bool:
        """Send a control message on the system channel (sub-50ms)."""
        return self.send(message, channel=ChannelPriority.SYSTEM)

    def send_to_channel(self, channel: int, message: Dict[str, Any]) -> bool:
        """Send a message to a specific channel."""
        return self.send(message, channel=channel)

    def _start_heartbeat(self):
        """Start the client-side heartbeat."""
        def heartbeat_loop():
            while self._running and self._connected:
                time.sleep(self.config.heartbeat_interval_ms / 1000.0)
                if self._connected:
                    ping_payload = uuid.uuid4().hex[:8].encode()
                    self._pending_pings[ping_payload.decode()] = time.time()
                    frame = WebSocketFrame.encode_ping(ping_payload)
                    self._send_raw(frame)
                    elapsed = (time.time() - self._last_pong_time) * 1000
                    if elapsed > self.config.heartbeat_timeout_ms:
                        self._invoke_callbacks("heartbeat_missed", {"elapsed_ms": elapsed})
        self._heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self._heartbeat_thread.start()

    def on(self, event: str, callback: Callable):
        """Register a callback for an event type."""
        with self._lock:
            if event not in self._callbacks:
                self._callbacks[event] = []
            self._callbacks[event].append(callback)

    def off(self, event: str, callback: Callable):
        """Remove a callback for an event type."""
        with self._lock:
            if event in self._callbacks:
                try:
                    self._callbacks[event].remove(callback)
                except ValueError:
                    pass

    def _invoke_callbacks(self, event: str, data: Any):
        """Invoke all callbacks for an event."""
        with self._lock:
            callbacks = self._callbacks.get(event, [])
        for cb in callbacks:
            try:
                cb(data)
            except Exception:
                logger.exception(f"Callback error for {event}")

    @property
    def is_connected(self) -> bool:
        """Check if the client is connected."""
        return self._connected

    def disconnect(self):
        """Disconnect from the server."""
        self._running = False
        if self._socket:
            try:
                close_frame = WebSocketFrame.encode_close()
                self._send_raw(close_frame)
                self._socket.close()
            except Exception:
                pass
            self._socket = None
        self._connected = False

    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        return {
            "client_id": self.client_id,
            "user_id": self.user_id,
            "connected": self._connected,
            "channel": self.channel,
            "pending_pings": len(self._pending_pings),
            "last_pong_time": self._last_pong_time,
            "reconnect_count": self._reconnect_count,
        }


# ─────────────────────────────────────────────
# Convenience functions
# ─────────────────────────────────────────────

def create_brp_server(config: Optional[BRPConfig] = None) -> BRPServer:
    """Factory function to create and start a BRP server."""
    server = BRPServer(config)
    server.start()
    return server


def create_brp_client(config: Optional[BRPConfig] = None,
                       user_id: Optional[str] = None) -> BRPClient:
    """Factory function to create a BRP client."""
    return BRPClient(config, user_id)


# Need to store server reference on the handler
BRPWebSocketHandler.server = None
