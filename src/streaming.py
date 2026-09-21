"""
TokenRelay v2 Streaming Transport — SSE, WebSocket Simulation, Event System.

Provides:
- SSE (Server-Sent Events) endpoint support via http.server
- WebSocket connection simulation with heartbeat
- Event-driven callback system for token message routing
- Streaming batch processing with configurable batch sizes
- Backpressure handling via window-based flow control
"""

import json
import time
import threading
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable, Dict, List, Optional, Tuple

from codec import MessageEncoder, MessageDecoder
from registry import TokenRegistry, get_registry


# ─────────────────────────────────────────────
# Types & Enums
# ─────────────────────────────────────────────

class StreamEventType(Enum):
    """Event types emitted by the streaming system."""
    TOKEN_RECEIVED = "token_received"
    TOKEN_PROCESSED = "token_processed"
    BATCH_COMPLETE = "batch_complete"
    ERROR = "error"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    BACKPRESSURE_DROPPED = "backpressure_dropped"
    HEARTBEAT = "heartbeat"
    PROTOCOL_MIGRATED = "protocol_migrated"


class StreamFormat(Enum):
    """Output formats for streaming."""
    SSE = "sse"
    JSON = "json"
    WS = "websocket"


class BackpressureStrategy(Enum):
    """Backpressure handling strategies."""
    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"
    BLOCK = "block"
    REQUEUE = "requeue"


# ─────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────

@dataclass
class StreamEvent:
    """Represents a single streaming event."""
    event_type: StreamEventType
    data: Any
    timestamp: float = field(default_factory=time.time)
    source: str = "unknown"
    token_ids: Optional[List[int]] = None

    def to_sse(self) -> str:
        """Convert to Server-Sent Events format."""
        payload = {
            "event": self.event_type.value,
            "event_name": self.event_type.name,
            "data": self.data,
            "timestamp": self.timestamp,
            "source": self.source,
        }
        if self.token_ids:
            payload["token_ids"] = self.token_ids
        return f"data: {json.dumps(payload)}\n\n"

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps({
            "event": self.event_type.value,
            "data": self.data,
            "timestamp": self.timestamp,
            "source": self.source,
        })


@dataclass
class BatchConfig:
    """Configuration for streaming batch processing."""
    max_batch_size: int = 50
    flush_interval_ms: int = 100
    max_inflight: int = 1000


@dataclass
class BackpressureState:
    """Tracks backpressure window state."""
    window_size: int = 100
    current_count: int = 0
    dropped_count: int = 0
    strategy: BackpressureStrategy = BackpressureStrategy.DROP_OLDEST
    high_watermark: float = 0.8
    low_watermark: float = 0.3

    @property
    def utilization(self) -> float:
        """Current window utilization ratio."""
        return self.current_count / self.window_size if self.window_size > 0 else 0.0

    def can_accept(self) -> bool:
        """Check if new items can be accepted."""
        return self.current_count < self.window_size

    def record_accept(self):
        """Record an accepted item."""
        self.current_count += 1

    def record_release(self):
        """Record a released item."""
        self.current_count = max(0, self.current_count - 1)

    def record_drop(self):
        """Record a dropped item."""
        self.dropped_count += 1


# ─────────────────────────────────────────────
# Event Callback System
# ─────────────────────────────────────────────

class EventCallback:
    """A registered event callback with metadata."""

    def __init__(self, callback: Callable[[StreamEvent], None],
                 event_types: Optional[List[StreamEventType]] = None,
                 priority: int = 0,
                 filter_source: Optional[str] = None):
        self.callback = callback
        self.event_types = event_types or list(StreamEventType)
        self.priority = priority
        self.filter_source = filter_source
        self.call_count = 0
        self.last_called: Optional[float] = None

    def matches(self, event: StreamEvent) -> bool:
        """Check if this callback should fire for the given event."""
        if event.event_type not in self.event_types:
            return False
        if self.filter_source and event.source != self.filter_source:
            return False
        return True


class EventBus:
    """Event-driven callback bus for streaming events."""

    def __init__(self):
        self._callbacks: Dict[str, List[EventCallback]] = {}
        self._lock = threading.Lock()
        self._global_callbacks: List[EventCallback] = []

    def register(self, callback: Callable[[StreamEvent], None],
                 event_types: Optional[List[StreamEventType]] = None,
                 priority: int = 0,
                 filter_source: Optional[str] = None,
                 callback_id: Optional[str] = None) -> str:
        """Register a callback for streaming events. Returns callback ID."""
        cb_id = callback_id or str(uuid.uuid4())[:8]
        cb = EventCallback(callback, event_types, priority, filter_source)
        with self._lock:
            if event_types and len(event_types) > 0:
                # Type-specific callback
                for et in event_types:
                    key = et.value
                    if key not in self._callbacks:
                        self._callbacks[key] = []
                    self._callbacks[key].append(cb)
            else:
                # Global callback - fires for all events
                self._global_callbacks.append(cb)
            # Sort by priority (higher = first)
            self._global_callbacks.sort(key=lambda c: -c.priority)
        return cb_id

    def unregister(self, callback_id: str) -> bool:
        """Unregister a callback by ID. Returns True if found and removed."""
        with self._lock:
            for key, cbs in self._callbacks.items():
                self._callbacks[key] = [c for c in cbs if c.callback is not None
                                        and str(id(c.callback)) != callback_id
                                        or True]
                self._callbacks[key] = [c for c in cbs
                                        if not (hasattr(c, '_id') and c._id == callback_id)]
            self._global_callbacks = [c for c in self._global_callbacks
                                      if not (hasattr(c, '_id') and c._id == callback_id)]
        return True

    def emit(self, event: StreamEvent) -> int:
        """Emit an event to all matching callbacks. Returns number of handlers called."""
        with self._lock:
            count = 0
            called_ids = set()
            # Call type-specific callbacks
            key = event.event_type.value
            if key in self._callbacks:
                for cb in sorted(self._callbacks[key], key=lambda c: -c.priority):
                    if cb.matches(event):
                        called_ids.add(id(cb.callback))
                        try:
                            cb.callback(event)
                            cb.call_count += 1
                            cb.last_called = time.time()
                            count += 1
                        except Exception:
                            pass
            # Also call global callbacks that match (and weren't already called)
            for cb in self._global_callbacks:
                if id(cb.callback) in called_ids:
                    continue
                if cb.matches(event):
                    try:
                        cb.callback(event)
                        cb.call_count += 1
                        cb.last_called = time.time()
                        count += 1
                    except Exception:
                        pass
        return count

    def get_stats(self) -> Dict[str, Any]:
        """Get event bus statistics."""
        with self._lock:
            type_specific = sum(len(v) for v in self._callbacks.values())
            return {
                "registered_callbacks": len(self._global_callbacks) + type_specific,
                "event_types": len(self._callbacks),
                "total_by_type": {k: len(v) for k, v in self._callbacks.items()},
                "global_callbacks": len(self._global_callbacks),
            }


# ─────────────────────────────────────────────
# Streaming Core
# ─────────────────────────────────────────────

class StreamSession:
    """Represents an active streaming session."""

    def __init__(self, session_id: str, format: StreamFormat = StreamFormat.SSE):
        self.session_id = session_id
        self.format = format
        self.created_at = time.time()
        self.last_activity = time.time()
        self.active = True
        self.events_sent = 0
        self.token_count = 0
        self.metadata: Dict[str, Any] = {}

    def touch(self):
        """Update last activity timestamp."""
        self.last_activity = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "format": self.format.value,
            "active": self.active,
            "events_sent": self.events_sent,
            "token_count": self.token_count,
            "created_at": self.created_at,
            "last_activity": self.last_activity,
            "metadata": self.metadata,
        }


class StreamingProcessor:
    """Core streaming processor with batch processing and backpressure.

    Handles encoding token messages into streams, applying backpressure,
    and dispatching events through the EventBus.
    """

    def __init__(self, registry: Optional[TokenRegistry] = None,
                 encoder: Optional[MessageEncoder] = None,
                 batch_config: Optional[BatchConfig] = None):
        self.registry = registry or get_registry()
        self.encoder = encoder or MessageEncoder(self.registry)
        self.decoder = MessageDecoder(self.registry)
        self.batch_config = batch_config or BatchConfig()
        self.event_bus = EventBus()
        self.backpressure = BackpressureState()
        self._sessions: Dict[str, StreamSession] = {}
        self._batch_queues: Dict[str, deque] = {}
        self._lock = threading.Lock()
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None

        # Create default batch queues per session
        self._batch_queues["default"] = deque()

    def create_session(self, session_id: Optional[str] = None,
                       format: StreamFormat = StreamFormat.SSE) -> StreamSession:
        """Create a new streaming session."""
        sid = session_id or str(uuid.uuid4())[:8]
        session = StreamSession(sid, format)
        with self._lock:
            self._sessions[sid] = session
            if sid not in self._batch_queues:
                self._batch_queues[sid] = deque()
        self.event_bus.emit(StreamEvent(
            event_type=StreamEventType.CONNECTED,
            data={"session_id": sid, "format": format.value},
            source=sid
        ))
        return session

    def get_session(self, session_id: str) -> Optional[StreamSession]:
        """Get a session by ID."""
        return self._sessions.get(session_id)

    def close_session(self, session_id: str):
        """Close a streaming session."""
        with self._lock:
            if session_id in self._sessions:
                self._sessions[session_id].active = False
                del self._sessions[session_id]
        self.event_bus.emit(StreamEvent(
            event_type=StreamEventType.DISCONNECTED,
            data={"session_id": session_id},
            source=session_id
        ))

    def stream_token(self, session_id: str, token_ids: List[int],
                      payload: Optional[Dict] = None) -> Optional[StreamEvent]:
        """Stream a single token message through the processor.

        Applies backpressure and emits events. Returns the event or None
        if dropped due to backpressure.
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if not session or not session.active:
                return None

            # Check backpressure
            if not self.backpressure.can_accept():
                self.backpressure.record_drop()
                self.event_bus.emit(StreamEvent(
                    event_type=StreamEventType.BACKPRESSURE_DROPPED,
                    data={"session_id": session_id, "token_ids": token_ids},
                    source=session_id
                ))
                return None

            self.backpressure.record_accept()
            session.token_count += len(token_ids)

        # Encode the message
        msg = self.encoder.encode(str(token_ids[0]), payload or {})
        msg["tokens"] = token_ids

        event = StreamEvent(
            event_type=StreamEventType.TOKEN_RECEIVED,
            data=msg,
            source=session_id,
            token_ids=token_ids
        )
        self.event_bus.emit(event)

        # Process and emit processed event
        processed = self.decoder.decode(msg)
        event2 = StreamEvent(
            event_type=StreamEventType.TOKEN_PROCESSED,
            data=processed,
            source=session_id,
            token_ids=token_ids
        )
        self.event_bus.emit(event2)
        session.events_sent += 1
        session.touch()
        return event

    def stream_batch(self, session_id: str, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Stream a batch of token messages with batch processing.

        Returns batch statistics including count, processed count, and any dropped items.
        """
        with self._lock:
            session = self._sessions.get(session_id)
            if not session:
                return {"error": "session not found", "processed": 0}
            queue = self._batch_queues.get(session_id, deque())

        results = []
        dropped = 0

        for msg_data in messages:
            token_ids = msg_data.get("tokens", [])
            payload = msg_data.get("payload", {})

            event = self.stream_token(session_id, token_ids, payload)
            if event:
                results.append(event)
            else:
                dropped += 1

        # Emit batch complete
        self.event_bus.emit(StreamEvent(
            event_type=StreamEventType.BATCH_COMPLETE,
            data={
                "session_id": session_id,
                "total": len(messages),
                "processed": len(results),
                "dropped": dropped,
            },
            source=session_id
        ))

        self.backpressure.record_release()
        return {
            "session_id": session_id,
            "total": len(messages),
            "processed": len(results),
            "dropped": dropped,
            "backpressure_utilization": round(self.backpressure.utilization, 3),
        }

    def get_backpressure_state(self) -> Dict[str, Any]:
        """Get current backpressure state."""
        return {
            "window_size": self.backpressure.window_size,
            "current_count": self.backpressure.current_count,
            "utilization": round(self.backpressure.utilization, 3),
            "dropped_count": self.backpressure.dropped_count,
            "strategy": self.backpressure.strategy.value,
            "high_watermark": self.backpressure.high_watermark,
            "low_watermark": self.backpressure.low_watermark,
        }

    def set_backpressure_strategy(self, strategy: BackpressureStrategy,
                                   window_size: Optional[int] = None):
        """Configure backpressure handling strategy."""
        self.backpressure.strategy = strategy
        if window_size is not None:
            self.backpressure.window_size = window_size

    def start_background_processor(self, interval_ms: int = 50):
        """Start a background thread for batch processing."""
        self._running = True
        self._worker_thread = threading.Thread(
            target=self._background_loop, args=(interval_ms,), daemon=True
        )
        self._worker_thread.start()

    def stop_background_processor(self):
        """Stop the background processor."""
        self._running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=2)

    def _background_loop(self, interval_ms: int):
        """Background processing loop."""
        while self._running:
            time.sleep(interval_ms / 1000)
            # Flush any pending batches
            with self._lock:
                for sid, queue in self._batch_queues.items():
                    if queue and sid in self._sessions:
                        count = len(queue)
                        if count > 0:
                            batch = list(queue)
                            queue.clear()
                            # Process batch
                            self.stream_batch(sid, batch)

    def format_event(self, event: StreamEvent,
                     fmt: Optional[StreamFormat] = None) -> str:
        """Format an event according to the specified format."""
        format_type = fmt or (self._sessions.get(
            event.source, StreamSession("x", StreamFormat.SSE)).format)
        if format_type == StreamFormat.SSE:
            return event.to_sse()
        return event.to_json()


# ─────────────────────────────────────────────
# SSE Endpoint (http.server based)
# ─────────────────────────────────────────────

class SSEHandler(BaseHTTPRequestHandler):
    """HTTP request handler that serves SSE streams for token events.

    Handles GET /stream for SSE, POST /message for sending tokens,
    GET /events for event log, and GET /backpressure for status.
    """

    # Store references to the streaming processor (class-level)
    processor: Optional[StreamingProcessor] = None

    def log_message(self, format, *args):
        """Suppress default logging."""
        pass

    def _send_headers(self, status=200, content_type="application/json",
                       extra_headers=None):
        """Send HTTP response headers."""
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        if extra_headers:
            for k, v in extra_headers.items():
                self.send_header(k, v)
        self.end_headers()

    def do_OPTIONS(self):
        """Handle CORS preflight."""
        self._send_headers(200, "text/plain")
        self.end_headers()

    def do_GET(self):
        """Handle GET requests."""
        if self.path == "/stream":
            self._handle_stream()
        elif self.path == "/events":
            self._handle_events()
        elif self.path == "/backpressure":
            self._handle_backpressure()
        elif self.path == "/sessions":
            self._handle_sessions()
        else:
            self._send_headers(404)
            self.wfile.write(json.dumps({"error": "Not found"}).encode())

    def do_POST(self):
        """Handle POST requests."""
        if self.path == "/message":
            self._handle_post_message()
        elif self.path == "/batch":
            self._handle_post_batch()
        elif self.path.startswith("/stream/"):
            self._handle_stream_connect()
        else:
            self._send_headers(404)
            self.wfile.write(json.dumps({"error": "Not found"}).encode())

    def _handle_stream(self):
        """SSE endpoint: streams token events continuously."""
        session_id = self._get_query_param("session_id") or "default"
        if not SSEHandler.processor:
            self._send_headers(503)
            self.wfile.write(json.dumps({"error": "Processor not initialized"}).encode())
            return

        session = SSEHandler.processor.create_session(session_id, StreamFormat.SSE)
        self._send_headers(
            200,
            "text/event-stream",
            {
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            }
        )

        # Send initial connection event
        self.wfile.write(
            f"data: {json.dumps({'event': 'connected', 'session_id': session_id})}\n\n".encode()
        )
        self.wfile.flush()

        # Stream events for a limited duration
        start_time = time.time()
        timeout = 30  # seconds
        while time.time() - start_time < timeout:
            if not session.active:
                break
            # Send a heartbeat every 5 seconds
            if int(time.time() - start_time) % 5 == 0:
                self.wfile.write(
                    f"data: {json.dumps({'event': 'heartbeat', 'timestamp': time.time()})}\n\n".encode()
                )
                self.wfile.flush()
            time.sleep(0.5)

        SSEHandler.processor.close_session(session_id)

    def _handle_events(self):
        """Return recent event log as JSON."""
        if not SSEHandler.processor:
            self._send_headers(503)
            return
        stats = SSEHandler.processor.event_bus.get_stats()
        self._send_headers(200)
        self.wfile.write(json.dumps(stats).encode())

    def _handle_backpressure(self):
        """Return backpressure state."""
        if not SSEHandler.processor:
            self._send_headers(503)
            return
        state = SSEHandler.processor.get_backpressure_state()
        self._send_headers(200)
        self.wfile.write(json.dumps(state).encode())

    def _handle_sessions(self):
        """Return active sessions."""
        if not SSEHandler.processor:
            self._send_headers(503)
            return
        sessions = [s.to_dict() for s in SSEHandler.processor._sessions.values()]
        self._send_headers(200)
        self.wfile.write(json.dumps(sessions).encode())

    def _handle_post_message(self):
        """Handle POST /message — accept a token message and stream it."""
        if not SSEHandler.processor:
            self._send_headers(503)
            return
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            data = json.loads(body)
            token_ids = data.get("tokens", [0])
            payload = data.get("payload", {})
            session_id = data.get("session_id", "default")
            session = SSEHandler.processor.create_session(session_id, StreamFormat.JSON)
            event = SSEHandler.processor.stream_token(session_id, token_ids, payload)
            if event:
                self._send_headers(200)
                self.wfile.write(json.dumps({
                    "status": "streamed",
                    "event": event.event_type.value,
                    "token_ids": token_ids,
                }).encode())
            else:
                self._send_headers(503)
                self.wfile.write(json.dumps({
                    "status": "dropped",
                    "reason": "backpressure"
                }).encode())
        except (json.JSONDecodeError, ValueError) as e:
            self._send_headers(400)
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def _handle_post_batch(self):
        """Handle POST /batch — accept multiple token messages."""
        if not SSEHandler.processor:
            self._send_headers(503)
            return
        try:
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            data = json.loads(body)
            messages = data.get("messages", [])
            session_id = data.get("session_id", "default")
            result = SSEHandler.processor.stream_batch(session_id, messages)
            self._send_headers(200)
            self.wfile.write(json.dumps(result).encode())
        except (json.JSONDecodeError, ValueError) as e:
            self._send_headers(400)
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def _handle_stream_connect(self):
        """Handle WebSocket-style connection simulation."""
        session_id = self.path.split("/")[-1] or "default"
        if not SSEHandler.processor:
            return
        session = SSEHandler.processor.create_session(session_id, StreamFormat.WS)
        self._send_headers(200, "application/json")
        self.wfile.write(json.dumps({
            "status": "connected",
            "session_id": session_id,
            "format": "websocket_simulation"
        }).encode())

    def _get_query_param(self, name: str) -> Optional[str]:
        """Extract a query parameter from the path."""
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        return params.get(name, [None])[0]


def start_sse_server(port: int = 8765,
                     processor: Optional[StreamingProcessor] = None) -> HTTPServer:
    """Start an SSE HTTP server for TokenRelay streaming.

    Args:
        port: Port to bind to.
        processor: StreamingProcessor instance. If None, creates a default one.

    Returns:
        The started HTTPServer instance.
    """
    if processor:
        SSEHandler.processor = processor
    else:
        SSEHandler.processor = StreamingProcessor()

    server = HTTPServer(("127.0.0.1", port), SSEHandler)
    return server


class WebSocketSimulator:
    """Simulates WebSocket connections using HTTP long-polling.

    Since Python stdlib doesn't include WebSocket, this provides a
    compatible interface for bidirectional communication over HTTP.
    """

    def __init__(self, processor: StreamingProcessor):
        self.processor = processor
        self._connections: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()

    def connect(self, client_id: str, session_id: str) -> str:
        """Simulate a WebSocket connection."""
        # Ensure the session exists on the processor
        if self.processor.get_session(session_id) is None:
            self.processor.create_session(session_id, StreamFormat.JSON)
        with self._lock:
            self._connections[client_id] = {
                "session_id": session_id,
                "connected_at": time.time(),
                "last_ping": time.time(),
                "messages": deque(maxlen=100),
                "active": True,
            }
        return client_id

    def disconnect(self, client_id: str):
        """Simulate a WebSocket disconnection."""
        with self._lock:
            if client_id in self._connections:
                self._connections[client_id]["active"] = False
                del self._connections[client_id]

    def send(self, client_id: str, token_ids: List[int],
              payload: Optional[Dict] = None) -> bool:
        """Send a token message through a WebSocket-like connection."""
        with self._lock:
            conn = self._connections.get(client_id)
            if not conn or not conn["active"]:
                return False
            session_id = conn["session_id"]

        event = self.processor.stream_token(session_id, token_ids, payload)
        if event:
            conn["messages"].append(event.to_json())
            conn["last_ping"] = time.time()
            return True
        return False

    def receive(self, client_id: str) -> List[str]:
        """Receive queued messages for a client."""
        with self._lock:
            conn = self._connections.get(client_id)
            if not conn:
                return []
            messages = list(conn["messages"])
            conn["messages"].clear()
            return messages

    def ping(self, client_id: str) -> bool:
        """Ping a WebSocket connection to check liveness."""
        with self._lock:
            conn = self._connections.get(client_id)
            if not conn or not conn["active"]:
                return False
            conn["last_ping"] = time.time()
            return True

    def get_active_connections(self) -> int:
        """Get the number of active WebSocket-like connections."""
        with self._lock:
            return sum(1 for c in self._connections.values() if c["active"])

    def broadcast(self, token_ids: List[int],
                   payload: Optional[Dict] = None) -> int:
        """Broadcast a token message to all active connections."""
        count = 0
        with self._lock:
            for client_id, conn in list(self._connections.items()):
                if conn["active"]:
                    session_id = conn["session_id"]
                    event = self.processor.stream_token(session_id, token_ids, payload)
                    if event:
                        conn["messages"].append(event.to_json())
                        count += 1
        return count
