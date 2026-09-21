"""Tests for the TokenRelay v2 Streaming Transport system."""

import sys
import os
import json
import time
import threading
from pathlib import Path

# Ensure src is on the path
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

from streaming import (
    StreamEventType, StreamFormat, BackpressureStrategy,
    StreamEvent, BatchConfig, BackpressureState,
    EventCallback, EventBus, StreamSession, StreamingProcessor,
    SSEHandler, start_sse_server, WebSocketSimulator,
)
from codec import MessageEncoder, MessageDecoder
from registry import get_registry


# ─────────────────────────────────────────────
# StreamEvent Tests
# ─────────────────────────────────────────────

def test_stream_event_to_sse():
    """StreamEvent.to_sse() produces valid SSE format."""
    event = StreamEvent(
        event_type=StreamEventType.TOKEN_RECEIVED,
        data={"tokens": [42]},
        source="test_session",
        token_ids=[42],
    )
    sse = event.to_sse()
    assert "data:" in sse
    assert "TOKEN_RECEIVED" in sse
    assert "\n\n" in sse
    print("✓ test_stream_event_to_sse")


def test_stream_event_to_json():
    """StreamEvent.to_json() produces valid JSON."""
    event = StreamEvent(
        event_type=StreamEventType.ERROR,
        data={"error": "test"},
        source="test",
    )
    json_str = event.to_json()
    parsed = json.loads(json_str)
    assert parsed["event"] == "error"
    assert parsed["data"]["error"] == "test"
    print("✓ test_stream_event_to_json")


# ─────────────────────────────────────────────
# BackpressureState Tests
# ─────────────────────────────────────────────

def test_backpressure_state():
    """BackpressureState tracks utilization correctly."""
    bp = BackpressureState(window_size=100)
    assert bp.can_accept()
    assert bp.utilization == 0.0

    for _ in range(80):
        bp.record_accept()
    assert bp.utilization == 0.8
    assert bp.can_accept()

    bp.record_drop()
    assert bp.dropped_count == 1

    bp.record_release()
    assert bp.current_count == 79
    print("✓ test_backpressure_state")


def test_backpressure_watermarks():
    """Backpressure respects high/low watermarks."""
    bp = BackpressureState(window_size=100, high_watermark=0.8, low_watermark=0.3)
    for _ in range(80):
        bp.record_accept()
    assert bp.utilization == 0.8
    # At high watermark, still accepts (can_accept checks < window_size)
    assert bp.can_accept()
    print("✓ test_backpressure_watermarks")


# ─────────────────────────────────────────────
# EventBus Tests
# ─────────────────────────────────────────────

def test_event_bus_register_and_emit():
    """EventBus.register() and emit() work correctly."""
    bus = EventBus()
    calls = []

    def callback(event):
        calls.append(event.event_type.value)

    cb_id = bus.register(callback, event_types=[StreamEventType.TOKEN_RECEIVED])
    event = StreamEvent(
        event_type=StreamEventType.TOKEN_RECEIVED,
        data={}, source="test", token_ids=[42]
    )
    count = bus.emit(event)
    assert count >= 1
    assert StreamEventType.TOKEN_RECEIVED.value in calls
    print("✓ test_event_bus_register_and_emit")


def test_event_bus_stats():
    """EventBus.get_stats() returns correct counts."""
    bus = EventBus()
    bus.register(lambda e: None, event_types=[StreamEventType.CONNECTED])
    bus.register(lambda e: None, event_types=[StreamEventType.DISCONNECTED])
    stats = bus.get_stats()
    assert stats["registered_callbacks"] == 2
    print("✓ test_event_bus_stats")


def test_event_bus_multiple_event_types():
    """Callbacks fire only for their registered event types."""
    bus = EventBus()
    token_calls = []
    error_calls = []

    bus.register(lambda e: token_calls.append(1),
                 event_types=[StreamEventType.TOKEN_RECEIVED])
    bus.register(lambda e: error_calls.append(1),
                 event_types=[StreamEventType.ERROR])

    bus.emit(StreamEvent(StreamEventType.TOKEN_RECEIVED, {}, "test"))
    bus.emit(StreamEvent(StreamEventType.ERROR, {}, "test"))

    assert len(token_calls) == 1
    assert len(error_calls) == 1
    print("✓ test_event_bus_multiple_event_types")


# ─────────────────────────────────────────────
# StreamingProcessor Tests
# ─────────────────────────────────────────────

def test_streaming_processor_create_session():
    """StreamingProcessor.create_session() works."""
    proc = StreamingProcessor()
    session = proc.create_session("test_session", StreamFormat.SSE)
    assert session.session_id == "test_session"
    assert session.format == StreamFormat.SSE
    assert session.active
    print("✓ test_streaming_processor_create_session")


def test_streaming_processor_stream_token():
    """StreamingProcessor.stream_token() encodes and emits events."""
    proc = StreamingProcessor()
    proc.create_session("test", StreamFormat.JSON)
    event = proc.stream_token("test", [42], {"result": "success"})
    assert event is not None
    assert event.event_type == StreamEventType.TOKEN_RECEIVED
    assert event.token_ids == [42]
    print("✓ test_streaming_processor_stream_token")


def test_streaming_processor_backpressure_drop():
    """StreamingProcessor drops messages when backpressure is active."""
    proc = StreamingProcessor()
    proc.create_session("test", StreamFormat.JSON)
    proc.backpressure.window_size = 1  # Very small window
    proc.backpressure.record_accept()  # Fill the window

    event = proc.stream_token("test", [42], {"result": "success"})
    assert event is None  # Dropped due to backpressure
    print("✓ test_streaming_processor_backpressure_drop")


def test_streaming_processor_stream_batch():
    """StreamingProcessor.stream_batch() processes multiple messages."""
    proc = StreamingProcessor()
    proc.create_session("test", StreamFormat.JSON)

    messages = [
        {"tokens": [42], "payload": {"result": "a"}},
        {"tokens": [0], "payload": {"error_code": "ERR"}},
        {"tokens": [151], "payload": {"attention_level": "high"}},
    ]
    result = proc.stream_batch("test", messages)
    assert result["processed"] == 3
    assert result["total"] == 3
    assert result["dropped"] == 0
    print("✓ test_streaming_processor_stream_batch")


def test_streaming_processor_batch_with_backpressure():
    """StreamingProcessor drops messages in batch under backpressure."""
    proc = StreamingProcessor()
    proc.create_session("test", StreamFormat.JSON)
    proc.backpressure.window_size = 2
    proc.backpressure.record_accept()  # Fill one slot, leave room for one

    messages = [
        {"tokens": [42]},
        {"tokens": [0]},
    ]
    result = proc.stream_batch("test", messages)
    assert result["processed"] == 1
    assert result["dropped"] == 1
    print("✓ test_streaming_processor_batch_with_backpressure")


def test_streaming_processor_backpressure_state():
    """StreamingProcessor.get_backpressure_state() returns correct state."""
    proc = StreamingProcessor()
    proc.backpressure.window_size = 200
    proc.backpressure.current_count = 100
    state = proc.get_backpressure_state()
    assert state["window_size"] == 200
    assert state["current_count"] == 100
    assert state["utilization"] == 0.5
    print("✓ test_streaming_processor_backpressure_state")


def test_streaming_processor_set_backpressure_strategy():
    """StreamingProcessor.set_backpressure_strategy() configures strategy."""
    proc = StreamingProcessor()
    proc.set_backpressure_strategy(BackpressureStrategy.DROP_NEWEST, window_size=50)
    assert proc.backpressure.strategy == BackpressureStrategy.DROP_NEWEST
    assert proc.backpressure.window_size == 50
    print("✓ test_streaming_processor_set_backpressure_strategy")


def test_streaming_processor_close_session():
    """StreamingProcessor.close_session() marks session inactive."""
    proc = StreamingProcessor()
    proc.create_session("test", StreamFormat.JSON)
    proc.close_session("test")
    session = proc.get_session("test")
    assert session is None or not session.active
    print("✓ test_streaming_processor_close_session")


def test_streaming_processor_format_event():
    """StreamingProcessor.format_event() returns correct format."""
    proc = StreamingProcessor()
    proc.create_session("test", StreamFormat.JSON)
    event = StreamEvent(StreamEventType.CONNECTED, {}, "test")
    sse_str = proc.format_event(event, StreamFormat.SSE)
    assert "data:" in sse_str
    json_str = proc.format_event(event, StreamFormat.JSON)
    assert json_str.startswith("{")
    print("✓ test_streaming_processor_format_event")


# ─────────────────────────────────────────────
# StreamSession Tests
# ─────────────────────────────────────────────

def test_stream_session():
    """StreamSession tracks state correctly."""
    session = StreamSession("sess_1", StreamFormat.SSE)
    assert session.session_id == "sess_1"
    assert session.active
    assert session.format == StreamFormat.SSE
    session.touch()
    assert session.last_activity > 0
    d = session.to_dict()
    assert d["session_id"] == "sess_1"
    print("✓ test_stream_session")


# ─────────────────────────────────────────────
# WebSocketSimulator Tests
# ─────────────────────────────────────────────

def test_websocket_simulator_connect():
    """WebSocketSimulator.connect() registers a connection."""
    from streaming import StreamingProcessor
    proc = StreamingProcessor()
    ws = WebSocketSimulator(proc)
    ws.connect("client_1", "session_1")
    assert ws.get_active_connections() == 1
    assert ws.ping("client_1")
    print("✓ test_websocket_simulator_connect")


def test_websocket_simulator_send_receive():
    """WebSocketSimulator.send() and receive() work."""
    from streaming import StreamingProcessor
    proc = StreamingProcessor()
    ws = WebSocketSimulator(proc)
    ws.connect("client_1", "default")
    sent = ws.send("client_1", [42], {"result": "success"})
    assert sent is True
    messages = ws.receive("client_1")
    assert len(messages) >= 1
    print("✓ test_websocket_simulator_send_receive")


def test_websocket_simulator_disconnect():
    """WebSocketSimulator.disconnect() removes a connection."""
    from streaming import StreamingProcessor
    proc = StreamingProcessor()
    ws = WebSocketSimulator(proc)
    ws.connect("client_1", "default")
    ws.disconnect("client_1")
    assert ws.get_active_connections() == 0
    print("✓ test_websocket_simulator_disconnect")


def test_websocket_simulator_broadcast():
    """WebSocketSimulator.broadcast() sends to all connections."""
    from streaming import StreamingProcessor
    proc = StreamingProcessor()
    ws = WebSocketSimulator(proc)
    ws.connect("client_1", "default")
    ws.connect("client_2", "default")
    count = ws.broadcast([42], {"result": "test"})
    assert count == 2
    print("✓ test_websocket_simulator_broadcast")


# ─────────────────────────────────────────────
# SSEHandler Tests
# ─────────────────────────────────────────────

def test_sse_handler_processor_set():
    """SSEHandler.processor can be set and retrieved."""
    from streaming import StreamingProcessor
    proc = StreamingProcessor()
    SSEHandler.processor = proc
    assert SSEHandler.processor is proc
    print("✓ test_sse_handler_processor_set")


def test_sse_handler_get_query_param():
    """SSEHandler._get_query_param() parses query strings."""
    handler = SSEHandler.__new__(SSEHandler)
    handler.path = "/stream?session_id=abc123"
    result = handler._get_query_param("session_id")
    assert result == "abc123"
    result2 = handler._get_query_param("missing")
    assert result2 is None
    print("✓ test_sse_handler_get_query_param")


# ─────────────────────────────────────────────
# SSE Server Tests
# ─────────────────────────────────────────────

def test_start_sse_server():
    """start_sse_server() creates an HTTPServer instance."""
    from streaming import StreamingProcessor
    proc = StreamingProcessor()
    server = start_sse_server(port=18765, processor=proc)
    assert server is not None
    assert server.server_address == ("127.0.0.1", 18765)
    server.server_close()
    print("✓ test_start_sse_server")


# ─────────────────────────────────────────────
# Integration Tests
# ─────────────────────────────────────────────

def test_full_streaming_pipeline():
    """End-to-end streaming: create session, stream tokens, check backpressure."""
    proc = StreamingProcessor()
    session = proc.create_session("integration_test", StreamFormat.JSON)

    # Stream several tokens
    for token_id in [42, 151, 0]:
        event = proc.stream_token("integration_test", [token_id])
        assert event is not None

    # Check stats
    bp_state = proc.get_backpressure_state()
    assert bp_state["current_count"] > 0

    # Stream batch
    batch_messages = [
        {"tokens": [42], "payload": {"step": 1}},
        {"tokens": [153], "payload": {"phase": "done"}},
    ]
    result = proc.stream_batch("integration_test", batch_messages)
    assert result["processed"] == 2

    print("✓ test_full_streaming_pipeline")


def test_event_bus_with_processor():
    """EventBus integrates with StreamingProcessor."""
    proc = StreamingProcessor()
    events_received = []

    def on_event(event):
        events_received.append(event.event_type.value)

    proc.event_bus.register(on_event, event_types=list(StreamEventType))
    proc.create_session("test", StreamFormat.JSON)
    proc.stream_token("test", [42], {"result": "ok"})

    assert len(events_received) > 0
    assert any(e == "token_received" for e in events_received)
    assert any(e == "token_processed" for e in events_received)
    print("✓ test_event_bus_with_processor")


# ─────────────────────────────────────────────
# Run All Tests
# ─────────────────────────────────────────────

def run_all_tests():
    """Run all streaming tests."""
    tests = [
        test_stream_event_to_sse,
        test_stream_event_to_json,
        test_backpressure_state,
        test_backpressure_watermarks,
        test_event_bus_register_and_emit,
        test_event_bus_stats,
        test_event_bus_multiple_event_types,
        test_streaming_processor_create_session,
        test_streaming_processor_stream_token,
        test_streaming_processor_backpressure_drop,
        test_streaming_processor_stream_batch,
        test_streaming_processor_batch_with_backpressure,
        test_streaming_processor_backpressure_state,
        test_streaming_processor_set_backpressure_strategy,
        test_streaming_processor_close_session,
        test_streaming_processor_format_event,
        test_stream_session,
        test_websocket_simulator_connect,
        test_websocket_simulator_send_receive,
        test_websocket_simulator_disconnect,
        test_websocket_simulator_broadcast,
        test_sse_handler_processor_set,
        test_sse_handler_get_query_param,
        test_start_sse_server,
        test_full_streaming_pipeline,
        test_event_bus_with_processor,
    ]

    print("=" * 72)
    print("  TokenRelay Streaming Transport — Test Suite")
    print("=" * 72)
    print()

    passed = 0
    failed = 0
    errors = []

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            failed += 1
            errors.append(f"{test.__name__}: {e}")
            print(f"✗ {test.__name__}: {e}")

    print(f"\n  {passed}/{passed + failed} tests passed")
    if errors:
        print("\n  Failures:")
        for err in errors:
            print(f"    - {err}")
    print("=" * 72)
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)