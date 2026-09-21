# TokenRelay Documentation

## Table of Contents

1. [Introduction](#introduction)
2. [Quick Start](#quick-start)
3. [Architecture](#architecture)
4. [Token Protocol](#token-protocol)
5. [Usage in Python](#usage-in-python)
6. [Usage with CLI](#usage-with-cli)
7. [Benchmark — Why Is It Faster?](#benchmark--why-is-it-faster)
8. [Subagent Integration](#subagent-integration)
9. [Extending the Protocol](#extending-the-protocol)
10. [Troubleshooting](#troubleshooting)
11. [License](#license)
12. [TokenRelay v2 — Advanced Features](#tokenrelay-v2--advanced-features)
13. [v2 Testing Suite](#v2-testing-suite)

---

## TokenRelay v2 — Advanced Features

### Overview

TokenRelay v2 extends the core protocol with eight major feature areas:

| Feature | Description |
|---------|-------------|
| **Compression** | zlib-based payload compression reducing message size |
| **Signing** | HMAC-SHA256 message integrity verification |
| **Encryption** | AES-256-GCM optional end-to-end encryption |
| **Broker** | Priority queue with dedup and dead letter queue |
| **Circuit Breaker** | Fault tolerance with exponential backoff and recovery |
| **Streaming** | SSE, WebSocket, and event-driven transport |
| **Plugin System** | Extensible protocol definitions with schema versioning |
| **Benchmark v2** | Scalability testing, latency percentiles, memory profiling |

### Compression

Compress token payloads to reduce bandwidth and storage:

```python
from src.protocol_v2 import compress_payload, decompress_payload, compression_ratio

payload = {"result": "success", "data": "x" * 1000}
compressed = compress_payload(payload, level=6)
ratio = compression_ratio(payload, compressed)
decompressed = decompress_payload(compressed)
assert decompressed == payload
```

### Signing

Ensure message integrity with HMAC-SHA256:

```python
from src.protocol_v2 import MessageSigner

signer = MessageSigner()
message = {"tokens": [42], "payload": {"result": "ok"}}
signed = signer.sign_message(message)
verification = signer.verify_message(signed)
assert verification["valid"]
```

### Encryption

Optional AES-256-GCM encryption for sensitive payloads:

```python
from src.protocol_v2 import MessageEncryptor, EncryptionConfig

config = EncryptionConfig(enabled=True)
encryptor = MessageEncryptor(config)
encryptor.set_key(b"x" * 32)
encrypted = encryptor.encrypt(message)
decrypted = encryptor.decrypt(encrypted)
```

### Message Broker

Priority-based message routing with deduplication and DLQ:

```python
from src.broker import MessageBroker, PRIORITY_CRITICAL, PRIORITY_HIGH

broker = MessageBroker(max_depth=10000)
broker.enqueue({"tokens": [42]}, priority=PRIORITY_CRITICAL)
msg = broker.dequeue()
broker.send_to_dlq(msg, reason="test failure")
stats = broker.get_metrics()
```

### Circuit Breaker

Automatic fault detection and recovery:

```python
from src.circuit_breaker import CircuitBreaker

cb = CircuitBreaker(failure_threshold=5, recovery_timeout=30)

def call_service():
    return risky_call()

try:
    result = cb.call(call_service)
except CircuitBreakerError:
    # Circuit is OPEN, service is unhealthy
    pass

cb.reset()  # Manual reset
```

### Streaming Transport

Real-time event-driven messaging:

```python
from src.streaming import StreamingProcessor, StreamEvent, StreamEventType

processor = StreamingProcessor()
session = processor.create_session("my_session")

def on_token(event):
    print(f"Token received: {event.token_ids}")

processor.event_bus.register(on_token, event_types=[StreamEventType.TOKEN_RECEIVED])
event = processor.stream_token("my_session", [42], {"result": "ok"})
```

### Plugin System

Register custom token types and protocols:

```python
from src.plugin import register_protocol, PluginRegistry, Plugin

# Register a custom token
register_protocol(
    token_id="999",
    meaning="Custom token",
    protocol_type="custom",
    routing_priority=50,
)

# Plugin class
class MyPlugin(Plugin):
    @property
    def manifest(self): ...
    def register(self, registry): ...
    def unregister(self, registry): ...
```

### Benchmark v2

Comprehensive performance testing:

```python
from src.benchmark_v2 import BenchmarkV2, Protocol

bench = BenchmarkV2()

# Run scalability test
results = bench.run_scalability_test(Protocol.TOKEN_RELAY)

# Comparative analysis
comparison = bench.run_comparative_benchmark(message_count=1000)

# Full suite
suite = bench.run_full_benchmark_suite()
```

### v1 → v2 Compatibility

All v1 features remain fully backward compatible. v2 features are opt-in:

| v1 Feature | v2 Extension | Compatibility |
|------------|-------------|---------------|
| `MessageEncoder` | `compress_message()` | Backward compatible |
| `MessageDecoder` | `decrypt_message()` | Backward compatible |
| `TokenRelayBridge` | `MessageBroker` | Independent |
| `TokenRegistry` | `PluginRegistry` | Independent |
| `tests/benchmark.py` | `src/benchmark_v2.py` | Both functional |

---

## TokenRelay v3 — Adaptive Token Compression (ATC)

### Overview

TokenRelay v3 introduces Adaptive Token Compression to dynamically reduce
input token consumption based on user intent classification. User queries
are classified into 5 intents, each with a different compression strategy.

|| Intent | Description | Target Compression |
||--------|----------|-------------------|
|| **query** | Quick questions ("What is...?", "How do I...?") | 90% |
|| **command** | Instructions ("Build...", "Run...", "Create...") | 70% |
|| **request** | Polite requests ("Could you...", "I need...") | 50% |
|| **feedback** | Responses/evaluations ("Works great", "Fixed the bug") | 30% |
|| **system** | Configuration/system commands ("Set the port...", "Config...") | 10% |

### Features

- **IntentClassifier**: Weighted keyword matching classifying text into
  5 intents with confidence scores
- **AdaptiveCompressor**: Intent-aware compression using stop-word
  stripping, keyword preservation, and zlib encoding
- **TokenSavingsTracker**: Cumulative savings tracking with per-intent
  statistics
- **ATCPipeline**: Convenience class combining all three into a single
  workflow

### Quick Start

```python
from src.atc import IntentClassifier, AdaptiveCompressor, TokenSavingsTracker, ATCPipeline

# ── Intent Classification ──
classifier = IntentClassifier()
intent, confidence = classifier.classify("How do I fix the bug?")
# intent = "query", confidence ≈ 0.30

# Get top-N alternatives
results = classifier.classify_with_alternatives("Build a web server", top_n=3)
# [("command", 0.45), ("query", 0.12), ...]

# ── Compression ──
compressor = AdaptiveCompressor()
compressed_text, ratio = compressor.compress("What is the meaning of life?", "query")
# compressed_text = "meaning life" (or similar, ~90% reduction)
# ratio ≈ 0.90

# Decompress zlib-compressed text
original = compressor.decompress(compressed_text)

# ── Token Savings Tracking ──
tracker = TokenSavingsTracker()
tracker.record("query", 50, 5)  # 50 original tokens → 5 compressed
summary = tracker.get_summary()
# {total_operations: 1, total_tokens_saved: 45, ...}

# Per-intent stats
query_stats = tracker.get_intent_stats("query")

# ── Full Pipeline ──
pipeline = ATCPipeline()
result = pipeline.compress_and_track("What is Python?")
# result = {intent: "query", confidence: 0.30, compressed_text: "...",
#           compression_ratio: 0.90, tokens_saved: 4, ...}
pipeline_summary = pipeline.get_tracker_summary()
```

### Configuration

Customize compression behavior with `ATCConfig`:

```python
from src.atc import ATCConfig, AdaptiveCompressor

config = ATCConfig(
    compression_levels={"query": 0.95, "command": 0.75},  # Custom ratios
    min_compression_ratio=0.10,       # Safety floor
    use_zlib=True,                    # Apply zlib on top
    zlib_level=6,                     # zlib compression level (1-9)
    confidence_threshold=0.10,        # Minimum classification confidence
    strip_stop_words=True,            # Remove stop words
)
compressor = AdaptiveCompressor(config=config)
```

### Intent Classification Details

The `IntentClassifier` uses weighted keyword matching:
- Each intent has a dictionary of trigger words/phrases with weights
- Text is scored against each intent by summing matched keyword weights
- Scores are normalized to produce confidence scores
- Multi-word phrases receive extra weight for higher accuracy
- Classification is case-insensitive

### Compression Strategy by Intent

- **Query (90%)**: Aggressive — strips stop words and fillers, keeps
  only keywords. Preserves capitalized words (proper nouns).
- **Command (70%)**: Moderate — removes stop words, preserves imperative
  verbs and key objects.
- **Request (50%)**: Balanced — removes politeness words ("please",
  "could you"), preserves core request content.
- **Feedback (30%)**: Light — removes some stop words, minimal
  restructuring to preserve meaning.
- **System (10%)**: Minimal — only normalizes whitespace. Configuration
  text must remain precise.

### Exception Hierarchy

```
ATCError
├── ClassificationError  — Raised when intent classification fails
└── CompressionError     — Raised when compression/decompression fails
```

### Testing

```bash
PYTHONPATH=src python3 -m unittest tests.test_atc -v
```

---

## v2 Testing Suite

### Running Tests

```bash
# Run all tests
PYTHONPATH=src python3 tests/run_all.py

# Run individual test suites
PYTHONPATH=src python3 -m unittest tests.test_protocol_ext
PYTHONPATH=src python3 -m unittest tests.test_broker
PYTHONPATH=src python3 -m unittest tests.test_circuit_breaker
PYTHONPATH=src python3 -m unittest tests.test_streaming
PYTHONPATH=src python3 -m unittest tests.test_plugin
PYTHONPATH=src python3 -m unittest tests.test_benchmark_v2
PYTHONPATH=src python3 -m unittest tests.test_integration
```

### Test Coverage

| Test File | Coverage Target | Features Tested |
|-----------|----------------|-----------------|
| `test_protocol_ext.py` | 90%+ | Compression, signing, encryption, batching, pipeline, v1 compat |
| `test_broker.py` | 90%+ | Priority queue, dedup, DLQ, batch, metrics, depth |
| `test_circuit_breaker.py` | 90%+ | State transitions, backoff, recovery, decorator, fallback |
| `test_streaming.py` | 90%+ | SSE, WebSocket, events, batch, backpressure |
| `test_plugin.py` | 90%+ | Schema versioning, migrations, discovery, custom tokens |
| `test_benchmark_v2.py` | 90%+ | Latency, memory, scalability, comparison, JSON output |
| `test_integration.py` | 90%+ | Full pipeline v1→v2, error handling, throughput |

### Test Results

Run `PYTHONPATH=src python3 tests/run_all.py` for a complete summary with pass/fail counts and coverage estimates. Results are saved to `tests/test_results.json`.

---

## Extending the Protocol

---

## Introduction

**TokenRelay** is a token-based communication protocol that accelerates inter-subagent messaging in the Hermes `delegate_task` framework.

In the traditional approach, subagents return verbose natural language text that consumes expensive LLM context tokens. TokenRelay replaces this with compact token-ID sequences like `[42, 153]` that the parent decodes instantly.

**Key result**: 89.6% fewer LLM context tokens consumed per subagent message.

Project: `/home/columbo/ExtData/TokenRelay/`

---

## Quick Start

### Python

```python
from src.bridge import TokenRelayBridge

bridge = TokenRelayBridge()

# Encode subagent result
result = bridge.encode_result("auth_service", "JWT + OAuth2 built")
# → {'tokens': [42], 'payload': {...}, ...}

# Decode and route
status = bridge.extract_status(result)  # → 'complete'

# Validate response
v = bridge.validate_response(result, expected_tokens=[42])
```

### CLI

```bash
python3 src/cli.py encode task_complete --summary "API deployed"
python3 src/cli.py decode "42,153"
python3 src/cli.py route "0,151,42"
python3 src/cli.py list
python3 src/cli.py test
python3 tests/benchmark.py
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    TokenRelay Stack                       │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌─────────────┐     ┌──────────────┐                  │
│  │  Registry    │────→│   Encoder     │                  │
│  │  (tokens +   │     │              │                  │
│  │   protocol)  │     └──────┬───────┘                  │
│  └─────────────┘            │                            │
│                              ▼                            │
│  ┌────────────────────────────────────┐                 │
│  │     Token Sequence + Payload       │                 │
│  │     {"tokens": [42], "payload": {}} │                │
│  └────────────────────┬───────────────┘                 │
│                       │                                  │
│                       ▼                                  │
│  ┌────────────────────────────────────┐                 │
│  │          Decoder                   │                  │
│  │  (token → meaning + routing)       │                  │
│  └────────────────────┬───────────────┘                 │
│                       │                                  │
│                       ▼                                  │
│  ┌────────────────────────────────────┐                 │
│  │         Bridge                     │                  │
│  │  (delegate_task wrapper)           │                  │
│  └────────────────────────────────────┘                 │
│                                                           │
│  ┌────────────────────────────────────┐                 │
│  │         CLI (optional)             │                  │
│  └────────────────────────────────────┘                 │
└─────────────────────────────────────────────────────────┘
```

---

## Token Protocol

Every token derives from the Tokenizer project's `meanings.json`, extended with `protocol_tokens.json`.

| Token | Name | Type | Priority | Meaning |
|-------|------|------|----------|---------|
| 0 | Void | error | 0 | Error handling |
| 1 | Unit | data | 50 | Unit measurement |
| 2 | Pair | data | 50 | Two-element relationship |
| 3 | Triad | data | 50 | Three-element structure |
| 42 | Answer | status | 1 | Task complete |
| 100 | Anchor | data | 60 | Task anchor |
| 101 | Connector | control | 10 | Concept bridge |
| 102 | Temporal | data | 60 | Time sequence |
| 103 | Spatial | data | 60 | Location/direction |
| 104 | Causal | control | 10 | Cause-effect |
| 150 | Boundary | boundary | 0 | Message start |
| 151 | Attention | control | 1 | Focus needed |
| 152 | Padding | control | 100 | Neutral space |
| 153 | Separator | boundary | 0 | Message end |
| 154 | System | control | 0 | System instruction |

### Protocol Types

```
error       → Error handling (token 0)
status      → Task completion (token 42)
boundary    → Message boundaries (150, 153)
control     → Control flow (101, 104, 151, 154)
data        → Data payload (1-3, 100, 102, 103)
```

---

## Usage in Python

### TokenRelayBridge

The `TokenRelayBridge` class is the main entry point.

```python
from src.bridge import TokenRelayBridge

bridge = TokenRelayBridge()

# TASK COMPLETE
result = bridge.encode_result("task_id", "summary text")
# Token: [42] | Status: "complete"

# ERROR
err = bridge.encode_error("TIMEOUT", "Connection failed", "30s exceeded")
# Token: [0] | Status: "error"

# REQUEST
req = bridge.encode_request("build_api", "REST endpoints", attention="high")
# Token: [100, 151] | Status: "attention"

# STATUS
status = bridge.encode_status("running", location="server-1")
# Token: [103]

# ATTENTION
attn = bridge.encode_attention("critical", "auth service")
# Token: [151]

# SEPARATOR
sep = bridge.encode_separator("phase", "next step")
# Token: [153]

# START BOUNDARY
bnd = bridge.encode_start_boundary("msg-001", "request")
# Token: [150]
```

### Validation

```python
# Check if subagent returned the expected token
validation = bridge.validate_response(result, expected_tokens=[42])
# {'valid': True, 'expected': [42], 'actual': [42], ...}

# Multiple expected tokens
validation = bridge.validate_response(result, expected_tokens=[42, 153])
# {'valid': False, 'mismatches': [{'expected': 153, 'found': False}]}
```

### Subagent Prompt

```python
# Build delegate_task prompt with token protocol
prompt = bridge.build_prompt(tokens=[42], task="build_auth", context="JWT needed")

# Full subagent chain
chain = bridge.create_subagent_prompt("build_api", [42, 153])
# Returns dict with boundary, request, separator, protocol, expected_response_tokens
```

### TokenRegistry

```python
from src.registry import TokenRegistry, get_registry

reg = get_registry()

# Lookup token
token = reg.get_token(42)
print(token['meaning'])  # "The answer to everything..."

# Protocol type
ptype = reg.get_protocol_type(42)  # "status"

# Routing
route = reg.route_message([42, 153])
# {'action': 'status', 'priority': 1, ...}

# Type checks
reg.is_status_token(42)   # True
reg.is_error_token(0)     # True
reg.is_control_token(151) # True
```

### Codec

```python
from src.codec import MessageEncoder, MessageDecoder

encoder = MessageEncoder()
decoder = MessageDecoder()

# Encode
msg = encoder.encode("42", {"result": "success"})
msg = encoder.encode_task_complete("result", "summary")
msg = encoder.encode_error("CODE", "msg")
msg = encoder.encode_request("action", "desc")
msg = encoder.encode_attention("level", "focus")
msg = encoder.encode_causal("cause", "effect", [102])
msg = encoder.encode_start_boundary("id", "direction")

# Decode
interpretation = decoder.decode(msg)
formatted = decoder.format_for_display(interpretation)

# Quick functions
from src.codec import encode_message, decode_message, format_message
msg = encode_message("42", {"data": "value"})
interpretation = decode_message(msg)
text = format_message(msg)
```

---

## Usage with CLI

### Encode

```bash
python3 src/cli.py encode task_complete --summary "API deployed"
python3 src/cli.py encode error --type_arg "TIMEOUT" --summary "Connection refused"
python3 src/cli.py encode request --type_arg "process_data" --summary "Analyze results"
python3 src/cli.py encode attention --type_arg "critical" --summary "urgent"
python3 src/cli.py encode task_complete --payload result=api_server --payload summary=done
```

### Decode

```bash
python3 src/cli.py decode "42,153"
python3 src/cli.py decode "42,153" --format json
python3 src/cli.py decode "42" --payload '{"result":"success"}'
```

### Route

```bash
python3 src/cli.py route "0,151,42"
python3 src/cli.py route "42,153"
```

### List & Test

```bash
python3 src/cli.py list
python3 src/cli.py test
python3 tests/benchmark.py
```

---

## Benchmark — Why Is It Faster?

### The Problem

When `delegate_task` uses subagents, these bottlenecks emerge:

1. **LLM context consumption**: Every text response consumes many tokens
2. **LLM interpretation**: The parent must interpret the text
3. **Ambiguity**: Text-based status is not deterministic

### The Solution

TokenRelay eliminates these by:
- Subagent sends `[42]` instead of a full paragraph → 1-2 LLM tokens
- Parent reads `[42]` = "task complete" and acts immediately → no LLM interpretation
- Typed protocol → 100% deterministic routing

### Benchmark Results

| Metric | Traditional | TokenRelay | Savings |
|--------|-------------|------------|---------|
| **LLM Context** | 241 tokens | 25 tokens | **89.6%** |
| **Batch Payload** | 61,890 chars | 16,000 chars | **25.9%** |
| **Error Detection** | 3/7 keywords | 100% typed | **Typed** |
| **5-Step Workflow** | 103 tokens | 36 tokens | **65.0%** |
| **Throughput** | — | 205K msg/s | — |

```
── LLM Context Consumption ──────────────
   Traditional:  241 LLM tokens
   TokenRelay:    25 LLM tokens
   📉 Saved:     216 tokens (89.6%)

── Batch Throughput (1000 messages) ───
   Traditional chars:  61,890
   TokenRelay chars:   16,000
   📉 Payload ratio:   25.9%

── Error Detection ────────────────────────
   Token: 100% (typed protocol)
   Text:  3/7 detected (keyword matching)
```

**Why not Python speed?** Python encoding/decoding is only microseconds (0.56 μs per call). The real speedup comes from:
- Fewer tokens → smaller LLM prompt → faster generation
- No LLM interpretation needed → instant routing
- Deterministic → 100% reliable state machine

---

## Subagent Integration

### With delegate_task

```python
from src.bridge import TokenRelayBridge, make_subagent_call
from hermes_tools import delegate_task

bridge = TokenRelayBridge()

# Prepare subagent call with token protocol
subagent_msg = make_subagent_call(
    bridge,
    task="build_auth",
    tokens=[42, 153],  # Expected response tokens
    context="JWT, OAuth2, rate limiting"
)

# delegate_task(goal=subagent_msg["prompt"], context=subagent_msg["chain"])
```

### Multi-Agent Pipeline

```python
from src.bridge import TokenRelayBridge

bridge = TokenRelayBridge()

# Agent 1
msg1 = bridge.encode_request("setup_env", "Init project")
# Agent 1 responds: [42, 153] (complete + separator)

# Agent 2
msg2 = bridge.encode_request("build_api", "REST endpoints")
# Agent 2 responds: [42] (complete)

# Agent 3
msg3 = bridge.encode_result("deployed", "Production ready")
# Final: [42] (task complete)
```

### Error Handling Pipeline

```python
# Subagent signals an error
error_msg = bridge.encode_error("CONN_REFUSED", "DB down", "30s timeout")

# Parent handles immediately
status = bridge.extract_status(error_msg)
if status == "error":
    payload = error_msg["payload"]
    print(f"Error {payload['error_code']}: {payload['message']}")
    # Retry or escalate
```

---

## Extending the Protocol

### Adding a New Token

1. Add to `data/protocol_tokens.json`:

```json
"999": {
    "protocol_type": "status",
    "routing_priority": 5,
    "payload_schema": {
        "type": "object",
        "properties": {
            "custom_field": {"type": "string"}
        },
        "required": ["custom_field"]
    }
}
```

2. Add to `data/meanings.json` (from Tokenizer project):

```json
"999": {
    "id": 999,
    "meaning": "Custom token meaning",
    "category": "custom",
    "context": "Why it matters"
}
```

### Adding a New Protocol Type

Define any `protocol_type` in `protocol_tokens.json`:

```json
"999": {
    "protocol_type": "custom_type",
    "routing_priority": 10
}
```

Add the mapping in `bridge.extract_status()`:

```python
mapping = {
    ...,
    "custom_type": "custom_status"
}
```

---

## Troubleshooting

### Token Not Found

```
ValueError: Unknown message type '999'
```

**Fix**: Verify the token exists in `data/protocol_tokens.json`.

### Route Not Working

```python
route = bridge.registry.route_message([999])
# Returns {'action': 'unknown', 'priority': 999}
```

**Fix**: Check the `protocol_type` field in the token data.

### Decode Errors

```
KeyError: 'routing'
```

**Fix**: Use `decoder.decode(message)` before calling `format_for_display()`.

### Benchmark Import Errors

```
ImportError: No module named 'src'
```

**Fix**: Run with `PYTHONPATH=src python3 tests/benchmark.py`.

---

## Message Broker v2

The `MessageBroker` provides priority-queue-based message routing with deduplication, depth monitoring, and dead letter queue support.

### Features

- **Priority Queue**: Lower priority number = higher priority (uses `heapq`)
- **Message Deduplication**: Content-hash based dedup within a configurable time window
- **FIFO Ordering**: Guaranteed within the same priority level
- **Queue Depth Monitoring**: Configurable max depth with health status
- **Dead Letter Queue (DLQ)**: Quarantines failed messages with enrichment metadata
- **Batch Operations**: Efficient bulk enqueue with result summaries

### Quick Start

```python
from src.broker import MessageBroker, PRIORITY_CRITICAL, PRIORITY_HIGH, PRIORITY_MEDIUM
from src.registry import get_registry

registry = get_registry()
broker = MessageBroker(registry=registry, max_depth=10000, dedup_window_seconds=300)

# Enqueue with explicit priority
broker.enqueue({"tokens": [42], "payload": {"result": "ok"}}, priority=PRIORITY_HIGH)

# Auto-infer priority from token registry
broker.enqueue_with_priority({"tokens": [42]})

# Dequeue highest-priority message
msg = broker.dequeue()

# Check queue health
health = broker.get_health()
# {"status": "healthy", "depth": 5, "max_depth": 10000, ...}

# Get metrics
metrics = broker.get_metrics()
# {"messages_enqueued": 10, "messages_dequeued": 5, ...}
```

### Dead Letter Queue

```python
# Send failed message to DLQ
broker.send_to_dlq(failed_msg, reason="timeout")

# Retrieve from DLQ
dlq_msg = broker.receive_from_dlq()

# Clear DLQ
cleared = broker.clear_dlq()
```

### Batch Operations

```python
results = broker.enqueue_batch([msg1, msg2, msg3])
# {"enqueued": 3, "rejected": 0, "duplicates": 0}

# Drain all messages in priority order
all_messages = broker.drain()
```

### Priority Levels

| Constant | Value | Description |
|----------|-------|-------------|
| `PRIORITY_CRITICAL` | 0 | Highest priority |
| `PRIORITY_HIGH` | 1 | High priority |
| `PRIORITY_MEDIUM` | 5 | Medium priority |
| `PRIORITY_LOW` | 50 | Low priority |
| `PRIORITY_BULK` | 100 | Lowest priority |

---

## Circuit Breaker v2

The `CircuitBreaker` implements the circuit breaker pattern for fault-tolerant message processing with automatic recovery detection.

### States

| State | Description |
|-------|-------------|
| `CLOSED` | Normal operation, requests pass through |
| `OPEN` | Circuit tripped, calls are rejected immediately |
| `HALF_OPEN` | Testing if service recovered after cooldown |

### Quick Start

```python
from src.circuit_breaker import CircuitBreaker, CircuitBreakerError, CircuitState

cb = CircuitBreaker(
    failure_threshold=5,      # Fail after 5 consecutive errors
    recovery_timeout=30,      # Wait 30s before testing recovery
    backoff_multiplier=2,     # Double backoff on each re-trip
    max_backoff=300           # Max 5 minutes backoff
)

try:
    result = cb.call(risky_function)
except CircuitBreakerError:
    # Circuit is OPEN — calls rejected
    pass
except Exception:
    # Service failed
    pass
```

### Half-Open Recovery

```python
# After recovery_timeout seconds, circuit transitions to HALF_OPEN
# A test call is allowed through
# If it succeeds → CLOSED (recovered)
# If it fails → OPEN (with increased backoff)

cb.wait_for_recovery()  # Block until HALF_OPEN
```

### Metrics & Health

```python
metrics = cb.get_metrics()
# {"state": "CLOSED", "failure_count": 3, "success_rate": 95.0, ...}

health = cb.get_health()
# {"health": "healthy", "state": "CLOSED", "consecutive_failures": 0, ...}

history = cb.get_history(last_n=20)  # [(status, timestamp), ...]
```

### Decorator Pattern

```python
from src.circuit_breaker import circuit_breaker

@circuit_breaker(failure_threshold=3, recovery_timeout=10)
def unreliable_operation():
    return call_external_service()

# Access the circuit breaker
unreliable_operation.circuit_breaker.reset()
```

### Fallback Support

```python
result = cb.call_with_fallback(
    primary_func, fallback_func, *args, **kwargs
)
# Returns primary result on success, fallback on failure or OPEN circuit
```

### State Transitions

```
CLOSED ──[threshold failures]──→ OPEN
  ↑                              │
  │                              │ recovery_timeout
  │                              ↓
  └──── [success] ←── HALF_OPEN ←── [failure]
```

---

## Registry Broker Integration

The `TokenRegistry` now includes broker-specific methods for priority routing:

```python
from src.registry import get_registry

reg = get_registry()

# Get broker priority for a token
priority = reg.get_broker_priority(42)  # 0 (critical)
label = reg.get_broker_prio_label(42)   # "critical"

# Broker-aware routing
routing = reg.broker_route([42, 153])
# {"action": "status", "priority": 1, "broker_priority": 0, ...}

# Queue recommendations
rec = reg.get_queue_recommendation([42])
# {"broker_priority": 0, "dlq_recommendation": "immediate", ...}

# Priority distribution across all tokens
dist = reg.get_broker_priority_distribution()
# {"critical": 3, "high": 2, "medium": 5, "low": 10, "bulk": 3}
```

---

## License

MIT

Copyright (c) 2026 columbo

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
