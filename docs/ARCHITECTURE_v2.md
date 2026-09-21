# TokenRelay v2 — Architecture

## Overview

TokenRelay v2 extends the v1 protocol into a full-featured subagent communication framework with compression, signing, encryption, message brokering, circuit breakers, streaming, and a plugin system.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        TokenRelay v2 Stack                                │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────┐       │
│  │                    Application Layer                         │       │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │       │
│  │  │ Subagent │  │ Subagent │  │ Subagent │  │ Subagent │  │       │
│  │  │    A     │  │    B     │  │    C     │  │    D     │  │       │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘  │       │
│  └───────┼──────────────┼──────────────┼──────────────┼──────────┘       │
│          │              │              │              │                   │
│  ┌───────▼──────────────▼──────────────▼──────────────▼──────────┐      │
│  │                    Transport Layer                             │      │
│  │  ┌──────────┐  ┌──────────┐  ┌───────────────────────────┐   │      │
│  │  │ SSE      │  │ WebSocket│  │  Event-Driven Callbacks   │   │      │
│  │  │ Transport│  │ Transport│  │  (StreamingProcessor)      │   │      │
│  │  └────┬─────┘  └────┬─────┘  └───────────┬───────────────┘   │      │
│  └───────┼──────────────┼────────────────────┼───────────────────┘      │
│          │              │                    │                          │
│  ┌───────▼──────────────▼────────────────────▼───────────────────┐      │
│  │                    Message Broker                              │      │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐    │      │
│  │  │ Priority │  │ Dedup    │  │ DLQ      │  │ Batch    │    │      │
│  │  │ Queue    │  │ Engine   │  │          │  │ Engine   │    │      │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘    │      │
│  └───────┼──────────────┼──────────────┼──────────────┼───────────┘      │
│          │              │              │              │                   │
│  ┌───────▼──────────────▼──────────────▼──────────────▼───────────┐      │
│  │                    Security Layer                              │      │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐                    │      │
│  │  │ HMAC     │  │ AES-256  │  │ zlib     │                    │      │
│  │  │ Signing  │  │ GCM      │  │ Compress │                    │      │
│  │  │ (SHA256) │  │ Encryption│ │          │                    │      │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘                    │      │
│  └───────┼──────────────┼──────────────┼───────────────────────────┘      │
│          │              │              │                                   │
│  ┌───────▼──────────────▼──────────────▼───────────────────────────┐      │
│  │                    Protocol Layer                               │      │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐                    │      │
│  │  │ Registry │  │ Encoder  │  │ Decoder  │                    │      │
│  │  │ v2       │  │ v2       │  │ v2       │                    │      │
│  │  └────┬─────┘  └────┬─────┘  └────┬─────┘                    │      │
│  └───────┼──────────────┼──────────────┼───────────────────────────┘      │
│          │              │              │                                   │
│  ┌───────▼──────────────▼──────────────▼───────────────────────────┐      │
│  │                    Bridge Layer                                 │      │
│  │  ┌──────────────────────────────────────────────────────────┐   │      │
│  │  │         TokenRelayBridge (delegate_task wrapper)         │   │      │
│  │  └──────────────────────────────────────────────────────────┘   │      │
│  └─────────────────────────────────────────────────────────────────┘      │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐      │
│  │                    Infrastructure Layer                         │      │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐      │      │
│  │  │ Circuit  │  │ Plugin   │  │ Benchmark│  │  CLI     │      │      │
│  │  │Breaker   │  │ System   │  │ v2       │  │          │      │      │
│  │  │          │  │          │  │          │  │          │      │      │
│  │  └──────────┘  └──────────┘  └──────────┘  └──────────┘      │      │
│  └─────────────────────────────────────────────────────────────────┘      │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

## Component Descriptions

### 1. Protocol Layer

**`src/registry.py`** — Extended token registry with communication protocol fields.
- `TokenRegistry` class with `protocol_type`, `routing_priority`, `payload_schema`
- `build_protocol_registry()` merges base meanings with protocol overrides
- `route_message(tokens)` determines routing based on token IDs
- Type checks: `is_control_token()`, `is_status_token()`, `is_error_token()`

**`src/codec.py`** (v2 Extended) — Message encoder/decoder with v2 extensions.
- `MessageEncoder` / `MessageDecoder` (v1 compatibility)
- `compress_message()` / `decompress_message()` — zlib payload compression
- `sign_message()` / `verify_signature()` — HMAC-SHA256 signing
- `encrypt_message()` / `decrypt_message()` — AES-256-GCM encryption
- `batch_encode()` / `batch_decode()` — Message batching
- `pipeline_encode()` / `pipeline_decode()` — Full pipeline processing
- `measure_compression_ratio()` / `benchmark_compression()` — Compression metrics

### 2. Security Layer

**`src/protocol_v2.py`** — Core v2 security extensions.
- `compress_payload()` / `decompress_payload()` — zlib compression functions
- `compression_ratio()` — Calculate compression effectiveness
- `SigningKey` / `MessageSigner` — HMAC-SHA256 signing
- `EncryptionConfig` / `MessageEncryptor` — AES-256-GCM encryption
- `ProtocolV2Message` — v2 message with compression, signing, encryption support

### 3. Broker Layer

**`src/broker.py`** — Priority-based message broker.
- `MessageBroker` class with priority queue (heapq-based)
- Message deduplication with configurable time window
- Dead letter queue (DLQ) for failed messages
- Batch enqueue and drain operations
- Queue depth monitoring with health status
- `PriorityMessage` wrapper and `create_broker()` factory

### 4. Circuit Breaker Layer

**`src/circuit_breaker.py`** — Fault tolerance pattern.
- `CircuitBreaker` with CLOSED → OPEN → HALF_OPEN → CLOSED states
- Configurable failure threshold, recovery timeout, exponential backoff
- `call()` / `call_with_fallback()` for protected execution
- `CircuitBreakerError` exception
- `@circuit_breaker` decorator for easy wrapping
- `create_breaker()` factory function

### 5. Streaming Layer

**`src/streaming.py`** — Real-time streaming transport.
- `StreamEvent` / `StreamEventType` / `StreamFormat` — Event types
- `EventBus` / `EventCallback` — Event-driven callback system
- `StreamingProcessor` — Core streaming processor with backpressure
- `SSEHandler` / `start_sse_server()` — Server-Sent Events endpoint
- `WebSocketSimulator` — WebSocket-like bidirectional communication
- `BackpressureState` / `BackpressureStrategy` — Flow control
- `BatchConfig` for batch processing configuration

### 6. Plugin System

**`src/plugin.py`** — Extensible protocol definitions.
- `PluginRegistry` for managing plugins and custom token types
- `SchemaVersion` with semantic versioning and compatibility checking
- `CustomTokenDefinition` / `PluginManifest` / `MigrationPath`
- `Plugin` abstract base class with `register()`, `unregister()`, `on_message()`
- `PluginStatus` enum for plugin lifecycle management
- `discover_plugins()` / `load_plugin()` / `discover_and_load_all()`
- Built-in plugins: `CorePlugin`, `ValidationPlugin`, `MigrationPlugin`
- `register_protocol()` convenience function

### 7. Benchmark Layer

**`src/benchmark_v2.py`** — Enhanced benchmarking system.
- `BenchmarkV2` class with comprehensive test scenarios
- `Protocol` enum: TOKEN_RELAY, TRADITIONAL_TEXT, JSON_PASSTHROUGH
- `LatencyStats` with p50, p95, p99 percentiles
- `MemorySnapshot` with RSS/VMS profiling
- `BenchmarkResult` with full metrics
- `run_scalability_test()` — Logarithmic scale testing
- `run_comparative_benchmark()` — Multi-protocol comparison
- `run_full_benchmark_suite()` — Complete benchmark suite
- `output_json()` — JSON output for HTML visualization

### 8. Bridge Layer

**`src/bridge.py`** — Delegate task bridge (v1, backward compatible).
- `TokenRelayBridge` wraps `delegate_task` with token-aware messaging
- `make_subagent_call()` creates structured subagent calls
- `validate_response()` validates subagent responses
- `build_prompt()` / `create_subagent_prompt()` for prompt construction

### 9. Infrastructure

**`src/cli.py`** — Command-line interface.
- Commands: encode, decode, format, list, route, test
- Supports all v1 token types plus v2 extensions

## Data Flow

### Standard Message Flow

```
Subagent Output → MessageEncoder.encode() → Token Sequence + Payload
    → [Optional: compress_payload()]
    → [Optional: MessageSigner.sign_message()]
    → [Optional: MessageEncryptor.encrypt()]
    → MessageBroker.enqueue()
    → [Optional: CircuitBreaker.call()]
    → StreamingProcessor.stream_token()
    → MessageDecoder.decode()
    → Human-readable Interpretation
```

### Error Handling Flow

```
Error Occurs → MessageEncoder.encode_error() → Token [0]
    → CircuitBreaker._on_failure()
    → [If threshold exceeded: CircuitState.OPEN]
    → MessageBroker.send_to_dlq()
    → [Retry after recovery_timeout]
    → CircuitBreaker._transition_to(HALF_OPEN)
```

### Plugin Registration Flow

```
Plugin Defined → PluginRegistry.register_plugin()
    → Plugin.register(registry)
    → CustomTokenDefinition registered
    → SchemaVersion validated
    → MigrationPath checked
    → PluginManifest stored
    → PluginStatus = ACTIVE
```

## Scaling Considerations

- **Message Broker**: Priority queue ensures O(log n) enqueue/dequeue; dedup window prevents duplicate processing
- **Circuit Breaker**: Prevents cascading failures by isolating unhealthy services
- **Streaming**: Backpressure prevents memory exhaustion under high load
- **Plugin System**: Custom tokens are registered at runtime without modifying core registry
- **Compression**: zlib level 6 provides good compression/speed tradeoff
- **Signing**: HMAC-SHA256 adds minimal overhead (~μs per operation)

## v1 → v2 Compatibility

All v1 features remain fully backward compatible:
- `MessageEncoder.encode()`, `encode_task_complete()`, etc. — unchanged signatures
- `MessageDecoder.decode()`, `format_for_display()` — unchanged
- `TokenRelayBridge` methods — unchanged
- `TokenRegistry` methods — unchanged (with v2 metadata additions)
- `cli.py` commands — unchanged
- `tests/benchmark.py` — unchanged

v2 features are opt-in:
- Compression: call `compress_payload()` explicitly
- Signing: call `MessageSigner.sign_message()` explicitly
- Broker: create `MessageBroker` instance explicitly
- Circuit Breaker: wrap calls with `cb.call()` explicitly
- Streaming: create `StreamingProcessor` session explicitly
- Plugin: call `register_protocol()` explicitly
