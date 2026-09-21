# TokenRelay v3 — Architecture

## Overview

TokenRelay v3 transforms the protocol from an internal subagent communication tool into an **end-to-end LLM communication optimization framework** — specifically for the **User ↔ AI** pathway. Where v1 optimized subagent-to-subagent and v2 added infrastructure (broker, streaming, plugins), v3 focuses on **minimizing latency and token consumption between the human user and the AI model**.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                    TokenRelay v3 Stack                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │              User ↔ AI Endpoints                                │  │
│  │                                                                  │  │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐              │  │
│  │  │ ATC      │───→│ BRP      │───→│ POS      │              │  │
│  │  │ Adaptive │    │ Bidirect │    │ Predict. │              │  │
│  │  │ Token    │    │ Real-Time│    │ Output   │              │  │
│  │  │ Compress │    │ Protocol │    │ Stream   │              │  │
│  │  └──────────┘    └──────────┘    └──────────┘              │  │
│  │                                                                  │  │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐              │  │
│  │  │ CAR      │←───│ TEQ      │←───│ EPC      │              │  │
│  │  │ Context  │    │ Token    │    │ Edge Pre │              │  │
│  │  │ Aware    │    │ Economy  │    │ Comput.  │              │  │
│  │  │ Routing  │    │ & QoS    │    │          │              │  │
│  │  └──────────┘    └──────────┘    └──────────┘              │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │              Infrastructure Layer (v2 Foundation)               │  │
│  │                                                                  │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │  │
│  │  │ Broker   │ │ Circuit  │ │ EventBus │ │ Streaming│       │  │
│  │  │ v2 +     │ │ Breaker  │ │ + SSE    │ │ Processor│       │  │
│  │  │ BRP      │ │ v2 + QoS │ │ Handler  │ │ v2       │       │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │  │
│  │                                                                  │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │  │
│  │  │ Codec    │ │ Bridge   │ │ Registry │ │ Plugin   │       │  │
│  │  │ v2 (     │ │ v1       │ │ v2       │ │ System   │       │  │
│  │  │ compress │ │ delegate │ │          │ │ v2 + v3  │       │  │
│  │  │ + sign)  │ │ _task    │ │          │ │          │       │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │  │
│  └──────────────────────────────────────────────────────────────┘  │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │              Transport Layer                                   │  │
│  │                                                                  │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │  │
│  │  │ WebSocket│ │ SSE      │ │ HTTP/2   │ │ gRPC     │       │  │
│  │  │ (BRP)    │ │ (Legacy) │ │ (Fallback)│ │(Enterprise)│      │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## v2 to v3 Feature Map

| Feature | v2 | v3 |
|---------|----|----|
| Scope | Subagent ↔ Subagent | User ↔ AI End-to-End |
| Input | Token encoding | Adaptive Token Compression (ATC) |
| Output | Static responses | Predictive Output Streaming (POS) |
| Transport | HTTP/SSE | Bidirectional Real-Time Protocol (BRP) |
| Routing | Static priority | Context-Aware Dynamic Routing (CAR) |
| Security | HMAC signing | Token Economy + QoS (TEQ) |
| Caching | None | Edge Pre-Computation (EPC) |
| Latency | ~200ms typical | <50ms target (control), <500ms (response) |
| Throughput | 205K msg/s | 1M+ events/s |
| Foundation | Broker, Circuit Breaker, Streaming, Codec, Bridge | All v2 modules as foundation |

## Pipeline Stages

### Stage 1: ATC — Adaptive Token Compression

**Module**: `src/atc.py`

Classifies user input into 5 intent categories and applies dynamic compression levels:

| Intent | Compression Target | Level |
|--------|-------------------|-------|
| Query | 90% reduction | HIGH |
| Command | 70% reduction | MEDIUM |
| Request | 50% reduction | LOW |
| Feedback | 30% reduction | MINIMAL |
| System | 10% reduction | NONE |

Uses `IntentClassifier` for classification, `AdaptiveCompressor` for compression, and `TokenSavingsTracker` for cumulative savings measurement. Integrates with v2 `compress_message()` from `codec.py` for payload compression.

### Stage 2: CAR — Context-Aware Routing

**Module**: `src/car.py`

Routes queries to optimal model/endpoint based on:

- **Query complexity**: Simple → fast model, Complex → reasoning model
- **User preference**: Speed vs quality trade-off
- **Current system load**: Dynamic load balancing
- **Historical performance**: Per-endpoint tracking

Uses `ComplexityAnalyzer`, `ModelSelector`, `PerformanceTracker`, and `CARRouter`. Performance data feeds back into v2 `MessageBroker` priority decisions.

### Stage 3: BRP — Bidirectional Real-Time Protocol

**Module**: `src/brp.py`

Full-duplex WebSocket channel with 4 priority channels:

- **Channel 0**: System/control messages (sub-50ms target)
- **Channel 1**: User queries (high priority)
- **Channel 2**: AI responses (normal priority)
- **Channel 3**: Streaming events (lowest priority)

Uses v2 `MessageBroker` for message queuing and routing, `CircuitBreaker` for fault tolerance, and `EventBus`/`SSEHandler` for streaming notifications.

### Stage 4: Model Processing

Simulated model processing based on CAR routing decisions. In production, this connects to the actual LLM endpoint.

### Stage 5: POS — Predictive Output Streaming

**Module**: `src/pos.py`

AI begins streaming before full response is generated:

- **Progressive rendering**: Skeleton → Details → Conclusion
- **Backpressure-aware chunk sizing**: Adapts to consumer throughput
- **Prediction caching**: LRU cache for response structure predictions

Uses v2 `StreamingProcessor` for streaming sessions, `EventBus` for event-driven callbacks, and `BackpressureController` for flow control.

### Stage 6: TEQ — Token Economy & QoS

**Module**: `src/teq.py`

Token-based billing with 4 QoS tiers:

| Tier | Rate Limit | Latency Budget | Priority |
|------|-----------|----------------|----------|
| Bronze | 5 req/s | 5000ms | Lowest |
| Silver | 10 req/s | 2000ms | Medium |
| Gold | 25 req/s | 500ms | High |
| Platinum | 50 req/s | 100ms | Highest |

Uses v2 `CircuitBreaker` for SLA enforcement and graceful degradation.

### Stage 7: EPC — Edge Pre-Computation

**Module**: `src/epc.py`

Pre-computes common responses at edge nodes:

- **TTL + LRU eviction**: Configurable cache with size limits
- **Delta encoding**: Only send changes from cached response
- **Compression**: zlib compression of cached responses
- **Prediction**: Pre-compute common query patterns

Uses v2 `compress_message()` / `decompress_message()` from `codec.py` for edge compression.

### Stage 8: User Output

Final response delivered to user via the configured transport.

## Component Descriptions

### V3Orchestrator (`src/v3_orchestrator.py`)

The central orchestrator class that integrates ALL v3 modules and v2 infrastructure.

**v2 Foundation modules used:**
- `MessageBroker` — Message routing with priority queue
- `CircuitBreaker` — Fault tolerance and SLA enforcement
- `EventBus` / `SSEHandler` — Event-driven streaming
- `StreamingProcessor` — Streaming sessions and backpressure
- `MessageEncoder` / `MessageDecoder` — Token encoding/decoding
- `compress_message()` / `decompress_message()` — Payload compression
- `TokenRegistry` — Token registry and routing
- `TokenRelayBridge` — delegate_task integration

**v3 Module integrations:**
- `IntentClassifier` → ATC intent classification
- `AdaptiveCompressor` → Dynamic compression
- `CARRouter` → Context-aware routing decisions
- `ChannelManager` → BRP multi-channel routing
- `ResponsePredictor` → POS response prediction
- `TokenBilling` → TEQ token consumption
- `RateLimiter` → TEQ rate limiting
- `EdgeCache` → EPC response caching

### ATC Module (`src/atc.py`)

**Key classes:**
- `IntentClassifier`: Classifies queries into 5 intents using keyword matching
- `AdaptiveCompressor`: Applies dynamic compression levels
- `TokenSavingsTracker`: Cumulative savings tracking
- `ATCPipeline`: End-to-end classify→compress→track pipeline
- `ATCConfig`: Configuration dataclass

### POS Module (`src/pos.py`)

**Key classes:**
- `ResponsePredictor`: Predicts response structure from query patterns
- `ProgressiveRenderer`: Renders skeleton→details→conclusion
- `ChunkManager`: Backpressure-aware chunk sizing
- `BackpressureController`: Adjusts chunk rate based on throughput
- `POSConfig`: Configuration dataclass

### BRP Module (`src/brp.py`)

**Key classes:**
- `ChannelManager`: Multi-channel priority message routing
- `ConnectionMonitor`: Connection quality tracking
- `HeartbeatManager`: Connection health monitoring
- `BRPServer`: WebSocket server implementation
- `BRPClient`: WebSocket client implementation
- `BRPConfig`: Configuration dataclass

### CAR Module (`src/car.py`)

**Key classes:**
- `ComplexityAnalyzer`: Query complexity scoring (1-10)
- `ModelSelector`: Best endpoint selection
- `PerformanceTracker`: Per-endpoint latency tracking
- `RouteOptimizer`: Historical routing adjustment
- `CARRouter`: Complete routing engine
- `CARConfig`: Configuration dataclass

### TEQ Module (`src/teq.py`)

**Key classes:**
- `TokenBilling`: Per-user token allowance tracking
- `RateLimiter`: Per-user rate limiting with sliding window
- `SLAMonitor`: SLA compliance tracking
- `QoSTier`: QoS tier management and enforcement
- `TEQConfig`: Configuration dataclass

### EPC Module (`src/epc.py`)

**Key classes:**
- `EdgeCache`: TTL + LRU eviction cache
- `DeltaEncoder`: JSON diff for change-only updates
- `ResponsePredictor`: Pre-compute common query patterns
- `CacheManager`: Distributed edge node coordination
- `EPCConfig`: Configuration dataclass

## Data Flow

### Complete Pipeline Flow

```
User Input
    │
    ▼
┌─────────┐  Intent Classification  ┌─────────┐
│  ATC    │ ──────────────────────→ │ Adaptive │
│  Stage  │                         │Compress  │
└─────────┘                         └────┬────┘
                                       │ Compressed
                                       ▼
┌─────────┐  Complexity Analysis    ┌─────────┐
│  CAR    │ ──────────────────────→ │ Model    │
│  Stage  │                         │ Selector │
└─────────┘                         └────┬────┘
                                       │ Route Decision
                                       ▼
┌─────────┐  Priority Channel      ┌─────────┐
│  BRP    │ ──────────────────────→ │ Message  │
│  Stage  │   (via Broker)          │ Broker   │
└─────────┘                         └────┬────┘
                                       │ Message
                                       ▼
┌─────────┐  Model Processing      ┌─────────┐
│  Model  │ ──────────────────────→ │   AI     │
│  Stage  │                         │  Model   │
└─────────┘                         └────┬────┘
                                       │ Response
                                       ▼
┌─────────┐  Progressive Streaming  ┌─────────┐
│  POS    │ ──────────────────────→ │ Streaming│
│  Stage  │   (via StreamingProc)   │ Processor│
└─────────┘                         └────┬────┘
                                       │ Chunks
                                       ▼
┌─────────┐  Token Billing          ┌─────────┐
│  TEQ    │ ──────────────────────→ │   TEQ    │
│  Stage  │   (via CircuitBreaker)  │  System  │
└─────────┘                         └────┬────┘
                                       │ Billing
                                       ▼
┌─────────┐  Edge Caching           ┌─────────┐
│  EPC    │ ──────────────────────→ │  Edge    │
│  Stage  │   (via compress/decomp) │  Cache   │
└─────────┘                         └────┬────┘
                                       │ Cached/Response
                                       ▼
┌─────────┐  User Output            ┌─────────┐
│  Output │ ──────────────────────→ │   User   │
│  Stage  │                         │          │
└─────────┘                         └──────────┘
```

### Error Handling Flow

```
Error Occurs
    │
    ▼
CircuitBreaker._on_failure()
    │
    ├── Failure threshold NOT exceeded → Retry
    │
    └── Failure threshold exceeded → Circuit OPEN
            │
            ├── Calls rejected immediately
            ├── After recovery_timeout → HALF_OPEN
            │       ├── Test call succeeds → CLOSED
            │       └── Test call fails → OPEN (reset timer)
            │
            └── DLQ (Dead Letter Queue) via MessageBroker
```

### v2 Module Integration Flow

```
MessageBroker ←── BRP sends messages
CircuitBreaker ←── TEQ enforces SLA
EventBus ←── POS streams events
StreamingProcessor ←── POS manages sessions
compress_message ←── ATC compresses payloads
MessageEncoder ←── All stages encode messages
TokenRelayBridge ←── delegate_task integration
```

## Scaling Considerations

- **MessageBroker**: O(log n) priority queue, dedup window prevents duplicates
- **CircuitBreaker**: Prevents cascading failures, automatic recovery
- **StreamingProcessor**: Backpressure prevents memory exhaustion
- **ATC**: Intent classification is O(n) in keywords, cache-friendly
- **CAR**: Routing decision is O(endpoints) per query
- **EPC**: LRU eviction O(1), TTL cleanup O(entries)
- **TEQ**: Rate limiting O(1) per request, sliding window efficient

## v1 → v2 → v3 Compatibility

All v1 features remain fully backward compatible:
- `MessageEncoder.encode_*()` methods unchanged
- `TokenRelayBridge` methods unchanged
- `TokenRegistry` methods unchanged (with v2/v3 metadata additions)
- `cli.py` commands unchanged

v2 features are opt-in:
- `compress_message()`, `sign_message()` — explicit calls
- `MessageBroker`, `CircuitBreaker` — explicit instantiation
- `StreamingProcessor`, `EventBus` — explicit session creation

v3 features build on v2:
- `V3Orchestrator` instantiates v2 modules and adds v3 intelligence
- Pipeline execution uses v2 `compress_message()` not re-implemented zlib
- BRP sends through v2 `MessageBroker`, not a new queue
- POS streams via v2 `StreamingProcessor`, not rebuilt WebSocket
- TEQ enforces SLA with v2 `CircuitBreaker`
- EPC compresses with v2 `compress_message()`

## Performance Targets

| Metric | v2 Baseline | v3 Target |
|--------|-------------|-----------|
| Input tokens/query | ~50 | ~15 (70% reduction) |
| Output latency (p50) | ~200ms | ~100ms |
| Output latency (p99) | ~500ms | ~200ms |
| End-to-end round-trip | ~400ms | ~100ms (control) |
| Throughput | 205K msg/s | 1M+ events/s |
| Cache hit rate | 0% | 40% (EPC) |
| Token savings (total) | 89.6% | 95%+ |

## Configuration

### V3Orchestrator Configuration

```python
from v3_orchestrator import create_orchestrator

config = {
    "atc": {
        "compression_levels": {"query": 9, "command": 6, "request": 1, "feedback": 0, "system": 0},
    },
    "pos": {
        "default_chunk_size": 100,
        "prediction_confidence_threshold": 0.5,
    },
    "brp": {
        "heartbeat_interval_ms": 3000,
        "control_message_target_ms": 50.0,
    },
    "car": {
        "degradation_latency_ms": 500.0,
        "overload_latency_ms": 2000.0,
    },
    "teq": {
        "default_allowance": 10_000,
        "default_rate_limit": 10.0,
    },
    "epc": {
        "default_ttl": 300.0,
        "max_entries": 10_000,
    },
    "broker_max_depth": 10000,
    "cb_failure_threshold": 5,
    "cb_recovery_timeout": 30,
}

orchestrator = create_orchestrator(config)
```

## Verification

```bash
cd /home/columbo/ExtData/TokenRelay
PYTHONPATH=src python3 -m unittest tests.test_v3_integration -v  # v3 integration tests
PYTHONPATH=src python3 -m unittest discover tests/ -v            # All tests
PYTHONPATH=src python3 tests/benchmark.py --mode v3               # v3 benchmarks
```

## Related Documentation

- `docs/PRD_v3.md` — Product Requirements Document for v3
- `docs/ARCHITECTURE_v2.md` — v2 architecture reference
- `README.md` — Project overview and quick start
