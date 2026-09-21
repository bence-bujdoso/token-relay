# TokenRelay v3 — Endpoint-to-Endpoint LLM Communication Optimization

## Vision

TokenRelay v3 transforms the protocol from an **internal subagent communication** tool into an **end-to-end LLM communication optimization framework** — specifically for the **User ↔ AI** pathway. Where v1 optimized subagent-to-subagent and v2 added infrastructure (broker, streaming, plugins), v3 focuses on **minimizing latency and token consumption between the human user and the AI model**.

## Core Problem

The User ↔ AI communication path has multiple inefficiencies:
1. **User input**: Natural language queries contain ~30% filler/context noise
2. **AI output**: Model generates verbose responses when concise would suffice
3. **Network round-trips**: Each message traverses HTTP overhead
4. **Context accumulation**: Every message in a conversation adds tokens to the window
5. **One-size-fits-all**: Same protocol for quick questions and deep analysis

v3 addresses all five.

## Key Innovations

### 1. Adaptive Token Compression (ATC)
- Dynamically compress user input based on intent classification
- "Quick question" → high compression, low overhead
- "Deep analysis" → low compression, preserve nuance
- Uses token-level intent detection (5 categories: query, command, request, feedback, system)
- **Target**: 60-90% input token reduction for routine queries

### 2. Predictive Output Streaming (POS)
- AI begins streaming before full response is generated
- Predicts response structure from query pattern
- Progressive rendering: skeleton → details → conclusion
- Backpressure-aware chunk sizing
- **Target**: Perceived latency reduction of 40-60%

### 3. Bidirectional Real-Time Protocol (BRP)
- Full-duplex WebSocket channel for User ↔ AI
- Priority channels:
  - `channel_0`: System/control messages (highest priority)
  - `channel_1`: User queries (high priority)
  - `channel_2`: AI responses (normal priority)
  - `channel_3`: Streaming events (lowest priority)
- Heartbeat and connection quality monitoring
- **Target**: Sub-50ms round-trip for control messages

### 4. Context-Aware Routing (CAR)
- Routes queries to optimal model/endpoint based on:
  - Query complexity (simple → fast model, complex → reasoning model)
  - User preference (speed vs quality)
  - Current system load
  - Historical performance per endpoint
- Dynamic model selection with fallbacks
- **Target**: 30% faster average response time

### 5. Token Economy & QoS (TEQ)
- Token-based billing: each user gets token allowance
- Quality of Service tiers:
  - `Bronze`: Standard latency, best effort
  - `Silver`: Priority queue, 2x faster
  - `Gold`: Dedicated queue, guaranteed latency
  - `Platinum`: Premium, near-instant
- Rate limiting with graceful degradation
- **Target**: Fair resource allocation with SLA guarantees

### 6. Edge Pre-Computation (EPC)
- Pre-compute common responses at edge nodes
- Cache frequent query patterns with TTL
- Edge-side token compression before sending to model
- Delta encoding: only send changes from cached response
- **Target**: 70% reduction for repeated patterns

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    TokenRelay v3 Stack                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              User ↔ AI Endpoints                            │  │
│  │                                                           │  │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐         │  │
│  │  │ ATC      │───→│ BRP      │───→│ POS      │         │  │
│  │  │ Adaptive │    │ Bidirect │    │ Predict. │         │  │
│  │  │ Token    │    │ Real-Time│    │ Output   │         │  │
│  │  │ Compress │    │ Protocol │    │ Stream   │         │  │
│  │  └──────────┘    └──────────┘    └──────────┘         │  │
│  │                                                           │  │
│  │  ┌──────────┐    ┌──────────┐    ┌──────────┐         │  │
│  │  │ CAR      │←───│ TEQ      │←───│ EPC      │         │  │
│  │  │ Context  │    │ Token    │    │ Edge Pre │         │  │
│  │  │ Aware    │    │ Economy  │    │ Comput.  │         │  │
│  │  │ Routing  │    │ & QoS    │    │          │         │  │
│  │  └──────────┘    └──────────┘    └──────────┘         │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              Infrastructure Layer                            │  │
│  │                                                           │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  │  │
│  │  │ Broker   │ │ Circuit  │ │ Plugin   │ │ Edge     │  │  │
│  │  │ v3       │ │ Breaker  │ │ System   │ │ Cache    │  │  │
│  │  │(v2 +     │ │ v2 +     │ │ v2 +     │ │          │  │  │
│  │  │ BRP)     │ │ QoS      │ │ EPC      │ │          │  │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │              Transport Layer                                │  │
│  │                                                           │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐  │  │
│  │  │ WebSocket│ │ SSE      │ │ HTTP/2   │ │ gRPC     │  │  │
│  │  │ (BRP)    │ │ (Legacy) │ │ (Fallback)│ │(Enterprise)│ │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## v2 to v3 Feature Map

| Feature | v2 | v3 |
|---------|----|----|
| Scope | Subagent ↔ Subagent | User ↔ AI End-to-End |
| Input | Token encoding | Adaptive Token Compression |
| Output | Static responses | Predictive Output Streaming |
| Transport | HTTP/SSE | Bidirectional Real-Time Protocol |
| Routing | Static priority | Context-Aware Dynamic Routing |
| Security | HMAC signing | Token Economy + QoS |
| Caching | None | Edge Pre-Computation |
| Latency | ~200ms typical | <50ms target (control), <500ms (response) |
| Throughput | 205K msg/s | 1M+ events/s |

## Implementation Plan

### Phase 1: Adaptive Token Compression (ATC)
- Intent classification (5 categories)
- Dynamic compression levels
- Token savings measurement
- Tests + benchmarks

### Phase 2: Predictive Output Streaming (POS)
- Response prediction model
- Progressive rendering pipeline
- Backpressure handling
- Tests + benchmarks

### Phase 3: Bidirectional Real-Time Protocol (BRP)
- WebSocket server with multi-channel support
- Connection quality monitoring
- Heartbeat system
- Tests + benchmarks

### Phase 4: Context-Aware Routing (CAR)
- Query complexity analysis
- Model selection engine
- Performance tracking
- Tests + benchmarks

### Phase 5: Token Economy & QoS (TEQ)
- Token-based billing system
- QoS tier enforcement
- Rate limiting
- Tests + benchmarks

### Phase 6: Edge Pre-Computation (EPC)
- Response caching
- Delta encoding
- Edge node simulation
- Tests + benchmarks

### Phase 7: Integration & Full Stack
- End-to-end integration tests
- Combined benchmark suite
- Documentation
- v3 skill creation

## Files to Create/Modify

### New Files
- `src/atc.py` — Adaptive Token Compression
- `src/pos.py` — Predictive Output Streaming
- `src/brp.py` — Bidirectional Real-Time Protocol
- `src/car.py` — Context-Aware Routing
- `src/teq.py` — Token Economy & QoS
- `src/epc.py` — Edge Pre-Computation
- `src/v3_orchestrator.py` — v3 orchestration layer
- `src/edge_cache.py` — Edge caching system
- `tests/test_atc.py` — ATC tests
- `tests/test_pos.py` — POS tests
- `tests/test_brp.py` — BRP tests
- `tests/test_car.py` — CAR tests
- `tests/test_teq.py` — TEQ tests
- `tests/test_epc.py` — EPC tests
- `tests/test_v3_integration.py` — Full integration
- `docs/PRD_v3.md` — This file
- `docs/ARCHITECTURE_v3.md` — Detailed architecture
- `docs/benchmark_v3.html` — Updated dashboard

### Modified Files
- `src/__init__.py` — v3.0.0 exports
- `src/codec.py` — ATC integration
- `src/broker.py` — BRP multi-channel support
- `src/streaming.py` — Enhanced streaming for POS
- `src/plugin.py` — v3 plugin extensions
- `src/server.py` — BRP WebSocket support
- `docs/benchmark.html` — v3 metrics
- `README.md` — v3 features

## Expected Performance Targets

| Metric | v2 Baseline | v3 Target |
|--------|-------------|-----------|
| Input tokens/query | ~50 | ~15 (70% reduction) |
| Output latency (p50) | ~200ms | ~100ms |
| Output latency (p99) | ~500ms | ~200ms |
| End-to-end round-trip | ~400ms | ~100ms (control) |
| Throughput | 205K msg/s | 1M+ events/s |
| Cache hit rate | 0% | 40% (EPC) |
| Model switching overhead | N/A | <50ms |
| Token savings (total) | 89.6% | 95%+ |

## Verification

```bash
cd /home/columbo/ExtData/TokenRelay
PYTHONPATH=src python3 -m unittest discover tests/ -v  # All tests
PYTHONPATH=src python3 tests/benchmark.py --mode v3     # v3 benchmarks
PYTHONPATH=src python3 src/server.py --port 8081         # Start server
# Open http://localhost:8081/benchmark.html for dashboard
```

## Related Skills

- `token-relay` — v1 basic protocol
- `token-relay2` — v2 infrastructure
- `token-relay3` — v3 endpoint optimization (to be created)
- `hermes-agent` — Agent framework
- `token-translator` — Token meaning translation
