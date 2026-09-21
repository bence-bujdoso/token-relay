# TokenRelay v2 — Product Requirements Document

## Vision

Expand TokenRelay from a simple token-encoding protocol into a full-featured subagent communication framework with advanced routing, streaming, encryption, and comprehensive benchmarking.

## v1 → v2 Comparison

| Feature | v1 | v2 |
|---------|----|----|
| Protocol | Basic token IDs | Token IDs + compression + signing + batching |
| Routing | Dict lookup | Priority queues + message broker |
| Error Handling | Simple encode_error | Retry, circuit breaker, dead letter queue |
| Benchmark | Simulation only | Real LLM integration, scalability curves |
| Streaming | None | SSE, WebSocket, event-driven |
| Extensibility | Manual JSON edits | Plugin system, versioned schemas |
| Testing | 6 unit tests | Full suite: unit, integration, load, chaos |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    TokenRelay v2 Stack                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌─────────────┐    ┌──────────────┐    ┌─────────────┐   │
│  │  Registry   │───→│   Encoder     │───→│  Compressor │   │
│  │  v2         │    │  v2          │    │  (zlib)     │   │
│  └─────────────┘    └──────┬───────┘    └─────────────┘   │
│                            │                                │
│  ┌─────────────┐    ┌──────▼───────┐    ┌─────────────┐   │
│  │   Broker    │←───│   Router     │←───│  Prioritizer│   │
│  │  (queue)    │    │  v2          │    │             │   │
│  └─────────────┘    └──────┬───────┘    └─────────────┘   │
│                            │                                │
│  ┌─────────────┐    ┌──────▼───────┐    ┌─────────────┐   │
│  │  Retry/     │←───│  Error       │←───│  Handler    │   │
│  │  Circuit    │    │  Manager     │    │             │   │
│  └─────────────┘    └──────┬───────┘    └─────────────┘   │
│                            │                                │
│  ┌─────────────┐    ┌──────▼───────┐                      │
│  │   Stream    │←───│  Transport   │                      │
│  │  (SSE/WSS)  │    │  (HTTP/WS)   │                      │
│  └─────────────┘    └──────────────┘                      │
│                                                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Benchmark v2                              │   │
│  │  - Real LLM integration                              │   │
│  │  - Scalability curves                                │   │
│  │  - Memory profiling                                  │   │
│  │  - Latency percentiles                               │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Feature List

### F1: Token Compression
- zlib-based compression for token payloads
- Configurable compression level (1-9)
- Compression ratio measurement
- Decompression validation

### F2: Token Signing
- HMAC-SHA256 signature for message integrity
- Optional encryption (AES-256-GCM)
- Key management (in-memory, file-based)
- Signature verification on decode

### F3: Message Broker
- Priority queue (configurable priorities)
- Message deduplication
- Message ordering guarantees
- Queue depth monitoring
- Dead letter queue for failed messages

### F4: Circuit Breaker
- Failure threshold configuration
- Exponential backoff retry
- Half-open state for recovery
- Automatic recovery detection
- Metrics and logging

### F5: Streaming Transport
- Server-Sent Events (SSE) endpoint
- WebSocket support
- Event-driven callback system
- Streaming batch processing
- Backpressure handling

### F6: Plugin System
- `register_protocol()` function
- Custom token type definitions
- Schema versioning
- Migration path support
- Plugin discovery and loading

### F7: Benchmark v2
- Real LLM integration (via subprocess)
- Scalability testing (10 to 100K messages)
- Memory usage profiling
- Latency percentiles (p50, p95, p99)
- Comparative analysis vs gRPC/WebSocket/REST
- Visualization output (HTML + JSON)

### F8: Testing Suite
- Unit tests for all new features
- Integration tests for full pipeline
- Load tests (concurrent message processing)
- Chaos engineering (random failures)
- Benchmark validation tests

## Acceptance Criteria

- All v1 features backward compatible
- 20+ new features implemented
- All tests passing (90%+ coverage)
- Benchmark shows measurable improvements
- Plugin system documented and extensible
- Full documentation updated

## Out of Scope

- Replacing the underlying LLM framework
- Building a new messaging protocol from scratch
- Cross-language bindings
- Production deployment tooling