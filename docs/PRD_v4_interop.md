# TokenRelay v4 — Cross-Protocol Interoperability PRD

## Vision

TokenRelay v4 transforms the protocol from an **internal communication framework** into an **open interoperability layer** that connects TokenRelay's token-based messaging to the broader external protocol ecosystem. Any protocol — HTTP/REST, gRPC, MQTT, WebSocket, SSE, NATS, AMQP, Kafka — can now participate in token-based communication through standardized adapters.

## Core Problem

TokenRelay v1–v3 excelled at internal token-based communication between subagents and User↔AI pathways, but remained siloed within its own protocol. External systems could not participate in token-based messaging without custom integration code. Every new external protocol required bespoke bridging logic, creating fragmentation and maintenance overhead.

v4 solves this by providing a **universal interoperability layer** with:
- Adapter-based registration for any external protocol
- Automatic format translation between TokenRelay tokens and external message formats
- Rule-based field mapping and conversion between protocol representations
- Unified gateway for bidirectional protocol communication

## Architecture Overview

```
┌───────────────────────────────────────────────────────────────────────┐
│                  TokenRelay v4 — Cross-Protocol Interop Layer            │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐  │
│  │  ProtocolBridge   │◄──►│ CrossProtocol   │◄──►│ Universal        │  │
│  │  (Bridge & Route) │    │ Gateway          │    │ Translator       │  │
│  └────────┬─────────┘    └────────┬─────────┘    └────────┬─────────┘  │
│           │                       │                       │            │
│           │    ┌──────────────────┼──────────────────┐    │            │
│           │    │                  │                  │    │            │
│           ▼    ▼                  ▼                  ▼    ▼            │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                    AdapterRegistry                            │   │
│  │  • Register/Unregister external protocol adapters             │   │
│  │  • Connection lifecycle management                            │   │
│  │  • Protocol-specific statistics                               │   │
│  │  • Best-adapter routing                                       │   │
│  └──────────────────────────────────────────────────────────────┘   │
│           │                                                        │
│           ▼                                                        │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │              External Protocol Ecosystem                      │   │
│  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐    │   │
│  │  │HTTP/ │ │gRPC  │ │MQTT  │ │WS    │ │SSE   │ │Kafka │    │   │
│  │  │REST  │ │      │ │      │ │      │ │      │ │      │    │   │
│  │  └──────┘ └──────┘ └──────┘ └──────┘ └──────┘ └──────┘    │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │  v3 Foundation (unchanged)                                    │   │
│  │  • BRPServer ── Bidirectional Real-Time Protocol               │   │
│  │  • CARRouter ── Context-Aware Routing                          │   │
│  │  • TokenBilling ── Token Economy & QoS                         │   │
│  │  • MessageBroker ── Priority Message Queue                     │   │
│  │  • CircuitBreaker ── Fault Tolerance                           │   │
│  │  • EventBus ── Async Event Streaming                           │   │
│  └──────────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────┘
```

## Key Components

### 1. ProtocolBridge

Bridges TokenRelay to external protocols with bidirectional forwarding.

**Responsibilities:**
- Forward TokenRelay tokens to external protocol endpoints
- Receive external messages and translate to TokenRelay format
- Route messages through CAR for quality optimization
- Consume tokens via TEQ for each cross-protocol operation
- Maintain connection state through AdapterRegistry

**Key Methods:**
- `connect_external()` — Register and connect to an external protocol endpoint
- `forward_token()` — Forward a token to an external protocol
- `receive_external()` — Receive and translate an external message
- `get_bridge_stats()` — Get bridge performance statistics

**Integration Points:**
- Uses CARRouter for intelligent routing
- Uses TokenBilling for token consumption tracking
- Uses AdapterRegistry for endpoint management

### 2. CrossProtocolGateway

Translates between TokenRelay tokens and external message formats.

**Responsibilities:**
- Bidirectional translation between TokenRelay format and external formats
- Support for JSON, Protobuf, MessagePack, YAML, CBOR, XML
- Format-specific serialization/deserialization handlers
- Translation result caching for performance
- Batch translation support

**Key Methods:**
- `translate()` — Translate a single token between formats
- `batch_translate()` — Translate multiple tokens in batch
- `get_supported_formats()` — List all supported translation formats
- `clear_cache()` — Clear the translation cache

**Supported Formats:**
| Format | Serialization | Use Case |
|--------|--------------|----------|
| JSON | json.dumps/json.loads | Universal interchange |
| Protobuf | Binary encoding | High-performance microservices |
| MessagePack | Compact binary | IoT/edge devices |
| YAML | Human-readable | Configuration files |
| CBOR | Concise Binary | Constrained environments |
| XML | Markup-based | Enterprise integration |

### 3. AdapterRegistry

Manages the lifecycle of external protocol adapters.

**Responsibilities:**
- Register and unregister external protocol adapters
- Track adapter connection states (Registered, Connected, Active, Paused, Disconnected, Error)
- Maintain protocol-specific adapter indices
- Broadcast messages to all adapters of a given protocol type
- Route messages to the best available adapter
- Integrate with TEQ for adapter billing

**Key Methods:**
- `register_adapter()` — Register a new external protocol adapter
- `unregister_adapter()` — Remove an adapter from the registry
- `connect_adapter()` / `disconnect_adapter()` — Connection management
- `pause_adapter()` / `resume_adapter()` — Pause/resume message processing
- `broadcast_to_protocol()` — Broadcast to all adapters of a protocol type
- `route_to_best_adapter()` — Select best adapter based on activity and error rate

**Adapter States:**
```
REGISTERED → CONNECTED → ACTIVE
                 ↓           ↓
           DISCONNECTED  PAUSED
                 ↓           ↓
                 ERROR ←───┘
```

### 4. UniversalTranslator

Convert between different protocol representations using translation rules.

**Responsibilities:**
- Convert messages between any supported protocol representations
- Apply field mapping rules for semantic translation
- Support custom translation rules with priority ordering
- Batch conversion support
- Integrate with CAR for route optimization

**Key Methods:**
- `convert()` — Convert a single message between formats
- `convert_batch()` — Convert multiple messages in batch
- `add_rule()` / `remove_rule()` — Manage translation rules
- `get_rules()` — Get all registered translation rules

**Default Rules:**
- JSON ↔ Protobuf (priority 1)
- JSON ↔ MessagePack (priority 2)
- JSON ↔ YAML (priority 2)
- JSON ↔ CBOR (priority 3)
- JSON ↔ XML (priority 3)

### 5. InteropBRPServer

BRP server enhanced with cross-protocol bridging capabilities.

**Responsibilities:**
- Wrap v3 BRPServer with protocol bridging
- Allow WebSocket connections to translate to external protocols
- Provide unified status and metrics

## Data Flow

### Forward Flow (TokenRelay → External Protocol)

```
User Token → ProtocolBridge.forward_token()
    ↓
CAR Router: Route query to optimal endpoint
    ↓
TEQ Billing: Consume tokens for cross-protocol operation
    ↓
CrossProtocolGateway: Translate token to external format
    ↓
AdapterRegistry: Select best adapter for target protocol
    ↓
External Protocol Endpoint
```

### Receive Flow (External Protocol → TokenRelay)

```
External Message → ProtocolBridge.receive_external()
    ↓
CrossProtocolGateway: Translate external format to TokenRelay token
    ↓
CAR Router: Route to optimal internal endpoint
    ↓
TEQ Billing: Consume tokens for translation
    ↓
Internal TokenRelay Processing
```

## v2→v3→v4 Feature Map

| Feature | v2 | v3 | v4 |
|---------|----|----|----|
| Scope | Subagent infrastructure | User↔AI | Universal interoperability |
| Protocols | HTTP/SSE | Bidirectional Real-Time | HTTP/gRPC/MQTT/WS/SSE/NATS/AMQP/Kafka |
| Message Format | Token encoding | Token encoding + compression | JSON/Protobuf/MessagePack/YAML/CBOR/XML |
| Routing | Priority queues | Context-aware routing | Protocol-aware routing |
| Extensibility | Plugin system | Plugin system | Adapter-based protocol registration |
| Translation | None | None | Universal format translation |
| External Integration | None | None | Full ecosystem connectivity |
| Source Lines | ~5,000 | ~11,400 | ~2,500 (interop) |
| Tests | 211 | 1,111 | 40+ (interop) |

## Configuration

### BridgeConfig

| Parameter | Default | Description |
|-----------|---------|-------------|
| `default_protocol` | HTTP_REST | Default external protocol |
| `bridge_port` | 8766 | Port for the bridge server |
| `enable_bidirectional` | true | Enable bidirectional bridging |
| `max_pending_messages` | 10000 | Max pending messages queue |
| `token_budget` | 100000 | Token budget for cross-protocol ops |
| `circuit_breaker_enabled` | true | Enable circuit breaker |
| `retry_attempts` | 3 | Retry attempts for failed translations |

### GatewayConfig

| Parameter | Default | Description |
|-----------|---------|-------------|
| `default_source_format` | JSON | Default source format |
| `default_target_format` | PROTOBUF | Default target format |
| `enable_compression` | true | Compress during translation |
| `max_format_conversions` | 3 | Max format conversions per translation |
| `token_consumption_per_translation` | 5 | Token cost per translation |

## QoS Integration

v4 integrates with the v3 Token Economy & QoS system:

- **TokenBilling**: Each cross-protocol operation consumes tokens
- **QoSTier**: Different tiers get different rate limits and latency guarantees
- **RateLimiter**: Protects against excessive cross-protocol traffic
- **SLAMonitor**: Monitors cross-protocol SLA compliance

## Error Handling

### Custom Exceptions

| Exception | Trigger |
|-----------|---------|
| `InteropError` | Base interoperability error |
| `AdapterNotFoundError` | Adapter not registered |
| `ProtocolNotSupportedError` | Protocol type not supported |
| `TranslationError` | Format translation failed |

### Circuit Breaker Pattern

All external protocol calls go through the v3 CircuitBreaker:
- Failure threshold: 5 consecutive failures
- Recovery timeout: 30 seconds
- Graceful degradation: Fallback to internal processing

## Performance Targets

| Metric | Target |
|--------|--------|
| Translation latency | <10ms for JSON↔JSON |
| Translation latency | <50ms for JSON↔Protobuf |
| Gateway throughput | >10K translations/second |
| Adapter connection time | <100ms |
| Bridge forwarding latency | <20ms overhead |

## Security

- Adapter authentication via `auth_token` configuration
- Message signing through v3 HMAC-SHA256 signing
- Optional AES-256-GCM encryption for sensitive payloads
- Circuit breaker prevents cascade failures to external systems
- Token consumption ensures resource accountability

## Testing Strategy

The v4 test suite covers:

1. **AdapterRegistry Tests**: Registration, connection lifecycle, broadcast, routing, statistics
2. **ProtocolBridge Tests**: Forward/receive operations, error handling, bidirectional mode
3. **CrossProtocolGateway Tests**: All format translations, batch operations, caching
4. **UniversalTranslator Tests**: Convert operations, rule management, batch conversions
5. **Integration Tests**: Full stack creation, end-to-end message flow
6. **Factory Tests**: Factory function creation of all components
7. **Exception Tests**: All custom exception classes
8. **Enum Tests**: All enum values and constants

**Minimum test count**: 40+ unit tests across all modules.

## Deployment

v4 requires no additional infrastructure beyond the v3 stack. The interoperability layer is pure Python with no external dependencies beyond what v2/v3 already use (json, threading, dataclasses, etc.).

### Quick Start

```python
from src.interop import create_interop_stack

# Create complete v4 interoperability stack
stack = create_interop_stack()

# Connect to external protocols
bridge = stack["protocol_bridge"]
bridge.connect_external(
    protocol_type=ProtocolType.MQTT,
    endpoint="mqtt://iot-broker:1883",
)

# Forward tokens to external systems
result = bridge.forward_token(
    token_id="42",
    payload={"data": "sensor_reading"},
    target_protocol=ProtocolType.MQTT,
)

# Translate between formats
gateway = stack["cross_protocol_gateway"]
result = gateway.translate(
    token_id="42",
    payload={"key": "value"},
    source_format=TranslationFormat.JSON,
    target_format=TranslationFormat.PROTOBUF,
)
```

## Future Work

- **gRPC streaming**: Native gRPC bidirectional streaming support
- **Protocol auto-discovery**: Dynamic protocol detection via mDNS/DNS-SD
- **Schema registry**: Versioned schema management for Protobuf/Avro
- **Metrics export**: Prometheus/Grafana integration for interop metrics
- **Protocol-specific adapters**: Dedicated adapter packages for major platforms

## Dependencies

- Python 3.14 stdlib only
- v3 foundation modules: BRPServer, CARRouter, TokenBilling
- v2 foundation modules: MessageBroker, CircuitBreaker, MessageEncoder/Decoder
- No additional pip packages required
