# TokenRelay v4 — Architecture Document

## System Overview

TokenRelay v4 is a multi-agent framework with four autonomous modules built on a robust v2/v3 foundation. The architecture follows a layered design where v4 modules sit on top of established infrastructure components, ensuring backward compatibility while enabling new capabilities.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        APPLICATION LAYER                            │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────┐  │
│  │  Swarm    │  │ Self-Learning│  │  Privacy     │  │ Interop  │  │
│  │  Agents   │  │ Optimizer    │  │  Verifier    │  │ Bridge   │  │
│  │           │  │              │  │              │  │          │  │
│  │ • create_ │  │ • create_    │  │ • create_    │  │ • create_│  │
│  │   swarms  │  │   learner    │  │   zk_verifier│  │   bridge │  │
│  │ • elect_  │  │ • create_    │  │ • create_    │  │ • create_│  │
│  │   leader  │  │   allocator  │  │   token      │  │   gateway│  │
│  │ • coord.  │  │ • create_    │  │ • create_    │  │ • create_│  │
│  │ • decide  │  │   tracker    │  │   relay      │  │   translator││
│  └─────┬─────┘  └──────┬───────┘  └──────┬───────┘  └────┬─────┘  │
│        │               │                  │                 │        │
├────────┼───────────────┼──────────────────┼─────────────────┼────────┤
│        │               │                  │                 │        │
│  ┌─────▼───────────────▼──────────────────▼─────────────────▼─────┐ │
│  │              v4 CORE SERVICES LAYER                              │ │
│  │                                                                  │ │
│  │  ┌──────────┐  ┌───────────┐  ┌──────────┐  ┌──────────┐     │ │
│  │  │  Swarm    │  │ Self-Learn│  │  Privacy  │  │  Interop  │     │ │
│  │  │  Engine   │  │ Engine    │  │  Engine   │  │  Engine   │     │ │
│  │  │           │  │           │  │           │  │           │     │ │
│  │  │ - Agents  │  │ - Q-Table │  │ - ZK Proof│  │ - Bridge  │     │ │
│  │  │ - Tasks   │  │ - Policy  │  │ - Token   │  │ - Gateway │     │ │
│  │  │ - Votes   │  │ - Tracker │  │ - Relay   │  │ - Adapter │     │ │
│  │  │ - Org.    │  │ - Feedback│  │ - Diff.P. │  │ - Trans.  │     │ │
│  │  └──────────┘  └───────────┘  └──────────┘  └──────────┘     │ │
│  └───────────────────────────────────────────────────────────────┘ │
│                                                                     │
├─────────────────────────────────────────────────────────────────────┤
│                    v2/v3 FOUNDATION LAYER                            │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │  Broker  │  │Circuit   │  │  Codec   │  │  BRP     │          │
│  │  Priority │  │Breaker   │  │  Compress │  │  Channel │          │
│  │  Queue    │  │          │  │  Decode   │  │  Manager │          │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘          │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │  Registry│  │ATC      │  │  TEQ     │  │  EPC     │          │
│  │  Token   │  │Classifier│  │  Billing │  │  Edge    │          │
│  │          │  │Compressor│  │  QoS     │  │  Cache   │          │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘          │
├─────────────────────────────────────────────────────────────────────┤
│                       TRANSPORT LAYER                               │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │  HTTP/REST │ gRPC │ MQTT │ WebSocket │ SSE │ NATS │ AMQP    │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Module Architecture

### 1. Swarm Intelligence Architecture

```
┌──────────────────────────────────────────┐
│              SwarmCoordinator              │
│  ┌──────────────────────────────────┐    │
│  │  Agent Registry (Dict[str, SwarmAgent]) │
│  │  Task Registry (Dict[str, SwarmTask])   │
│  │  Load Distributor                  │
│  │  Leader Elector                    │
│  └──────────────────────────────────┘    │
│                                          │
│  ┌─────────────┐  ┌──────────────────┐  │
│  │ SwarmAgent  │──│ SwarmAgent       │  │
│  │ • agent_id  │  │ • agent_id       │  │
│  │ • role      │  │ • role           │  │
│  │ • capabilities│ │ • capabilities  │  │
│  │ • state     │  │ • state          │  │
│  │ • broker    │  │ • broker         │  │
│  │ • circuit   │  │ • circuit        │  │
│  └─────────────┘  └──────────────────┘  │
│                                          │
│  ┌─────────────┐  ┌──────────────────┐  │
│  │ Collective  │  │ SelfOrganization │  │
│  │ Decision    │  │                  │  │
│  │ • vote()    │  │ • organize()     │  │
│  │ • Consensus │  │ • Groups         │  │
│  └─────────────┘  └──────────────────┘  │
└──────────────────────────────────────────┘
```

### 2. Self-Learning Architecture

```
┌──────────────────────────────────────────┐
│            SLBundle                      │
│  ┌──────────────┐  ┌──────────────────┐  │
│  │  Reinforcement│  │  AdaptiveToken   │  │
│  │  Learner      │  │  Allocator       │  │
│  │               │  │                  │  │
│  │ • Q-table     │  │ • allocate()     │  │
│  │ • choose()    │  │ • get_optimal()  │  │
│  │ • learn()     │  │                  │  │
│  │ • policy()    │  │                  │  │
│  └──────────────┘  └──────────────────┘  │
│  ┌──────────────┐  ┌──────────────────┐  │
│  │  Performance  │  │  FeedbackLoop    │  │
│  │  Tracker      │  │                  │  │
│  │               │  │ • get_feedback() │  │
│  │ • record()    │  │ • quality()      │  │
│  │ • get_stats() │  │                  │  │
│  └──────────────┘  └──────────────────┘  │
└──────────────────────────────────────────┘
```

### 3. Privacy Architecture

```
┌──────────────────────────────────────────┐
│             ZKProofVerifier               │
│  ┌──────────────────────────────────┐    │
│  │  Proof Store (Dict[str, ZKProof]) │    │
│  │  Index (Dict[str, List[str]])     │    │
│  │  Circuit Breaker                  │    │
│  │  Message Broker                   │    │
│  └──────────────────────────────────┘    │
│                                          │
│  ┌─────────────┐  ┌──────────────────┐  │
│  │ ZKProof     │  │ PrivacyToken     │  │
│  │ • proof_id  │  │ • visibility     │  │
│  │ • commitment│  │ • config         │  │
│  │ • witness   │  │ • TokenBilling   │  │
│  └─────────────┘  └──────────────────┘  │
│                                          │
│  ┌─────────────┐  ┌──────────────────┐  │
│  │ Anonymous   │  │ Differential     │  │
│  │ Relay       │  │ Privacy          │  │
│  │ • level     │  │ • epsilon        │  │
│  │ • mixer     │  │ • delta          │  │
│  └─────────────┘  └──────────────────┘  │
└──────────────────────────────────────────┘
```

### 4. Interop Architecture

```
┌──────────────────────────────────────────┐
│          CrossProtocolGateway             │
│  ┌──────────────────────────────────┐    │
│  │  ProtocolBridge                   │    │
│  │  • BridgeState (CONNECTED/PAUSED) │    │
│  │  • connect_external()             │    │
│  │  • disconnect_external()          │    │
│  │  • get_bridge_stats()             │    │
│  └──────────────────────────────────┘    │
│                                          │
│  ┌─────────────┐  ┌──────────────────┐  │
│  │ Adapter     │  │ Translation      │  │
│  │ Registry    │  │ Rule             │  │
│  │ • adapters  │  │ • source_format  │  │
│  │ • protocol  │  │ • target_format  │  │
│  │   index     │  │ • rules          │  │
│  └─────────────┘  └──────────────────┘  │
│                                          │
│  ┌─────────────┐  ┌──────────────────┐  │
│  │ Universal   │  │ InteropBRPServer │  │
│  │ Translator  │  │ • port           │  │
│  │ • translate │  │ • gateway        │  │
│  └─────────────┘  └──────────────────┘  │
└──────────────────────────────────────────┘
```

## Data Flow

### Swarm Task Flow

1. User submits task to `SwarmCoordinator.submit_task()`
2. Task registered in `task_registry`
3. `SwarmCoordinator.distribute_load()` assigns tasks
4. `SwarmAgent` receives task via `task_queue`
5. Agent executes task and calls `record_result()`
6. `CollectiveDecision.vote()` reaches consensus
7. Results stored in `completed_tasks`

### Learning Optimization Flow

1. `PerformanceTracker.record_interaction()` captures agent metrics
2. `ReinforcementLearner.learn()` updates Q-table
3. `AdaptiveTokenAllocator.allocate()` determines optimal compression
4. `FeedbackLoop.get_feedback_quality()` evaluates learning progress
5. `SLBundle` aggregates all components for unified access

### Privacy Verification Flow

1. Prover generates ZK proof via `ZKProofVerifier.issue_proof()`
2. Proof stored with commitment and witness hash
3. Verifier checks proof via `verify_proof()` or `verify_batch()`
4. `PrivacyPreservingToken` manages token visibility
5. `AnonymousRelay` routes messages without revealing identity
6. `DifferentialPrivacy` adds noise for additional protection

### Interop Communication Flow

1. Message arrives at `ProtocolBridge` with source protocol
2. `AdapterRegistry` routes to appropriate `ExternalAdapter`
3. `UniversalTranslator` converts message format using `TranslationRule`
4. Message delivered via `CrossProtocolGateway`
5. `InteropBRPServer` handles BRP-specific requests
6. Bridge state tracked and reported via `get_bridge_stats()`

## Component Integration Matrix

| v4 Module | v2/v3 Component | Integration Point |
|-----------|----------------|-------------------|
| SwarmAgent | MessageBroker | Agent message routing |
| SwarmCoordinator | CircuitBreaker | Fault tolerance |
| SwarmCoordinator | EventBus | Event-driven task dispatch |
| ReinforcementLearner | codec | Message compression |
| ZKProofVerifier | CircuitBreaker | Proof verification protection |
| ZKProofVerifier | MessageBroker | Proof queue management |
| ProtocolBridge | AdapterRegistry | External protocol adapters |
| ProtocolBridge | CARRouter | Compression-aware routing |
| ProtocolBridge | TokenBilling | QoS-aware billing |
| ProtocolBridge | codec | Cross-protocol compression |
| All Modules | EventBus | Event subscription |
| All Modules | MessageBroker | Message queuing |
| All Modules | codec | Message encoding |

## Configuration

All modules use configuration objects that extend base configurations:

- `SwarmConfig`: Max agents, task timeout, consensus method
- `SLConfig`: Learning rate, discount factor, max episodes
- `BridgeConfig`: Max adapters, auto-reconnect, heartbeat interval
- `GatewayConfig`: Port, host, max connections
- `TEQConfig`: Billing tier, QoS parameters
- `BRPConfig`: Channel management, priority levels

## Error Handling

All v4 modules follow the v2/v3 error handling pattern:

- `SwarmError` base class with specific subclasses
- `InteropError` base class with protocol-specific errors
- `CircuitBreaker` wraps all external calls
- `MessageBroker` handles queue overflow with DLQ
- Graceful degradation when modules fail

## Performance Characteristics

| Metric | Target | Measurement |
|--------|--------|-------------|
| Swarm agent count | 50+ | `SwarmCoordinator.get_metrics()` |
| Learning episodes | 1,000+ | `ReinforcementLearner.get_metrics()` |
| ZK proof throughput | 10,000+ | `ZKProofVerifier.get_metrics()` |
| Protocol support | 8+ | `ProtocolBridge.get_bridge_stats()` |
| Source lines | ~3,000 | vs v3 11,400 (>90% savings) |
| Integration tests | 181 | `tests/test_v4_integration.py` |

## Deployment

The v4 server runs on port 8081 with the following endpoints:

- `GET /metrics/v4` — Returns all v4 module metrics
- `GET /status/v4` — Returns v4 system status
- `POST /benchmark` — Runs benchmark tests
- `GET /health` — Health check
