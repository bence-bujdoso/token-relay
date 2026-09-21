# TokenRelay v4 — Swarm Intelligence & Coordination

## Vision

TokenRelay v4 transforms the protocol from a **point-to-point communication** framework into a **swarm intelligence system** where multiple autonomous agents coordinate, self-organize, and reach collective decisions. Where v1 optimized token encoding, v2 added infrastructure, v3 optimized User↔AI paths, and v4 adds **multi-agent coordination intelligence**.

## Core Problem

Single-agent systems face inherent limitations:
1. **Limited capability**: One agent cannot excel at all tasks
2. **Single point of failure**: If the agent fails, the entire system stalls
3. **No collective reasoning**: Individual agents lack diverse perspectives
4. **Static resource allocation**: Fixed agent-to-task mappings waste resources
5. **No adaptive behavior**: Systems cannot reorganize based on changing conditions

v4 addresses all five through swarm intelligence.

## Key Innovations

### 1. SwarmAgent — Autonomous Agent with Identity

Each agent in the swarm is an autonomous entity with:
- **Unique ID**: UUID-based identity for traceability
- **Role**: Worker, Leader, Specialist, Observer, or Gateway
- **Capability Vector**: 6-dimensional representation (reasoning, speed, memory, communication, domain knowledge, reliability)
- **Neighbor List**: Local communication topology
- **Circuit Breaker**: Fault tolerance via v2 infrastructure
- **Token Billing**: Resource consumption tracking via v3 TEQ

Built on v2/v3 foundation:
- `MessageEncoder/Decoder` for serialization
- `CircuitBreaker` for health monitoring
- `TokenBilling` for resource tracking
- `IntentClassifier` for task analysis
- `CARRouter` for intelligent task evaluation

### 2. SwarmCoordinator — Central Management & Discovery

The coordinator manages the entire swarm lifecycle:
- **Agent Registration**: Enforces capacity limits, tracks all agents
- **Task Distribution**: Submits tasks with priority and capability requirements
- **Load Balancing**: Capability-weighted task distribution using CARRouter
- **Conflict Resolution**: Weighted voting when agents compete for tasks
- **Health Monitoring**: Aggregated swarm health via circuit breakers
- **Heartbeat Broadcasting**: Collective health signals

Built on v2/v3 foundation:
- `MessageBroker` for task distribution and priority queuing
- `CARRouter` for intelligent task routing
- `EventBus` for swarm-wide event propagation
- `TokenBilling` for resource consumption tracking

### 3. CollectiveDecision — Voting-Based Consensus

Agents reach decisions through structured voting:
- **Multiple Consensus Methods**:
  - Majority (>50%): Quick decisions for routine matters
  - Supermajority (2/3): Important policy changes
  - Unanimous (100%): Critical system changes
  - Weighted: Weight by capability/stake/role
  - Round-Robin: Rotating leader decides
- **Weighted Voting**: Vote weight = capability × health × role modifier
- **Proposal Lifecycle**: Create → Vote → Consensus → Finalize
- **Result Tracking**: Complete audit trail of all decisions

Built on v2/v3 foundation:
- `MessageEncoder/Decoder` for vote serialization
- `CircuitBreaker` for vote integrity
- `TokenBilling` for vote weight calculation
- `EventBus` for vote propagation

### 4. SelfOrganization — Adaptive Agent Reorganization

Agents dynamically reorganize based on conditions:
- **Three Strategies**:
  - Task Similarity: Group agents by capability vector similarity
  - Resource Awareness: Organize by available resources and capability tiers
  - Hybrid: Combine both strategies for optimal results
- **Dynamic Clustering**: Agents form and reform clusters as conditions change
- **Task Reassignment**: Redistribute tasks when organization changes
- **Anomaly Detection**: Identify stuck agents, compromised nodes, low reliability
- **Adaptive Adaptation**: Automatic reorganization when health degrades

Built on v2/v3 foundation:
- `MessageBroker` for cluster coordination
- `IntentClassifier` for task similarity analysis
- `CARRouter` for resource-based routing
- `AdaptiveCompressor` for message optimization
- `CircuitBreaker` for reorganization safety

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    TokenRelay v4 Stack                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │               Swarm Intelligence Layer                      │  │
│  │                                                            │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐     │  │
│  │  │ SwarmAgent   │  │ SwarmAgent   │  │ SwarmAgent   │     │  │
│  │  │ (LEADER)     │  │ (WORKER)     │  │ (SPECIALIST) │     │  │
│  │  │ Capability:  │  │ Capability:  │  │ Capability:  │     │  │
│  │  │ [0.9,0.7,...]│  │ [0.6,0.8,...]│  │ [0.8,0.5,...]│     │  │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │  │
│  │         │                 │                 │               │  │
│  │         └────────┬────────┴────────────────┘               │  │
│  │                  │                                         │  │
│  │  ┌───────────────▼──────────────────────┐                 │  │
│  │  │         SwarmCoordinator               │                 │  │
│  │  │  • Agent Discovery & Registration     │                 │  │
│  │  │  • Load Balancing                     │                 │  │
│  │  │  • Conflict Resolution                │                 │  │
│  │  │  • Heartbeat Management               │                 │  │
│  │  └───────────────┬───────────────────────┘                 │  │
│  │                  │                                         │  │
│  │  ┌───────────────▼───────────────────────┐                 │  │
│  │  │       CollectiveDecision Engine        │                 │  │
│  │  │  • Proposals & Voting                  │                 │  │
│  │  │  • Majority/Supermajority/Weighted      │                 │  │
│  │  │  • Consensus Results                    │                 │  │
│  │  └───────────────┬───────────────────────┘                 │  │
│  │                  │                                         │  │
│  │  ┌───────────────▼───────────────────────┐                 │  │
│  │  │      SelfOrganization Engine           │                 │  │
│  │  │  • Dynamic Clustering                   │                 │  │
│  │  │  • Task Reassignment                    │                 │  │
│  │  │  • Anomaly Detection                    │                 │  │
│  │  │  • Adaptive Reorganization              │                 │  │
│  │  └───────────────┬───────────────────────┘                 │  │
│  └──────────────────┼──────────────────────────────────────────┘  │
│                     │                                             │
│  ┌──────────────────▼──────────────────────────────────────────┐  │
│  │              v2/v3 Foundation Layer                            │  │
│  │                                                              │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐     │  │
│  │  │  Broker  │ │Circuit   │ │  Codec   │ │  Event   │     │  │
│  │  │          │ │ Breaker  │ │          │ │  Bus     │     │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘     │  │
│  │                                                              │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐     │  │
│  │  │ATC/      │ │BRP/      │ │CAR/      │ │TEQ/      │     │  │
│  ││IntentCls  │ │BRPClient │ │CARRouter │ │TokenBill │     │  │
│  │└──────────┘ └──────────┘ └──────────┘ └──────────┘     │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Use Cases

### Distributed Task Processing
A swarm of specialized agents processes tasks in parallel, with the coordinator distributing work based on capability matching and current load.

### Collaborative Decision-Making
Critical system changes require consensus across multiple agents, ensuring no single point of failure in decision-making.

### Adaptive Resource Management
The swarm automatically reorganizes when agents fail or conditions change, redistributing tasks and reforming clusters.

### Multi-Domain Expertise
Different agents contribute domain-specific knowledge through weighted voting, producing better decisions than any single agent.

### Fault-Tolerant Systems
With circuit breakers and health monitoring, the swarm gracefully handles agent failures without system-wide disruption.

## Configuration

```python
from swarm import SwarmConfig, create_swarm

config = SwarmConfig(
    max_agents=50,
    heartbeat_interval=5.0,
    discovery_timeout=30.0,
    consensus_method=ConsensusMethod.MAJORITY,
    consensus_threshold=0.5,
    enable_self_organization=True,
    organization_strategy=OrganizationStrategy.HYBRID,
)

swarm = create_swarm(config)
```

## Integration with v2/v3

v4 does not reimplement v2/v3 functionality. It leverages:
- **v2**: `MessageBroker`, `CircuitBreaker`, `MessageEncoder/Decoder`, `EventBus`
- **v3**: `IntentClassifier`, `TokenBilling`, `CARRouter`, `BRPServer/BRPClient`
- **v3 Orchestrator**: Pipeline patterns for agent task processing

All v4 components import from v2/v3 foundation modules, never re-implementing their features.

## Performance Targets

| Metric | Target |
|--------|--------|
| Agent Discovery Latency | <100ms for 50 agents |
| Load Balance Distribution | 95%+ capability-weighted accuracy |
| Consensus Time | <500ms for 10 agents |
| Self-Organization Time | <1s for 50 agents |
| Task Reassignment | <200ms per task |
| Swarm Health Check | <50ms for 50 agents |

## Verification

```bash
cd /home/columbo/ExtData/TokenRelay
PYTHONPATH=src python3 -m unittest tests.test_swarm -v
```

The test suite includes 50+ tests covering all four modules, their interactions, edge cases, and full integration workflows.
