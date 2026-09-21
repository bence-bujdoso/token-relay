# TokenRelay v4 — Self-Learning & Adaptive Protocol

## Vision

TokenRelay v4 transforms the protocol from a **static configuration** system into a **self-learning, adaptive communication framework**. Where v3 used fixed intent-based compression rules and one-size-fits-all policies, v4 introduces **reinforcement learning** to discover optimal token allocation strategies through trial-and-error interaction.

The core insight: optimal token compression is not static — it depends on the communication pattern between specific agent pairs, the intent of each message, and the evolving performance characteristics of the system. By learning from past interactions, v4 reduces waste and improves speed over time.

## Core Problem

TokenRelay v3 solved many inefficiencies with **hand-tuned compression rules**:
- Query → 90% compression, Command → 70%, Request → 50%, etc.
- These ratios are optimal for average cases but suboptimal for specific agent pairs
- No mechanism to discover better policies through experience
- Static thresholds cannot adapt to changing communication patterns

v4 addresses these limitations by introducing a **closed-loop learning system** that continuously observes, evaluates, and improves token allocation decisions.

## Key Innovations

### 1. PerformanceTracker — Observing Communication Patterns

Tracks latency, token savings, and success rates per agent pair with full aggregation and top-performer analysis.

- **Per-pair statistics**: Maintains separate metrics for every agent pair
- **Aggregate views**: Global statistics across all pairs
- **Thread-safe**: All operations use RLock for concurrent access
- **EdgeCache integration**: Persists statistics via v3 EdgeCache

```python
tracker = PerformanceTracker()
tracker.record_interaction("agent_a", "agent_b", latency_ms=45.0,
                           tokens_saved=120, success=True)
stats = tracker.get_pair_stats("agent_a", "agent_b")
top = tracker.get_top_performers(n=5)
```

### 2. ReinforcementLearner — Q-Learning Token Allocation

A Q-learning agent that discovers optimal compression policies through trial-and-error. Learns a mapping from `(intent, compression_bucket, agent_pair)` states to actions (INCREASE, DECREASE, MAINTAIN, RESET) that maximize cumulative reward.

**State representation**: Discrete tuple encoding the current intent, compression level bucket, and agent pair hash.

**Learning algorithm**: Standard Q-learning update rule:
```
Q(s,a) ← Q(s,a) + α * [r + γ * max_a' Q(s',a') - Q(s,a)]
```

**Exploration strategy**: Epsilon-greedy with exponential decay. Starts at 100% exploration, decays per episode until reaching minimum floor.

```python
learner = ReinforcementLearner()
action = learner.select_action("query::0.5::agent_a::agent_b")
learner.update_q_value("query::0.5::agent_a::agent_b", "INCREASE", reward=0.8)
policy = learner.get_policy("query::0.5::agent_a::agent_b")
```

### 3. AdaptiveTokenAllocator — Dynamic Compression Levels

Combines PerformanceTracker data with ReinforcementLearner Q-values to select the optimal compression level for each agent pair and intent. Falls back to intent-based defaults when no learned policy exists.

**Allocation flow**:
1. Check if RL learner has a policy for the (pair, intent) state
2. If yes, map the best action to a compression level adjustment
3. If no, fall back to intent-based compression (query=0.9, command=0.7, etc.)
4. On each interaction, adapt: compute reward, update Q-values, adjust allocation

```python
allocator = AdaptiveTokenAllocator()
level = allocator.allocate("agent_a", "agent_b", intent="query")
result = allocator.adapt("agent_a", "agent_b", latency_ms=45.0,
                          tokens_saved=120, success=True)
```

**Reward computation**: Weighted combination of three factors:
- Token savings (weight: 0.4)
- Latency improvement (weight: 0.3)  
- Successful delivery (weight: 0.3)
- Failed delivery penalty: -1.0

### 4. FeedbackLoop — Closed-Loop Optimization

Orchestrates the complete learning cycle: observe → reward → learn → adapt. Maintains a closed-loop system where the allocator's decisions are continuously evaluated and improved.

**Components**:
- `process_interaction()`: Single interaction through the full pipeline
- `run_episode()`: Multiple simulated interactions for training
- `run_adaptation_cycle()`: Multiple cycles across agent pairs
- `get_feedback_quality()`: Evaluates learning progress per pair
- `get_adaptation_summary()`: Comprehensive system state overview

**Fault tolerance**: Uses CircuitBreaker for graceful degradation when the allocator fails.

```python
loop = FeedbackLoop()
result = loop.process_interaction("agent_a", "agent_b", 50.0, 100, True)
training = loop.run_episode("agent_a::agent_b", episodes=20)
quality = loop.get_feedback_quality("agent_a", "agent_b")
```

### 5. SLBundle — Integrated Pipeline

Convenience bundle combining all v4 components into a single initialization point. Provides the simplest API for deploying the self-learning system.

```python
sl = SLBundle()
result = sl.process("agent_a", "agent_b", latency=50.0, tokens_saved=100)
level = sl.allocate("agent_a", "agent_b", intent="query")
summary = sl.get_summary()
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    TokenRelay v4 Stack                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Performance   │  │ Reinforcement │  │ Adaptive     │     │
│  │ Tracker       │  │ Learner       │  │ Token        │     │
│  │              │  │              │  │ Allocator    │     │
│  │ - Per-pair   │  │ - Q-table    │  │              │     │
│  │ - Latency    │  │ - TD update  │  │ - Allocate() │     │
│  │ - Success    │  │ - ε-greedy   │  │ - Adapt()    │     │
│  │ - Aggregate  │  │ - Converge   │  │              │     │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │
│         │                 │                  │              │
│         ▼                 ▼                  ▼              │
│  ┌────────────────────────────────────────────────────┐ │
│  │              FeedbackLoop                          │ │
│  │  - process_interaction()                           │ │
│  │  - run_episode()                                   │ │
│  │  - run_adaptation_cycle()                          │ │
│  │  - CircuitBreaker protection                       │ │
│  │  - EventBus feedback events                        │ │
│  └─────────────────────────────────────────────────────┘ │
│                                                                     │
│  ┌──────────────────────────────────────────────────────┐ │
│  │              SLBundle (convenience)                   │ │
│  └─────────────────────────────────────────────────────────┘ │
│                                                                     │
│  v3 Foundation:                                                   │
│  - atc.IntentClassifier → current intent classification           │
│  - teq.TokenBilling → token economy context                       │
│  - epc.EdgeCache → Q-value and stats persistence                  │
│  - broker.MessageBroker → event logging                           │
│  - circuit_breaker.CircuitBreaker → fault tolerance               │
│  - streaming.EventBus → real-time feedback                        │
└─────────────────────────────────────────────────────────────────┘
```

## v3→v4 Feature Map

| Feature | v3 | v4 |
|---------|----|----|
| Compression | Static intent ratios | RL-adaptive per agent pair |
| Policy | Hand-tuned | Q-learned through interaction |
| Exploration | None | ε-greedy with decay |
| Feedback | None | Closed-loop with reward signal |
| Allocation | Fixed per intent | Dynamic based on history |
| Learning | Not applicable | Q-learning with TD updates |
| Fault tolerance | CircuitBreaker per module | CircuitBreaker for full loop |
| Tests | 1,111 (v3) | 93+ (v4) |

## How to Use

### Quick Start

```python
from src.selflearning import SLBundle

# Initialize the self-learning system
sl = SLBundle()

# Process interactions — the system learns automatically
for i in range(100):
    sl.process("agent_a", "agent_b", latency=50.0, tokens_saved=100, success=True)

# Get the optimal compression level
level = sl.allocate("agent_a", "agent_b", intent="query")

# View learning progress
summary = sl.get_summary()
print(f"Episodes: {summary['total_episodes']}")
print(f"Q-table size: {summary['q_table_size']}")
print(f"Exploration rate: {summary['exploration_rate']}")
```

### Advanced: Custom Configuration

```python
from src.selflearning import SLConfig, ReinforcementLearner, AdaptiveTokenAllocator

# Customize learning parameters
cfg = SLConfig(
    learning_rate=0.2,       # Faster learning
    discount_factor=0.9,     # Less future-focused
    exploration_rate=0.5,    # Start with less exploration
    exploration_decay=0.99,  # Slower decay
    min_exploration=0.05,    # Higher exploration floor
)

learner = ReinforcementLearner(config=cfg)
allocator = AdaptiveTokenAllocator(config=cfg, learner=learner)
tracker = PerformanceTracker(config=cfg)
```

### Training Episodes

```python
from src.selflearning import FeedbackLoop

loop = FeedbackLoop()

# Run a complete training episode for a pair
result = loop.run_episode("agent_a::agent_b", episodes=50)
print(f"Q-table grew to: {result['q_table_size']} entries")
print(f"Exploration decayed to: {result['final_exploration_rate']}")
print(f"Converged: {result['is_converged']}")
```

### Monitoring Learning Progress

```python
# Check feedback quality for a pair
quality = loop.get_feedback_quality("agent_a", "agent_b")
print(f"Quality: {quality['quality']} (score: {quality['score']})")

# Get adaptation summary
summary = loop.get_adaptation_summary()
print(f"Total episodes: {summary['total_episodes']}")
print(f"Q-table entries: {summary['q_table_size']}")
print(f"Breaker state: {summary['breaker_state']}")
```

## Configuration Reference

### SLConfig Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `learning_rate` | 0.1 | Q-learning alpha — how fast Q-values update |
| `discount_factor` | 0.95 | Q-learning gamma — importance of future rewards |
| `exploration_rate` | 1.0 | Initial epsilon for ε-greedy policy |
| `exploration_decay` | 0.995 | Rate of exploration decay per episode |
| `min_exploration` | 0.01 | Minimum exploration rate floor |
| `episodes` | 1000 | Learning episodes before convergence |
| `default_compression` | 0.5 | Fallback compression when no policy exists |
| `reward_token_weight` | 0.4 | Weight of token savings in reward |
| `reward_latency_weight` | 0.3 | Weight of latency improvement in reward |
| `reward_success_weight` | 0.3 | Weight of success in reward |
| `failure_penalty` | -1.0 | Penalty for failed interactions |
| `q_table_size` | 10,000 | Max state-action pairs tracked |
| `history_window` | 100 | Recent interactions to track |
| `adaptation_interval` | 10 | Interactions before policy re-evaluation |

## Reward Signal Design

The reward function is the core of the learning signal:

```
reward = w1 * token_score + w2 * latency_score + w3 * success

where:
  token_score = min(1, tokens_saved / 100)
  latency_score = max(0, 1 - latency_ms / 500)
  success = 1.0 if interaction succeeded else 0.0
  w1 = 0.4, w2 = 0.3, w3 = 0.3

If interaction failed:
  reward = failure_penalty (-1.0)
```

This design ensures the RL agent learns to:
1. Maximize token savings (higher compression when safe)
2. Minimize latency (lower compression when speed matters)
3. Maintain high success rates (avoid excessive compression)

## Convergence Behavior

The system converges when the exploration rate drops below `2 * min_exploration`. At this point, the RL agent primarily exploits its learned policy. The convergence timeline depends on `exploration_decay`:

- **Fast convergence** (decay=0.9): ~200 episodes
- **Moderate convergence** (decay=0.99): ~900 episodes
- **Slow convergence** (decay=0.999): ~4600 episodes

More exploration generally leads to better policies but requires more episodes.

## Testing

```bash
# Run v4 tests
PYTHONPATH=src python3 -m unittest tests.test_selflearning -v

# Run all tests (including v3)
PYTHONPATH=src python3 -m unittest discover tests/ -v
```

The test suite includes 93+ tests covering all v4 components, edge cases, and integration scenarios.

## Dependencies

v4 depends entirely on v3 foundation modules:
- `src.atc` — IntentClassifier, AdaptiveCompressor
- `src.teq` — TokenBilling, QoSTier, TEQConfig
- `src.epc` — EdgeCache, EPCConfig, CacheHitStats
- `src.codec` — compress_message, decompress_message
- `src.broker` — MessageBroker
- `src.circuit_breaker` — CircuitBreaker
- `src.streaming` — EventBus, StreamEvent
- `src.registry` — TokenRegistry, get_registry

No new dependencies are introduced. All v4 modules use Python 3.14 stdlib only.

## Pitfalls

- **Exploration vs exploitation**: High initial exploration rate means random actions early on. Monitor `exploration_rate` to understand learning progress.
- **Cold start**: New agent pairs have no Q-values and fall back to intent-based defaults until sufficient interactions are recorded.
- **Q-table size**: Large numbers of agent pairs can grow the Q-table. Set `q_table_size` to limit memory usage.
- **Reward sparsity**: The reward signal is dense (every interaction produces a reward), which is good for learning but means the signal is noisy.
- **Circuit breaker**: The FeedbackLoop uses CircuitBreaker — if too many interactions fail, the circuit opens and prevents further learning until recovery.
- **Thread safety**: All classes use threading.RLock, but the Q-learning update is not atomic across multiple processes.

## Verification

```bash
cd /home/columbo/ExtData/TokenRelay
# Individual v4 tests
PYTHONPATH=src python3 -m unittest tests.test_selflearning -v

# All tests
PYTHONPATH=src python3 -m unittest discover tests/ -v

# Verify package imports
PYTHONPATH=src python3 -c "from src import ReinforcementLearner, AdaptiveTokenAllocator, FeedbackLoop, SLBundle; print('v4 OK')"
```

## Related

- `token-relay` — v1 basic token communication protocol
- `token-relay2` — v2 infrastructure layer
- `token-relay3` — v3 endpoint-to-endpoint optimization
- `hermes-agent` — Use Hermes Agent framework including `delegate_task`
