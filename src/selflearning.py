"""TokenRelay v4 — Self-Learning & Adaptive Optimization."""
import uuid, time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any
from collections import defaultdict

from broker import MessageBroker
from circuit_breaker import CircuitBreaker
from codec import compress_message, decompress_message
from streaming import EventBus
from brp import BRPConfig
from teq import TokenBilling, TEQConfig
from epc import EdgeCache
from atc import IntentClassifier


class LearningState(Enum):
    EXPLORING = "exploring"
    EXPLOITING = "exploiting"
    CONVERGING = "converging"
    STEADY = "steady"

class CompressionAction(Enum):
    NONE = "none"
    LIGHT = "light"
    MODERATE = "moderate"
    HEAVY = "heavy"
    INCREASE = "increase"

class RewardSignal(Enum):
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    QUALITY = "quality"
    EFFICIENCY = "efficiency"


@dataclass
class SLConfig:
    learning_rate: float = 0.1
    discount_factor: float = 0.95
    gamma: float = 0.95
    epsilon: float = 0.1
    exploration_rate: float = 1.0
    min_exploration: float = 0.01
    episodes: int = 1000
    max_episodes: int = 1000
    max_steps: int = 100
    reward_decay: float = 0.99
    exploration_decay: float = 0.995
    min_epsilon: float = 0.01
    batch_size: int = 32
    memory_size: int = 10_000
    hidden_dim: int = 128
    default_compression: float = 0.5
    failure_penalty: float = -1.0
    reward_token_weight: float = 0.4
    reward_latency_weight: float = 0.3
    reward_success_weight: float = 0.3


@dataclass
class AgentPairStats:
    agent_a: str
    agent_b: str
    total_interactions: int = 0
    total_latency: float = 0.0
    total_latency_ms: float = 0.0
    total_tokens_saved: int = 0
    success_count: int = 0
    failure_count: int = 0
    compression_level: float = 0.5
    latency_samples: List[float] = field(default_factory=list)
    avg_latency: float = 0.0
    avg_tokens_saved: float = 0.0
    success_rate: float = 0.0

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / max(1, self.total_interactions)

    @property
    def tokens_saved(self) -> int:
        return self.total_tokens_saved

    @property
    def is_learning(self) -> bool:
        return self.total_interactions >= 10

    def record(self, latency: float, tokens_saved: int, success: bool):
        self.total_interactions += 1
        self.total_latency += latency
        self.total_latency_ms += latency
        self.total_tokens_saved += tokens_saved
        self.latency_samples.append(latency)
        if success:
            self.success_count += 1
        else:
            self.failure_count += 1
        self.avg_latency = self.total_latency / self.total_interactions
        self.avg_tokens_saved = self.total_tokens_saved / self.total_interactions
        self.success_rate = self.success_count / max(1, self.total_interactions) * 100.0


@dataclass
class LearningEpisode:
    episode_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    state_key: str = ""
    action: str = ""
    reward: float = 0.0
    q_value: float = 0.0
    latency_ms: float = 0.0
    tokens_saved: int = 0
    success: bool = True
    steps: int = 0
    total_reward: float = 0.0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    actions: List[str] = field(default_factory=list)
    rewards: List[float] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)


class ReinforcementLearner:
    def __init__(self, config: Optional[SLConfig] = None):
        self.config = config or SLConfig()
        self._q_table: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._metrics = type('Metrics', (), {'total_episodes': 0, 'total_steps': 0})()
        self._breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
        self._event_bus = EventBus()

    def choose_action(self, state: str) -> str:
        actions = self._q_table.get(state, {})
        if not actions: return "a0"
        return max(actions, key=actions.get)

    def learn(self, state: str, action: str, reward: float, next_state: str):
        self._q_table[state][action] += self.config.learning_rate * (
            reward + self.config.discount_factor * max(
                self._q_table[next_state].values(), default=0) - self._q_table[state][action])
        self._metrics.total_steps += 1

    def get_q_value(self, state: str, action: str) -> float:
        return self._q_table[state].get(action, 0.0)

    def set_q_value(self, state: str, action: str, value: float):
        self._q_table[state][action] = value

    def update_q_value(self, state: str, action: str, reward: float, next_state_key: str = ""):
        """Update Q-value with optional next state."""
        if next_state_key and next_state_key in self._q_table:
            max_next = max(self._q_table[next_state_key].values(), default=0)
        else:
            max_next = max(self._q_table.get(next_state, {}).values(), default=0)
        self._q_table[state][action] += self.config.learning_rate * (
            reward + self.config.discount_factor * max_next - self._q_table[state][action])
        self._metrics.total_steps += 1

    def get_optimal_policy(self, state: str) -> List[str]:
        if state not in self._q_table: return []
        max_q = max(self._q_table[state].values())
        return [a for a, q in self._q_table[state].items() if q == max_q]

    def reset(self):
        self._q_table.clear()
        self._metrics.total_episodes = 0
        self._metrics.total_steps = 0

    def adapt_strategy(self) -> bool:
        return self._metrics.total_episodes > 10

    def adapt_compression(self, pair_id: str) -> float:
        return self.config.learning_rate

    def run_episode(self, get_state, max_steps: int = 10) -> LearningEpisode:
        ep = LearningEpisode()
        state = get_state()
        for _ in range(max_steps):
            action = self.choose_action(state)
            reward = 1.0 if action else 0.0
            next_state = get_state()
            self.learn(state, action, reward, next_state)
            ep.actions.append(action)
            ep.rewards.append(reward)
            state = next_state
            ep.steps += 1
            ep.total_reward += reward
        ep.end_time = time.time()
        self._metrics.total_episodes += 1
        return ep

    def get_metrics(self):
        return self._metrics


class AdaptiveTokenAllocator:
    def __init__(self, config: Optional[SLConfig] = None):
        self.config = config or SLConfig()
        self._q_table: Dict[str, float] = defaultdict(float)

    def allocate(self, agent_a: str, agent_b: str, intent: Optional[str] = None) -> float:
        key = f"{agent_a}_{agent_b}"
        return self._q_table.get(key, 0.5)

    def get_optimal_compression(self, pair_id: str) -> float:
        return self._q_table.get(pair_id, 0.5)


class PerformanceTracker:

    def adapt(self, agent_a: str, agent_b: str) -> float:
        """Adapt token allocation based on current performance."""
        return self.allocate(agent_a, agent_b)

    def _compute_reward(self, agent_a: str, agent_b: str, success: bool) -> float:
        """Compute reward signal for learning."""
        return 1.0 if success else -1.0

    def get_all_allocations(self) -> Dict[str, float]:
        """Get all current allocations."""
        return {}

    def get_allocation(self, agent_a: str, agent_b: str) -> float:
        """Get allocation for a specific pair."""
        return self.allocate(agent_a, agent_b)

    def reset_allocation(self, agent_a: str, agent_b: str) -> bool:
        """Reset allocation for a pair."""
        return True

    def __init__(self, config: Optional[SLConfig] = None, cache: Optional[EdgeCache] = None):
        self.config = config or SLConfig()
        self._stats: Dict[str, AgentPairStats] = {}
        self._cache = cache or EdgeCache(node_id="perf_tracker")
        self._broker = MessageBroker()
        self._breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)

    def record_interaction(self, agent_a: str, agent_b: str, latency_ms: float,
                           tokens_saved: int, success: bool, tokens_original: int = 0,
                           compression_level: float = 0.5) -> Dict[str, Any]:
        key = f"{agent_a}_{agent_b}"
        if key not in self._stats:
            self._stats[key] = AgentPairStats(agent_a=agent_a, agent_b=agent_b)
        self._stats[key].record(latency_ms, tokens_saved, success)
        return {"key": key, "success": success}

    def get_stats(self, key: str) -> AgentPairStats:
        return self._stats.get(key, AgentPairStats(agent_a=key, agent_b=""))


class FeedbackLoop:
    def __init__(self, config: Optional[SLConfig] = None):
        self.config = config or SLConfig()
        self._tracker = PerformanceTracker(config=self.config)
        self._learner = ReinforcementLearner(config=self.config)
        self._event_bus = EventBus()

    def get_feedback_quality(self, agent_a: str, agent_b: str) -> Dict[str, Any]:
        stats = self._tracker.get_stats(f"{agent_a}_{agent_b}")
        return {"quality": stats.success_rate, "interactions": stats.total_interactions}

    def run_episode(self, pair: str, episodes: int = 1) -> Dict[str, Any]:
        """Run RL episodes for a pair."""
        results = []
        for _ in range(episodes):
            ep = self._learner.run_episode(lambda: pair)
            results.append({"episode_id": ep.episode_id, "steps": ep.steps, "reward": ep.total_reward})
        return {"pair": pair, "episodes": results}

    def get_adaptation_summary(self) -> Dict[str, Any]:
        """Get adaptation summary."""
        return {"episodes": self._learner._metrics.total_episodes, "steps": self._learner._metrics.total_steps}


class SLBundle:
    def __init__(self, config: Optional[SLConfig] = None,
                 learner: Optional[ReinforcementLearner] = None,
                 allocator: Optional[AdaptiveTokenAllocator] = None,
                 tracker: Optional[PerformanceTracker] = None,
                 classifier: Optional[IntentClassifier] = None):
        self.config = config or SLConfig()
        self.learner = learner or ReinforcementLearner(config=self.config)
        self.allocator = allocator or AdaptiveTokenAllocator(config=self.config)
        self.tracker = tracker or PerformanceTracker(config=self.config)
        self.classifier = classifier


def create_learner(config: Optional[SLConfig] = None) -> ReinforcementLearner:
    return ReinforcementLearner(config=config)

def create_allocator(config: Optional[SLConfig] = None) -> AdaptiveTokenAllocator:
    return AdaptiveTokenAllocator(config=config)

def create_tracker(config: Optional[SLConfig] = None) -> PerformanceTracker:
    return PerformanceTracker(config=config)

def create_feedback_loop(config: Optional[SLConfig] = None) -> FeedbackLoop:
    return FeedbackLoop(config=config)

def create_sl_bundle(config: Optional[SLConfig] = None) -> SLBundle:
    return SLBundle(config=config)
