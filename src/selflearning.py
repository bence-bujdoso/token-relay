from collections import defaultdict
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
import time
import uuid
from threading import Lock

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
    INCREASE = "increase"
    DECREASE = "decrease"
    MAINTAIN = "maintain"
    RESET = "reset"
    

class RewardSignal(Enum):
    THROUGHPUT = "throughput"
    LATENCY = "latency"
    QUALITY = "quality"
    EFFICIENCY = "efficiency"
    OVERALL = "overall"
@dataclass
class SLConfig:
    learning_rate: float = 0.1
    discount_factor: float = 0.95
    gamma: float = 0.95
    epsilon: float = 0.1
    max_episodes: int = 1000
    max_steps: int = 100
    reward_decay: float = 0.99
    exploration_rate: float = 1.0
    exploration_decay: float = 0.995
    min_epsilon: float = 0.01
    batch_size: int = 32
    episodes: int = 100
    memory_size: int = 10_000
    hidden_dim: int = 128
    min_exploration: float = 0.01
    default_compression: float = 0.5
    failure_penalty: float = -1.0
    reward_token_weight: float = 0.4
    reward_latency_weight: float = 0.3
    reward_success_weight: float = 0.3
@dataclass

class AgentPairStats:
    agent_a: str = ""
    agent_b: str = ""
    total_interactions: int = 0
    total_latency: float = 0.0
    total_latency_ms: float = 0.0
    total_tokens_saved: int = 0
    success_count: int = 0
    failure_count: int = 0
    avg_latency: float = 0.0
    avg_tokens_saved: float = 0.0
    success_count: int = 0
    failure_count: int = 0
    compression_level: float = 0.5
    latency_samples: list = field(default_factory=list)

    def record(self, latency: float, tokens_saved: int, success: bool, compression_level: float = 0.5):
        self.total_interactions += 1
        self.total_latency += latency
        self.total_latency_ms += latency
        self.total_tokens_saved += tokens_saved
        if success:
            self.success_count += 1
        else:
            self.failure_count += 1
        self.avg_latency = self.total_latency / max(1, self.total_interactions)
        self.avg_tokens_saved = self.total_tokens_saved / max(1, self.total_interactions)
        self.success_rate = self.success_count / max(1, self.total_interactions)
        self.latency_samples.append(latency)
        self.compression_level = compression_level

    @property
    def avg_latency_ms(self) -> float:
        if self.latency_samples:
            return sum(self.latency_samples) / len(self.latency_samples)
        return self.total_latency / max(1, self.total_interactions)

    @property
    def tokens_saved(self) -> int:
        return self.total_tokens_saved

    @tokens_saved.setter
    def tokens_saved(self, value: int):
        self.total_tokens_saved = value

    @property
    def is_learning(self) -> bool:
        return self.total_interactions >= 10

    @property
    def is_learning_threshold(self) -> bool:
        return self.total_interactions >= 10
@dataclass

class LearningEpisode:
    episode_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    steps: int = 0
    total_reward: float = 0.0
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    actions: List[str] = field(default_factory=list)
    rewards: List[float] = field(default_factory=list)

class AdaptiveTokenAllocator:
    def __init__(self, config: Optional[SLConfig] = None):
        self.config = config or SLConfig()
        self._q_table: Dict[str, float] = defaultdict(float)
        self._stats: Dict[str, AgentPairStats] = {}
        self._cache: Dict[str, float] = {}
        self._broker = MessageBroker()
        self._breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
        self.episode_count = 0
        self.is_converged = False
        self.exploration_rate = config.exploration_rate if config else 1.0
        self._learner = ReinforcementLearner(config=config)

    def allocate(self, agent_a: str, agent_b: str, intent: Optional[str] = None) -> float:
        key = f"{agent_a}_{agent_b}"
        if key not in self._q_table:
            self._q_table[key] = 0.5
        return self._q_table[key]

    def get_optimal_compression(self, pair_id: str) -> float:
        return self._q_table.get(pair_id, 0.5)

    def get_allocation(self, agent_a: str, agent_b: str) -> float:
        key = f"{agent_a}_{agent_b}"
        return self._stats.get(key, AgentPairStats()).compression_level

    def get_all_allocations(self) -> dict:
        return {k: v.compression_level for k, v in self._stats.items()}

    def reset_allocation(self, agent_a: str, agent_b: str) -> bool:
        key = f"{agent_a}_{agent_b}"
        if key in self._stats:
            del self._stats[key]
            return True
        return False

    def record_interaction(self, agent_a, agent_b, latency_ms=0.0, tokens_saved=0,
                           success=True, tokens_original=0, compression_level=0.5):
        key = f"{agent_a}_{agent_b}"
        if key not in self._stats:
            self._stats[key] = AgentPairStats(agent_a=agent_a, agent_b=agent_b)
        self._stats[key].record(latency_ms, tokens_saved, success, compression_level)
        return {"pair_id": key, "success": success}

    def get_stats(self, key: str) -> AgentPairStats:
        return self._stats.get(key, AgentPairStats(agent_a=key, agent_b=""))

    def get_all_pairs(self) -> dict:
        return dict(self._stats)

    def get_aggregate_stats(self) -> dict:
        all_pairs = list(self._stats.values())
        if not all_pairs:
            return {"global_success_rate": 0.0, "total_interactions": 0, "total_pairs": 0}
        total_success = sum(p.success_count for p in all_pairs)
        total_fail = sum(p.failure_count for p in all_pairs)
        total_inter = sum(p.total_interactions for p in all_pairs)
        return {"global_success_rate": round(total_success / max(1, total_success + total_fail) * 100, 2),
                "total_interactions": total_inter, "total_pairs": len(all_pairs)}

    def adapt(self, agent_a, agent_b, latency_ms=0.0, tokens_saved=0, success=True, intent=None):
        key = f"{agent_a}_{agent_b}"
        previous_level = self.allocate(agent_a, agent_b)
        reward = 1.0 if success else -1.0
        if latency_ms > 200: reward -= 0.5
        if tokens_saved > 50: reward += 0.3
        self.record_interaction(agent_a, agent_b, latency_ms, tokens_saved, success)
        return {"pair_id": key, "reward": reward, "new_compression_level": self.allocate(agent_a, agent_b),
                "previous_level": previous_level, "action_taken": "increase" if reward > 0 else "decrease", "success": success}

    def _compute_reward(self, agent_a, agent_b, success):
        return 1.0 if success else -1.0


class PerformanceTracker(AdaptiveTokenAllocator):
    """Backward-compatible PerformanceTracker that inherits from AdaptiveTokenAllocator."""
    def get_pair_stats(self, agent_a: str, agent_b: str) -> AgentPairStats:
        """Get stats for a pair."""
        return self.get_stats(f"{agent_a}_{agent_b}")


class ReinforcementLearner:
    def __init__(self, config: Optional[SLConfig] = None):
        self.config = config or SLConfig()
        self._q_table: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        self._metrics = type('Metrics', (), {'total_episodes': 0, 'total_steps': 0})()
        self._breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
        self.episode_count = 0
        self.is_converged = False
        self.exploration_rate = config.exploration_rate if config else 1.0
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

    def update_q_value(self, state: str, action: str, reward: float, next_state_key: str = ""):
        if next_state_key and next_state_key in self._q_table:
            max_next = max(self._q_table[next_state_key].values(), default=0)
        else:
            max_next = max(self._q_table.get(state, {}).values(), default=0)
        self._q_table[state][action] += self.config.learning_rate * (
            reward + self.config.discount_factor * max_next - self._q_table[state][action])
        self._metrics.total_steps += 1

    def set_q_value(self, state: str, action: str, value: float):
        self._q_table[state][action] = value

    def get_optimal_policy(self, state: str) -> List[str]:
        if state not in self._q_table: return []
        max_q = max(self._q_table[state].values())
        return [a for a, q in self._q_table[state].items() if q == max_q]

    @property
    def q_table_size(self) -> int:
        return sum(len(v) for v in self._q_table.values())

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
        self.episode_count = self._metrics.total_episodes
        return ep

    def get_metrics(self):
        return self._metrics

class FeedbackLoop:
    def __init__(self, config=None):
        self.config = config or SLConfig()
        self._tracker = PerformanceTracker(config=self.config)
        self._learner = ReinforcementLearner(config=self.config)
        self._event_bus = EventBus()
        self._breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
        self._breaker.name = "feedback_loop"
        self._allocator = AdaptiveTokenAllocator(config=self.config)
        self._tracker._learner = self._learner
    def process_interaction(self, agent_a, agent_b, latency_ms=0.0, tokens_saved=0, success=True, intent=None):
        return self._allocator.adapt(agent_a, agent_b, latency_ms=latency_ms, tokens_saved=tokens_saved, success=success, intent=intent)
    def get_feedback_quality(self, agent_a, agent_b):
        stats = self._tracker.get_stats(f"{agent_a}_{agent_b}")
        if stats.total_interactions == 0:
            return {"quality": "insufficient_data", "score": 0.0, "pair_id": f"{agent_a}_{agent_b}", "interactions": 0}
        return {"quality": "good", "score": stats.success_rate, "pair_id": f"{agent_a}_{agent_b}", "interactions": stats.total_interactions}
    def run_episode(self, pair, episodes=10):
        results = []
        for _ in range(episodes):
            ep = self._learner.run_episode(lambda p=pair: p)
            results.append({"episode_id": ep.episode_id, "steps": ep.steps, "reward": ep.total_reward})
        return {"episodes_run": len(results), "total_episodes": self._learner._metrics.total_episodes,
                "final_exploration_rate": self.config.exploration_rate, "q_table_size": self._learner.q_table_size,
                "is_converged": self._learner.adapt_strategy()}
    def run_adaptation_cycle(self, pairs, cycles=1):
        cycle_results = []
        for cycle in range(cycles):
            cycle_reward = 0.0
            for pair in pairs:
                parts = pair.split("::")
                if len(parts) == 2:
                    result = self._allocator.adapt(parts[0], parts[1], latency_ms=50.0, tokens_saved=100, success=True)
                    cycle_reward += result["reward"]
            cycle_results.append({"cycle": cycle, "reward": cycle_reward})
        return {"cycles_completed": cycles, "aggregate_reward": sum(c["reward"] for c in cycle_results), "cycle_results": cycle_results}
    def get_adaptation_summary(self):
        tracked_pairs = len(self._tracker.get_all_pairs())
        q_size = self._learner.q_table_size
        return {"total_episodes": self._learner._metrics.total_episodes, "exploration_rate": self.config.exploration_rate,
                "is_converged": self._learner.adapt_strategy(), "q_table_size": q_size,
                "breaker_state": self._breaker.state, "tracked_pairs": tracked_pairs,
                "aggregate_stats": self._tracker.get_aggregate_stats()}
    def reset(self):
        self._learner.reset()
        self._tracker = PerformanceTracker(config=self.config)
        self._tracker._learner = self._learner
        self._breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30)
        self._breaker.name = "feedback_loop"
        self._allocator = AdaptiveTokenAllocator(config=self.config)
        self._allocator._tracker = self._tracker
        self._tracker._learner = self._learner

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
