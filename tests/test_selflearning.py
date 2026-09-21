"""
Tests for TokenRelay v4 Self-Learning & Adaptive Protocol.

Covers:
- PerformanceTracker: per-pair latency, token savings, success rate tracking
- ReinforcementLearner: Q-learning agent, epsilon-greedy policy, TD updates
- AdaptiveTokenAllocator: dynamic compression level allocation, adaptation
- FeedbackLoop: closed-loop optimization, episode training, adaptation cycles
- SLBundle: integrated bundle with full pipeline
- SLConfig: configuration defaults and customization
- AgentPairStats: per-pair statistics and computed properties
- LearningEpisode: episode recording and properties
- LearningState, CompressionAction, RewardSignal: enum types
- Exceptions and edge cases
"""

import sys
from pathlib import Path
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from selflearning import (
    SLConfig, AgentPairStats, LearningEpisode,
    LearningState, CompressionAction, RewardSignal,
    PerformanceTracker, ReinforcementLearner,
    AdaptiveTokenAllocator, FeedbackLoop, SLBundle,
    create_learner, create_allocator, create_tracker,
    create_feedback_loop, create_sl_bundle,
)


# ═══════════════════════════════════════════════════════════
# Configuration & Types Tests
# ═══════════════════════════════════════════════════════════

class TestSLConfig(unittest.TestCase):
    """Tests for SLConfig dataclass."""

    def test_defaults(self):
        """Default configuration values."""
        cfg = SLConfig()
        self.assertEqual(cfg.learning_rate, 0.1)
        self.assertEqual(cfg.discount_factor, 0.95)
        self.assertEqual(cfg.exploration_rate, 1.0)
        self.assertEqual(cfg.min_exploration, 0.01)
        self.assertEqual(cfg.episodes, 1000)
        self.assertEqual(cfg.default_compression, 0.5)
        self.assertEqual(cfg.failure_penalty, -1.0)

    def test_custom_config(self):
        """Custom configuration values."""
        cfg = SLConfig(
            learning_rate=0.2, discount_factor=0.9,
            exploration_rate=0.5, min_exploration=0.001,
        )
        self.assertEqual(cfg.learning_rate, 0.2)
        self.assertEqual(cfg.discount_factor, 0.9)
        self.assertEqual(cfg.exploration_rate, 0.5)
        self.assertEqual(cfg.min_exploration, 0.001)

    def test_reward_weights(self):
        """Reward weight configuration."""
        cfg = SLConfig()
        self.assertEqual(cfg.reward_token_weight, 0.4)
        self.assertEqual(cfg.reward_latency_weight, 0.3)
        self.assertEqual(cfg.reward_success_weight, 0.3)


class TestLearningStateEnum(unittest.TestCase):
    """Tests for LearningState enum."""

    def test_enum_values(self):
        """All four learning states exist."""
        states = [s.name for s in LearningState]
        self.assertIn("EXPLOITATION", states)
        self.assertIn("EXPLORATION", states)
        self.assertIn("CONVERGED", states)
        self.assertIn("DECAYING", states)

    def test_enum_count(self):
        """Four learning states."""
        self.assertEqual(len(list(LearningState)), 4)


class TestCompressionActionEnum(unittest.TestCase):
    """Tests for CompressionAction enum."""

    def test_enum_values(self):
        """All four compression actions exist."""
        actions = [a.name for a in CompressionAction]
        self.assertIn("INCREASE", actions)
        self.assertIn("DECREASE", actions)
        self.assertIn("MAINTAIN", actions)
        self.assertIn("RESET", actions)

    def test_enum_count(self):
        """Four compression actions."""
        self.assertEqual(len(list(CompressionAction)), 4)


class TestRewardSignalEnum(unittest.TestCase):
    """Tests for RewardSignal enum."""

    def test_enum_count(self):
        """Five reward signal types."""
        self.assertEqual(len(list(RewardSignal)), 5)


# ═══════════════════════════════════════════════════════════
# AgentPairStats Tests
# ═══════════════════════════════════════════════════════════

class TestAgentPairStats(unittest.TestCase):
    """Tests for AgentPairStats dataclass."""

    def test_defaults(self):
        """Default stats values."""
        stats = AgentPairStats()
        self.assertEqual(stats.total_interactions, 0)
        self.assertEqual(stats.total_tokens_saved, 0)
        self.assertEqual(stats.total_latency_ms, 0.0)
        self.assertEqual(stats.success_count, 0)
        self.assertEqual(stats.failure_count, 0)
        self.assertEqual(stats.compression_level, 0.5)

    def test_success_rate_no_data(self):
        """Success rate is 100% with no data."""
        stats = AgentPairStats()
        self.assertEqual(stats.success_rate, 100.0)

    def test_success_rate_with_data(self):
        """Success rate calculated correctly."""
        stats = AgentPairStats()
        stats.success_count = 8
        stats.failure_count = 2
        self.assertEqual(stats.success_rate, 80.0)

    def test_avg_latency_no_samples(self):
        """Average latency is 0.0 with no samples."""
        stats = AgentPairStats()
        self.assertEqual(stats.avg_latency_ms, 0.0)

    def test_avg_latency_with_samples(self):
        """Average latency calculated correctly."""
        stats = AgentPairStats()
        stats.latency_samples.extend([10.0, 20.0, 30.0])
        self.assertEqual(stats.avg_latency_ms, 20.0)

    def test_tokens_saved_property(self):
        """Tokens saved property returns correct value."""
        stats = AgentPairStats()
        stats.total_tokens_saved = 500
        self.assertEqual(stats.tokens_saved, 500)

    def test_is_learning_threshold(self):
        """is_learning returns True when interactions >= 10."""
        stats = AgentPairStats()
        stats.total_interactions = 9
        self.assertFalse(stats.is_learning)
        stats.total_interactions = 10
        self.assertTrue(stats.is_learning)

    def test_is_learning_below_threshold(self):
        """is_learning returns False when interactions < 10."""
        stats = AgentPairStats()
        stats.total_interactions = 5
        self.assertFalse(stats.is_learning)


# ═══════════════════════════════════════════════════════════
# LearningEpisode Tests
# ═══════════════════════════════════════════════════════════

class TestLearningEpisode(unittest.TestCase):
    """Tests for LearningEpisode dataclass."""

    def test_custom_timestamp(self):
        """Episode with custom timestamp."""
        ts = time.time() - 100
        ep = LearningEpisode(
            episode_id=1, state_key="s1", action="a1",
            reward=0.5, q_value=0.3, latency_ms=50.0,
            tokens_saved=100, success=True, timestamp=ts,
        )
        self.assertEqual(ep.episode_id, 1)
        self.assertEqual(ep.reward, 0.5)
        self.assertAlmostEqual(ep.timestamp, ts, delta=0.01)

    def test_default_timestamp(self):
        """Episode gets current time when timestamp is 0."""
        ts_before = time.time()
        ep = LearningEpisode(
            episode_id=1, state_key="s1", action="a1",
            reward=0.5, q_value=0.3, latency_ms=50.0,
            tokens_saved=100, success=True,
        )
        ts_after = time.time()
        self.assertGreaterEqual(ep.timestamp, ts_before)
        self.assertLessEqual(ep.timestamp, ts_after)

    def test_post_init_sets_timestamp(self):
        """__post_init__ sets timestamp when not provided."""
        ep = LearningEpisode(
            episode_id=0, state_key="s", action="a",
            reward=1.0, q_value=0.5, latency_ms=10.0,
            tokens_saved=50, success=True,
        )
        self.assertGreater(ep.timestamp, 0)


# ═══════════════════════════════════════════════════════════
# PerformanceTracker Tests
# ═══════════════════════════════════════════════════════════

class TestPerformanceTracker(unittest.TestCase):
    """Tests for PerformanceTracker."""

    def setUp(self):
        self.tracker = PerformanceTracker()

    def test_record_interaction(self):
        """Recording an interaction creates pair stats."""
        result = self.tracker.record_interaction(
            "agent_a", "agent_b", latency_ms=50.0,
            tokens_saved=100, success=True,
        )
        self.assertIn("pair_id", result)
        self.assertEqual(result["total_interactions"], 1)
        self.assertEqual(result["success_rate"], 100.0)

    def test_record_multiple_interactions(self):
        """Multiple interactions accumulate correctly."""
        self.tracker.record_interaction(
            "a", "b", latency_ms=50.0, tokens_saved=100, success=True,
        )
        self.tracker.record_interaction(
            "a", "b", latency_ms=100.0, tokens_saved=200, success=False,
        )
        stats = self.tracker.get_pair_stats("a", "b")
        self.assertEqual(stats.total_interactions, 2)
        self.assertEqual(stats.success_count, 1)
        self.assertEqual(stats.failure_count, 1)

    def test_get_pair_stats(self):
        """Getting pair stats returns correct data."""
        self.tracker.record_interaction(
            "x", "y", latency_ms=30.0, tokens_saved=50, success=True,
        )
        stats = self.tracker.get_pair_stats("x", "y")
        self.assertIsNotNone(stats)
        self.assertEqual(stats.total_interactions, 1)

    def test_get_pair_stats_nonexistent(self):
        """Getting stats for nonexistent pair returns None."""
        stats = self.tracker.get_pair_stats("unknown", "pair")
        self.assertIsNone(stats)

    def test_get_all_pairs(self):
        """get_all_pairs returns all tracked pairs."""
        self.tracker.record_interaction("a", "b", 50.0, 100, True)
        self.tracker.record_interaction("c", "d", 30.0, 50, True)
        pairs = self.tracker.get_all_pairs()
        self.assertEqual(len(pairs), 2)

    def test_get_aggregate_stats(self):
        """Aggregate stats include all recorded interactions."""
        self.tracker.record_interaction("a", "b", 50.0, 100, True)
        self.tracker.record_interaction("a", "c", 100.0, 50, False)
        stats = self.tracker.get_aggregate_stats()
        self.assertEqual(stats["total_interactions"], 2)
        self.assertEqual(stats["total_successes"], 1)
        self.assertEqual(stats["total_failures"], 1)
        self.assertIn("global_success_rate", stats)

    def test_get_top_performers(self):
        """Top performers sorted by token savings."""
        self.tracker.record_interaction("a", "b", 50.0, 500, True)
        self.tracker.record_interaction("c", "d", 30.0, 200, True)
        self.tracker.record_interaction("e", "f", 40.0, 800, True)
        top = self.tracker.get_top_performers(n=2)
        self.assertEqual(len(top), 2)
        self.assertEqual(top[0]["total_tokens_saved"], 800)
        self.assertEqual(top[1]["total_tokens_saved"], 500)

    def test_get_recent_interactions(self):
        """Recent interactions returned in order."""
        self.tracker.record_interaction("a", "b", 50.0, 100, True)
        self.tracker.record_interaction("c", "d", 30.0, 50, True)
        recent = self.tracker.get_recent_interactions(limit=1)
        self.assertEqual(len(recent), 1)

    def test_reset_pair(self):
        """Resetting a pair removes its stats."""
        self.tracker.record_interaction("a", "b", 50.0, 100, True)
        self.assertEqual(len(self.tracker.get_all_pairs()), 1)
        result = self.tracker.reset_pair("a", "b")
        self.assertTrue(result)
        self.assertEqual(len(self.tracker.get_all_pairs()), 0)

    def test_reset_pair_nonexistent(self):
        """Resetting nonexistent pair returns False."""
        result = self.tracker.reset_pair("x", "y")
        self.assertFalse(result)

    def test_reset_all(self):
        """Reset all clears everything."""
        self.tracker.record_interaction("a", "b", 50.0, 100, True)
        self.tracker.record_interaction("c", "d", 30.0, 50, True)
        self.tracker.reset_all()
        self.assertEqual(len(self.tracker.get_all_pairs()), 0)
        agg = self.tracker.get_aggregate_stats()
        self.assertEqual(agg["total_interactions"], 0)

    def test_pair_key_deterministic(self):
        """Pair keys are deterministic and symmetric."""
        key1 = self.tracker._pair_key("a", "b")
        key2 = self.tracker._pair_key("b", "a")
        self.assertEqual(key1, key2)

    def test_record_interaction_with_all_params(self):
        """Recording with all optional parameters works."""
        result = self.tracker.record_interaction(
            "a", "b", latency_ms=50.0, tokens_saved=100,
            success=True, tokens_original=200, compression_level=0.7,
        )
        self.assertIn("pair_id", result)
        stats = self.tracker.get_pair_stats("a", "b")
        self.assertEqual(stats.compression_level, 0.7)

    def test_aggregate_success_rate(self):
        """Aggregate success rate calculated correctly."""
        for _ in range(8):
            self.tracker.record_interaction("a", "b", 50.0, 100, True)
        for _ in range(2):
            self.tracker.record_interaction("a", "b", 50.0, 100, False)
        agg = self.tracker.get_aggregate_stats()
        self.assertEqual(agg["global_success_rate"], 80.0)

    def test_get_all_pairs_empty(self):
        """get_all_pairs returns empty dict when no pairs."""
        pairs = self.tracker.get_all_pairs()
        self.assertEqual(pairs, {})


# ═══════════════════════════════════════════════════════════
# ReinforcementLearner Tests
# ═══════════════════════════════════════════════════════════

class TestReinforcementLearner(unittest.TestCase):
    """Tests for ReinforcementLearner."""

    def setUp(self):
        self.learner = ReinforcementLearner()

    def test_initial_state(self):
        """Fresh learner has empty Q-table."""
        self.assertEqual(self.learner.q_table_size, 0)
        self.assertEqual(self.learner.exploration_rate, 1.0)
        self.assertEqual(self.learner.episode_count, 0)
        self.assertFalse(self.learner.is_converged)

    def test_select_action_random(self):
        """Selecting action returns a valid action string."""
        action = self.learner.select_action("state_1")
        self.assertIn(action, [a.name for a in CompressionAction])

    def test_get_q_value_default(self):
        """Default Q-value for unknown state-action is 0.0."""
        q = self.learner.get_q_value("unknown_state", "INCREASE")
        self.assertEqual(q, 0.0)

    def test_update_q_value(self):
        """Updating Q-value stores the new value."""
        self.learner.update_q_value("s1", "INCREASE", reward=0.8)
        q = self.learner.get_q_value("s1", "INCREASE")
        self.assertGreater(q, 0.0)

    def test_q_learning_update_rule(self):
        """Q-values update according to learning rule."""
        self.learner.update_q_value("s1", "INCREASE", reward=1.0)
        q1 = self.learner.get_q_value("s1", "INCREASE")
        self.learner.update_q_value("s1", "INCREASE", reward=1.0)
        q2 = self.learner.get_q_value("s1", "INCREASE")
        self.assertGreater(q2, q1)

    def test_policy_selection(self):
        """After training, get_policy returns the best action."""
        for _ in range(10):
            self.learner.update_q_value("s1", "INCREASE", reward=1.0)
        policy = self.learner.get_policy("s1")
        self.assertIsNotNone(policy)

    def test_q_values_for_state(self):
        """Getting all Q-values for a state works."""
        self.learner.update_q_value("s1", "INCREASE", reward=0.5)
        self.learner.update_q_value("s1", "DECREASE", reward=0.3)
        q_values = self.learner.get_q_values_for_state("s1")
        self.assertIn("INCREASE", q_values)
        self.assertIn("DECREASE", q_values)

    def test_decay_exploration(self):
        """Exploration rate decays after each episode."""
        initial_rate = self.learner.exploration_rate
        self.learner.decay_exploration()
        self.assertLess(self.learner.exploration_rate, initial_rate)
        self.assertEqual(self.learner.episode_count, 1)

    def test_exploration_rate_floor(self):
        """Exploration rate never goes below min_exploration."""
        learner = ReinforcementLearner(
            SLConfig(min_exploration=0.05, exploration_decay=0.5)
        )
        for _ in range(100):
            learner.decay_exploration()
        self.assertGreaterEqual(learner.exploration_rate, 0.05)

    def test_total_rewards_accumulate(self):
        """Total rewards accumulate correctly."""
        self.learner.update_q_value("s1", "INCREASE", reward=0.5)
        self.learner.update_q_value("s1", "DECREASE", reward=0.3)
        self.assertGreater(self.learner.total_rewards, 0.0)

    def test_get_policy_returns_none_for_unknown(self):
        """get_policy returns None for unknown state."""
        policy = self.learner.get_policy("unknown")
        self.assertIsNone(policy)

    def test_reset_clears_q_table(self):
        """Reset clears the Q-table and all learning state."""
        self.learner.update_q_value("s1", "INCREASE", reward=1.0)
        self.learner.reset()
        self.assertEqual(self.learner.q_table_size, 0)
        self.assertEqual(self.learner.episode_count, 0)
        self.assertEqual(self.learner.exploration_rate, 1.0)

    def test_convergence_check(self):
        """is_converged returns True when exploration is below threshold."""
        learner = ReinforcementLearner(
            SLConfig(min_exploration=0.01, exploration_decay=0.5)
        )
        self.assertFalse(learner.is_converged)
        for _ in range(20):
            learner.decay_exploration()
        # Should converge quickly with decay=0.5
        self.assertTrue(learner.is_converged)

    def test_learning_log(self):
        """Learning log records actions and updates."""
        self.learner.update_q_value("s1", "INCREASE", reward=0.5)
        log = self.learner.get_learning_log(limit=5)
        self.assertGreater(len(log), 0)

    def test_set_q_value(self):
        """Manually setting Q-value works."""
        self.learner.set_q_value("s1", "INCREASE", 0.95)
        q = self.learner.get_q_value("s1", "INCREASE")
        self.assertEqual(q, 0.95)

    def test_select_action_with_valid_actions(self):
        """Selecting action with restricted valid actions."""
        action = self.learner.select_action("s1", valid_actions=["INCREASE", "MAINTAIN"])
        self.assertIn(action, ["INCREASE", "MAINTAIN"])

    def test_update_q_value_with_next_state(self):
        """Q-update with next state uses TD target correctly."""
        self.learner.update_q_value("s1", "INCREASE", reward=0.5, next_state_key="s2")
        self.learner.update_q_value("s2", "DECREASE", reward=0.8)
        q = self.learner.get_q_value("s1", "INCREASE")
        self.assertGreater(q, 0.0)


# ═══════════════════════════════════════════════════════════
# AdaptiveTokenAllocator Tests
# ═══════════════════════════════════════════════════════════

class TestAdaptiveTokenAllocator(unittest.TestCase):
    """Tests for AdaptiveTokenAllocator."""

    def setUp(self):
        self.allocator = AdaptiveTokenAllocator()

    def test_allocate_returns_float(self):
        """Allocate returns a compression level float."""
        level = self.allocator.allocate("a", "b")
        self.assertIsInstance(level, float)
        self.assertGreaterEqual(level, 0.0)
        self.assertLessEqual(level, 1.0)

    def test_allocate_with_intent(self):
        """Allocate works with explicit intent."""
        level = self.allocator.allocate("a", "b", intent="query")
        self.assertIsInstance(level, float)

    def test_adapt_records_interaction(self):
        """Adapt records interaction and returns adaptation result."""
        result = self.allocator.adapt(
            "a", "b", latency_ms=50.0, tokens_saved=100, success=True,
        )
        self.assertIn("pair_id", result)
        self.assertIn("reward", result)
        self.assertIn("new_compression_level", result)
        self.assertIn("previous_level", result)
        self.assertIn("action_taken", result)

    def test_adapt_with_intent(self):
        """Adapt works with explicit intent."""
        result = self.allocator.adapt(
            "a", "b", latency_ms=50.0, tokens_saved=100,
            success=True, intent="query",
        )
        self.assertIn("reward", result)

    def test_adapt_failure_returns_negative_reward(self):
        """Failed interactions produce negative reward."""
        result = self.allocator.adapt(
            "a", "b", latency_ms=500.0, tokens_saved=0, success=False,
        )
        self.assertLess(result["reward"], 0)

    def test_get_allocation(self):
        """Get allocation returns the level for a pair."""
        self.allocator.allocate("a", "b")
        level = self.allocator.get_allocation("a", "b")
        self.assertIsNotNone(level)
        self.assertIsInstance(level, float)

    def test_get_allocation_nonexistent(self):
        """Get allocation for unknown pair returns None."""
        level = self.allocator.get_allocation("unknown", "pair")
        self.assertIsNone(level)

    def test_get_all_allocations(self):
        """Get all allocations returns dict."""
        self.allocator.allocate("a", "b")
        self.allocator.allocate("c", "d")
        allocs = self.allocator.get_all_allocations()
        self.assertGreaterEqual(len(allocs), 2)

    def test_reset_allocation(self):
        """Resetting allocation works."""
        self.allocator.allocate("a", "b")
        result = self.allocator.reset_allocation("a", "b")
        self.assertTrue(result)
        self.assertIsNone(self.allocator.get_allocation("a", "b"))

    def test_reset_allocation_nonexistent(self):
        """Resetting nonexistent allocation returns False."""
        result = self.allocator.reset_allocation("x", "y")
        self.assertFalse(result)

    def test_adapt_updates_q_values(self):
        """Adapt updates the underlying learner's Q-values."""
        self.allocator.adapt("a", "b", 50.0, 100, True)
        q_size = self.allocator._learner.q_table_size
        self.assertGreater(q_size, 0)

    def test_allocate_returns_consistent_level(self):
        """Multiple allocations for same pair return consistent level."""
        level1 = self.allocator.allocate("a", "b")
        level2 = self.allocator.allocate("a", "b")
        self.assertEqual(level1, level2)

    def test_adapt_creates_allocation(self):
        """Adapt creates an allocation entry for the pair."""
        self.allocator.adapt("x", "y", 50.0, 100, True)
        level = self.allocator.get_allocation("x", "y")
        self.assertIsNotNone(level)


# ═══════════════════════════════════════════════════════════
# FeedbackLoop Tests
# ═══════════════════════════════════════════════════════════

class TestFeedbackLoop(unittest.TestCase):
    """Tests for FeedbackLoop."""

    def setUp(self):
        self.loop = FeedbackLoop()

    def test_process_interaction(self):
        """Processing an interaction returns result dict."""
        result = self.loop.process_interaction(
            "a", "b", latency_ms=50.0, tokens_saved=100, success=True,
        )
        self.assertIn("pair_id", result)
        self.assertIn("reward", result)
        self.assertIn("new_compression_level", result)
        self.assertIn("success", result)
        self.assertTrue(result["success"])

    def test_process_interaction_failure(self):
        """Failed interactions produce negative reward."""
        result = self.loop.process_interaction(
            "a", "b", latency_ms=500.0, tokens_saved=0, success=False,
        )
        self.assertLess(result["reward"], 0)

    def test_run_episode(self):
        """Running an episode produces learning results."""
        result = self.loop.run_episode("agent_a::agent_b", episodes=5)
        self.assertEqual(result["episodes_run"], 5)
        self.assertEqual(result["total_episodes"], 5)
        self.assertIn("final_exploration_rate", result)
        self.assertIn("q_table_size", result)
        self.assertIn("is_converged", result)

    def test_run_episode_default(self):
        """Running episode with default episodes count."""
        result = self.loop.run_episode("a::b")
        self.assertEqual(result["episodes_run"], 10)

    def test_run_episode_multiple(self):
        """Running multiple episodes converges exploration."""
        result = self.loop.run_episode("a::b", episodes=20)
        self.assertGreater(result["q_table_size"], 0)
        self.assertLess(result["final_exploration_rate"], 1.0)

    def test_run_adaptation_cycle(self):
        """Running adaptation cycles produces aggregate results."""
        result = self.loop.run_adaptation_cycle(
            ["a::b", "c::d"], cycles=1,
        )
        self.assertIn("cycles_completed", result)
        self.assertIn("aggregate_reward", result)
        self.assertIn("cycle_results", result)

    def test_get_feedback_quality(self):
        """Feedback quality evaluation works after interactions."""
        self.loop.process_interaction("a", "b", 50.0, 100, True)
        quality = self.loop.get_feedback_quality("a", "b")
        self.assertIn("quality", quality)
        self.assertIn("score", quality)
        self.assertIn("pair_id", quality)
        self.assertIn("interactions", quality)

    def test_get_feedback_quality_insufficient(self):
        """Quality is 'insufficient_data' with no interactions."""
        quality = self.loop.get_feedback_quality("x", "y")
        self.assertEqual(quality["quality"], "insufficient_data")
        self.assertEqual(quality["score"], 0.0)

    def test_get_adaptation_summary(self):
        """Adaptation summary contains all expected keys."""
        summary = self.loop.get_adaptation_summary()
        self.assertIn("total_episodes", summary)
        self.assertIn("exploration_rate", summary)
        self.assertIn("is_converged", summary)
        self.assertIn("q_table_size", summary)
        self.assertIn("breaker_state", summary)
        self.assertIn("tracked_pairs", summary)
        self.assertIn("aggregate_stats", summary)

    def test_process_interaction_with_intent(self):
        """Processing with intent parameter works."""
        result = self.loop.process_interaction(
            "a", "b", latency_ms=50.0, tokens_saved=100,
            success=True, intent="query",
        )
        self.assertIn("pair_id", result)
        self.assertIn("new_compression_level", result)

    def test_reset(self):
        """Reset clears all feedback loop state."""
        self.loop.process_interaction("a", "b", 50.0, 100, True)
        self.loop.run_episode("a::b", episodes=2)
        self.loop.reset()
        summary = self.loop.get_adaptation_summary()
        self.assertEqual(summary["total_episodes"], 0)
        self.assertEqual(summary["tracked_pairs"], 0)

    def test_feedback_loop_has_breaker(self):
        """Feedback loop has a circuit breaker."""
        self.assertEqual(self.loop._breaker.name, "feedback_loop")


# ═══════════════════════════════════════════════════════════
# SLBundle Tests
# ═══════════════════════════════════════════════════════════

class TestSLBundle(unittest.TestCase):
    """Tests for SLBundle."""

    def setUp(self):
        self.sl = SLBundle()

    def test_process(self):
        """Process delegates to feedback loop."""
        result = self.sl.process(
            "a", "b", latency_ms=50.0, tokens_saved=100, success=True,
        )
        self.assertIn("pair_id", result)
        self.assertIn("reward", result)

    def test_allocate(self):
        """Allocate returns a compression level."""
        level = self.sl.allocate("a", "b")
        self.assertIsInstance(level, float)
        self.assertGreaterEqual(level, 0.0)
        self.assertLessEqual(level, 1.0)

    def test_get_summary(self):
        """Get summary returns comprehensive stats."""
        self.sl.process("a", "b", 50.0, 100, True)
        summary = self.sl.get_summary()
        self.assertIn("total_episodes", summary)
        self.assertIn("top_performers", summary)
        self.assertIn("recent_interactions", summary)
        self.assertIn("aggregate_stats", summary)

    def test_reset(self):
        """Reset clears all bundle components."""
        self.sl.process("a", "b", 50.0, 100, True)
        self.sl.reset()
        summary = self.sl.get_summary()
        self.assertEqual(summary["total_episodes"], 0)


# ═══════════════════════════════════════════════════════════
# Factory Function Tests
# ═══════════════════════════════════════════════════════════

class TestFactoryFunctions(unittest.TestCase):
    """Tests for factory functions."""

    def test_create_learner(self):
        """create_learner returns ReinforcementLearner."""
        learner = create_learner()
        self.assertIsInstance(learner, ReinforcementLearner)

    def test_create_allocator(self):
        """create_allocator returns AdaptiveTokenAllocator."""
        allocator = create_allocator()
        self.assertIsInstance(allocator, AdaptiveTokenAllocator)

    def test_create_tracker(self):
        """create_tracker returns PerformanceTracker."""
        tracker = create_tracker()
        self.assertIsInstance(tracker, PerformanceTracker)

    def test_create_feedback_loop(self):
        """create_feedback_loop returns FeedbackLoop."""
        loop = create_feedback_loop()
        self.assertIsInstance(loop, FeedbackLoop)

    def test_create_sl_bundle(self):
        """create_sl_bundle returns SLBundle."""
        bundle = create_sl_bundle()
        self.assertIsInstance(bundle, SLBundle)

    def test_create_with_config(self):
        """Factory functions work with custom config."""
        cfg = SLConfig(learning_rate=0.3, exploration_rate=0.5)
        learner = create_learner(cfg)
        self.assertEqual(learner.config.learning_rate, 0.3)
        self.assertEqual(learner.config.exploration_rate, 0.5)


# ═══════════════════════════════════════════════════════════
# Integration Tests
# ═══════════════════════════════════════════════════════════

class TestIntegration(unittest.TestCase):
    """Integration tests for the complete v4 pipeline."""

    def test_full_learning_cycle(self):
        """Complete cycle: allocate → process → adapt → converge."""
        loop = FeedbackLoop()

        # Process multiple interactions
        for _ in range(20):
            loop.process_interaction("a", "b", 50.0, 100, True)

        # Run learning episodes
        result = loop.run_episode("a::b", episodes=10)
        self.assertGreater(result["q_table_size"], 0)
        self.assertGreater(result["total_episodes"], 0)

        # Check that Q-values were learned
        summary = loop.get_adaptation_summary()
        self.assertGreater(summary["q_table_size"], 0)

    def test_allocator_with_tracker(self):
        """Allocator and tracker share data correctly."""
        tracker = PerformanceTracker()
        learner = ReinforcementLearner(config=SLConfig(episodes=100))
        allocator = AdaptiveTokenAllocator(
            config=SLConfig(), learner=learner, tracker=tracker,
        )

        allocator.allocate("a", "b")
        allocator.adapt("a", "b", 50.0, 100, True)

        stats = tracker.get_pair_stats("a", "b")
        self.assertIsNotNone(stats)
        self.assertGreater(stats.total_interactions, 0)

    def test_bundle_pipeline(self):
        """SLBundle full pipeline works end-to-end."""
        sl = SLBundle()

        for _ in range(10):
            sl.process("a", "b", 50.0, 100, True)
            sl.process("c", "d", 30.0, 50, True)
            sl.process("e", "f", 200.0, 0, False)

        # Run learning episodes to build up state
        sl._feedback.run_episode("a::b", episodes=5)

        summary = sl.get_summary()
        self.assertGreater(summary["total_episodes"], 0)
        self.assertIn("tracked_pairs", summary)

    def test_convergence_over_time(self):
        """Exploration rate decreases over episodes."""
        loop = FeedbackLoop(
            config=SLConfig(exploration_decay=0.9, min_exploration=0.01)
        )
        rates = []
        for _ in range(10):
            loop.run_episode("a::b", episodes=5)
            rates.append(loop._learner.exploration_rate)

        # Rates should generally decrease
        self.assertLess(rates[-1], rates[0])

    def test_multiple_pairs_learning(self):
        """Multiple agent pairs learn independently."""
        loop = FeedbackLoop()
        pairs = ["a::b", "c::d", "e::f"]
        for pair in pairs:
            loop.run_episode(pair, episodes=5)

        summary = loop.get_adaptation_summary()
        self.assertGreaterEqual(summary["tracked_pairs"], 3)

    def test_reward_computation_consistency(self):
        """Reward computation is deterministic for same inputs."""
        allocator = AdaptiveTokenAllocator()
        r1 = allocator._compute_reward(50.0, 100, True)
        r2 = allocator._compute_reward(50.0, 100, True)
        self.assertEqual(r1, r2)

    def test_failure_penalty(self):
        """Failures produce negative rewards."""
        allocator = AdaptiveTokenAllocator()
        r_success = allocator._compute_reward(50.0, 100, True)
        r_failure = allocator._compute_reward(50.0, 0, False)
        self.assertGreater(r_success, r_failure)
        self.assertLess(r_failure, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
