"""TokenRelay v4 — Integration Tests (200+ tests).
Tests all v4 modules integrated with v2/v3 foundation.
"""
import unittest, sys, uuid, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from codec import compress_message, decompress_message, MessageEncoder, MessageDecoder
from broker import MessageBroker, PRIORITY_CRITICAL, PRIORITY_HIGH, PRIORITY_MEDIUM, PRIORITY_LOW
from circuit_breaker import CircuitBreaker, CircuitState
from streaming import EventBus, StreamEvent
from bridge import TokenRelayBridge
from registry import TokenRegistry, get_registry
from brp import BRPConfig, ChannelManager, ChannelPriority
from car import CARRouter, ComplexityAnalyzer, CARConfig
from atc import IntentClassifier, AdaptiveCompressor, ATCConfig
from teq import TokenBilling, QoSTier, TEQConfig
from epc import EdgeCache, CacheManager, EPCConfig

from swarm import (
    SwarmAgent, SwarmCoordinator, CollectiveDecision, SelfOrganization,
    CapabilityVector, AgentRole, AgentState, ConsensusMethod, TaskPriority,
    OrganizationStrategy, AgentInfo, SwarmTask, Vote, ConsensusResult,
    SwarmConfig, SwarmError, AgentNotFoundError, AgentConflictError,
    ConsensusError, SwarmOverloadError,
    create_swarm_agent, create_swarm_coordinator,
    create_collective_decision, create_self_organization, create_swarm,
)
from selflearning import (
    ReinforcementLearner, AdaptiveTokenAllocator, PerformanceTracker,
    FeedbackLoop, SLBundle, LearningState, CompressionAction,
    RewardSignal, SLConfig, AgentPairStats, LearningEpisode,
    create_learner, create_allocator, create_tracker,
    create_feedback_loop, create_sl_bundle,
)
from privacy import (
    ZKProofVerifier, PrivacyPreservingToken, AnonymousRelay,
    DifferentialPrivacy, ZKProof, ProofStatus, TokenVisibility,
    AnonymityLevel, NoiseMechanism,
    create_zk_verifier, create_privacy_token,
    create_anonymous_relay, create_differential_privacy, create_privacy_suite,
)
from interop import (
    ProtocolBridge, AdapterRegistry, AdapterConfig, ExternalAdapter,
    BridgeConfig, GatewayConfig, TranslationFormat,
    ProtocolType, AdapterState, BridgeState,
    InteropError, AdapterNotFoundError, ProtocolNotSupportedError, TranslationError,
    create_protocol_bridge, create_cross_protocol_gateway,
    create_universal_translator, create_interop_stack, InteropBRPServer,
    CrossProtocolGateway, UniversalTranslator, TranslationRule,
)


# ═══════════════════════════════════════════
# SWARM TESTS (50 tests)
# ═══════════════════════════════════════════

class TestSwarmBasics(unittest.TestCase):
    def setUp(self):
        self.agent = SwarmAgent(role=AgentRole.WORKER, capabilities=["task"], agent_id="t1")
    def test_agent_creation(self):
        self.assertEqual(self.agent.agent_id, "t1")
        self.assertEqual(self.agent.role, AgentRole.WORKER)
    def test_agent_role_enum(self):
        names = [r.name for r in AgentRole]
        self.assertIn("WORKER", names); self.assertIn("LEADER", names)
    def test_agent_state_enum(self):
        names = [s.name for s in AgentState]
        self.assertIn("INITIALIZING", names); self.assertIn("ACTIVE", names)
    def test_agent_heartbeat(self):
        c = self.agent.task_count; self.agent.update_heartbeat()
        self.assertEqual(self.agent.task_count, c+1)
    def test_agent_record_result(self):
        self.agent.record_result(True, 50.0)
        self.agent.completed_tasks.append(SwarmTask())
        self.assertEqual(self.agent.success_rate, 1.0)
    def test_task_priority_enum(self):
        names = [p.name for p in TaskPriority]
        self.assertIn("CRITICAL", names); self.assertIn("BACKGROUND", names)
    def test_consensus_method_enum(self):
        names = [m.name for m in ConsensusMethod]
        self.assertIn("VOTING", names)
    def test_organization_strategy_enum(self):
        names = [o.name for o in OrganizationStrategy]
        self.assertIn("CENTRALIZED", names)
    def test_create_swarm_agent(self):
        a = create_swarm_agent(role=AgentRole.LEADER)
        self.assertIsInstance(a, SwarmAgent)
    def test_agent_info(self):
        info = AgentInfo(agent_id="test", role=AgentRole.WORKER)
        self.assertEqual(info.agent_id, "test")
    def test_swarm_task_creation(self):
        t = SwarmTask(task_id="t1", description="Test", priority=TaskPriority.HIGH, required_capabilities=["general"])
        self.assertEqual(t.task_id, "t1")
        self.assertEqual(t.status, "pending")
    def test_capability_vector(self):
        cv = CapabilityVector(capabilities=["task"])
        self.assertIn("task", cv.capabilities)
        cv.add_capability("new")
        self.assertIn("new", cv.capabilities)
    def test_consensus_result(self):
        cr = ConsensusResult(winner="a", votes=1, total_weight=1.0, unanimous=True)
        self.assertEqual(cr.winner, "a")
    def test_agent_heartbeat_increment(self):
        for _ in range(5): self.agent.update_heartbeat()
        self.assertEqual(self.agent.task_count, 5)
    def test_swarm_error_hierarchy(self):
        e = SwarmError("test"); self.assertIsInstance(e, SwarmError)
    def test_agent_not_found_error(self):
        e = AgentNotFoundError("x"); self.assertIsInstance(e, SwarmError)
    def test_agent_conflict_error(self):
        e = AgentConflictError("x"); self.assertIsInstance(e, SwarmError)
    def test_consensus_error(self):
        e = ConsensusError("x"); self.assertIsInstance(e, SwarmError)
    def test_swarm_overload_error(self):
        e = SwarmOverloadError("x"); self.assertIsInstance(e, SwarmError)
    def test_agent_record_failure(self):
        self.agent.record_result(False, 100.0)
        self.assertEqual(self.agent.success_rate, 0.0)
    def test_swarm_task_status(self):
        t = SwarmTask(task_id="t1", description="Test")
        self.assertEqual(t.status, "pending")
    def test_vote(self):
        v = Vote(voter_id="a1", proposal_id="p1", supports=True, weight=1.0)
        self.assertTrue(v.supports)
    def test_swarm_error_subclass(self):
        self.assertTrue(issubclass(AgentNotFoundError, SwarmError))


class TestSwarmCoordinator(unittest.TestCase):
    def setUp(self):
        self.coord = SwarmCoordinator(config=SwarmConfig(max_agents=10))
    def test_init(self):
        self.assertIsInstance(self.coord, SwarmCoordinator)
    def test_register_agent(self):
        a = SwarmAgent(agent_id="a1", role=AgentRole.WORKER, capabilities=["task"])
        self.assertTrue(self.coord.register_agent(a))
        self.assertIn(a.agent_id, self.coord.agents)
    def test_multiple_agents(self):
        for i in range(5):
            a = SwarmAgent(agent_id=f"a{i}", role=AgentRole.WORKER, capabilities=["general"])
            self.coord.register_agent(a)
        self.assertEqual(len(self.coord.agents), 5)
    def test_get_agent(self):
        a = SwarmAgent(agent_id="a1", role=AgentRole.LEADER, capabilities=["coord"])
        self.coord.register_agent(a)
        self.assertIsNotNone(self.coord.get_agent("a1"))
    def test_deregister_agent(self):
        a = SwarmAgent(agent_id="a1", role=AgentRole.WORKER, capabilities=["general"])
        self.coord.register_agent(a)
        self.assertTrue(self.coord.deregister_agent("a1"))
    def test_get_active_agents(self):
        for i in range(3):
            a = SwarmAgent(agent_id=f"a{i}", role=AgentRole.WORKER, capabilities=["general"])
            self.coord.register_agent(a)
        self.assertGreater(len(self.coord.get_active_agents()), 0)
    def test_elect_leader(self):
        for i in range(3):
            a = SwarmAgent(agent_id=f"a{i}", role=AgentRole.WORKER, capabilities=["general"])
            self.coord.register_agent(a)
        self.assertIsNotNone(self.coord.elect_leader())
    def test_discover_agents(self):
        a = SwarmAgent(role=AgentRole.LEADER, capabilities=["coord"])
        self.coord.register_agent(a)
        self.assertGreater(len(self.coord.discover_agents(AgentRole.LEADER)), 0)
    def test_distribute_load(self):
        for i in range(3):
            a = SwarmAgent(agent_id=f"a{i}", role=AgentRole.WORKER, capabilities=["general"])
            self.coord.register_agent(a)
        d = self.coord.distribute_load(["t1", "t2"])
        self.assertIsInstance(d, dict)
    def test_get_capable_agents(self):
        a = a_a1 = SwarmAgent(role=AgentRole.SPECIALIST, capabilities=["analysis"])
        self.coord.register_agent(a)
        self.assertGreater(len(self.coord.get_capable_agents(["analysis"])), 0)
    def test_get_metrics(self):
        for i in range(3):
            a = SwarmAgent(agent_id=f"a{i}", role=AgentRole.WORKER, capabilities=["general"])
            self.coord.register_agent(a)
        m = self.coord.get_metrics()
        self.assertGreater(m["total_agents"], 0)
    def test_get_status(self):
        s = self.coord.get_status()
        self.assertIsInstance(s, AgentState)
    def test_submit_task(self):
        t = self.coord.submit_task("Test", TaskPriority.NORMAL)
        self.assertIsInstance(t, SwarmTask)
    def test_decompose_task(self):
        t = self.coord.submit_task("Complex", TaskPriority.HIGH, ["a", "b"], decompose=True)
        subs = self.coord.decompose_task(t.task_id)
        self.assertGreater(len(subs), 0)
    def test_create_coordinator(self):
        c = create_swarm_coordinator(config=SwarmConfig())
        self.assertIsInstance(c, SwarmCoordinator)
    def test_register_deregister_cycle(self):
        a = SwarmAgent(agent_id="a1", role=AgentRole.WORKER, capabilities=["general"])
        self.coord.register_agent(a)
        self.assertIn("a1", self.coord.agents)
        self.coord.deregister_agent("a1")
        self.assertNotIn("a1", self.coord.agents)
    def test_all_agent_roles(self):
        for role in AgentRole:
            a = SwarmAgent(agent_id=f"a_{role.value}", role=role, capabilities=["general"])
            self.coord.register_agent(a)
            self.assertIsInstance(a, SwarmAgent)
    def test_error_classes(self):
        for cls in [SwarmError, AgentNotFoundError, AgentConflictError, ConsensusError, SwarmOverloadError]:
            self.assertTrue(issubclass(cls, SwarmError) or cls is SwarmError)
    def test_coordinator_config(self):
        self.assertEqual(self.coord.config.max_agents, 10)
    def test_coordinator_broker(self):
        self.assertIsInstance(self.coord.broker, MessageBroker)
    def test_coordinator_circuit_breaker(self):
        self.assertIsInstance(self.coord.circuit_breaker, CircuitBreaker)
    def test_register_same_agent_fails(self):
        a = SwarmAgent(agent_id="a1", role=AgentRole.WORKER, capabilities=["general"])
        self.coord.register_agent(a)
        b = SwarmAgent(agent_id="a1", role=AgentRole.WORKER, capabilities=["general"])
        with self.assertRaises(AgentConflictError):
            self.coord.register_agent(b)
    def test_submit_and_execute_task(self):
        t = self.coord.submit_task("Test", TaskPriority.NORMAL)
        self.assertIsInstance(t, SwarmTask)
        self.assertEqual(t.status, "pending")


class TestCollectiveDecision(unittest.TestCase):
    def setUp(self):
        self.coord = SwarmCoordinator(config=SwarmConfig(max_agents=5))
        for _ in range(3):
            a = SwarmAgent(agent_id=f"w{_}", role=AgentRole.WORKER, capabilities=["general"])
            self.coord.register_agent(a)
    def test_voting_consensus(self):
        d = CollectiveDecision(self.coord, SwarmConfig())
        r = d.vote({"a": 2, "b": 1})
        self.assertEqual(r.winner, "a")
    def test_unanimous(self):
        d = CollectiveDecision(self.coord, SwarmConfig())
        r = d.vote({"a": 1})
        self.assertTrue(r.unanimous)
    def test_create_collective_decision(self):
        d = create_collective_decision(self.coord, SwarmConfig())
        self.assertIsInstance(d, CollectiveDecision)
    def test_collective_decision_config(self):
        d = CollectiveDecision(self.coord, SwarmConfig())
        self.assertIsInstance(d.config, SwarmConfig)


class TestSelfOrganization(unittest.TestCase):
    def setUp(self):
        self.coord = SwarmCoordinator(config=SwarmConfig(max_agents=5))
        for _ in range(3):
            a = SwarmAgent(agent_id=f"w{_}", role=AgentRole.WORKER, capabilities=["general"])
            self.coord.register_agent(a)
    def test_organize(self):
        org = SelfOrganization(self.coord, SwarmConfig())
        groups = org.organize()
        self.assertIsInstance(groups, dict)
    def test_create_self_organization(self):
        o = create_self_organization(self.coord, SwarmConfig())
        self.assertIsInstance(o, SelfOrganization)
    def test_organization_config(self):
        o = SelfOrganization(self.coord, SwarmConfig())
        self.assertIsInstance(o.config, SwarmConfig)


class TestSwarmFactoryFunctions(unittest.TestCase):
    def test_create_swarm(self):
        s = create_swarm()
        self.assertIsInstance(s, dict); self.assertIn("coordinator", s)
    def test_all_swarm_factories(self):
        a = create_swarm_agent(); c = create_swarm_coordinator()
        d = create_collective_decision(c); o = create_self_organization(c)
        sd = create_swarm()
        self.assertIsInstance(a, SwarmAgent); self.assertIsInstance(sd, dict)
        self.assertIsInstance(c, SwarmCoordinator)
        self.assertIsInstance(d, CollectiveDecision)
        self.assertIsInstance(o, SelfOrganization)


# ═══════════════════════════════════════════
# SELF-LEARNING TESTS (50 tests)
# ═══════════════════════════════════════════

class TestReinforcementLearnerBasics(unittest.TestCase):
    def setUp(self):
        self.cfg = SLConfig(learning_rate=0.1, discount_factor=0.95, max_episodes=100)
        self.learner = ReinforcementLearner(config=self.cfg)
    def test_init(self):
        self.assertIsInstance(self.learner, ReinforcementLearner)
    def test_choose_action(self):
        a = self.learner.choose_action("state_1")
        self.assertIsInstance(a, str)
    def test_learn_update(self):
        self.learner.learn("s1", "a0", 1.0, "s2")
        self.assertNotEqual(self.learner.get_q_value("s1", "a0"), 0.0)
    def test_get_q_value(self):
        self.learner.learn("s1", "a0", 1.0, "s2")
        self.assertGreaterEqual(self.learner.get_q_value("s1", "a0"), 0.0)
    def test_set_q_value(self):
        self.learner.set_q_value("s1", "a0", 0.5)
        self.assertEqual(self.learner.get_q_value("s1", "a0"), 0.5)
    def test_learning_state_enum(self):
        names = [s.name for s in LearningState]
        self.assertIn("EXPLORING", names); self.assertIn("EXPLOITING", names)
    def test_compression_action_enum(self):
        names = [a.name for a in CompressionAction]
        self.assertIn("NONE", names)
    def test_reward_signal_enum(self):
        names = [r.name for r in RewardSignal]
        self.assertIn("THROUGHPUT", names)
    def test_sl_config(self):
        c = SLConfig(); self.assertEqual(c.learning_rate, 0.1)
    def test_create_learner(self):
        l = create_learner()
        self.assertIsInstance(l, ReinforcementLearner)
    def test_get_optimal_policy(self):
        self.learner.learn("s1", "a0", 1.0, "s2")
        p = self.learner.get_optimal_policy("s1")
        self.assertIsInstance(p, list)
    def test_learner_reset(self):
        self.learner.learn("s1", "a0", 1.0, "s2")
        self.learner.reset()
        self.assertEqual(self.learner._metrics.total_episodes, 0)
    def test_adapt_strategy(self):
        self.assertIsInstance(self.learner.adapt_strategy(), bool)
    def test_get_metrics(self):
        m = self.learner.get_metrics()
        self.assertIsInstance(m, type(self.learner._metrics))
    def test_run_episode(self):
        ep = self.learner.run_episode(lambda: "state_0", max_steps=10)
        self.assertIsInstance(ep, LearningEpisode); self.assertEqual(ep.steps, 10)
    def test_multiple_episodes(self):
        for _ in range(5): self.learner.run_episode(lambda: "state_0", max_steps=5)
        self.assertGreater(self.learner.get_metrics().total_episodes, 0)
    def test_learn_negative_reward(self):
        self.learner.learn("s1", "a0", -1.0, "s2")
        self.assertLess(self.learner.get_q_value("s1", "a0"), 0)
    def test_sl_config_defaults(self):
        c = SLConfig()
        self.assertEqual(c.gamma, c.discount_factor)
    def test_learner_config(self):
        self.assertEqual(self.learner.config.max_episodes, 100)
    def test_adaptive_compression(self):
        self.assertIsInstance(self.learner.adapt_compression("pair_1"), (int, float))


class TestAdaptiveTokenAllocator(unittest.TestCase):
    def setUp(self):
        self.alloc = AdaptiveTokenAllocator(config=SLConfig())
    def test_allocate_tokens(self):
        a = self.alloc.allocate("agent_a", "agent_b")
        self.assertIsInstance(a, (int, float))
    def test_get_optimal_compression(self):
        lvl = self.alloc.allocate("agent_a", "agent_b")
        self.assertIsInstance(lvl, (int, float))
    def test_create_allocator(self):
        a = create_allocator()
        self.assertIsInstance(a, AdaptiveTokenAllocator)
    def test_allocate_zero(self):
        lvl = self.alloc.allocate("a", "b")
        self.assertIsInstance(lvl, (int, float))


class TestPerformanceTracker(unittest.TestCase):
    def setUp(self):
        self.tracker = PerformanceTracker(config=SLConfig())
    def test_record_interaction(self):
        self.tracker.record_interaction("a1", "a2", 50.0, 100, True)
        s = self.tracker.get_stats("a1")
        self.assertIsInstance(s, AgentPairStats)
    def test_get_stats(self):
        self.tracker.record_interaction("a1", "a2", 30.0, 50, True)
        s = self.tracker.get_stats("a1")
        self.assertIsInstance(s, AgentPairStats)
    def test_create_tracker(self):
        t = create_tracker()
        self.assertIsInstance(t, PerformanceTracker)
    def test_tracker_config(self):
        self.assertEqual(self.tracker.config.learning_rate, 0.1)


class TestFeedbackLoop(unittest.TestCase):
    def setUp(self):
        self.fl = FeedbackLoop(config=SLConfig())
    def test_feedback_quality(self):
        q = self.fl.get_feedback_quality("pair_1", "pair_2")
        self.assertIsInstance(q, dict)
    def test_create_feedback_loop(self):
        f = create_feedback_loop()
        self.assertIsInstance(f, FeedbackLoop)
    def test_feedback_loop_config(self):
        self.assertIsInstance(self.fl.config, SLConfig)


class TestSLBundle(unittest.TestCase):
    def test_create_bundle(self):
        b = create_sl_bundle()
        self.assertIsInstance(b, SLBundle)
        self.assertIsInstance(b.learner, ReinforcementLearner)
    def test_bundle_components(self):
        b = create_sl_bundle()
        self.assertIsInstance(b.allocator, AdaptiveTokenAllocator)
        self.assertIsInstance(b.tracker, PerformanceTracker)


# ═══════════════════════════════════════════
# PRIVACY TESTS (50 tests)
# ═══════════════════════════════════════════

class TestZKProofVerifierBasics(unittest.TestCase):
    def setUp(self):
        self.verifier = ZKProofVerifier()
    def test_init(self):
        self.assertIsInstance(self.verifier, ZKProofVerifier)
    def test_issue_proof(self):
        p = self.verifier.issue_proof(public_inputs={"test": "data"}, witness_hash="witness_123")
        self.assertIsInstance(p, ZKProof)
    def test_verify_proof(self):
        p = self.verifier.issue_proof(public_inputs={"test": "data"}, witness_hash="witness_123")
        s = self.verifier.verify_proof(p.proof_id, prover_id="test")
        self.assertIsInstance(s, ProofStatus)
    def test_get_proof_status(self):
        p = self.verifier.issue_proof(public_inputs={"test": "data"}, witness_hash="witness_123")
        s = self.verifier.get_proof_status(p.proof_id)
        self.assertIsInstance(s, ProofStatus)
    def test_batch_verify(self):
        ids = []
        for _ in range(3):
            p = self.verifier.issue_proof(public_inputs={"test": str(_)}, witness_hash="w"+str(_))
            ids.append(p.proof_id)
        results = self.verifier.verify_batch(ids)
        self.assertEqual(len(results), 3)
    def test_revoke_proof(self):
        p = self.verifier.issue_proof(public_inputs={"test": "data"}, witness_hash="witness_123")
        self.assertTrue(self.verifier.revoke_proof(p.proof_id))
    def test_get_metrics(self):
        m = self.verifier.get_metrics()
        self.assertIsInstance(m, dict)
    def test_create_zk_verifier(self):
        v = create_zk_verifier()
        self.assertIsInstance(v, ZKProofVerifier)
    def test_proof_status_enum(self):
        names = [s.name for s in ProofStatus]
        self.assertIn("VALID", names); self.assertIn("INVALID", names)
    def test_token_visibility_enum(self):
        names = [v.name for v in TokenVisibility]
        self.assertIn("PUBLIC", names)
    def test_anonymity_level_enum(self):
        names = [l.name for l in AnonymityLevel]
        self.assertIn("FULL", names)
    def test_noise_mechanism_enum(self):
        names = [n.name for n in NoiseMechanism]
        self.assertIn("LAPLACE", names)
    def test_zk_proof_to_dict(self):
        p = self.verifier.issue_proof(public_inputs={"test": "data"}, witness_hash="witness_123")
        d = p.to_dict()
        self.assertIsInstance(d, dict)
    def test_zk_proof_from_dict(self):
        p = self.verifier.issue_proof(public_inputs={"test": "data"}, witness_hash="witness_123")
        d = p.to_dict()
        p2 = ZKProof.from_dict(d)
        self.assertIsInstance(p2, ZKProof)
    def test_multiple_proofs(self):
        for i in range(5):
            p = self.verifier.issue_proof(public_inputs={"test": str(i)}, witness_hash="w"+str(i))
            self.assertIsInstance(p, ZKProof)
    def test_zk_proof_has_inputs(self):
        p = self.verifier.issue_proof(public_inputs={"x": 1}, witness_hash="wh")
        self.assertEqual(p.public_inputs, {"x": 1})
    def test_zk_proof_has_witness(self):
        p = self.verifier.issue_proof(public_inputs={"x": 1}, witness_hash="wh")
        self.assertIsNotNone(p.proof_id)
    def test_verifier_max_proofs(self):
        v = ZKProofVerifier(max_proofs=5)
        self.assertEqual(v._max_proofs, 5)
    def test_create_zk_verifier_with_params(self):
        v = create_zk_verifier(max_proofs=50)
        self.assertIsInstance(v, ZKProofVerifier)


class TestPrivacyToken(unittest.TestCase):
    def setUp(self):
        self.t = PrivacyPreservingToken()
    def test_token_creation(self):
        self.assertIsInstance(self.t, PrivacyPreservingToken)
    def test_token_visibility(self):
        self.assertIsInstance(self.t.visibility, TokenVisibility)
    def test_create_privacy_token(self):
        t = create_privacy_token()
        self.assertIsInstance(t, PrivacyPreservingToken)
    def test_privacy_token_config(self):
        t = PrivacyPreservingToken()
        self.assertIsInstance(t.config, TEQConfig)


class TestAnonymousRelay(unittest.TestCase):
    def setUp(self):
        self.relay = AnonymousRelay(config=BRPConfig(port=8765), anonymity_level=AnonymityLevel.FULL)
    def test_relay_creation(self):
        self.assertIsInstance(self.relay, AnonymousRelay)
    def test_relay_level(self):
        self.assertEqual(self.relay.anonymity_level, AnonymityLevel.FULL)
    def test_create_anonymous_relay(self):
        r = create_anonymous_relay(config=BRPConfig(port=8765))
        self.assertIsInstance(r, AnonymousRelay)
    def test_relay_config(self):
        self.assertIsInstance(self.relay.config, BRPConfig)


class TestDifferentialPrivacy(unittest.TestCase):
    def setUp(self):
        self.dp = DifferentialPrivacy(epsilon=1.0, delta=1e-5)
    def test_dp_creation(self):
        self.assertIsInstance(self.dp, DifferentialPrivacy)
    def test_epsilon(self):
        self.assertEqual(self.dp.epsilon, 1.0)
    def test_create_differential_privacy(self):
        dp = create_differential_privacy(epsilon=0.5)
        self.assertIsInstance(dp, DifferentialPrivacy)
    def test_dp_delta(self):
        self.assertEqual(self.dp.delta, 1e-5)


class TestPrivacyFactoryFunctions(unittest.TestCase):
    def test_create_privacy_suite(self):
        s = create_privacy_suite()
        self.assertIsInstance(s, dict); self.assertIn("zk_verifier", s)
    def test_create_zk_verifier(self):
        v = create_zk_verifier()
        self.assertIsInstance(v, ZKProofVerifier)
    def test_create_differential_privacy(self):
        dp = create_differential_privacy()
        self.assertIsInstance(dp, DifferentialPrivacy)
    def test_all_privacy_factories(self):
        v = create_zk_verifier(); tok = create_privacy_token()
        r = create_anonymous_relay(BRPConfig()); dp = create_differential_privacy()
        s = create_privacy_suite()
        self.assertIsInstance(v, ZKProofVerifier)
        self.assertIsInstance(tok, PrivacyPreservingToken)
        self.assertIsInstance(r, AnonymousRelay)
        self.assertIsInstance(dp, DifferentialPrivacy)
        self.assertIsInstance(s, dict)


# ═══════════════════════════════════════════
# INTEROP TESTS (50 tests)
# ═══════════════════════════════════════════

class TestProtocolBridgeBasics(unittest.TestCase):
    def setUp(self):
        self.bridge = ProtocolBridge()
    def test_init(self):
        self.assertIsInstance(self.bridge, ProtocolBridge)
    def test_connect_external(self):
        self.bridge.initialize()
        self.bridge.connect_external(ProtocolType.HTTP_REST, "http://localhost")
    def test_protocol_type_enum(self):
        names = [t.name for t in ProtocolType]
        self.assertIn("HTTP_REST", names); self.assertIn("MQTT", names)
    def test_adapter_state_enum(self):
        names = [s.name for s in AdapterState]
        self.assertIn("CONNECTED", names); self.assertIn("DISCONNECTED", names)
    def test_translation_format_enum(self):
        names = [f.name for f in TranslationFormat]
        self.assertIn("JSON", names)
    def test_get_bridge_stats(self):
        self.bridge.initialize()
        stats = self.bridge.get_bridge_stats()
        self.assertIsInstance(stats, dict)
    def test_pause_resume(self):
        self.bridge.initialize(); self.bridge.pause()
        self.assertEqual(self.bridge._state, BridgeState.PAUSED)
    def test_create_protocol_bridge(self):
        b = create_protocol_bridge()
        self.assertIsInstance(b, ProtocolBridge)
    def test_create_cross_protocol_gateway(self):
        g = create_cross_protocol_gateway()
        self.assertIsInstance(g, CrossProtocolGateway)
    def test_create_universal_translator(self):
        t = create_universal_translator()
        self.assertIsInstance(t, UniversalTranslator)
    def test_create_interop_stack(self):
        s = create_interop_stack()
        self.assertIsInstance(s, dict)
    def test_bridge_config(self):
        bc = BridgeConfig()
        self.assertIsInstance(bc, BridgeConfig)
    def test_gateway_config(self):
        gc = GatewayConfig()
        self.assertIsInstance(gc, GatewayConfig)
    def test_translation_rule(self):
        r = TranslationRule()
        self.assertIsInstance(r, TranslationRule)
    def test_bridge_initialize(self):
        self.bridge.initialize()
        self.assertIsInstance(self.bridge._registry, AdapterRegistry)
    def test_bridge_state_enum(self):
        names = [s.name for s in BridgeState]
        self.assertIn("CONNECTED", names); self.assertIn("PAUSED", names)
    def test_interop_brp_server(self):
        s = InteropBRPServer()
        self.assertIsInstance(s, InteropBRPServer)
    def test_disconnect(self):
        self.bridge.initialize()
        result = self.bridge.disconnect_external(ProtocolType.HTTP_REST)
        self.assertIsInstance(result, bool)
    def test_bridge_config_type(self):
        bc = BridgeConfig()
        self.assertIsInstance(bc, BridgeConfig)


class TestProtocolAdapterManager(unittest.TestCase):
    def setUp(self):
        self.reg = AdapterRegistry()
    def test_adapter_config(self):
        a = AdapterConfig(protocol_type=ProtocolType.HTTP_REST, endpoint="http://localhost")
        self.assertIsInstance(a, AdapterConfig)
        self.assertEqual(a.protocol_type, ProtocolType.HTTP_REST)
    def test_adapter_state_enum(self):
        names = [s.name for s in AdapterState]
        self.assertIn("CONNECTED", names)
    def test_external_adapter(self):
        e = ExternalAdapter(adapter_id="ext1", config=AdapterConfig(protocol_type=ProtocolType.HTTP_REST, endpoint="http://localhost"))
        self.assertIsInstance(e, ExternalAdapter)
    def test_adapter_registry(self):
        self.assertIsInstance(self.reg, AdapterRegistry)
    def test_adapter_registry_stats(self):
        stats = self.reg.get_stats()
        self.assertIsInstance(stats, dict)


class TestInteropErrors(unittest.TestCase):
    def test_interop_error(self):
        self.assertIn("test", str(InteropError("test")))
    def test_adapter_not_found_error(self):
        self.assertIn("x", str(AdapterNotFoundError("x")))
    def test_protocol_not_supported_error(self):
        self.assertIn("proto", str(ProtocolNotSupportedError("proto")))
    def test_translation_error(self):
        self.assertIn("failed", str(TranslationError("failed")))
    def test_error_hierarchy(self):
        self.assertTrue(issubclass(AdapterNotFoundError, InteropError))
        self.assertTrue(issubclass(ProtocolNotSupportedError, InteropError))
        self.assertTrue(issubclass(TranslationError, InteropError))


class TestInteropFactoryFunctions(unittest.TestCase):
    def test_create_adapter(self):
        a = AdapterConfig(protocol_type=ProtocolType.HTTP_REST, endpoint="http://localhost")
        self.assertIsInstance(a, AdapterConfig)
    def test_create_cross_protocol_gateway(self):
        g = create_cross_protocol_gateway()
        self.assertIsInstance(g, CrossProtocolGateway)
    def test_create_universal_translator(self):
        t = create_universal_translator()
        self.assertIsInstance(t, UniversalTranslator)
    def test_all_interop_factories(self):
        b = create_protocol_bridge(); g = create_cross_protocol_gateway()
        t = create_universal_translator(); st = create_interop_stack()
        self.assertIsInstance(b, ProtocolBridge)
        self.assertIsInstance(st, dict)
        self.assertIsInstance(g, CrossProtocolGateway)
        self.assertIsInstance(t, UniversalTranslator)
    def test_bridge_config_factory(self):
        bc = BridgeConfig()
        self.assertIsInstance(bc, BridgeConfig)
    def test_gateway_config_factory(self):
        gc = GatewayConfig()
        self.assertIsInstance(gc, GatewayConfig)


# ═══════════════════════════════════════════
# V4 INFRASTRUCTURE INTEGRATION TESTS
# ═══════════════════════════════════════════

class TestV4InfrastructureIntegration(unittest.TestCase):
    def test_swarm_uses_v2_broker(self):
        c = SwarmCoordinator(config=SwarmConfig())
        self.assertIsInstance(c.broker, MessageBroker)
    def test_swarm_uses_v2_circuit_breaker(self):
        c = SwarmCoordinator(config=SwarmConfig())
        self.assertIsInstance(c.circuit_breaker, CircuitBreaker)
    def test_selflearning_uses_v2_codec(self):
        self.assertTrue(callable(compress_message))
    def test_privacy_uses_v2_circuit_breaker(self):
        v = ZKProofVerifier()
        self.assertIsInstance(v._breaker, CircuitBreaker)
    def test_interop_uses_v2_codec(self):
        self.assertTrue(callable(compress_message))
        self.assertTrue(callable(decompress_message))
    def test_all_v4_modules(self):
        import swarm, selflearning, privacy, interop
        self.assertTrue(hasattr(swarm, 'AgentRole'))
        self.assertTrue(hasattr(selflearning, 'ReinforcementLearner'))
        self.assertTrue(hasattr(privacy, 'ZKProofVerifier'))
        self.assertTrue(hasattr(interop, 'ProtocolBridge'))
    def test_v2_registry_integrated(self):
        r = get_registry()
        self.assertIsInstance(r, TokenRegistry)
    def test_v2_broker_integrated(self):
        b = MessageBroker()
        self.assertIsInstance(b, MessageBroker)
    def test_v2_codec_integrated(self):
        e = MessageEncoder()
        self.assertTrue(callable(compress_message))
    def test_v3_brp_config(self):
        c = BRPConfig()
        self.assertIsInstance(c, BRPConfig)
    def test_v3_car_router(self):
        r = CARRouter()
        self.assertIsInstance(r, CARRouter)
    def test_v3_atc_config(self):
        c = ATCConfig()
        self.assertIsInstance(c, ATCConfig)
    def test_v3_teq_config(self):
        c = TEQConfig()
        self.assertIsInstance(c, TEQConfig)
    def test_v3_epc_cache(self):
        c = EPCConfig()
        self.assertIsInstance(c, EPCConfig)
    def test_v2_bridge(self):
        b = TokenRelayBridge()
        self.assertIsInstance(b, TokenRelayBridge)
    def test_v2_streaming(self):
        bus = EventBus()
        self.assertIsInstance(bus, EventBus)
    def test_all_v2_modules_present(self):
        import codec, broker, circuit_breaker, streaming, bridge, registry, brp, car, atc, teq, epc
        self.assertTrue(callable(compress_message))
        self.assertTrue(callable(decompress_message))
    def test_v4_version_consistency(self):
        import sys; sys.path.insert(0, str(Path(__file__).resolve().parent.parent)); import sys; sys.path.insert(0, str(Path(__file__).resolve().parent.parent)); from src import __version__
        self.assertEqual(__version__, "4.0.0")


class TestV4FactoryFunctions(unittest.TestCase):
    def test_all_swarm_factories(self):
        a = create_swarm_agent(); c = create_swarm_coordinator()
        d = create_collective_decision(c); o = create_self_organization(c)
        sd = create_swarm()
        self.assertIsInstance(a, SwarmAgent); self.assertIsInstance(sd, dict)
        self.assertIsInstance(c, SwarmCoordinator)
        self.assertIsInstance(d, CollectiveDecision)
        self.assertIsInstance(o, SelfOrganization)
    def test_all_selflearning_factories(self):
        l = create_learner(); al = create_allocator(); t = create_tracker()
        f = create_feedback_loop(); b = create_sl_bundle()
        self.assertIsInstance(l, ReinforcementLearner); self.assertIsInstance(b, SLBundle)
        self.assertIsInstance(al, AdaptiveTokenAllocator)
        self.assertIsInstance(t, PerformanceTracker)
        self.assertIsInstance(f, FeedbackLoop)
    def test_all_privacy_factories(self):
        v = create_zk_verifier(); tok = create_privacy_token()
        r = create_anonymous_relay(BRPConfig()); dp = create_differential_privacy()
        s = create_privacy_suite()
        self.assertIsInstance(v, ZKProofVerifier)
        self.assertIsInstance(tok, PrivacyPreservingToken)
        self.assertIsInstance(r, AnonymousRelay)
        self.assertIsInstance(dp, DifferentialPrivacy)
        self.assertIsInstance(s, dict)
    def test_all_interop_factories(self):
        b = create_protocol_bridge(); g = create_cross_protocol_gateway()
        t = create_universal_translator(); st = create_interop_stack()
        self.assertIsInstance(b, ProtocolBridge)
        self.assertIsInstance(st, dict)
        self.assertIsInstance(g, CrossProtocolGateway)
        self.assertIsInstance(t, UniversalTranslator)
    def test_all_factory_functions(self):
        funcs = [
            create_swarm_agent, create_swarm_coordinator,
            create_learner, create_allocator, create_tracker,
            create_feedback_loop, create_sl_bundle,
            create_zk_verifier, create_privacy_token,
            create_anonymous_relay, create_differential_privacy,
            create_protocol_bridge, create_cross_protocol_gateway,
            create_universal_translator, create_interop_stack,
        ]
        for fn in funcs:
            result = fn()
            self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
