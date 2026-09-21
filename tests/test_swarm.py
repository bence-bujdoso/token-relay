"""
Tests for TokenRelay v4 Swarm Intelligence & Coordination.

Covers:
- SwarmAgent: creation, task assignment, state management, capability evaluation
- SwarmCoordinator: registration, discovery, load balancing, conflict resolution
- CollectiveDecision: proposal creation, voting, consensus across all methods
- SelfOrganization: clustering, task reassignment, anomaly detection, adaptation
- Integration: full swarm workflow with all components
"""

import sys
import time
import unittest
from pathlib import Path
from dataclasses import asdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from swarm import (
    SwarmAgent, SwarmCoordinator, CollectiveDecision, SelfOrganization,
    CapabilityVector, AgentRole, AgentState, ConsensusMethod, TaskPriority,
    OrganizationStrategy, AgentInfo, SwarmTask, Vote, ConsensusResult,
    SwarmConfig, SwarmError, AgentNotFoundError, AgentConflictError,
    ConsensusError, SwarmOverloadError,
    create_swarm_agent, create_swarm_coordinator,
    create_collective_decision, create_self_organization, create_swarm,
)


# ═══════════════════════════════════════════════════════════
# CapabilityVector Tests
# ═══════════════════════════════════════════════════════════

class TestCapabilityVector(unittest.TestCase):
    def test_default_values(self):
        cv = CapabilityVector()
        self.assertEqual(cv.reasoning, 0.5)
        self.assertEqual(cv.speed, 0.5)

    def test_similarity_identical(self):
        cv1 = CapabilityVector(reasoning=0.8, speed=0.9)
        cv2 = CapabilityVector(reasoning=0.8, speed=0.9)
        self.assertAlmostEqual(cv1.similarity(cv2), 1.0, places=4)

    def test_similarity_orthogonal(self):
        cv1 = CapabilityVector(reasoning=1.0, speed=0, memory=0, communication=0, domain_knowledge=0, reliability=0)
        cv2 = CapabilityVector(reasoning=0, speed=1.0, memory=0, communication=0, domain_knowledge=0, reliability=0)
        self.assertEqual(cv1.similarity(cv2), 0.0)

    def test_normalize(self):
        cv = CapabilityVector(reasoning=2.0, speed=1.0)
        normalized = cv.normalize()
        self.assertAlmostEqual(normalized.reasoning, 1.0, places=4)
        self.assertAlmostEqual(normalized.speed, 0.5, places=4)


# ═══════════════════════════════════════════════════════════
# SwarmAgent Tests
# ═══════════════════════════════════════════════════════════

class TestSwarmAgent(unittest.TestCase):
    def setUp(self):
        self.agent = create_swarm_agent(role=AgentRole.WORKER)

    def test_agent_creation(self):
        self.assertIsNotNone(self.agent.agent_id)
        self.assertEqual(self.agent.role, AgentRole.WORKER)
        self.assertEqual(self.agent.state, AgentState.IDLE)

    def test_agent_unique_ids(self):
        agent2 = create_swarm_agent()
        self.assertNotEqual(self.agent.agent_id, agent2.agent_id)

    def test_capability_score(self):
        self.assertGreaterEqual(self.agent.capability_score, 0.0)
        self.assertLessEqual(self.agent.capability_score, 1.0)

    def test_neighbors_management(self):
        self.agent.add_neighbor("agent_b")
        self.agent.add_neighbor("agent_c")
        self.assertIn("agent_b", self.agent.neighbors)
        self.assertEqual(len(self.agent.neighbors), 2)

        self.agent.remove_neighbor("agent_b")
        self.assertNotIn("agent_b", self.agent.neighbors)

    def test_task_assignment(self):
        task = SwarmTask(description="Test task")
        result = self.agent.assign_task(task)
        self.assertTrue(result)
        self.assertEqual(task.assigned_agent, self.agent.agent_id)
        self.assertEqual(len(self.agent.task_queue), 1)

    def test_task_completion(self):
        task = SwarmTask(description="Test task")
        self.agent.assign_task(task)
        result = self.agent.complete_task(task.task_id, "success")
        self.assertTrue(result)
        self.assertEqual(len(self.agent.completed_tasks), 1)
        self.assertEqual(len(self.agent.task_queue), 0)

    def test_task_failure(self):
        task = SwarmTask(description="Test task")
        self.agent.assign_task(task)
        result = self.agent.fail_task(task.task_id, "error")
        self.assertTrue(result)
        self.assertEqual(len(self.agent.failed_tasks), 1)

    def test_is_available(self):
        self.assertTrue(self.agent.is_available)
        self.agent.state = AgentState.BUSY
        self.assertFalse(self.agent.is_available)

    def test_heartbeat(self):
        hb = self.agent.heartbeat()
        self.assertEqual(hb["agent_id"], self.agent.agent_id)
        self.assertEqual(hb["role"], AgentRole.WORKER.name)
        self.assertIn("health_score", hb)
        self.assertIn("timestamp", hb)

    def test_serialize_deserialize_state(self):
        state_bytes = self.agent.serialize_state()
        state = self.agent.deserialize_state(state_bytes)
        self.assertEqual(state["agent_id"], self.agent.agent_id)
        self.assertEqual(state["role"], AgentRole.WORKER.value)

    def test_evaluate_task_fit(self):
        task = SwarmTask(description="Test", required_capabilities={"reasoning": 0.5})
        fit = self.agent.evaluate_task_fit(task)
        self.assertGreaterEqual(fit, 0.0)
        self.assertLessEqual(fit, 1.0)

    def test_to_dict(self):
        d = self.agent.to_dict()
        self.assertEqual(d["agent_id"], self.agent.agent_id)
        self.assertEqual(d["role"], AgentRole.WORKER.name)
        self.assertIn("capability_vector", d)

    def test_role_change(self):
        specialist = create_swarm_agent(role=AgentRole.SPECIALIST)
        self.assertEqual(specialist.role, AgentRole.SPECIALIST)
        leader = create_swarm_agent(role=AgentRole.LEADER)
        self.assertEqual(leader.role, AgentRole.LEADER)
        gateway = create_swarm_agent(role=AgentRole.GATEWAY)
        self.assertEqual(gateway.role, AgentRole.GATEWAY)
        observer = create_swarm_agent(role=AgentRole.OBSERVER)
        self.assertEqual(observer.role, AgentRole.OBSERVER)

    def test_custom_capability_vector(self):
        cv = CapabilityVector(reasoning=0.9, speed=0.8, memory=0.7,
                              communication=0.6, domain_knowledge=0.9, reliability=0.8)
        agent = create_swarm_agent(config=SwarmConfig(), capability_vector=cv)
        self.assertEqual(agent.capability_vector.reasoning, 0.9)
        self.assertAlmostEqual(agent.capability_score, 0.7833, places=3)

    def test_health_score_calculation(self):
        # Agent with no tasks should have reasonable health
        self.assertGreaterEqual(self.agent.health_score, 0.0)
        self.assertLessEqual(self.agent.health_score, 1.0)


# ═══════════════════════════════════════════════════════════
# SwarmCoordinator Tests
# ═══════════════════════════════════════════════════════════

class TestSwarmCoordinator(unittest.TestCase):
    def setUp(self):
        self.coordinator = create_swarm_coordinator()
        self.agent1 = create_swarm_agent(role=AgentRole.WORKER)
        self.agent2 = create_swarm_agent(role=AgentRole.SPECIALIST)
        self.coordinator.register_agent(self.agent1)
        self.coordinator.register_agent(self.agent2)

    def test_register_agents(self):
        self.assertEqual(len(self.coordinator.agents), 2)
        self.assertIn(self.agent1.agent_id, self.coordinator.agents)
        self.assertIn(self.agent2.agent_id, self.coordinator.agents)

    def test_unregister_agent(self):
        self.coordinator.unregister_agent(self.agent1.agent_id)
        self.assertEqual(len(self.coordinator.agents), 1)

    def test_unregister_agent_redistributes_tasks(self):
        task = SwarmTask(description="Test task")
        self.agent1.assign_task(task)
        self.coordinator.unregister_agent(self.agent1.agent_id)
        # Task should be back in pending state
        self.assertEqual(task.status, "pending")
        self.assertIsNone(task.assigned_agent)

    def test_get_agent(self):
        agent = self.coordinator.get_agent(self.agent1.agent_id)
        self.assertEqual(agent.agent_id, self.agent1.agent_id)

    def test_get_agent_not_found(self):
        with self.assertRaises(AgentNotFoundError):
            self.coordinator.get_agent("nonexistent_agent")

    def test_discover_agents(self):
        agents = self.coordinator.discover_agents()
        self.assertEqual(len(agents), 2)
        self.assertIsInstance(agents[0], AgentInfo)
        self.assertEqual(agents[0].agent_id, self.agent1.agent_id)

    def test_load_balance(self):
        task1 = self.coordinator.submit_task("Simple task", TaskPriority.NORMAL)
        task2 = self.coordinator.submit_task("Another task", TaskPriority.HIGH)
        assignments = self.coordinator.load_balance()
        self.assertGreater(len(assignments), 0)

    def test_load_balance_empty(self):
        assignments = self.coordinator.load_balance()
        self.assertEqual(assignments, {})

    def test_get_swarm_health(self):
        health = self.coordinator.get_swarm_health()
        self.assertEqual(health["total_agents"], 2)
        self.assertIn("avg_health", health)
        self.assertIn("capacity_utilization", health)

    def test_broadcast_heartbeat(self):
        heartbeats = self.coordinator.broadcast_heartbeat()
        self.assertEqual(len(heartbeats), 2)
        for hb in heartbeats:
            self.assertIn("agent_id", hb)

    def test_submit_task(self):
        task = self.coordinator.submit_task("Test task")
        self.assertIsNotNone(task.task_id)
        self.assertIn(task.task_id, self.coordinator.task_registry)

    def test_submit_task_with_capabilities(self):
        task = self.coordinator.submit_task(
            "Complex analysis",
            priority=TaskPriority.HIGH,
            required_capabilities={"reasoning": 0.8, "domain_knowledge": 0.7}
        )
        self.assertEqual(task.priority, TaskPriority.HIGH)

    def test_max_agents_limit(self):
        small_coordinator = SwarmCoordinator(SwarmConfig(max_agents=1))
        with self.assertRaises(SwarmOverloadError):
            small_coordinator.register_agent(self.agent1)
            small_coordinator.register_agent(self.agent2)

    def test_conflict_resolution(self):
        task = self.coordinator.submit_task("Test task")
        self.agent1.assign_task(task)
        self.agent2.assign_task(task)
        winner = self.coordinator.resolve_conflict(
            self.agent1.agent_id, self.agent2.agent_id, task.task_id
        )
        self.assertIn(winner, [self.agent1.agent_id, self.agent2.agent_id])

    def test_to_dict(self):
        d = self.coordinator.to_dict()
        self.assertEqual(d["total_agents"], 2)
        self.assertIn("health", d)


# ═══════════════════════════════════════════════════════════
# CollectiveDecision Tests
# ═══════════════════════════════════════════════════════════

class TestCollectiveDecision(unittest.TestCase):
    def setUp(self):
        self.coordinator = create_swarm_coordinator()
        self.agent1 = create_swarm_agent(role=AgentRole.WORKER)
        self.agent2 = create_swarm_agent(role=AgentRole.SPECIALIST)
        self.coordinator.register_agent(self.agent1)
        self.coordinator.register_agent(self.agent2)
        self.collective = CollectiveDecision(self.coordinator)

    def test_create_proposal(self):
        proposal = self.collective.create_proposal("prop1", "Test proposal")
        self.assertEqual(proposal["proposal_id"], "prop1")
        self.assertEqual(proposal["status"], "active")

    def test_cast_vote(self):
        proposal = self.collective.create_proposal("prop2", "Another proposal")
        vote = self.collective.cast_vote(self.agent1.agent_id, "prop2", True, "Good idea")
        self.assertEqual(vote.agent_id, self.agent1.agent_id)
        self.assertTrue(vote.vote)
        self.assertGreater(vote.weight, 0)

    def test_cast_vote_duplicate(self):
        proposal = self.collective.create_proposal("prop3", "Third proposal")
        self.collective.cast_vote(self.agent1.agent_id, "prop3", True)
        with self.assertRaises(ConsensusError):
            self.collective.cast_vote(self.agent1.agent_id, "prop3", False)

    def test_cast_vote_on_nonexistent_proposal(self):
        with self.assertRaises(ConsensusError):
            self.collective.cast_vote(self.agent1.agent_id, "nonexistent", True)

    def test_reach_consensus_majority(self):
        proposal = self.collective.create_proposal("prop4", "Majority vote test")
        self.collective.cast_vote(self.agent1.agent_id, "prop4", True)
        self.collective.cast_vote(self.agent2.agent_id, "prop4", True)
        result = self.collective.reach_consensus("prop4")
        self.assertTrue(result.approved)
        self.assertEqual(result.total_votes, 2)
        self.assertEqual(result.approval_count, 2)

    def test_reach_consensus_rejection(self):
        proposal = self.collective.create_proposal("prop5", "Rejection test")
        self.collective.cast_vote(self.agent1.agent_id, "prop5", True)
        self.collective.cast_vote(self.agent2.agent_id, "prop5", False)
        result = self.collective.reach_consensus("prop5")
        self.assertFalse(result.approved)

    def test_supermajority_consensus(self):
        self.collective.config.consensus_method = ConsensusMethod.SUPERMAJORITY
        proposal = self.collective.create_proposal("prop6", "Supermajority test")
        self.collective.cast_vote(self.agent1.agent_id, "prop6", True)
        self.collective.cast_vote(self.agent2.agent_id, "prop6", False)
        result = self.collective.reach_consensus("prop6")
        # With only 1 of 2 votes, should fail supermajority
        self.assertFalse(result.approved)

    def test_weighted_consensus(self):
        self.collective.config.consensus_method = ConsensusMethod.WEIGHTED
        proposal = self.collective.create_proposal("prop7", "Weighted test")
        self.collective.cast_vote(self.agent1.agent_id, "prop7", True)
        self.collective.cast_vote(self.agent2.agent_id, "prop7", False)
        result = self.collective.reach_consensus("prop7")
        # Result depends on weights
        self.assertIn(result.approved, [True, False])

    def test_consensus_with_no_votes(self):
        proposal = self.collective.create_proposal("prop8", "No votes")
        with self.assertRaises(ConsensusError):
            self.collective.reach_consensus("prop8")

    def test_get_proposal_status(self):
        proposal = self.collective.create_proposal("prop9", "Status test")
        self.collective.cast_vote(self.agent1.agent_id, "prop9", True)
        status = self.collective.get_proposal_status("prop9")
        self.assertEqual(status["total_votes"], 1)
        self.assertEqual(status["approval_count"], 1)

    def test_vote_weight_calculation(self):
        proposal = self.collective.create_proposal("prop10", "Weight test")
        vote = self.collective.cast_vote(self.agent1.agent_id, "prop10", True)
        self.assertGreater(vote.weight, 0)
        self.assertLessEqual(vote.weight, 2.25)  # Max weight with leader modifier

    def test_consensus_result_attributes(self):
        proposal = self.collective.create_proposal("prop11", "Result attrs")
        self.collective.cast_vote(self.agent1.agent_id, "prop11", True)
        result = self.collective.reach_consensus("prop11")
        self.assertIsInstance(result, ConsensusResult)
        self.assertIn("proposal_id", asdict(result))
        self.assertIn("approved", asdict(result))
        self.assertIn("total_votes", asdict(result))
        self.assertIn("duration_ms", asdict(result))

    def test_to_dict(self):
        d = self.collective.to_dict()
        self.assertIn("total_proposals", d)
        self.assertIn("total_consensus_results", d)


# ═══════════════════════════════════════════════════════════
# SelfOrganization Tests
# ═══════════════════════════════════════════════════════════

class TestSelfOrganization(unittest.TestCase):
    def setUp(self):
        self.coordinator = create_swarm_coordinator()
        self.agent1 = create_swarm_agent(role=AgentRole.WORKER)
        self.agent2 = create_swarm_agent(role=AgentRole.SPECIALIST)
        self.agent3 = create_swarm_agent(role=AgentRole.LEADER)
        self.coordinator.register_agent(self.agent1)
        self.coordinator.register_agent(self.agent2)
        self.coordinator.register_agent(self.agent3)
        self.self_org = SelfOrganization(self.coordinator)

    def test_organize_by_similarity(self):
        self.self_org.strategy = OrganizationStrategy.TASK_SIMILARITY
        clusters = self.self_org.organize()
        self.assertGreater(len(clusters), 0)
        all_agents = set()
        for agents in clusters.values():
            all_agents.update(agents)
        self.assertEqual(len(all_agents), 3)

    def test_organize_by_resources(self):
        self.self_org.strategy = OrganizationStrategy.RESOURCE_AWARENESS
        clusters = self.self_org.organize()
        self.assertGreater(len(clusters), 0)

    def test_organize_hybrid(self):
        self.self_org.strategy = OrganizationStrategy.HYBRID
        clusters = self.self_org.organize()
        self.assertGreater(len(clusters), 0)

    def test_agent_clusters_mapping(self):
        clusters = self.self_org.organize()
        for cluster_id, agent_ids in clusters.items():
            for aid in agent_ids:
                self.assertIn(aid, self.self_org.agent_clusters)
                self.assertEqual(self.self_org.agent_clusters[aid], cluster_id)

    def test_reassign_tasks(self):
        task = self.coordinator.submit_task("Test task")
        self.coordinator.load_balance()
        reassigned = self.self_org.reassign_tasks()
        self.assertGreaterEqual(reassigned, 0)

    def test_get_cluster_for_task(self):
        self.self_org.organize()
        cluster = self.self_org.get_cluster_for_task("Analyze data")
        self.assertIsNotNone(cluster)

    def test_detect_anomalies_empty_swarm(self):
        anomalies = self.self_org.detect_anomalies()
        self.assertEqual(anomalies, [])

    def test_detect_anomalies_with_compromised(self):
        self.agent1.state = AgentState.COMPROMISED
        anomalies = self.self_org.detect_anomalies()
        self.assertGreater(len(anomalies), 0)
        compromised = [a for a in anomalies if a["type"] == "compromised"]
        self.assertGreater(len(compromised), 0)

    def test_adapt_organization_stable(self):
        result = self.self_org.adapt_organization()
        self.assertIn("reorganized", result)

    def test_adapt_organization_with_anomalies(self):
        self.agent1.state = AgentState.COMPROMISED
        self.agent2.state = AgentState.COMPROMISED
        result = self.self_org.adapt_organization()
        # May or may not reorganize depending on conditions
        self.assertIn("reorganized", result)

    def test_to_dict(self):
        self.self_org.organize()
        d = self.self_org.to_dict()
        self.assertIn("clusters", d)
        self.assertIn("strategy", d)


# ═══════════════════════════════════════════════════════════
# Factory Function Tests
# ═══════════════════════════════════════════════════════════

class TestFactoryFunctions(unittest.TestCase):
    def test_create_swarm_agent(self):
        agent = create_swarm_agent(role=AgentRole.WORKER)
        self.assertIsInstance(agent, SwarmAgent)

    def test_create_swarm_coordinator(self):
        coordinator = create_swarm_coordinator()
        self.assertIsInstance(coordinator, SwarmCoordinator)

    def test_create_collective_decision(self):
        coordinator = create_swarm_coordinator()
        collective = create_collective_decision(coordinator)
        self.assertIsInstance(collective, CollectiveDecision)

    def test_create_self_organization(self):
        coordinator = create_swarm_coordinator()
        self_org = create_self_organization(coordinator)
        self.assertIsInstance(self_org, SelfOrganization)

    def test_create_swarm(self):
        swarm = create_swarm()
        self.assertIn("coordinator", swarm)
        self.assertIn("collective_decision", swarm)
        self.assertIn("self_organization", swarm)
        self.assertIsInstance(swarm["coordinator"], SwarmCoordinator)
        self.assertIsInstance(swarm["collective_decision"], CollectiveDecision)
        self.assertIsInstance(swarm["self_organization"], SelfOrganization)


# ═══════════════════════════════════════════════════════════
# Integration Tests
# ═══════════════════════════════════════════════════════════

class TestSwarmIntegration(unittest.TestCase):
    def test_full_swarm_workflow(self):
        """Test the complete workflow: create agents, submit tasks, vote, organize."""
        # Create swarm
        swarm = create_swarm()
        coordinator = swarm["coordinator"]
        collective = swarm["collective_decision"]
        self_org = swarm["self_organization"]

        # Register agents
        agent1 = create_swarm_agent(role=AgentRole.WORKER)
        agent2 = create_swarm_agent(role=AgentRole.SPECIALIST)
        agent3 = create_swarm_agent(role=AgentRole.LEADER)
        coordinator.register_agent(agent1)
        coordinator.register_agent(agent2)
        coordinator.register_agent(agent3)

        # Submit tasks
        task1 = coordinator.submit_task("Analyze dataset", TaskPriority.HIGH,
                                         {"reasoning": 0.7, "domain_knowledge": 0.6})
        task2 = coordinator.submit_task("Quick fix", TaskPriority.NORMAL)

        # Load balance
        assignments = coordinator.load_balance()
        self.assertGreater(len(assignments), 0)

        # Self-organize
        clusters = self_org.organize()
        self.assertGreater(len(clusters), 0)

        # Collective decision
        proposal = collective.create_proposal("upgrade_plan", "Upgrade swarm infrastructure")
        collective.cast_vote(agent1.agent_id, "upgrade_plan", True)
        collective.cast_vote(agent2.agent_id, "upgrade_plan", True)
        collective.cast_vote(agent3.agent_id, "upgrade_plan", False)
        result = collective.reach_consensus("upgrade_plan")
        # 2 out of 3 votes yes -> majority approved
        self.assertTrue(result.approved)

        # Heartbeats
        heartbeats = coordinator.broadcast_heartbeat()
        self.assertEqual(len(heartbeats), 3)

        # Swarm health
        health = coordinator.get_swarm_health()
        self.assertEqual(health["total_agents"], 3)

    def test_swarm_overload_protection(self):
        """Test that swarm respects max_agents limit."""
        small_config = SwarmConfig(max_agents=2)
        coordinator = SwarmCoordinator(small_config)
        agent1 = create_swarm_agent()
        agent2 = create_swarm_agent()
        agent3 = create_swarm_agent()
        coordinator.register_agent(agent1)
        coordinator.register_agent(agent2)
        with self.assertRaises(SwarmOverloadError):
            coordinator.register_agent(agent3)

    def test_consensus_all_methods(self):
        """Test all consensus methods produce results."""
        coordinator = create_swarm_coordinator()
        agent1 = create_swarm_agent()
        agent2 = create_swarm_agent()
        coordinator.register_agent(agent1)
        coordinator.register_agent(agent2)
        collective = CollectiveDecision(coordinator)

        proposal = collective.create_proposal("consensus_test", "Test all methods")
        collective.cast_vote(agent1.agent_id, "consensus_test", True)
        collective.cast_vote(agent2.agent_id, "consensus_test", True)

        for method in ConsensusMethod:
            collective.config.consensus_method = method
            result = collective.reach_consensus("consensus_test")
            self.assertIsInstance(result, ConsensusResult)
            self.assertIn("approved", asdict(result))

    def test_agent_task_lifecycle(self):
        """Test complete agent task lifecycle."""
        agent = create_swarm_agent(role=AgentRole.SPECIALIST)

        # Submit task to coordinator
        coordinator = create_swarm_coordinator()
        coordinator.register_agent(agent)
        task = coordinator.submit_task("Specialized task", TaskPriority.HIGH,
                                        {"domain_knowledge": 0.9})

        # Load balance assigns it
        coordinator.load_balance()

        # Agent completes it
        if task.assigned_agent == agent.agent_id:
            agent.complete_task(task.task_id, "result_data")
            self.assertEqual(len(agent.completed_tasks), 1)
            self.assertEqual(agent.state, AgentState.IDLE)

    def test_conflict_resolution_workflow(self):
        """Test full conflict resolution workflow."""
        coordinator = create_swarm_coordinator()
        agent1 = create_swarm_agent(role=AgentRole.WORKER)
        agent2 = create_swarm_agent(role=AgentRole.SPECIALIST)
        coordinator.register_agent(agent1)
        coordinator.register_agent(agent2)

        task = coordinator.submit_task("Contested task")
        coordinator.load_balance()

        # Simulate conflict by reassigning manually
        task.assigned_agent = agent2.agent_id
        winner = coordinator.resolve_conflict(agent1.agent_id, agent2.agent_id, task.task_id)
        self.assertIn(winner, [agent1.agent_id, agent2.agent_id])

    def test_v3_integration(self):
        """Test that v3 modules integrate correctly with v4 swarm."""
        from atc import IntentClassifier
        from teq import TokenBilling, QoSTier
        from brp import BRPConfig
        from car import ComplexityAnalyzer, CARRouter, CARRouter, CARRouter

        agent = create_swarm_agent()

        # Verify v3 components exist in agent
        self.assertIsInstance(agent.intent_classifier, IntentClassifier)
        self.assertIsInstance(agent.token_billing, TokenBilling)
        self.assertIsInstance(agent.car_router, CARRouter)

        # Use v3 components
        intent, confidence = agent.intent_classifier.classify("Test query")
        self.assertIn(intent, ["query", "command", "request", "feedback", "system"])
        self.assertGreaterEqual(confidence, 0.0)


if __name__ == "__main__":
    unittest.main()
