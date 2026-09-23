"""TokenRelay v4 — Swarm Dashboard Tests.

Tests for src/swarm_dashboard.py:
- SwarmTopology: visual representation, JSON export
- AgentStateTracker: state tracking, history, summary
- ConsensusVisualizer: voting, leader election
- SwarmDashboard: integrated dashboard
"""
import sys
import json
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from swarm import SwarmAgent, SwarmCoordinator, AgentState, AgentRole, SwarmConfig, SwarmTask, TaskPriority
from swarm_dashboard import SwarmTopology, AgentStateTracker, ConsensusVisualizer, SwarmDashboard


# ═══════════════════════════════════════════
# SWARM TOPOLOGY TESTS
# ═══════════════════════════════════════════

class TestSwarmTopology(unittest.TestCase):
    def setUp(self):
        self.coord = SwarmCoordinator(config=SwarmConfig(max_agents=10))
        self.topology = SwarmTopology(self.coord)

    def test_init(self):
        self.assertIsInstance(self.topology, SwarmTopology)

    def test_capture_topology_empty(self):
        result = self.topology.capture_topology()
        self.assertEqual(result["total_agents"], 0)
        self.assertEqual(result["active_agents"], 0)
        self.assertIsNone(result["leader"])
        self.assertEqual(len(result["agents"]), 0)
        self.assertIn("timestamp", result)

    def test_capture_topology_with_agents(self):
        for i in range(3):
            agent = SwarmAgent(agent_id=f"test-agent-{i}", role=AgentRole.WORKER, capabilities=["general"])
            self.coord.register_agent(agent)
        result = self.topology.capture_topology()
        self.assertEqual(result["total_agents"], 3)
        self.assertEqual(len(result["agents"]), 3)
        self.assertIn("connections", result)
        self.assertGreater(len(result["connections"]), 0)

    def test_agent_info_in_topology(self):
        agent = SwarmAgent(agent_id="a1", role=AgentRole.LEADER, capabilities=["task"])
        self.coord.register_agent(agent)
        result = self.topology.capture_topology()
        agent_info = next((a for a in result["agents"] if a and a["agent_id"] == "a1"), None)
        self.assertIsNotNone(agent_info)
        self.assertEqual(agent_info["role"], "leader")
        self.assertIsNotNone(agent_info.get("heartbeat"))

    def test_export_json(self):
        agent = SwarmAgent(agent_id="a1", role=AgentRole.WORKER)
        self.coord.register_agent(agent)
        json_str = self.topology.export_json()
        self.assertIsInstance(json_str, str)
        data = json.loads(json_str)
        self.assertIn("total_agents", data)

    def test_get_agent_states(self):
        agent1 = SwarmAgent(agent_id="a1", role=AgentRole.WORKER)
        agent2 = SwarmAgent(agent_id="a2", role=AgentRole.LEADER)
        self.coord.register_agent(agent1)
        self.coord.register_agent(agent2)
        states = self.topology.get_agent_states()
        self.assertIn("a1", states)
        self.assertIn("a2", states)

    def test_connections_built(self):
        for i in range(3):
            a = SwarmAgent(agent_id=f"a{i}", role=AgentRole.WORKER)
            self.coord.register_agent(a)
        result = self.topology.capture_topology()
        # Each agent connects to every other: n*(n-1) connections
        self.assertEqual(len(result["connections"]), 3 * 2)

    def test_task_distribution(self):
        agent = SwarmAgent(agent_id="a1", role=AgentRole.WORKER)
        self.coord.register_agent(agent)
        result = self.topology.capture_topology()
        self.assertIn("task_distribution", result)


# ═══════════════════════════════════════════
# AGENT STATE TRACKER TESTS
# ═══════════════════════════════════════════

class TestAgentStateTracker(unittest.TestCase):
    def setUp(self):
        self.tracker = AgentStateTracker()

    def test_init(self):
        self.assertIsInstance(self.tracker, AgentStateTracker)

    def test_track_state(self):
        self.tracker.track_state("agent-1", AgentState.IDLE)
        self.assertEqual(self.tracker.get_state("agent-1"), AgentState.IDLE)

    def test_track_state_change(self):
        self.tracker.track_state("agent-1", AgentState.IDLE)
        self.tracker.track_state("agent-1", AgentState.BUSY)
        self.assertEqual(self.tracker.get_state("agent-1"), AgentState.BUSY)

    def test_get_state_unknown(self):
        self.assertIsNone(self.tracker.get_state("unknown"))

    def test_get_all_states(self):
        self.tracker.track_state("a1", AgentState.IDLE)
        self.tracker.track_state("a2", AgentState.BUSY)
        states = self.tracker.get_all_states()
        self.assertEqual(states["a1"], "idle")
        self.assertEqual(states["a2"], "busy")

    def test_get_agents_by_state(self):
        self.tracker.track_state("a1", AgentState.IDLE)
        self.tracker.track_state("a2", AgentState.IDLE)
        self.tracker.track_state("a3", AgentState.BUSY)
        idle = self.tracker.get_agents_by_state(AgentState.IDLE)
        self.assertEqual(len(idle), 2)
        self.assertIn("a1", idle)
        self.assertIn("a2", idle)

    def test_get_summary(self):
        self.tracker.track_state("a1", AgentState.IDLE)
        self.tracker.track_state("a2", AgentState.BUSY)
        self.tracker.track_state("a3", AgentState.IDLE)
        summary = self.tracker.get_summary()
        self.assertEqual(summary["total_agents"], 3)
        self.assertEqual(summary["state_breakdown"]["idle"], 2)
        self.assertEqual(summary["state_breakdown"]["busy"], 1)

    def test_state_history(self):
        self.tracker.track_state("a1", AgentState.IDLE)
        time.sleep(0.01)
        self.tracker.track_state("a1", AgentState.BUSY)
        history = self.tracker.get_state_history("a1")
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["state"], "idle")
        self.assertEqual(history[1]["state"], "busy")

    def test_export_json(self):
        self.tracker.track_state("a1", AgentState.IDLE)
        json_str = self.tracker.export_json()
        self.assertIsInstance(json_str, str)
        data = json.loads(json_str)
        self.assertIn("summary", data)
        self.assertIn("history_entries", data)


# ═══════════════════════════════════════════
# CONSENSUS VISUALIZER TESTS
# ═══════════════════════════════════════════

class TestConsensusVisualizer(unittest.TestCase):
    def setUp(self):
        self.coord = SwarmCoordinator(config=SwarmConfig(max_agents=5))
        self.visualizer = ConsensusVisualizer(self.coord)

    def test_init(self):
        self.assertIsInstance(self.visualizer, ConsensusVisualizer)

    def test_visualize_vote(self):
        votes = {"a": True, "b": True, "c": False}
        result = self.visualizer.visualize_vote({"id": "p1"}, votes)
        self.assertEqual(result["total_votes"], 3)
        self.assertEqual(result["yes_votes"], 2)
        self.assertEqual(result["no_votes"], 1)
        self.assertTrue(result["consensus_reached"])
        self.assertIn("proposal_id", result)
        self.assertIn("timestamp", result)

    def test_visualize_vote_no_consensus(self):
        votes = {"a": True, "b": False}
        result = self.visualizer.visualize_vote({"id": "p2"}, votes)
        self.assertFalse(result["consensus_reached"])

    def test_visualize_leader_election(self):
        result = self.visualizer.visualize_leader_election("a1", ["a1", "a2", "a3"])
        self.assertEqual(result["leader_id"], "a1")
        self.assertEqual(result["total_candidates"], 3)
        self.assertTrue(result["leader_elected"])

    def test_consensus_history(self):
        self.visualizer.visualize_vote({"id": "p1"}, {"a": True})
        self.visualizer.visualize_vote({"id": "p2"}, {"a": False, "b": False})
        history = self.visualizer.get_consensus_history()
        self.assertEqual(len(history), 2)

    def test_export_json(self):
        self.visualizer.visualize_vote({"id": "p1"}, {"a": True})
        json_str = self.visualizer.export_json()
        self.assertIsInstance(json_str, str)
        data = json.loads(json_str)
        self.assertIn("history", data)
        self.assertIn("total_events", data)

    def test_get_topology_for_dashboard(self):
        topology = SwarmTopology(self.coord)
        combined = self.visualizer.get_topology_for_dashboard(topology)
        self.assertIn("topology", combined)
        self.assertIn("state_summary", combined)
        self.assertIn("timestamp", combined)


# ═══════════════════════════════════════════
# SWARM DASHBOARD TESTS
# ═══════════════════════════════════════════

class TestSwarmDashboard(unittest.TestCase):
    def setUp(self):
        self.coord = SwarmCoordinator(config=SwarmConfig(max_agents=10))
        self.dashboard = SwarmDashboard(self.coord)

    def test_init(self):
        self.assertIsInstance(self.dashboard, SwarmDashboard)
        self.assertIsInstance(self.dashboard.topology, SwarmTopology)
        self.assertIsInstance(self.dashboard.state_tracker, AgentStateTracker)
        self.assertIsInstance(self.dashboard.consensus_viz, ConsensusVisualizer)

    def test_update(self):
        agent = SwarmAgent(agent_id="a1", role=AgentRole.WORKER)
        self.coord.register_agent(agent)
        data = self.dashboard.update()
        self.assertIn("topology", data)
        self.assertIn("state_summary", data)
        self.assertIn("agent_states", data)
        self.assertIn("timestamp", data)

    def test_update_populates_state_tracker(self):
        agent = SwarmAgent(agent_id="a1", role=AgentRole.WORKER)
        self.coord.register_agent(agent)
        self.dashboard.update()
        states = self.dashboard.state_tracker.get_all_states()
        self.assertIn("a1", states)

    def test_export_json(self):
        agent = SwarmAgent(agent_id="a1", role=AgentRole.WORKER)
        self.coord.register_agent(agent)
        json_str = self.dashboard.export_json()
        self.assertIsInstance(json_str, str)
        data = json.loads(json_str)
        self.assertIn("topology", data)
        self.assertIn("state_tracking", data)
        self.assertIn("consensus", data)
        self.assertIn("dashboard_summary", data)

    def test_multiple_updates(self):
        for i in range(3):
            agent = SwarmAgent(agent_id=f"a{i}", role=AgentRole.WORKER)
            self.coord.register_agent(agent)
        for _ in range(3):
            self.dashboard.update()
        # Should have 3 snapshots in topology
        json_str = self.dashboard.export_json()
        data = json.loads(json_str)
        # Verify structure is valid
        self.assertIsInstance(data["dashboard_summary"], dict)


# ═══════════════════════════════════════════
# RUN ALL TESTS
# ═══════════════════════════════════════════

if __name__ == "__main__":
    unittest.main()
