"""TokenRelay v4 — Swarm Intelligence Dashboard.

Real-time agent visualization: topology, state tracking, consensus rendering.
Uses Python standard library only (stdlib).
"""
import json
import time
from collections import deque, defaultdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from swarm import SwarmAgent, SwarmCoordinator, AgentState, AgentRole, SwarmTask, Vote, ConsensusResult


class SwarmTopology:
    """Visual representation of agent connections, states, and task distribution."""

    def __init__(self, coordinator: SwarmCoordinator):
        self.coordinator = coordinator
        self._snapshots: deque = deque(maxlen=100)
        self._topology_history: deque = deque(maxlen=50)

    def capture_topology(self) -> Dict[str, Any]:
        """Capture current swarm topology snapshot."""
        agents_info = []
        for agent_id, agent in self.coordinator.agents.items():
            cap_vec = getattr(agent, 'capability_vector', None)
            capabilities = list(cap_vec.capabilities) if cap_vec else []
            agents_info.append({
                "agent_id": agent_id,
                "role": agent.role.value if hasattr(agent.role, 'value') else str(agent.role),
                "state": agent.state.value if hasattr(agent.state, 'value') else str(agent.state),
                "capabilities": capabilities,
                "task_count": agent.task_count,
                "success_rate": agent.success_rate,
                "heartbeat": agent.heartbeat_count if hasattr(agent, "heartbeat_count") else 0,
                "is_available": agent.is_available if hasattr(agent, "is_available") else True,
            })

        topology = {
            "timestamp": datetime.now().isoformat(),
            "total_agents": len(self.coordinator.agents),
            "active_agents": len(self.coordinator.get_active_agents()),
            "leader": self.coordinator.elect_leader().agent_id if self.coordinator.elect_leader() and self.coordinator.elect_leader() is not None else None,
            "agents": agents_info,
            "connections": self._build_connections(),
            "task_distribution": self._get_task_distribution(),
            "metrics": self.coordinator.get_metrics(),
        }
        self._snapshots.append(topology)
        return topology

    def _build_connections(self) -> List[Dict[str, str]]:
        """Build agent-to-agent connection map."""
        connections = []
        agents = list(self.coordinator.agents.keys())
        for i, agent_id in enumerate(agents):
            for j, target_id in enumerate(agents):
                if i != j:
                    connections.append({"from": agent_id, "to": target_id, "type": "peer"})
        return connections

    def _get_task_distribution(self) -> Dict[str, int]:
        """Get task distribution across priority levels."""
        distribution = defaultdict(int)
        for agent_id, agent in self.coordinator.agents.items():
            distribution[agent_id] = agent.task_count
        return dict(distribution)

    def export_json(self) -> str:
        """Export topology as JSON string for dashboard rendering."""
        topology = self.capture_topology()
        return json.dumps(topology, indent=2, default=str)

    def get_agent_states(self) -> Dict[str, str]:
        """Get mapping of agent_id -> state string."""
        states = {}
        for agent_id, agent in self.coordinator.agents.items():
            states[agent_id] = agent.state.value if hasattr(agent.state, 'value') else str(agent.state)
        return states


class AgentStateTracker:
    """Tracks all agent states (IDLE/BUSY/OFFLINE/ERROR) with timestamps."""

    def __init__(self):
        self._state_log: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self._current_states: Dict[str, AgentState] = {}
        self._state_history: deque = deque(maxlen=1000)

    def track_state(self, agent_id: str, state: AgentState):
        """Record a state change for an agent."""
        entry = {
            "agent_id": agent_id,
            "state": state.value if hasattr(state, 'value') else str(state),
            "timestamp": datetime.now().isoformat(),
            "unix_time": time.time(),
        }
        self._state_log[agent_id].append(entry)
        self._current_states[agent_id] = state
        self._state_history.append(entry)

    def get_state(self, agent_id: str) -> Optional[AgentState]:
        """Get current state of an agent."""
        return self._current_states.get(agent_id)

    def get_state_history(self, agent_id: str) -> List[Dict[str, Any]]:
        """Get full state history for an agent."""
        return self._state_log.get(agent_id, [])

    def get_all_states(self) -> Dict[str, str]:
        """Get all current agent states."""
        return {
            agent_id: state.value if hasattr(state, 'value') else str(state)
            for agent_id, state in self._current_states.items()
        }

    def get_agents_by_state(self, state: AgentState) -> List[str]:
        """Get all agents currently in a given state."""
        return [
            agent_id for agent_id, s in self._current_states.items()
            if s == state
        ]

    def get_summary(self) -> Dict[str, Any]:
        """Get summary of all agent states."""
        summary = defaultdict(int)
        for state in self._current_states.values():
            summary[state.value if hasattr(state, 'value') else str(state)] += 1
        return {
            "total_agents": len(self._current_states),
            "state_breakdown": dict(summary),
            "timestamp": datetime.now().isoformat(),
        }

    def export_json(self) -> str:
        """Export state tracking data as JSON."""
        data = {
            "summary": self.get_summary(),
            "history_entries": list(self._state_history)[-50:],
            "per_agent": {
                agent_id: self.get_state_history(agent_id)
                for agent_id in self._current_states
            },
        }
        return json.dumps(data, indent=2, default=str)


class ConsensusVisualizer:
    """Renders voting-based decisions and leader election."""

    def __init__(self, coordinator: SwarmCoordinator):
        self.coordinator = coordinator
        self._consensus_history: deque = deque(maxlen=100)

    def visualize_vote(self, proposal: Dict[str, Any], votes: Dict[str, bool]) -> Dict[str, Any]:
        """Visualize a voting round."""
        total_votes = len(votes)
        yes_votes = sum(1 for v in votes.values() if v)
        no_votes = total_votes - yes_votes
        consensus = yes_votes > no_votes

        result = {
            "proposal_id": proposal.get("id", "unknown"),
            "proposal": proposal,
            "total_votes": total_votes,
            "yes_votes": yes_votes,
            "no_votes": no_votes,
            "consensus_reached": consensus,
            "vote_breakdown": votes,
            "timestamp": datetime.now().isoformat(),
        }
        self._consensus_history.append(result)
        return result

    def visualize_leader_election(self, leader_id: str, candidates: List[str]) -> Dict[str, Any]:
        """Visualize leader election result."""
        result = {
            "leader_id": leader_id,
            "candidates": candidates,
            "election_timestamp": datetime.now().isoformat(),
            "total_candidates": len(candidates),
            "leader_elected": leader_id in candidates,
        }
        self._consensus_history.append(result)
        return result

    def get_consensus_history(self) -> List[Dict[str, Any]]:
        """Get consensus visualization history."""
        return list(self._consensus_history)

    def export_json(self) -> str:
        """Export consensus visualization data as JSON."""
        data = {
            "history": list(self._consensus_history),
            "total_events": len(self._consensus_history),
            "latest": self._consensus_history[-1] if self._consensus_history else None,
        }
        return json.dumps(data, indent=2, default=str)

    def get_topology_for_dashboard(self, swarm_topology: SwarmTopology) -> Dict[str, Any]:
        """Get combined dashboard data for frontend rendering."""
        return {
            "topology": swarm_topology.capture_topology(),
            "state_summary": AgentStateTracker().get_summary(),
            "consensus": self._consensus_history[-5:] if self._consensus_history else [],
            "timestamp": datetime.now().isoformat(),
        }


class SwarmDashboard:
    """Main dashboard aggregator — combines topology, state tracking, and consensus."""

    def __init__(self, coordinator: SwarmCoordinator):
        self.coordinator = coordinator
        self.topology = SwarmTopology(coordinator)
        self.state_tracker = AgentStateTracker()
        self.consensus_viz = ConsensusVisualizer(coordinator)

    def update(self) -> Dict[str, Any]:
        """Update all dashboard components and return consolidated data."""
        # Update state tracker from coordinator agents
        for agent_id, agent in self.coordinator.agents.items():
            self.state_tracker.track_state(agent_id, agent.state)

        topology = self.topology.capture_topology()
        summary = self.state_tracker.get_summary()

        dashboard_data = {
            "topology": topology,
            "state_summary": summary,
            "agent_states": self.state_tracker.get_all_states(),
            "consensus_events": self.consensus_viz.get_consensus_history()[-5:],
            "timestamp": datetime.now().isoformat(),
        }
        return dashboard_data

    def export_json(self) -> str:
        """Export full dashboard data as JSON for benchmark.html."""
        self.update()
        data = {
            "topology": self.topology.export_json(),
            "state_tracking": self.state_tracker.export_json(),
            "consensus": self.consensus_viz.export_json(),
            "dashboard_summary": self.update(),
        }
        return json.dumps(data, indent=2, default=str)
