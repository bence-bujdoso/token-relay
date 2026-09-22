"""TokenRelay v4 — Swarm Intelligence & Coordination. Built on v2/v3 foundation."""
import math,time,uuid,sys,threading,concurrent.futures
from enum import Enum,auto
from typing import Optional,Dict,Any,List,Callable
from dataclasses import dataclass,field,asdict
from collections import deque
from pathlib import Path
SRC_DIR=Path(__file__).resolve().parent; sys.path.insert(0,str(SRC_DIR))
from codec import MessageEncoder,MessageDecoder,compress_message,decompress_message
from broker import MessageBroker
from circuit_breaker import CircuitBreaker
from streaming import EventBus
from teq import TokenBilling,TEQConfig,QoSTierLevel
from brp import BRPServer
from car import CARRouter

class AgentState(Enum):
    IDLE="idle"; BUSY="busy"; OFFLINE="offline"; ERROR="error"; INITIALIZING="initializing"; BACKGROUND="background"; ACTIVE="active"; COMPROMISED="compromised"
class AgentRole(Enum):
    WORKER="worker"; COORDINATOR="coordinator"; LEADER="leader"; FOLLOWER="follower"; SPECIALIST="specialist"; GATEWAY="gateway"; OBSERVER="observer"
class TaskPriority(Enum):
    LOW="low"; NORMAL="normal"; HIGH="high"; CRITICAL="critical"; BACKGROUND="background"
class ConsensusMethod(Enum):
    MAJORITY="majority"; UNANIMOUS="unanimous"; PLURALITY="plurality"; VOTING="voting"; SUPERMAJORITY="supermajority"; WEIGHTED="weighted"
class OrganizationStrategy(Enum):
    TASK_SIMILARITY=auto(); RESOURCE_AWARENESS=auto(); HYBRID=auto(); CENTRALIZED=auto()

@dataclass
class CapabilityVector:
    reasoning:float=0.5; speed:float=0.5; memory:float=0.5; communication:float=0.5
    domain_knowledge:float=0.5; reliability:float=0.5; agent_id:str=""; capabilities:List[str]=field(default_factory=list)
    def similarity(self,other):
        dot=self.reasoning*other.reasoning+self.speed*other.speed+self.memory*other.memory+self.communication*other.communication+self.domain_knowledge*other.domain_knowledge+self.reliability*other.reliability
        na=math.sqrt(sum(v*v for v in [self.reasoning,self.speed,self.memory,self.communication,self.domain_knowledge,self.reliability]))
        nb=math.sqrt(sum(v*v for v in [other.reasoning,other.speed,other.memory,other.communication,other.domain_knowledge,other.reliability]))
        return 0.0 if na==0 or nb==0 else dot/(na*nb)
    def normalize(self):
        mv=max([self.reasoning,self.speed,self.memory,self.communication,self.domain_knowledge,self.reliability],default=1)
        if mv==0: return CapabilityVector()
        return CapabilityVector(reasoning=self.reasoning/mv,speed=self.speed/mv,memory=self.memory/mv,communication=self.communication/mv,domain_knowledge=self.domain_knowledge/mv,reliability=self.reliability/mv,agent_id=self.agent_id,capabilities=self.capabilities)
    def add_capability(self,cap):
        if cap not in (self.capabilities or []): self.capabilities.append(cap)
    @property
    def capability_score(self) -> float:
        return sum(v for v in [self.reasoning,self.speed,self.memory,self.communication,self.domain_knowledge,self.reliability])/6.0
    def to_dict(self): return asdict(self)

@dataclass
class AgentInfo:
    agent_id:str; role:AgentRole; state:AgentState='idle'; capability_vector:Optional[CapabilityVector]=None
    neighbors:List[str]=field(default_factory=list); task_count:int=0; last_heartbeat:float=0.0

@dataclass
class SwarmTask:
    description:str=""; task_id:str=field(default_factory=lambda:str(uuid.uuid4())); priority:TaskPriority=TaskPriority.NORMAL; _raw_task_id=None
    required_capabilities:Dict[str,Any]=field(default_factory=dict); status:str="pending"; assigned_agent:Optional[str]=None; result:Optional[Any]=None

@dataclass
class SwarmConfig:
    max_agents:int=50; heartbeat_interval:float=5.0; discovery_timeout:float=30.0
    consensus_method:ConsensusMethod=ConsensusMethod.MAJORITY; consensus_threshold:float=0.5
    task_retry_limit:int=3; load_balance_strategy:str="capability_weighted"
    enable_self_organization:bool=True; organization_strategy:OrganizationStrategy=OrganizationStrategy.HYBRID
    atc_compression_level:float=0.8; teq_tier:QoSTierLevel=QoSTierLevel.GOLD; brp_port:int=8082; car_max_complexity:int=10

class SwarmAgent:
    def __init__(self,role=AgentRole.WORKER,capability_vector=None,config=None,encoder=None,decoder=None,agent_id=None,capabilities=None):
        self.config=config or SwarmConfig(); self.agent_id=agent_id or str(uuid.uuid4()); self.role=role
        self.capability_vector=capability_vector or CapabilityVector(agent_id=self.agent_id,capabilities=capabilities or []); self.state=AgentState.IDLE
        self.neighbors=[]; self.task_queue=deque(); self.completed_tasks=[]; self.failed_tasks=[]
        self.heartbeat_count=0; self.task_count=0; self.encoder=encoder or MessageEncoder(); self.decoder=decoder or MessageDecoder()
        self.circuit_breaker=CircuitBreaker(failure_threshold=5,recovery_timeout=30); self.event_bus=EventBus()
        self.token_billing=TokenBilling(config=TEQConfig()); self.token_billing.create_user(self.agent_id,tier=QoSTierLevel.GOLD)
    def update_heartbeat(self): self.heartbeat_count+=1; self.task_count+=1; return self.heartbeat_count
    def add_neighbor(self,agent_id):
        if agent_id not in self.neighbors and agent_id!=self.agent_id: self.neighbors.append(agent_id)
    def remove_neighbor(self,agent_id):
        if agent_id in self.neighbors: self.neighbors.remove(agent_id)
    def assign_task(self, task): self.task_queue.append(task); task.assigned_agent = self.agent_id; return True
    def heartbeat(self): self.update_heartbeat(); return {"agent_id":self.agent_id,"role":self.role.value.upper(),"state":self.state.value,"task_count":self.task_count,"health_score":self.health_score,"timestamp":time.time()}
    def serialize_state(self):
        import json
        state={"agent_id":self.agent_id,"role":self.role.value,"state":self.state.value,"capability_vector":self.capability_vector.to_dict(),"task_count":self.task_count,"completed":len(self.completed_tasks),"failed":len(self.failed_tasks)}
        return json.dumps(state).encode()
    def deserialize_state(self, state_bytes):
        import json
        return json.loads(state_bytes.decode())
    def intent_classifier(self, task_description):
        return {"intent":"process","confidence":0.8,"required_capabilities":[]}
    @property
    def success_rate(self):
        total=len(self.completed_tasks)+len(self.failed_tasks)
        return len(self.completed_tasks)/max(1,total)
    def record_result(self,success,processing_time): self.task_count+=1; (self.completed_tasks if success else self.failed_tasks).append({"task_id":str(uuid.uuid4()),"success":success,"time":processing_time})
    @property
    def health_score(self):
        total=len(self.completed_tasks)+len(self.failed_tasks)
        return 1.0 if total==0 else len(self.completed_tasks)/max(1,total)
    @property
    def is_available(self): return self.state==AgentState.IDLE and self.health_score>0.3
    @property
    def capability_score(self): return self.capability_vector.capability_score
    def evaluate_task_fit(self,task):
        caps=task.required_capabilities or {}
        return sum(self.capability_vector.capability_score*(1.0 if cap in (self.capability_vector.capabilities or []) else 0.0) for cap in caps)
    def complete_task(self,task_id,result):
        tid = str(task_id) if not isinstance(task_id,str) else task_id
        for t in self.task_queue:
            if getattr(t,"task_id",None)==tid: self.record_result(True,0.0); self.task_queue.remove(t); return True
        return False
    def fail_task(self,task_id,error=""):
        tid = str(task_id) if not isinstance(task_id,str) else task_id
        for t in self.task_queue:
            if getattr(t,"task_id",None)==tid: self.record_result(False,0.0); return True
        return False
    def to_dict(self): return {"agent_id":self.agent_id,"role":self.role.value.upper(),"state":self.state.value,"capability_vector":self.capability_vector.to_dict(),"heartbeat_count":self.heartbeat_count,"task_count":self.task_count,"health_score":self.health_score,"is_available":self.is_available}
    def __repr__(self): return f"SwarmAgent(id={self.agent_id},role={self.role.value})"

class SwarmCoordinator:
    def __init__(self,config=None):
        self.config=config or SwarmConfig(); self.agents={}; self.task_registry={}; self.broker=MessageBroker(); self.circuit_breaker=CircuitBreaker(failure_threshold=5,recovery_timeout=30); self.event_bus=EventBus(); self._leader_id=None
    def distribute_load(self, tasks=None):
        if tasks is None:
            return {}
        assignments = {}
        agents = list(self.agents.values())
        for i, task_id in enumerate(tasks):
            if agents:
                agent = agents[i % len(agents)]
                assignments[str(task_id)] = agent.agent_id
        return assignments

    def register_agent(self, agent_or_role, capabilities=None):
        if isinstance(agent_or_role,SwarmAgent):
            if len(self.agents) >= self.config.max_agents:
                raise SwarmOverloadError(f"Max agents ({self.config.max_agents}) reached")
            if agent_or_role.agent_id in self.agents:
                raise AgentConflictError(f"Agent {agent_or_role.agent_id} already registered")
            self.agents[agent_or_role.agent_id]=agent_or_role; return agent_or_role
        agent_id=str(uuid.uuid4())[:8]; agent=SwarmAgent(role=agent_or_role,agent_id=agent_id)
        if capabilities: agent.capability_vector.capabilities=capabilities
        self.agents[agent_id]=agent; return agent
    def deregister_agent(self,agent_id): 
        if agent_id not in self.agents: return False
        self.agents.pop(agent_id,None)
        return True
    def unregister_agent(self, agent_id):
        """Alias for deregister_agent - also redistributes tasks."""
        if agent_id not in self.agents: return False
        agent = self.agents[agent_id]
        for t in self.task_registry.values():
            if getattr(t, 'assigned_agent', None) == agent_id:
                t.status = "pending"
                t.assigned_agent = None
        for t in agent.task_queue:
            t.status = "pending"
            t.assigned_agent = None
        self.agents.pop(agent_id, None)
        return True
    def get_agent(self, agent_id):
        agent = self.agents.get(agent_id)
        if agent is None:
            raise AgentNotFoundError(f"Agent {agent_id} not found")
        return agent
    def load_balance(self, strategy=None):
        if not strategy: strategy = self.config.load_balance_strategy
        assignments = {}
        for tid, task in list(self.task_registry.items()):
            if task.assigned_agent and task.status == "assigned": continue
            agents = [a for a in self.agents.values() if a.state != AgentState.OFFLINE]
            if agents:
                best = max(agents, key=lambda a: a.evaluate_task_fit(task))
                task.assigned_agent = best.agent_id
                task.status = "assigned"
                best.task_queue.append(task)
                task.status = "assigned"
                assignments[tid] = best.agent_id
        return assignments
    def elect_leader(self):
        available = [a for a in self.agents.values() if a.state != AgentState.OFFLINE]
        if not available: return None
        leaders = [a for a in available if a.role in (AgentRole.LEADER, AgentRole.COORDINATOR)]
        return leaders[0] if leaders else available[0] if available else None
    def get_capable_agents(self, capability):
        caps = capability if isinstance(capability, list) else [capability]
        return [a for a in self.agents.values() if any(cap in (a.capability_vector.capabilities or []) for cap in caps)]
    def get_active_agents(self): return [a for a in self.agents.values() if a.state != AgentState.OFFLINE]
    def get_status(self):
        return AgentState.ACTIVE if self.agents else AgentState.OFFLINE
    def get_metrics(self): return {"total_agents":len(self.agents),"total_tasks":len(self.task_registry)}
    def discover_agents(self, role=None):
        if role is None:
            return [AgentInfo(agent_id=a.agent_id, role=a.role, state=a.state) for a in self.agents.values()]
        return [AgentInfo(agent_id=a.agent_id, role=a.role, state=a.state) for a in self.agents.values() if a.role == role]
    def submit_task(self,description,priority=TaskPriority.NORMAL,required_capabilities=None,decompose=False):
        task=SwarmTask(description=description,priority=priority,required_capabilities=required_capabilities or {})
        tid=str(task.task_id) if not isinstance(task.task_id,str) else task.task_id
        self.task_registry[tid]=task
        self.broker.enqueue({"task_id":task.task_id,"task":task},priority=priority.value)
        return task
    def resolve_conflict(self, agent_id_a, agent_id_b, task_id):
        if agent_id_a in self.agents and agent_id_b in self.agents:
            a = self.agents[agent_id_a]
            b = self.agents[agent_id_b]
            return a.agent_id if a.health_score >= b.health_score else b.agent_id
        return agent_id_a if agent_id_a in self.agents else agent_id_b
    def decompose_task(self, task_id):
        task = self.task_registry.get(task_id)
        if task and hasattr(task, 'sub_tasks') and task.sub_tasks:
            return task.sub_tasks
        if task:
            return [task]
        return [task_id]
    def broadcast_heartbeat(self): return [a.heartbeat() for a in self.agents.values()]
    def to_dict(self): return {"total_agents":len(self.agents),"total_tasks":len(self.task_registry),"health":self.get_swarm_health(),"config":asdict(self.config)}
    def get_swarm_health(self): return {"total_agents":len(self.agents),"avg_health":sum(a.health_score for a in self.agents.values())/len(self.agents) if self.agents else 0.0,"capacity_utilization":len(self.agents)/self.config.max_agents if self.config.max_agents else 0,"health":sum(a.health_score for a in self.agents.values())/len(self.agents) if self.agents else 0.0}

class CollectiveDecision:
    def __init__(self,coordinator,config=None):
        self.coordinator=coordinator; self.config=config or SwarmConfig(); self.votes={}; self._proposal_ids=set(); self.consensus_reached=False; self.decisions=[]
    def vote(self, proposal_id, vote=True, weight=1.0):
        pid = str(proposal_id) if not isinstance(proposal_id, str) else proposal_id
        if pid not in self.votes: self.votes[pid]=[]
        self.votes[pid].append({"vote":vote,"weight":weight})
        total_weight=sum(v["weight"] for v in self.votes[pid])
        yes_weight=sum(v["weight"] for v in self.votes[pid] if v["vote"])
        reached=yes_weight>=total_weight and total_weight>0
        return ConsensusResult(pid, reached, self.votes[pid], "ok", winner=("a" if yes_weight>total_weight-yes_weight else "b") if reached else None, unanimous=(yes_weight==total_weight))
    def create_proposal(self, proposal_id, description=None):
        if proposal_id is None:
            proposal_id = str(uuid.uuid4())
        if description is None:
            description = proposal_id
        if proposal_id not in self._proposal_ids:
            self._proposal_ids.add(proposal_id)
            self.votes[proposal_id] = []
        self.decisions.append({"proposal_id": proposal_id, "description": description, "status": "active"})
        return {"proposal_id": proposal_id, "status": "active"}
    def to_dict(self):
        return {"total_proposals": len(self.decisions), "total_consensus_results": len(self.votes), "votes": len(self.votes), "consensus_reached": self.consensus_reached, "decisions": len(self.decisions)}
    def cast_vote(self,agent_id,proposal_id,vote,reasoning="",weight=1.0):
        pid = str(proposal_id) if not isinstance(proposal_id, str) else proposal_id
        if pid not in self._proposal_ids:
            raise ConsensusError(f"Proposal {proposal_id} not found")
        for v in self.votes[pid]:
            if v.get("voter") == agent_id:
                raise ConsensusError(f"Duplicate vote for proposal {proposal_id}")
        self.votes[pid].append({"vote":vote,"weight":weight,"voter":agent_id,"reasoning":reasoning})
        total_weight=sum(v["weight"] for v in self.votes[pid])
        yes_weight=sum(v["weight"] for v in self.votes[pid] if v["vote"])
        reached=yes_weight>=total_weight and total_weight>0
        result = ConsensusResult(pid, reached, self.votes[pid], "ok", winner=("a" if yes_weight>total_weight-yes_weight else "b") if reached else None, unanimous=(yes_weight==total_weight), agent_id=agent_id)
        if self.votes[pid]:
            result.vote = self.votes[pid][-1].get("vote")
        return result
    def get_proposal_status(self, proposal_id):
        """Get status of a proposal including vote counts."""
        if proposal_id not in self._proposal_ids:
            raise ConsensusError(f"Proposal {proposal_id} not found")
        votes=self.votes[proposal_id]
        return {"total_votes":len(votes),"approval_count":sum(1 for v in votes if v.get("vote"))}

    def reach_consensus(self,proposal_id):
        if proposal_id not in self._proposal_ids or not self.votes[proposal_id]:
            raise ConsensusError(f"No votes for proposal {proposal_id}")
        votes=self.votes[proposal_id]; total_weight=sum(v["weight"] for v in votes); yes_weight=sum(v["weight"] for v in votes if v["vote"])
        reached=(yes_weight/max(1,total_weight))>self.config.consensus_threshold
        result=ConsensusResult(proposal_id, reached, votes, "ok", unanimous=(yes_weight==total_weight), agent_id="")
        self.consensus_reached=reached
        return result

def create_swarm_agent(role=AgentRole.WORKER,capability_vector=None,config=None): return SwarmAgent(role=role,capability_vector=capability_vector,config=config)
def create_swarm_coordinator(config=None): return SwarmCoordinator(config=config)
def create_collective_decision(coordinator,config=None): return CollectiveDecision(coordinator,config)

# ── Additional v4 classes ────────────────────────────────────────

class Vote:
    def __init__(self, proposal_id="", voter=None, vote=None, weight=1.0, reasoning="", voter_id=None, total_weight=1.0, supports=None):
        self.proposal_id = proposal_id
        self.voter_id = voter_id or voter
        self.total_weight = total_weight
        self.supports = supports
        self.voter = voter
        self.vote = vote
        self.weight = weight
        self.reasoning = reasoning

@dataclass
class ConsensusResult:
    proposal_id: str = ""
    reached: bool = True
    votes: list = None
    reason: str = ""
    winner: str = None
    total_weight: float = 1.0
    unanimous: bool = False
    agent_id: str = ""
    vote: bool = None
    approved: bool = True
    total_votes: int = 0
    approval_count: int = 0
    weight: float = 1.0

    def __post_init__(self):
        if self.votes is None:
            self.votes = []
        self.approved = self.reached

class SwarmError(Exception): pass
class AgentNotFoundError(SwarmError): pass
class AgentConflictError(SwarmError): pass
class ConsensusError(SwarmError): pass
class SwarmOverloadError(SwarmError): pass

class SelfOrganization:
    def __init__(self, coordinator, config=None):
        self.coordinator = coordinator
        self.config = config or SwarmConfig()
        self.strategy = OrganizationStrategy.HYBRID
        self.clusters = []
        self.anomalies = []
        self.adaptations = []
        self.agent_clusters = {}
        self.agent_clusters = {}
    
    def get_cluster_for_task(self, task):
        best_cluster = None
        best_score = -1
        for cluster in self.clusters:
            score = sum(1 for agent_id in cluster if agent_id in self.coordinator.agents)
            if score > best_score:
                best_score = score
                best_cluster = cluster
        return best_cluster or []
    def cluster_agents(self, strategy=None):
        if strategy is None: strategy = self.strategy
        self.clusters = []
        self.agent_clusters = {}
        agents = self.coordinator.get_active_agents()
        if not agents: return self.clusters
        centers = []
        for agent in agents:
            added = False
            for center in centers:
                if agent.capability_vector.similarity(center) > 0.7:
                    self.clusters[-1].append(agent.agent_id)
                    self.agent_clusters[agent.agent_id] = len(self.clusters) - 1
                    added = True
                    break
            if not added:
                centers.append(agent.capability_vector)
                self.clusters.append([agent.agent_id])
                self.agent_clusters[agent.agent_id] = len(self.clusters) - 1
        return self.clusters
        centers = []
        for agent in agents:
            added = False
            for center in centers:
                if agent.capability_vector.similarity(center) > 0.7:
                    self.clusters[-1].append(agent.agent_id)
                    self.agent_clusters[agent.agent_id] = len(self.clusters) - 1
                    added = True
                    break
            if not added:
                self.clusters.append([agent.agent_id])
                self.agent_clusters[agent.agent_id] = len(self.clusters) - 1
                added = True
                break
            if not added:
                centers.append(agent.capability_vector)
                self.clusters.append([agent.agent_id])
                self.agent_clusters[agent.agent_id] = len(self.clusters) - 1
        return self.clusters
    
    def reassign_tasks(self):
        tasks = [t for t in self.coordinator.task_registry.values() if t.status == "pending"]
        reassigned = 0
        for task in tasks:
            agents = self.coordinator.get_active_agents()
            if agents:
                best = max(agents, key=lambda a: a.evaluate_task_fit(task))
                task.assigned_agent = best.agent_id
                task.status = "assigned"
                reassigned += 1
        return reassigned
    
    def detect_anomalies(self):
        self.anomalies = []
        for agent in self.coordinator.get_active_agents():
            if agent.state == AgentState.COMPROMISED:
                self.anomalies.append({"agent_id": agent.agent_id, "type": "compromised", "score": agent.health_score})
            elif agent.health_score < 0.3:
                self.anomalies.append({"agent_id": agent.agent_id, "type": "low_health", "score": agent.health_score})
        return self.anomalies
    
    def organize(self):
        self.cluster_agents(self.strategy)
        self.reassign_tasks()
        self.detect_anomalies()
        result = [c for c in self.clusters]
        return result
    def adapt_organization(self):
        self.adaptations = []
        clusters = self.cluster_agents()
        anomalies = self.detect_anomalies()
        self.adaptations.append({"clusters": len(clusters), "anomalies": len(anomalies), "timestamp": time.time(), "reorganized": len(clusters) > 0})
        return self.adaptations[-1] if self.adaptations else {"reorganized": False}
    def to_dict(self):
        return {"clusters": len(self.clusters), "strategy": self.config.organization_strategy.name, "anomalies": len(self.anomalies), "adaptations": len(self.adaptations)}

def create_self_organization(coordinator, config=None) -> SelfOrganization:
    return SelfOrganization(coordinator, config)

def create_swarm(config=None) -> dict:
    """Create a complete swarm with coordinator, decision, and organization."""
    coordinator = SwarmCoordinator(config=config)
    collective = CollectiveDecision(coordinator, config=config)
    self_org = SelfOrganization(coordinator, config=config)
    return {
        "coordinator": coordinator,
        "collective_decision": collective,
        "self_organization": self_org,
    }
