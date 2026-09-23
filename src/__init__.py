"""TokenRelay v4.0.0 - Autonomous Multi-Agent Framework.

TokenRelay v4 extends the protocol from end-to-end LLM communication
optimization to a fully autonomous, privacy-preserving, multi-agent framework.

Key modules:
    - swarm: Multi-agent swarm coordination with emergent behavior
    - selflearning: Reinforcement learning-based adaptive optimization
    - privacy: Zero-knowledge proof verification and privacy preservation
    - interop: Cross-protocol interoperability bridge
    - codec: Message encoding/decoding and compression
    - broker: Priority message queue and routing
    - circuit_breaker: Fault tolerance and circuit breaking
    - streaming: Event-driven streaming infrastructure
"""

__version__ = "4.0.0"
__description__ = "Autonomous Multi-Agent Framework with Privacy & Interoperability"

# v4 modules
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

# v3 modules
from v3_orchestrator import V3Orchestrator
from streaming import EventBus, StreamEvent
from codec import compress_message, decompress_message

# v2 foundation
from broker import MessageBroker, PriorityMessage
from circuit_breaker import CircuitBreaker, CircuitState
from bridge import TokenRelayBridge
from registry import TokenRegistry, get_registry
from brp import BRPConfig, ChannelManager, ChannelPriority
from car import CARRouter, ComplexityAnalyzer, CARConfig
from atc import IntentClassifier, AdaptiveCompressor, ATCConfig
from teq import TokenBilling, QoSTier, TEQConfig
from epc import EdgeCache, CacheManager, EPCConfig

# v4 - New modules
from ab_testing import ABTestRunner, ExperimentRegistry, Experiment, ExperimentStatus, StatisticalTest, MetricsTracker
from webhook import WebhookManager, WebhookEvent
from session_memory import SessionManager
from swarm_dashboard import SwarmDashboard
from token_savings import SavingsTracker

__all__ = [
    # v4 - Swarm
    "SwarmAgent", "SwarmCoordinator", "CollectiveDecision", "SelfOrganization",
    "CapabilityVector", "AgentRole", "AgentState", "ConsensusMethod", "TaskPriority",
    "OrganizationStrategy", "AgentInfo", "SwarmTask", "Vote", "ConsensusResult",
    "SwarmConfig", "SwarmError", "AgentNotFoundError", "AgentConflictError",
    "ConsensusError", "SwarmOverloadError",
    "create_swarm_agent", "create_swarm_coordinator",
    "create_collective_decision", "create_self_organization", "create_swarm",
    # v4 - Self-Learning
    "ReinforcementLearner", "AdaptiveTokenAllocator", "PerformanceTracker",
    "FeedbackLoop", "SLBundle", "LearningState", "CompressionAction",
    "RewardSignal", "SLConfig", "AgentPairStats", "LearningEpisode",
    "create_learner", "create_allocator", "create_tracker",
    "create_feedback_loop", "create_sl_bundle",
    # v4 - Privacy
    "ZKProofVerifier", "PrivacyPreservingToken", "AnonymousRelay",
    "DifferentialPrivacy", "ZKProof", "ProofStatus", "TokenVisibility",
    "AnonymityLevel", "NoiseMechanism",
    "create_zk_verifier", "create_privacy_token",
    "create_anonymous_relay", "create_differential_privacy", "create_privacy_suite",
    # v4 - Interop
    "ProtocolBridge", "AdapterRegistry", "AdapterConfig", "ExternalAdapter",
    "BridgeConfig", "GatewayConfig", "TranslationFormat",
    "ProtocolType", "AdapterState", "BridgeState",
    "InteropError", "AdapterNotFoundError", "ProtocolNotSupportedError", "TranslationError",
    "create_protocol_bridge", "create_cross_protocol_gateway",
    "create_universal_translator", "create_interop_stack", "InteropBRPServer",
    "CrossProtocolGateway", "UniversalTranslator", "TranslationRule",
    # v3
    "V3Orchestrator", "EventBus", "StreamEvent",
    "compress_message", "decompress_message",
    # v2
    "MessageBroker", "PriorityMessage", "CircuitBreaker", "CircuitState",
    "TokenRelayBridge", "TokenRegistry", "get_registry",
    "BRPConfig", "ChannelManager", "ChannelPriority",
    "CARRouter", "ComplexityAnalyzer", "CARConfig",
    "IntentClassifier", "AdaptiveCompressor", "ATCConfig",
    "TokenBilling", "QoSTier", "TEQConfig",
    "EdgeCache", "CacheManager", "EPCConfig",
    # v4 - New modules
    "ABTestRunner", "ExperimentRegistry", "Experiment", "ExperimentStatus",
    "StatisticalTest", "MetricsTracker",
    "WebhookManager", "WebhookEvent",
    "SessionManager",
    "SwarmDashboard",
    "SavingsTracker",
    "__version__", "__description__",
]
