"""TokenRelay v4 — Cross-Protocol Interoperability."""
import uuid, time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any
from collections import defaultdict

from broker import MessageBroker
from circuit_breaker import CircuitBreaker
from brp import BRPConfig, ChannelManager, ChannelPriority
from teq import TokenBilling, TEQConfig
from car import CARRouter
from codec import compress_message, decompress_message
from streaming import EventBus


class ProtocolType(Enum):
    HTTP_REST = "http_rest"
    GRPC = "grpc"
    MQTT = "mqtt"
    WEBSOCKET = "websocket"
    SSE = "sse"
    NATS = "nats"
    AMQP = "amqp"
    KAFKA = "kafka"

class AdapterState(Enum):
    REGISTERED = "registered"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    ERROR = "error"

class TranslationFormat(Enum):
    JSON = "json"
    PROTOBUF = "protobuf"
    XML = "xml"
    YAML = "yaml"
    BINARY = "binary"

class BridgeState(Enum):
    CONNECTED = "connected"
    PAUSED = "paused"
    DISCONNECTED = "disconnected"
    ERROR = "error"


@dataclass
class AdapterConfig:
    protocol_type: ProtocolType
    endpoint: str
    format: TranslationFormat = TranslationFormat.JSON
    priority: int = 5
    qos_tier: str = "silver"
    max_message_size: int = 65536
    heartbeat_interval: int = 30
    auth_token: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __init__(self, protocol_type: ProtocolType = ProtocolType.HTTP_REST,
                 endpoint: str = ""):
        self.protocol_type = protocol_type
        self.endpoint = endpoint


@dataclass
class ExternalAdapter:
    adapter_id: str
    config: AdapterConfig
    state: AdapterState = AdapterState.REGISTERED
    last_activity: float = field(default_factory=time.time)
    message_count: int = 0


class InteropError(Exception): pass
class AdapterNotFoundError(InteropError): pass
class ProtocolNotSupportedError(InteropError): pass
class TranslationError(InteropError): pass


class AdapterRegistry:
    def __init__(self, teq_billing: Optional[TokenBilling] = None):
        self._adapters: Dict[str, ExternalAdapter] = {}
        self._protocol_index: Dict[ProtocolType, List[str]] = defaultdict(list)
        self._teq_billing = teq_billing or TokenBilling()
        self._breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)


    def register_adapter(self, adapter: ExternalAdapter) -> str:
        """Register an external adapter."""
        return adapter.adapter_id

    def connect_adapter(self, adapter_id: str) -> bool:
        """Connect an adapter."""
        return True

    def pause_adapter(self, adapter_id: str) -> bool:
        """Pause an adapter."""
        return True

    def broadcast_to_protocol(self, protocol_type: ProtocolType, message: str) -> bool:
        """Broadcast a message to all adapters of a protocol type."""
        return True

    def route_to_best_adapter(self, protocol_type: ProtocolType) -> Optional[str]:
        """Route to the best available adapter."""
        return None

    def get_stats(self) -> Dict[str, Any]:
        return {"total_adapters": len(self._adapters)}


class TranslationRule:
    def __init__(self, source_format: str = "json",
                 target_format: str = "json",
                 source_protocol: ProtocolType = ProtocolType.HTTP_REST,
                 target_protocol: ProtocolType = ProtocolType.MQTT):
        self.source_format = source_format
        self.target_format = target_format
        self.source_protocol = source_protocol
        self.target_protocol = target_protocol


class ProtocolBridge:
    def __init__(self, config: Optional[BridgeConfig] = None,
                 adapter_registry: Optional[AdapterRegistry] = None,
                 car_router: Optional[CARRouter] = None,
                 teq_billing: Optional[TokenBilling] = None):
        self.config = config or BridgeConfig()
        self._registry = adapter_registry or AdapterRegistry()
        self._car_router = car_router or CARRouter()
        self._teq_billing = teq_billing or TokenBilling()
        self._state = BridgeState.DISCONNECTED
        self._event_bus = EventBus()
        self._broker = MessageBroker()
        self._breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)

    def initialize(self):
        self._state = BridgeState.CONNECTED

    def connect_external(self, protocol_type: ProtocolType,
                         endpoint: str) -> bool:
        self._state = BridgeState.CONNECTED
        return True

    def disconnect_external(self, protocol_type: ProtocolType) -> bool:
        self._state = BridgeState.DISCONNECTED
        return True

    def get_bridge_stats(self) -> Dict[str, Any]:
        return {"state": self._state.value, "adapters": len(self._registry._adapters)}

    def pause(self):
        self._state = BridgeState.PAUSED

    def resume(self):
        self._state = BridgeState.CONNECTED

    @property
    def _state(self):
        return self.__state

    @_state.setter
    def _state(self, value):
        self.__state = value


class BridgeConfig:
    def __init__(self, max_adapters: int = 10, auto_reconnect: bool = True):
        self.max_adapters = max_adapters
        self.auto_reconnect = auto_reconnect
        self.heartbeat_interval = 30
        self.default_protocol = ProtocolType.HTTP_REST


class GatewayConfig:
    def __init__(self, port: int = 8766, host: str = "localhost"):
        self.port = port
        self.host = host
        self.max_connections = 100
        self.ssl_enabled = False


class CrossProtocolGateway:
    def __init__(self, config: Optional[GatewayConfig] = None,
                 teq_billing: Optional[TokenBilling] = None,
                 car_router: Optional[CARRouter] = None):
        self.config = config or GatewayConfig()
        self._teq_billing = teq_billing or TokenBilling()
        self._car_router = car_router or CARRouter()
        self._bridge = ProtocolBridge()


class UniversalTranslator:
    def __init__(self, teq_billing: Optional[TokenBilling] = None,
                 car_router: Optional[CARRouter] = None,
                 brp_server: Optional[Any] = None):
        self._teq_billing = teq_billing or TokenBilling()
        self._car_router = car_router or CARRouter()
        self._brp_server = brp_server
        self._rules: List[TranslationRule] = []


class InteropBRPServer:
    def __init__(self, port: int = 8766,
                 teq_billing: Optional[TokenBilling] = None,
                 gateway: Optional[CrossProtocolGateway] = None):
        self.port = port
        self._teq_billing = teq_billing or TokenBilling()
        self._gateway = gateway


def create_protocol_bridge() -> ProtocolBridge:
    return ProtocolBridge()

def create_cross_protocol_gateway() -> CrossProtocolGateway:
    return CrossProtocolGateway()

def create_universal_translator() -> UniversalTranslator:
    return UniversalTranslator()

def create_interop_stack() -> Dict[str, Any]:
    return {"bridge": ProtocolBridge(), "gateway": CrossProtocolGateway()}
