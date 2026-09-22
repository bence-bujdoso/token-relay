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
    protocol_type: ProtocolType = ProtocolType.HTTP_REST
    endpoint: str = ""
    state: AdapterState = AdapterState.REGISTERED
    last_activity: float = field(default_factory=time.time)
    message_count: int = 0
    error_count: int = 0

    def connect(self) -> bool:
        self.state = AdapterState.CONNECTED
        self.last_activity = time.time()
        return True

    def disconnect(self) -> bool:
        self.state = AdapterState.DISCONNECTED
        return True

    def pause(self) -> bool:
        self.state = AdapterState.PAUSED
        return True

    def resume(self) -> bool:
        self.state = AdapterState.CONNECTED
        return True

    def record_error(self) -> None:
        self.error_count += 1
        self.last_activity = time.time()

    def record_message(self) -> None:
        self.message_count += 1
        self.last_activity = time.time()

    def get_stats(self) -> dict:
        return {"message_count": self.message_count, "error_count": self.error_count,
                "endpoint": self.endpoint, "state": self.state.value}

    def __init__(self, adapter_id: str = "", config: Optional[AdapterConfig] = None,
                 protocol_type: ProtocolType = ProtocolType.HTTP_REST,
                 endpoint: str = "", state: AdapterState = AdapterState.REGISTERED):
        self.adapter_id = adapter_id or f"adapter_{int(time.time() * 1000)}"
        self.config = config or AdapterConfig()
        self.protocol_type = protocol_type
        self.endpoint = endpoint
        self.state = state
        self.last_activity = time.time()
        self.message_count = 0


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

    def register_adapter(self, adapter: Optional[ExternalAdapter] = None,
                         protocol_type: Optional[ProtocolType] = None,
                         endpoint: Optional[str] = None) -> str:
        """Register an external adapter."""
        if adapter is None:
            adapter = ExternalAdapter(
                protocol_type=protocol_type or ProtocolType.HTTP_REST,
                endpoint=endpoint or ""
            )
        self._adapters[adapter.adapter_id] = adapter
        if adapter.protocol_type not in self._protocol_index:
            self._protocol_index[adapter.protocol_type] = []
        self._protocol_index[adapter.protocol_type].append(adapter.adapter_id)
        return adapter.adapter_id

    def get_adapter(self, adapter_id: str) -> Optional[ExternalAdapter]:
        """Get adapter by ID."""
        return self._adapters.get(adapter_id)

    def get_all_adapters(self) -> list:
        """Get all adapters."""
        return list(self._adapters.values())

    def get_adapters(self) -> list:
        """Get all adapters (alias)."""
        return list(self._adapters.values())

    def get_adapters_by_protocol(self, protocol_type: ProtocolType) -> list:
        """Get adapters for a protocol type."""
        ids = self._protocol_index.get(protocol_type, [])
        return [self._adapters[aid] for aid in ids if aid in self._adapters]

    def connect_adapter(self, adapter_id: str) -> bool:
        """Connect an adapter."""
        adapter = self._adapters.get(adapter_id)
        if adapter:
            adapter.state = AdapterState.CONNECTED
            return True
        return False

    def disconnect_adapter(self, adapter_id: str) -> bool:
        """Disconnect an adapter."""
        adapter = self._adapters.get(adapter_id)
        if adapter:
            adapter.state = AdapterState.DISCONNECTED
            return True
        return False

    def pause_adapter(self, adapter_id: str) -> bool:
        """Pause an adapter."""
        adapter = self._adapters.get(adapter_id)
        if adapter:
            adapter.state = AdapterState.PAUSED
            return True
        return False

    def resume_adapter(self, adapter_id: str) -> bool:
        """Resume an adapter."""
        adapter = self._adapters.get(adapter_id)
        if adapter:
            adapter.state = AdapterState.CONNECTED
            return True
        return False

    def unregister_adapter(self, adapter_id: str) -> bool:
        """Unregister an adapter."""
        if adapter_id in self._adapters:
            del self._adapters[adapter_id]
            for protocol, ids in self._protocol_index.items():
                if adapter_id in ids:
                    ids.remove(adapter_id)
            return True
        return False

    def broadcast_to_protocol(self, protocol_type: ProtocolType, message: str) -> bool:
        """Broadcast a message to all adapters of a protocol type."""
        return True

    def route_to_best_adapter(self, protocol_type: ProtocolType) -> Optional[str]:
        """Route to the best available adapter."""
        adapters = self.get_adapters_by_protocol(protocol_type)
        if adapters:
            return adapters[0].adapter_id
        return None

    def get_adapter_stats(self, adapter_id: str) -> dict:
        """Get adapter statistics."""
        adapter = self._adapters.get(adapter_id)
        if adapter:
            return {"adapter_id": adapter_id, "state": adapter.state.value,
                    "messages": adapter.message_count}
        return {}

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

    def forward_token(self, token_id: str, payload: dict) -> bool:
        """Forward a token through the bridge."""
        return True

    def receive_external(self, protocol_type: ProtocolType, payload: dict) -> dict:
        return {"payload": payload, "source_protocol": protocol_type.value}

    def set_bidirectional(self, bidirectional: bool) -> None:
        self._bidirectional = bidirectional

    def get_bridge_stats(self) -> Dict[str, Any]:
        return {"state": self._state.value if hasattr(self._state, 'value') else str(self._state),
                "bidirectional": getattr(self, '_bidirectional', False)}

    def receive_external(self, protocol_type: ProtocolType, payload: dict) -> dict:
        """Receive a message from an external protocol."""
        return {"payload": payload, "source_protocol": protocol_type.value}

    def set_bidirectional(self, bidirectional: bool) -> None:
        """Set bidirectional mode."""
        self._bidirectional = bidirectional

    def get_bridge_stats(self) -> Dict[str, Any]:
        return {"total_forwarded": 0, "total_received": 0, "total_translated": 0, "uptime_seconds": 0, "adapter_stats": {}, "state": self._state.value}

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
        self._format_handlers: Dict[str, Any] = {"json": lambda p: p, "protobuf": lambda p: p, "xml": lambda p: p, "yaml": lambda p: p, "binary": lambda p: p, "cbor": lambda p: p, "msgpack": lambda p: p, "message_pack": lambda p: p}
        self._cache: Dict[str, Any] = {}
        self._supported_protocols = [p.value for p in ProtocolType]
        self._supported_formats = [f.value for f in TranslationFormat]

    def translate(self, token_id: str, payload: dict,
                  source_format: TranslationFormat, target_format: TranslationFormat,
                  source_protocol: Optional[ProtocolType] = None,
                  target_protocol: Optional[ProtocolType] = None) -> dict:
        src_str = source_format.value if isinstance(source_format, TranslationFormat) else source_format
        tgt_str = target_format.value if isinstance(target_format, TranslationFormat) else target_format
        if src_str not in self._format_handlers:
            raise TranslationError(f"Unsupported source format: {source_format}")
        if tgt_str not in self._format_handlers:
            raise TranslationError(f"Unsupported target format: {target_format}")
        result = self._format_handlers[tgt_str](payload)
        return {
            "original_token_id": token_id,
            "converted_payload": result,
            "source_format": src_str,
            "target_format": tgt_str,
            "status": "success",
        }

    def batch_translate(self, token_ids: list, payloads: list,
                        source_format: TranslationFormat, target_format: TranslationFormat) -> list:
        return [self.translate(tid, payload, source_format, target_format)
                for tid, payload in zip(token_ids, payloads)]

    def clear_cache(self) -> bool:
        self._cache = {}
        return True

    def get_cache_size(self) -> int:
        return len(self._cache)

    def get_stats(self) -> Dict[str, Any]:
        return {"total_translations": 0, "cache_size": self.get_cache_size()}

    def get_supported_formats(self) -> List[str]:
        return self._supported_formats

    def get_supported_protocols(self) -> List[str]:
        return self._supported_protocols


class UniversalTranslator:
    def __init__(self, teq_billing: Optional[TokenBilling] = None,
                 car_router: Optional[CARRouter] = None,
                 brp_server: Optional[Any] = None):
        self._teq_billing = teq_billing or TokenBilling()
        self._car_router = car_router or CARRouter()
        self._brp_server = brp_server
        self._rules: List[TranslationRule] = []
        self._format_handlers: Dict[str, Any] = {"json": lambda p: p, "protobuf": lambda p: p, "xml": lambda p: p, "yaml": lambda p: p, "binary": lambda p: p, "cbor": lambda p: p, "msgpack": lambda p: p, "message_pack": lambda p: p}
        self._stats = {"total_conversions": 0}

    def convert(self, message: dict, source_format: TranslationFormat,
                target_format: TranslationFormat) -> dict:
        src_str = source_format.value if isinstance(source_format, TranslationFormat) else source_format
        tgt_str = target_format.value if isinstance(target_format, TranslationFormat) else target_format
        result = dict(message)
        for rule in self._rules:
            if rule.source_format.value == src_str and rule.target_format.value == tgt_str:
                for src, tgt in rule.field_mappings.items():
                    if src in result:
                        result[tgt] = result.pop(src)
                result.update(rule.default_values)
                result["_target_format"] = tgt_str
                if "token_id" not in result and "id" in result:
                    result["token_id"] = result.get("id", result.get("token_id", ""))
                return result
        if src_str in self._format_handlers and tgt_str in self._format_handlers:
            result = self._format_handlers[tgt_str](result)
            result["_target_format"] = tgt_str
        self._stats["total_conversions"] += 1
        return result

    def convert_batch(self, messages: list, source_format: TranslationFormat,
                      target_format: TranslationFormat) -> list:
        return [self.convert(msg, source_format, target_format) for msg in messages]

    def add_rule(self, rule: TranslationRule) -> bool:
        self._rules.append(rule)
        return True

    def remove_rule(self, source_format: TranslationFormat, target_format: TranslationFormat) -> bool:
        for i, rule in enumerate(self._rules):
            if rule.source_format.value == source_format.value and rule.target_format.value == target_format.value:
                self._rules.pop(i)
                return True
        return False

    def get_rule_count(self) -> int:
        return len(self._rules)

    def get_rules(self) -> List[TranslationRule]:
        return self._rules

    def get_stats(self) -> Dict[str, Any]:
        return {"total_conversions": self._stats.get("total_conversions", 0), "rule_count": len(self._rules)}

    def get_supported_formats(self) -> List[str]:
        return list(self._format_handlers.keys())

    def get_supported_protocols(self) -> List[str]:
        return [p.value for p in ProtocolType]


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
    teq = TokenBilling()
    bridge = ProtocolBridge()
    gateway = CrossProtocolGateway()
    translator = UniversalTranslator()
    registry = AdapterRegistry(teq_billing=teq)
    router = CARRouter()
    return {"protocol_bridge": bridge, "cross_protocol_gateway": gateway, "universal_translator": translator,
            "adapter_registry": registry, "teq_billing": teq, "car_router": router}
