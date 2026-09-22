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


class TranslationError(Exception):
    """Translation error."""
    pass


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
    ACTIVE = "active"
    PAUSED = "paused"
    DISCONNECTED = "disconnected"
    ERROR = "error"

class TranslationFormat(Enum):
    JSON = "json"
    PROTOBUF = "protobuf"
    XML = "xml"
    YAML = "yaml"
    BINARY = "binary"
    CBOR = "cbor"
    MESSAGE_PACK = "msgpack"

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
    error_count: int = 0
    connected_at: float = 0.0

    def connect(self) -> bool:
        self.state = AdapterState.CONNECTED
        self.connected_at = time.time()
        return True

    def disconnect(self) -> bool:
        self.state = AdapterState.DISCONNECTED
        return True

    def pause(self) -> bool:
        self.state = AdapterState.ERROR
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


    def register_adapter(self, adapter=None, **kwargs) -> str:
        """Register an external adapter."""
        if kwargs:
            config = AdapterConfig(**kwargs)
            adapter = ExternalAdapter(adapter_id=str(uuid.uuid4())[:8], config=config)
        if adapter.adapter_id in self._adapters:
            raise ValueError(f"Adapter {adapter.adapter_id} already registered")
        self._adapters[adapter.adapter_id] = adapter
        self._protocol_index[adapter.config.protocol_type].append(adapter.adapter_id)
        return adapter.adapter_id

    def connect_adapter(self, adapter_id: str) -> bool:
        """Connect an adapter."""
        adapter = self._adapters.get(adapter_id)
        if adapter:
            return adapter.connect()
        return False

    def disconnect_adapter(self, adapter_id: str) -> bool:
        """Disconnect an adapter."""
        adapter = self._adapters.get(adapter_id)
        if adapter:
            return adapter.disconnect()
        return False

    def pause_adapter(self, adapter_id: str) -> bool:
        """Pause an adapter."""
        adapter = self._adapters.get(adapter_id)
        if adapter:
            return adapter.pause()
        return False

    def resume_adapter(self, adapter_id: str) -> bool:
        """Resume an adapter."""
        adapter = self._adapters.get(adapter_id)
        if adapter:
            return adapter.resume()
        return False

    def unregister_adapter(self, adapter_id: str) -> bool:
        """Unregister an adapter."""
        if adapter_id in self._adapters:
            del self._adapters[adapter_id]
            for ids in self._protocol_index.values():
                if adapter_id in ids:
                    ids.remove(adapter_id)
            return True
        return False

    def get_adapter(self, adapter_id: str) -> Optional[ExternalAdapter]:
        """Get adapter by ID."""
        return self._adapters.get(adapter_id)

    def get_adapters(self) -> list:
        """Get all adapters."""
        return list(self._adapters.values())

    def get_adapters_by_protocol(self, protocol_type: ProtocolType) -> list:
        """Get all adapters for a protocol type."""
        ids = self._protocol_index.get(protocol_type, [])
        return [self._adapters[aid] for aid in ids if aid in self._adapters]

    def get_adapter_stats(self, adapter_id: str) -> dict:
        """Get adapter statistics."""
        adapter = self._adapters.get(adapter_id)
        if adapter:
            return {"messages": adapter.message_count, "errors": adapter.error_count,
                    "state": adapter.state.name, "last_activity": adapter.last_activity}
        return {}

    def get_stats(self) -> Dict[str, Any]:
        return {"total_adapters": len(self._adapters),
                "total_messages": sum(a.message_count for a in self._adapters.values()),
                "total_errors": sum(a.error_count for a in self._adapters.values())}


class TranslationRule:
    def __init__(self, source_format: TranslationFormat = TranslationFormat.JSON,
                 target_format: TranslationFormat = TranslationFormat.PROTOBUF,
                 source_protocol: ProtocolType = ProtocolType.HTTP_REST,
                 target_protocol: ProtocolType = ProtocolType.MQTT,
                 field_mappings: Optional[Dict[str, str]] = None,
                 default_values: Optional[Dict[str, Any]] = None,
                 priority: int = 5):
        self.source_format = source_format if isinstance(source_format, TranslationFormat) else TranslationFormat(source_format)
        self.target_format = target_format if isinstance(target_format, TranslationFormat) else TranslationFormat(target_format)
        self.source_protocol = source_protocol
        self.target_protocol = target_protocol
        self.field_mappings = field_mappings or {}
        self.default_values = default_values or {}
        self.priority = priority

    def apply(self, message: dict) -> dict:
        result = dict(message)
        for src, tgt in self.field_mappings.items():
            if src in result:
                result[tgt] = result.pop(src)
        result.update(self.default_values)
        return result


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
        return {"total_forwarded": 0, "total_received": 0, "total_translated": 0,
                "uptime_seconds": 0, "adapter_stats": {}, "state": self._state.value}

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
        self.bridge_port = 8766


class GatewayConfig:
    def __init__(self, port: int = 8766, host: str = "localhost"):
        self.port = port
        self.host = host
        self.max_connections = 100
        self.ssl_enabled = False
        self.default_source_format = TranslationFormat.JSON


class CrossProtocolGateway:
    def __init__(self, config: Optional[GatewayConfig] = None,
                 teq_billing: Optional[TokenBilling] = None,
                 car_router: Optional[CARRouter] = None):
        self.config = config or GatewayConfig()
        self._teq_billing = teq_billing or TokenBilling()
        self._car_router = car_router or CARRouter()
        self._bridge = ProtocolBridge()
        self._format_handlers = {
            "json": lambda p: dict(p),
            "protobuf": lambda p: dict(p),
            "xml": lambda p: dict(p),
            "yaml": lambda p: dict(p),
            "binary": lambda p: dict(p),
            "cbor": lambda p: dict(p),
            "msgpack": lambda p: dict(p),
        }

    def translate(self, token_id: str, payload: dict,
                  source_format: TranslationFormat, target_format: TranslationFormat,
                  source_protocol: Optional[ProtocolType] = None,
                  target_protocol: Optional[ProtocolType] = None) -> dict:
        """Translate a payload from source to target format."""
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
        """Batch translate multiple payloads."""
        return [self.translate(tid, payload, source_format, target_format)
                for tid, payload in zip(token_ids, payloads)]


class UniversalTranslator:
    def __init__(self, teq_billing: Optional[TokenBilling] = None,
                 car_router: Optional[CARRouter] = None,
                 brp_server: Optional[Any] = None):
        self._teq_billing = teq_billing or TokenBilling()
        self._car_router = car_router or CARRouter()
        self._brp_server = brp_server
        self._rules: List[TranslationRule] = []
        # Add default translation rules
        self._rules.append(TranslationRule(
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.YAML,
            priority=5,
        ))
        self._rules.append(TranslationRule(
            source_format=TranslationFormat.PROTOBUF,
            target_format=TranslationFormat.JSON,
            priority=5,
        ))
        self._format_handlers = {
            "json": lambda p: dict(p),
            "protobuf": lambda p: dict(p),
            "xml": lambda p: dict(p),
            "yaml": lambda p: dict(p),
            "binary": lambda p: dict(p),
            "cbor": lambda p: dict(p),
            "msgpack": lambda p: dict(p),
        }

    def convert(self, message: dict, source_format: TranslationFormat,
                target_format: TranslationFormat) -> dict:
        """Convert a message from source to target format."""
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
        return result

    def convert_batch(self, messages: list, source_format: TranslationFormat,
                      target_format: TranslationFormat) -> list:
        """Batch convert multiple messages."""
        return [self.convert(msg, source_format, target_format) for msg in messages]

    def get_rules(self) -> List[TranslationRule]:
        """Get all translation rules."""
        return self._rules

    def get_rule_count(self) -> int:
        """Get number of translation rules."""
        return len(self._rules)

    def add_rule(self, rule: TranslationRule) -> bool:
        """Add a translation rule."""
        self._rules.append(rule)
        return True


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
