"""
TokenRelay v2 Transport Abstractions — HTTP and WebSocket transports.

Provides:
- HTTPTransport for REST-based token message delivery
- WebSocketTransport abstraction for bidirectional streaming
- TransportFactory for creating transport instances
- TransportSession for managing connection state
"""

import json
import time
import threading
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from http.client import HTTPConnection
from typing import Any, Dict, List, Optional, Callable, Tuple
from urllib.parse import urlparse

from codec import MessageEncoder, MessageDecoder
from registry import TokenRegistry, get_registry


# ─────────────────────────────────────────────
# Types & Enums
# ─────────────────────────────────────────────

class TransportProtocol(Enum):
    """Transport protocol types."""
    HTTP = "http"
    HTTPS = "https"
    WEBSOCKET = "websocket"
    SSE = "sse"


class TransportStatus(Enum):
    """Transport connection status."""
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECTING = "reconnecting"
    ERROR = "error"


# ─────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────

@dataclass
class TransportConfig:
    """Configuration for a transport instance."""
    host: str = "localhost"
    port: int = 8765
    protocol: TransportProtocol = TransportProtocol.HTTP
    timeout: int = 30
    retry_attempts: int = 3
    retry_delay: float = 1.0
    headers: Dict[str, str] = field(default_factory=dict)
    compression: bool = False


@dataclass
class TransportMessage:
    """A message sent/received over a transport."""
    token_ids: List[int]
    payload: Dict[str, Any] = field(default_factory=dict)
    source: str = ""
    destination: str = ""
    timestamp: float = field(default_factory=time.time)
    message_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    schema_version: str = "1.0.0"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "token_ids": self.token_ids,
            "payload": self.payload,
            "source": self.source,
            "destination": self.destination,
            "timestamp": self.timestamp,
            "message_id": self.message_id,
            "schema_version": self.schema_version,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TransportMessage":
        return cls(
            token_ids=data.get("token_ids", []),
            payload=data.get("payload", {}),
            source=data.get("source", ""),
            destination=data.get("destination", ""),
            timestamp=data.get("timestamp", time.time()),
            message_id=data.get("message_id", str(uuid.uuid4())[:8]),
            schema_version=data.get("schema_version", "1.0.0"),
        )


@dataclass
class TransportSession:
    """Represents an active transport session."""
    session_id: str
    transport_type: TransportProtocol
    status: TransportStatus = TransportStatus.DISCONNECTED
    connected_at: Optional[float] = None
    messages_sent: int = 0
    messages_received: int = 0
    last_activity: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def connect(self):
        """Mark session as connected."""
        self.status = TransportStatus.CONNECTED
        self.connected_at = time.time()
        self.last_activity = time.time()

    def disconnect(self):
        """Mark session as disconnected."""
        self.status = TransportStatus.DISCONNECTED

    def to_dict(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id,
            "transport_type": self.transport_type.value,
            "status": self.status.value,
            "connected_at": self.connected_at,
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "last_activity": self.last_activity,
            "metadata": self.metadata,
        }


# ─────────────────────────────────────────────
# Abstract Transport Base
# ─────────────────────────────────────────────

class Transport(ABC):
    """Abstract base class for all transport implementations."""

    def __init__(self, config: TransportConfig,
                 registry: Optional[TokenRegistry] = None):
        self.config = config
        self.registry = registry or get_registry()
        self.encoder = MessageEncoder(self.registry)
        self.decoder = MessageDecoder(self.registry)
        self.session = TransportSession(
            session_id=str(uuid.uuid4())[:8],
            transport_type=config.protocol
        )
        self._callbacks: Dict[str, Callable] = {}
        self._lock = threading.Lock()
        self._running = False

    @abstractmethod
    def connect(self) -> bool:
        """Establish the transport connection."""
        pass

    @abstractmethod
    def disconnect(self) -> bool:
        """Disconnect the transport."""
        pass

    @abstractmethod
    def send(self, message: TransportMessage) -> bool:
        """Send a transport message."""
        pass

    @abstractmethod
    def receive(self) -> Optional[TransportMessage]:
        """Receive a transport message (blocking)."""
        pass

    def on_event(self, event: str, callback: Callable):
        """Register a callback for a transport event."""
        self._callbacks[event] = callback

    def _emit(self, event: str, data: Any = None):
        """Emit an event to registered callbacks."""
        cb = self._callbacks.get(event)
        if cb:
            try:
                cb(data)
            except Exception:
                pass

    @property
    @abstractmethod
    def status(self) -> TransportStatus:
        """Get the current transport status."""
        pass

    @abstractmethod
    def send_batch(self, messages: List[TransportMessage]) -> int:
        """Send a batch of messages. Returns count sent."""
        pass

    def encode_message(self, token_ids: List[int],
                       payload: Optional[Dict] = None) -> str:
        """Encode token IDs and payload into a transport message string."""
        payload = payload or {}
        msg = self.encoder.encode(str(token_ids[0]), payload)
        msg["tokens"] = token_ids
        return json.dumps(msg)

    def decode_message(self, data: str) -> Optional[Dict[str, Any]]:
        """Decode a transport message string into a dict."""
        try:
            parsed = json.loads(data)
            return self.decoder.decode(parsed)
        except (json.JSONDecodeError, ValueError):
            return None


# ─────────────────────────────────────────────
# HTTP Transport
# ─────────────────────────────────────────────

class HTTPTransport(Transport):
    """HTTP-based transport for token message delivery.

    Supports GET/POST for sending and receiving token messages
    over HTTP. Uses urllib from stdlib.
    """

    def __init__(self, config: Optional[TransportConfig] = None,
                 registry: Optional[TokenRegistry] = None):
        config = config or TransportConfig()
        super().__init__(config, registry)
        self._connected = False
        self._base_url = f"http://{config.host}:{config.port}"
        self._receive_buffer: List[TransportMessage] = []
        self._receive_lock = threading.Lock()

    def connect(self) -> bool:
        """Verify the HTTP endpoint is reachable."""
        try:
            import urllib.request
            url = f"{self._base_url}/sessions"
            req = urllib.request.Request(url, method="GET")
            urllib.request.urlopen(req, timeout=self.config.timeout)
            self._connected = True
            self.session.connect()
            self._emit("connected")
            return True
        except Exception:
            self._connected = False
            self.session.status = TransportStatus.ERROR
            return False

    def disconnect(self) -> bool:
        """Disconnect the HTTP transport."""
        self._connected = False
        self.session.disconnect()
        self._emit("disconnected")
        return True

    @property
    def status(self) -> TransportStatus:
        """Get the current transport status."""
        if self._connected:
            return TransportStatus.CONNECTED
        return TransportStatus.DISCONNECTED

    def send(self, message: TransportMessage) -> bool:
        """Send a token message via HTTP POST.

        Encodes the message as JSON and POSTs it to /message endpoint.
        """
        if not self._connected:
            if not self.connect():
                return False

        try:
            import urllib.request
            data = json.dumps(message.to_dict()).encode("utf-8")
            url = f"{self._base_url}/message"
            req = urllib.request.Request(
                url, data=data, method="POST",
                headers={"Content-Type": "application/json", **self.config.headers}
            )
            response = urllib.request.urlopen(req, timeout=self.config.timeout)
            self.session.messages_sent += 1
            self.session.last_activity = time.time()
            self._emit("message_sent", message.to_dict())
            return response.status == 200
        except Exception:
            self._emit("error", {"action": "send", "error": "request_failed"})
            return False

    def receive(self) -> Optional[TransportMessage]:
        """Receive a message from the HTTP endpoint."""
        try:
            import urllib.request
            url = f"{self._base_url}/events"
            req = urllib.request.Request(url, method="GET")
            response = urllib.request.urlopen(req, timeout=self.config.timeout)
            data = json.loads(response.read().decode("utf-8"))
            return TransportMessage(
                token_ids=[],
                payload=data,
                source="http_transport",
            )
        except Exception:
            return None

    def send_batch(self, messages: List[TransportMessage]) -> int:
        """Send multiple messages via HTTP POST /batch."""
        sent = 0
        for msg in messages:
            if self.send(msg):
                sent += 1
        return sent

    def get_backpressure_status(self) -> Optional[Dict]:
        """Get backpressure state from the server."""
        try:
            import urllib.request
            url = f"{self._base_url}/backpressure"
            req = urllib.request.Request(url, method="GET")
            response = urllib.request.urlopen(req, timeout=self.config.timeout)
            return json.loads(response.read().decode("utf-8"))
        except Exception:
            return None

    def get_active_sessions(self) -> List[Dict]:
        """Get active streaming sessions."""
        try:
            import urllib.request
            url = f"{self._base_url}/sessions"
            req = urllib.request.Request(url, method="GET")
            response = urllib.request.urlopen(req, timeout=self.config.timeout)
            return json.loads(response.read().decode("utf-8"))
        except Exception:
            return []


# ─────────────────────────────────────────────
# WebSocket Transport
# ─────────────────────────────────────────────

class WebSocketTransport(Transport):
    """WebSocket-like transport abstraction.

    Simulates WebSocket communication over HTTP long-polling.
    Provides bidirectional message exchange with the streaming server.
    """

    def __init__(self, config: Optional[TransportConfig] = None,
                 registry: Optional[TokenRegistry] = None):
        config = config or TransportConfig(protocol=TransportProtocol.WEBSOCKET)
        super().__init__(config, registry)
        self._connected = False
        self._client_id: Optional[str] = None
        self._receive_buffer: List[str] = []
        self._heartbeat_interval = 5
        self._last_heartbeat = 0

    def connect(self) -> bool:
        """Connect to the WebSocket endpoint."""
        if not self._connected:
            # Use the streaming processor's WebSocket simulator if available
            from streaming import WebSocketSimulator
            self._websocket_sim = WebSocketSimulator(None)
            # Create a session on the server
            try:
                import urllib.request
                base_url = f"http://{self.config.host}:{self.config.port}"
                url = f"{base_url}/stream/{self.session.session_id}"
                req = urllib.request.Request(url, method="GET")
                response = urllib.request.urlopen(req, timeout=2)
                data = json.loads(response.read().decode("utf-8"))
                self._client_id = self.session.session_id
                self._connected = True
                self.session.connect()
                self._emit("connected", {"session_id": self._client_id})
                return True
            except Exception:
                pass

        # If server not available, create a local-only WebSocket
        self._connected = True
        self.session.connect()
        return True

    def disconnect(self) -> bool:
        """Disconnect the WebSocket transport."""
        self._connected = False
        self.session.disconnect()
        self._emit("disconnected", {"session_id": self._client_id})
        return True

    @property
    def status(self) -> TransportStatus:
        """Get the current transport status."""
        if self._connected:
            return TransportStatus.CONNECTED
        return TransportStatus.DISCONNECTED

    def send(self, message: TransportMessage) -> bool:
        """Send a token message via WebSocket."""
        if not self._connected:
            if not self.connect():
                return False

        # Encode and send the message
        encoded = self.encode_message(message.token_ids, message.payload)
        self._receive_buffer.append(encoded)
        self.session.messages_sent += 1
        self._emit("message_sent", message.to_dict())
        return True

    def receive(self) -> Optional[TransportMessage]:
        """Receive a queued message from the WebSocket."""
        if not self._connected:
            return None

        if self._receive_buffer:
            data = self._receive_buffer.pop(0)
            self.session.messages_received += 1
            return TransportMessage.from_dict(json.loads(data))
        return None

    def send_batch(self, messages: List[TransportMessage]) -> int:
        """Send a batch of messages via WebSocket."""
        sent = 0
        for msg in messages:
            if self.send(msg):
                sent += 1
        return sent

    def ping(self) -> bool:
        """Ping the WebSocket connection."""
        self._last_heartbeat = time.time()
        return self._connected

    def broadcast(self, token_ids: List[int],
                   payload: Optional[Dict] = None) -> int:
        """Broadcast a message to all WebSocket connections."""
        if not hasattr(self, "_websocket_sim"):
            return 0
        return self._websocket_sim.broadcast(token_ids, payload)


# ─────────────────────────────────────────────
# Transport Factory
# ─────────────────────────────────────────────

class TransportFactory:
    """Factory for creating transport instances."""

    _transports: Dict[str, type] = {}

    @classmethod
    def register_transport(cls, name: str, transport_cls: type):
        """Register a transport type."""
        cls._transports[name] = transport_cls

    @classmethod
    def create_transport(cls, protocol: TransportProtocol,
                           config: Optional[TransportConfig] = None,
                           registry: Optional[TokenRegistry] = None
                           ) -> Transport:
        """Create a transport instance by protocol type."""
        if protocol == TransportProtocol.HTTP:
            return HTTPTransport(config, registry)
        elif protocol == TransportProtocol.WEBSOCKET:
            return WebSocketTransport(config, registry)
        elif protocol == TransportProtocol.SSE:
            return HTTPTransport(config, registry)  # SSE uses HTTP
        else:
            raise ValueError(f"Unknown transport protocol: {protocol}")

    @classmethod
    def get_transport_class(cls, name: str) -> Optional[type]:
        """Get a transport class by name."""
        return cls._transports.get(name)

    @classmethod
    def list_transports(cls) -> List[str]:
        """List all registered transport types."""
        return list(cls._transports.keys())


# Register built-in transports
TransportFactory.register_transport("http", HTTPTransport)
TransportFactory.register_transport("websocket", WebSocketTransport)
TransportFactory.register_transport("sse", HTTPTransport)


# ─────────────────────────────────────────────
# Transport Client
# ─────────────────────────────────────────────

class TransportClient:
    """High-level client for sending/receiving token messages over transports."""

    def __init__(self, transport: Transport):
        self.transport = transport
        self._connected = False
        self._readers: List[threading.Thread] = []

    def connect(self) -> bool:
        """Connect the underlying transport."""
        result = self.transport.connect()
        if result:
            self._connected = True
        return result

    def disconnect(self):
        """Disconnect the transport."""
        self.transport.disconnect()
        self._connected = False

    def send_tokens(self, token_ids: List[int], payload: Optional[Dict] = None,
                      destination: str = "") -> bool:
        """Send a token message through the transport."""
        msg = TransportMessage(
            token_ids=token_ids,
            payload=payload or {},
            destination=destination,
        )
        return self.transport.send(msg)

    def send_batch_tokens(self, messages: List[Dict[str, Any]]) -> int:
        """Send a batch of token messages."""
        transport_msgs = [
            TransportMessage(
                token_ids=m.get("tokens", []),
                payload=m.get("payload", {}),
                source="client",
            )
            for m in messages
        ]
        return self.transport.send_batch(transport_msgs)

    def receive_message(self) -> Optional[TransportMessage]:
        """Receive a message from the transport."""
        return self.transport.receive()

    @property
    def is_connected(self) -> bool:
        """Check if connected."""
        return self._connected and self.transport.status == TransportStatus.CONNECTED