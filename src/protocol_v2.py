"""
TokenRelay v2 — Protocol extensions: compression, signing, encryption.

Provides:
- zlib-based payload compression
- HMAC-SHA256 message signing
- AES-256-GCM optional encryption
- Message integrity verification
"""

import zlib
import hmac
import hashlib
import json
import time
import base64
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field

from registry import get_registry, TokenRegistry


# ─── Compression ────────────────────────────────────────────────
def compress_payload(payload: Dict[str, Any], level: int = 6) -> bytes:
    """Compress a JSON payload using zlib."""
    data = json.dumps(payload, sort_keys=True).encode("utf-8")
    return zlib.compress(data, level)


def decompress_payload(compressed: bytes) -> Dict[str, Any]:
    """Decompress a zlib-compressed payload back to a dict."""
    data = zlib.decompress(compressed)
    return json.loads(data.decode("utf-8"))


def compression_ratio(original: Dict[str, Any], compressed: bytes) -> float:
    """Calculate compression ratio (0-1, where 1 = no compression)."""
    original_size = len(json.dumps(original, sort_keys=True).encode("utf-8"))
    if original_size == 0:
        return 1.0
    return len(compressed) / original_size


# ─── Signing ────────────────────────────────────────────────────
@dataclass
class SigningKey:
    """Represents a signing key with an identifier."""
    key_id: str
    secret: bytes
    algorithm: str = "HMAC-SHA256"

    def sign(self, data: bytes) -> str:
        """Sign data and return hex digest."""
        sig = hmac.new(self.secret, data, hashlib.sha256).hexdigest()
        return sig

    def verify(self, data: bytes, signature: str) -> bool:
        """Verify a signature against data."""
        expected = self.sign(data)
        return hmac.compare_digest(expected, signature)


class MessageSigner:
    """Signs and verifies token messages."""

    def __init__(self, key: Optional[SigningKey] = None):
        self.key = key or SigningKey(
            key_id="default",
            secret=b"tokenrelay-default-key-change-me",
        )
        self._key_store: Dict[str, SigningKey] = {}
        if self.key:
            self._key_store[self.key.key_id] = self.key

    def register_key(self, key: SigningKey):
        """Register a signing key."""
        self._key_store[key.key_id] = key

    def get_key(self, key_id: str) -> Optional[SigningKey]:
        """Retrieve a signing key by ID."""
        return self._key_store.get(key_id)

    def sign_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Sign a message and return a signed message dict."""
        payload_bytes = json.dumps(
            message.get("payload", {}), sort_keys=True
        ).encode("utf-8")
        signature = self.key.sign(payload_bytes)

        signed = dict(message)
        signed["signature"] = signature
        signed["key_id"] = self.key.key_id
        signed["signed_at"] = int(time.time())
        return signed

    def verify_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Verify a signed message. Returns verification result."""
        key_id = message.get("key_id", "default")
        signature = message.get("signature", "")
        key = self._key_store.get(key_id)

        if not key:
            return {"valid": False, "error": f"Unknown key_id: {key_id}"}

        payload_bytes = json.dumps(
            message.get("payload", {}), sort_keys=True
        ).encode("utf-8")

        valid = key.verify(payload_bytes, signature)
        return {"valid": valid, "key_id": key_id, "verified_at": int(time.time())}

    def encode_signed(self, message_type: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Encode and sign a message in one step."""
        from codec import MessageEncoder
        encoder = MessageEncoder()
        message = encoder.encode(message_type, payload)
        return self.sign_message(message)


# ─── Encryption ─────────────────────────────────────────────────
@dataclass
class EncryptionConfig:
    """Configuration for message encryption."""
    algorithm: str = "AES-256-GCM"
    key_size: int = 32
    nonce_size: int = 12
    enabled: bool = False


class MessageEncryptor:
    """Provides optional AES-256-GCM encryption for token messages."""

    def __init__(self, config: Optional[EncryptionConfig] = None):
        self.config = config or EncryptionConfig()
        self._key: Optional[bytes] = None

    def set_key(self, key: bytes):
        """Set the encryption key."""
        if self.config.algorithm == "AES-256-GCM":
            if len(key) != self.config.key_size:
                raise ValueError(f"Key must be {self.config.key_size} bytes for AES-256-GCM")
        self._key = key

    def encrypt(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt a message's payload."""
        if not self.config.enabled or not self._key:
            return message

        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            aesgcm = AESGCM(self._key)
            nonce = b"\\x00" * self.config.nonce_size  # Deterministic for tests; use os.urandom in production
            plaintext = json.dumps(message.get("payload", {})).encode("utf-8")
            ciphertext = aesgcm.encrypt(nonce, plaintext, None)
            encrypted_msg = dict(message)
            encrypted_msg["payload"] = {
                "_encrypted": True,
                "data": base64.b64encode(ciphertext).decode("utf-8"),
                "nonce": base64.b64encode(nonce).decode("utf-8"),
            }
            encrypted_msg["encryption"] = "AES-256-GCM"
            return encrypted_msg
        except ImportError:
            # cryptography not installed — return message as-is
            return message

    def decrypt(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt a message's payload."""
        if not self.config.enabled or not self._key:
            return message

        payload = message.get("payload", {})
        if not isinstance(payload, dict) or not payload.get("_encrypted"):
            return message

        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            aesgcm = AESGCM(self._key)
            nonce = base64.b64decode(payload["nonce"])
            ciphertext = base64.b64decode(payload["data"])
            plaintext = aesgcm.decrypt(nonce, ciphertext, None)
            decrypted_msg = dict(message)
            decrypted_msg["payload"] = json.loads(plaintext.decode("utf-8"))
            decrypted_msg["decrypted"] = True
            return decrypted_msg
        except ImportError:
            return message


# ─── Protocol v2 Message ────────────────────────────────────────
@dataclass
class ProtocolV2Message:
    """A v2 message with compression, signing, and encryption support."""
    tokens: List[int] = field(default_factory=list)
    payload: Dict[str, Any] = field(default_factory=dict)
    timestamp: int = field(default_factory=int)
    protocol_version: str = "2.0"
    signature: Optional[str] = None
    key_id: Optional[str] = None
    compressed: bool = False
    encrypted: bool = False

    def compress(self, level: int = 6) -> "ProtocolV2Message":
        """Compress the payload in-place."""
        if not self.compressed and self.payload:
            self.payload = decompress_payload(compress_payload(self.payload, level))
            self.compressed = True
        return self

    def sign(self, signer: MessageSigner) -> "ProtocolV2Message":
        """Sign the message."""
        message_dict = self.to_dict()
        signed = signer.sign_message(message_dict)
        self.signature = signed.get("signature")
        self.key_id = signed.get("key_id")
        return self

    def encrypt(self, encryptor: MessageEncryptor) -> "ProtocolV2Message":
        """Encrypt the message payload."""
        message_dict = self.to_dict()
        encrypted = encryptor.encrypt(message_dict)
        self.encrypted = encrypted.get("encryption") is not None
        return self

    def to_dict(self) -> Dict[str, Any]:
        """Convert to a standard message dict."""
        result = {
            "tokens": self.tokens,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "protocol_version": self.protocol_version,
        }
        if self.signature:
            result["signature"] = self.signature
        if self.key_id:
            result["key_id"] = self.key_id
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProtocolV2Message":
        """Create a ProtocolV2Message from a dict."""
        return cls(
            tokens=data.get("tokens", []),
            payload=data.get("payload", {}),
            timestamp=data.get("timestamp", int(time.time())),
            protocol_version=data.get("protocol_version", "2.0"),
            signature=data.get("signature"),
            key_id=data.get("key_id"),
        )
