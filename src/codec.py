"""
TokenRelay — Message Encoder and Decoder.

Encodes structured data into token-ID sequences with payloads.
Decodes token sequences back into human-readable interpretations.

v2 Extensions:
- zlib-based compression/decompression
- HMAC-SHA256 message signing and verification
- AES-256-GCM optional encryption
- Message batching (batch_encode / batch_decode)
- Compression ratio measurement and benchmarking
"""

import json
import time
import zlib
import hmac
import hashlib
import secrets
import sys
from typing import Optional, Dict, Any, List, Tuple, Union
from pathlib import Path

from registry import TokenRegistry, build_protocol_registry, get_registry


# ──────────────────────────────────────────────
# AES-256-GCM encryption (optional dependency)
# ──────────────────────────────────────────────
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    _AES_AVAILABLE = True
except ImportError:
    _AES_AVAILABLE = False


# ──────────────────────────────────────────────
# Protocol constants
# ──────────────────────────────────────────────
PROTOCOL_VERSION = "2.0"
V1_VERSION = "1.0"

# Metadata keys for v2 message fields
METADATA_COMPRESSED = "compressed"
METADATA_SIGNED = "signed"
METADATA_ENCRYPTED = "encrypted"
METADATA_COMPRESSION_RATIO = "compression_ratio"
METADATA_SIGNATURE = "signature"
METADATA_SIGNER = "signer"
METADATA_IV = "iv"
METADATA_BATCH = "batch"
METADATA_BATCH_INDEX = "batch_index"
METADATA_BATCH_TOTAL = "batch_total"


class CompressionError(Exception):
    """Raised when compression/decompression fails."""
    pass


class SigningError(Exception):
    """Raised when signing or verification fails."""
    pass


class EncryptionError(Exception):
    """Raised when encryption or decryption fails."""
    pass


class BatchError(Exception):
    """Raised when batch encoding/decoding fails."""
    pass


def compress_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """Compress a message's payload using zlib.

    Compresses the JSON-serialized payload and stores metadata
    about the compression ratio. The original message structure
    is preserved with added compression metadata.

    Args:
        message: A message dict with 'payload' field

    Returns:
        Message dict with compressed payload and compression metadata
    """
    payload = message.get("payload", {})
    if not payload:
        message[METADATA_COMPRESSED] = False
        message[METADATA_COMPRESSION_RATIO] = 1.0
        return message

    try:
        payload_json = json.dumps(payload, sort_keys=True).encode("utf-8")
        compressed = zlib.compress(payload_json, level=9)

        # Store compressed data as base64-like hex string for JSON compatibility
        message["payload"] = {
            "__compressed": True,
            "__data": compressed.hex(),
            "__original_size": len(payload_json),
            "__compressed_size": len(compressed)
        }
        message[METADATA_COMPRESSED] = True
        ratio = len(payload_json) / max(len(compressed), 1)
        message[METADATA_COMPRESSION_RATIO] = round(ratio, 4)
    except Exception as e:
        raise CompressionError(f"Compression failed: {e}")

    return message


def decompress_message(message: Dict[str, Any]) -> Dict[str, Any]:
    """Decompress a message's payload that was compressed with compress_message().

    Args:
        message: A message dict with compressed payload metadata

    Returns:
        Message dict with restored original payload

    Raises:
        CompressionError: If the message is not compressed or decompression fails
    """
    payload = message.get("payload", {})

    # Check if payload is compressed
    if isinstance(payload, dict) and payload.get("__compressed"):
        try:
            compressed_data = bytes.fromhex(payload["__data"])
            decompressed = zlib.decompress(compressed_data)
            original_json = decompressed.decode("utf-8")
            message["payload"] = json.loads(original_json)
            message[METADATA_COMPRESSED] = False
        except Exception as e:
            raise CompressionError(f"Decompression failed: {e}")
    elif METADATA_COMPRESSED in message and message[METADATA_COMPRESSED]:
        # Legacy format: payload was directly compressed
        try:
            compressed_data = bytes.fromhex(message.get("_compressed_payload", ""))
            decompressed = zlib.decompress(compressed_data)
            message["payload"] = json.loads(decompressed.decode("utf-8"))
            message[METADATA_COMPRESSED] = False
        except Exception as e:
            raise CompressionError(f"Decompression failed: {e}")
    else:
        # Not compressed — return as-is
        message[METADATA_COMPRESSED] = False

    return message


def sign_message(message: Dict[str, Any], secret_key: bytes) -> Dict[str, Any]:
    """Sign a message using HMAC-SHA256.

    Computes HMAC-SHA256 over the canonical JSON representation
    of the message (excluding any existing signature), and adds
    the signature and signer metadata.

    Args:
        message: Message dict to sign
        secret_key: Shared secret key for HMAC

    Returns:
        Message dict with signature metadata added
    """
    # Create a copy without any v2 metadata for canonical signing
    signing_data = {k: v for k, v in message.items()
                     if k not in (METADATA_SIGNATURE, METADATA_SIGNER,
                                   METADATA_SIGNED, METADATA_COMPRESSED,
                                   METADATA_ENCRYPTED, METADATA_IV)}
    canonical = json.dumps(signing_data, sort_keys=True, separators=(',', ':'))

    signature = hmac.new(secret_key, canonical.encode("utf-8"), hashlib.sha256).hexdigest()

    message[METADATA_SIGNATURE] = signature
    message[METADATA_SIGNER] = secrets.token_hex(16)  # Unique signer ID
    message[METADATA_SIGNED] = True

    return message


def verify_signature(message: Dict[str, Any], secret_key: bytes) -> bool:
    """Verify a message's HMAC-SHA256 signature.

    Recomputes the HMAC over the canonical JSON representation
    and compares it with the stored signature.

    Args:
        message: Message dict with signature metadata
        secret_key: Shared secret key for HMAC

    Returns:
        True if signature is valid, False otherwise
    """
    if not message.get(METADATA_SIGNED):
        return False

    stored_signature = message.get(METADATA_SIGNATURE)
    if not stored_signature:
        return False

    # Recompute canonical signing data — strip all v2 metadata fields
    # that were added during signing to get the original canonical form
    signing_data = {k: v for k, v in message.items()
                     if k not in (METADATA_SIGNATURE, METADATA_SIGNER,
                                   METADATA_SIGNED, METADATA_COMPRESSED,
                                   METADATA_ENCRYPTED, METADATA_IV)}
    canonical = json.dumps(signing_data, sort_keys=True, separators=(',', ':'))

    expected = hmac.new(secret_key, canonical.encode("utf-8"), hashlib.sha256).hexdigest()

    return hmac.compare_digest(expected, stored_signature)


def encrypt_message(message: Dict[str, Any], secret_key: bytes) -> Dict[str, Any]:
    """Encrypt a message's payload using AES-256-GCM.

    Requires the `cryptography` package. If not available, raises
    EncryptionError.

    Args:
        message: Message dict to encrypt
        secret_key: 32-byte AES key

    Returns:
        Message dict with encrypted payload and IV metadata

    Raises:
        EncryptionError: If cryptography is unavailable or encryption fails
    """
    if not _AES_AVAILABLE:
        raise EncryptionError(
            "AES-256-GCM requires the 'cryptography' package. "
            "Install it with: pip install cryptography"
        )

    if len(secret_key) != 32:
        raise EncryptionError("AES-256-GCM requires a 32-byte key")

    payload = message.get("payload", {})
    payload_json = json.dumps(payload, sort_keys=True).encode("utf-8")

    # Generate a random 96-bit IV for GCM
    iv = secrets.token_bytes(12)
    aesgcm = AESGCM(secret_key)
    ciphertext = aesgcm.encrypt(iv, payload_json, None)

    # Store encrypted data with metadata
    message["payload"] = {
        "__encrypted": True,
        "__data": ciphertext.hex(),
        "__iv": iv.hex()
    }
    message[METADATA_ENCRYPTED] = True
    message[METADATA_IV] = iv.hex()

    return message


def decrypt_message(message: Dict[str, Any], secret_key: bytes) -> Dict[str, Any]:
    """Decrypt a message's payload that was encrypted with encrypt_message().

    Args:
        message: Message dict with encrypted payload metadata
        secret_key: 32-byte AES key (same key used for encryption)

    Returns:
        Message dict with restored original payload

    Raises:
        EncryptionError: If decryption fails or key is invalid
    """
    if not _AES_AVAILABLE:
        raise EncryptionError(
            "AES-256-GCM requires the 'cryptography' package. "
            "Install it with: pip install cryptography"
        )

    if len(secret_key) != 32:
        raise EncryptionError("AES-256-GCM requires a 32-byte key")

    payload = message.get("payload", {})

    if not (isinstance(payload, dict) and payload.get("__encrypted")):
        # Not encrypted — return as-is
        message[METADATA_ENCRYPTED] = False
        return message

    try:
        ciphertext = bytes.fromhex(payload["__data"])
        iv = bytes.fromhex(payload["__iv"])
        aesgcm = AESGCM(secret_key)
        plaintext = aesgcm.decrypt(iv, ciphertext, None)
        message["payload"] = json.loads(plaintext.decode("utf-8"))
        message[METADATA_ENCRYPTED] = False
    except Exception as e:
        raise EncryptionError(f"Decryption failed: {e}")

    return message


def batch_encode(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Encode multiple messages into a single batch message.

    Creates a batch envelope containing all messages with their
    individual indices. Each message retains its original structure.

    Args:
        messages: List of message dicts to batch

    Returns:
        A single batch message dict containing all encoded messages
    """
    if not messages:
        raise BatchError("Cannot batch encode an empty list")

    # Ensure all messages have protocol version
    for msg in messages:
        if "protocol_version" not in msg:
            msg["protocol_version"] = V1_VERSION

    batch_message = {
        "tokens": [0],  # Batch marker token
        "payload": {
            METADATA_BATCH: True,
            "messages": messages
        },
        "timestamp": int(time.time()),
        "protocol_version": PROTOCOL_VERSION,
        METADATA_BATCH_TOTAL: len(messages),
        METADATA_BATCH_INDEX: 0
    }

    return batch_message


def batch_decode(batch_message: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Decode a batch message back into individual messages.

    Args:
        batch_message: A batch message created by batch_encode()

    Returns:
        List of individual message dicts

    Raises:
        BatchError: If the message is not a valid batch
    """
    payload = batch_message.get("payload", {})

    if not isinstance(payload, dict) or not payload.get(METADATA_BATCH):
        raise BatchError("Message is not a batch message")

    messages = payload.get("messages", [])
    total = batch_message.get(METADATA_BATCH_TOTAL, len(messages))

    if len(messages) != total:
        raise BatchError(
            f"Batch size mismatch: expected {total}, got {len(messages)}"
        )

    return messages


def measure_compression_ratio(message: Dict[str, Any]) -> Dict[str, Any]:
    """Measure compression ratio for a message without modifying it.

    Compares the size of the JSON-serialized message before and after
    zlib compression.

    Args:
        message: Message dict to evaluate

    Returns:
        Dict with original_size, compressed_size, and ratio
    """
    original_json = json.dumps(message, sort_keys=True).encode("utf-8")
    original_size = len(original_json)

    payload = message.get("payload", {})
    if payload:
        compressed = zlib.compress(original_json, level=9)
        compressed_size = len(compressed)
        ratio = original_size / max(compressed_size, 1)
    else:
        compressed_size = 0
        ratio = 1.0

    return {
        "original_size": original_size,
        "compressed_size": compressed_size,
        "ratio": round(ratio, 4),
        "savings_pct": round((1 - compressed_size / max(original_size, 1)) * 100, 2)
    }


def benchmark_compression(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Benchmark compression performance across a list of messages.

    Args:
        messages: List of message dicts to benchmark

    Returns:
        Dict with aggregate compression statistics
    """
    if not messages:
        return {"messages": 0, "error": "No messages provided"}

    total_original = 0
    total_compressed = 0
    ratios = []
    times = []

    for msg in messages:
        start = time.perf_counter()
        original_json = json.dumps(msg, sort_keys=True).encode("utf-8")
        original_size = len(original_json)
        total_original += original_size

        compressed = zlib.compress(original_json, level=9)
        compressed_size = len(compressed)
        total_compressed += compressed_size

        elapsed = time.perf_counter() - start
        times.append(elapsed)
        ratios.append(original_size / max(compressed_size, 1))

    avg_ratio = sum(ratios) / len(ratios) if ratios else 1.0
    avg_time = sum(times) / len(times) if times else 0

    return {
        "messages": len(messages),
        "total_original_bytes": total_original,
        "total_compressed_bytes": total_compressed,
        "overall_ratio": round(total_original / max(total_compressed, 1), 4),
        "avg_ratio": round(avg_ratio, 4),
        "avg_compress_time_us": round(avg_time * 1_000_000, 2),
        "savings_pct": round((1 - total_compressed / max(total_original, 1)) * 100, 2)
    }


# ──────────────────────────────────────────────
# Convenience function: full compress-sign-encrypt pipeline
# ──────────────────────────────────────────────
def pipeline_encode(message: Dict[str, Any], secret_key: Optional[bytes] = None,
                     compress: bool = False, sign: bool = False,
                     encrypt: bool = False) -> Dict[str, Any]:
    """Apply compression and/or signing/encryption pipeline to a message.

    Args:
        message: Message dict to process
        secret_key: Optional key for HMAC signing and AES encryption.
            When provided, signing and encryption are applied by default.
        compress: Whether to compress the payload
        sign: Whether to HMAC-SHA256 sign the message (auto when secret_key)
        encrypt: Whether to AES-256-GCM encrypt the payload (auto when secret_key)

    Returns:
        Processed message dict
    """
    if compress:
        compress_message(message)

    if secret_key:
        sign_message(message, secret_key)
        encrypt_message(message, secret_key)

    return message


def pipeline_decode(message: Dict[str, Any], secret_key: Optional[bytes] = None) -> Dict[str, Any]:
    """Reverse the pipeline_encode pipeline.

    Args:
        message: Processed message dict
        secret_key: Key used for signing/encryption

    Returns:
        Decoded message dict
    """
    if message.get(METADATA_ENCRYPTED):
        decrypt_message(message, secret_key)

    if message.get(METADATA_SIGNED):
        verify_signature(message, secret_key)

    if message.get(METADATA_COMPRESSED):
        decompress_message(message)

    return message


class MessageEncoder:
    """Encodes structured data into token-ID sequences.

    v2 adds optional compression, signing, and encryption support.
    """

    def __init__(self, registry: Optional[TokenRegistry] = None):
        self.registry = registry or get_registry()

    def encode(self, message_type: str, payload: Optional[Dict] = None,
               custom_tokens: Optional[List[int]] = None,
               compress: bool = False,
               sign: bool = False,
               secret_key: Optional[bytes] = None,
               encrypt: bool = False) -> Dict[str, Any]:
        """Encode a message into a token sequence.

        Args:
            message_type: One of the protocol token IDs (as string or int)
            payload: Optional structured data
            custom_tokens: Additional token IDs for the sequence
            compress: Whether to zlib-compress the payload
            sign: Whether to HMAC-SHA256 sign the message
            secret_key: Shared secret for signing/encryption
            encrypt: Whether to AES-256-GCM encrypt the payload

        Returns:
            A message dict with tokens, payload, timestamp, and v2 metadata
        """
        token_id = str(message_type)
        token_data = self.registry.get_token(message_type)

        if not token_data:
            raise ValueError(
                f"Unknown message type '{message_type}'. "
                f"Valid tokens: {list(self.registry._registry.keys())}"
            )

        # Build token sequence
        tokens = [int(message_type)]
        if custom_tokens:
            tokens.extend(custom_tokens)

        message = {
            "tokens": tokens,
            "payload": payload or {},
            "timestamp": int(time.time()),
            "protocol_version": PROTOCOL_VERSION
        }

        # Apply v2 extensions
        if compress:
            compress_message(message)

        if sign and secret_key:
            sign_message(message, secret_key)

        if encrypt and secret_key:
            encrypt_message(message, secret_key)

        return message

    def encode_task_complete(self, result: str, summary: str = "",
                              compress: bool = False, sign: bool = False,
                              secret_key: Optional[bytes] = None) -> Dict[str, Any]:
        """Encode a task-complete message (token 42)."""
        return self.encode("42", {"result": result, "summary": summary},
                           compress=compress, sign=sign, secret_key=secret_key)

    def encode_error(self, error_code: str, message: str,
                     details: str = "", compress: bool = False,
                     sign: bool = False, secret_key: Optional[bytes] = None) -> Dict[str, Any]:
        """Encode an error message (token 0)."""
        return self.encode("0", {
            "error_code": error_code, "message": message, "details": details
        }, compress=compress, sign=sign, secret_key=secret_key)

    def encode_request(self, action: str, description: str,
                       attention: str = "medium", compress: bool = False,
                       sign: bool = False, secret_key: Optional[bytes] = None) -> Dict[str, Any]:
        """Encode a request message (token 100 + attention 151)."""
        return self.encode("100", {
            "action": action, "description": description
        }, custom_tokens=[151], compress=compress, sign=sign, secret_key=secret_key)

    def encode_status(self, status: str, location: str = "",
                       compress: bool = False, sign: bool = False,
                       secret_key: Optional[bytes] = None) -> Dict[str, Any]:
        """Encode a status message (token 103 for spatial anchor)."""
        return self.encode("103", {
            "status": status, "location": location
        }, compress=compress, sign=sign, secret_key=secret_key)

    def encode_separator(self, separator_type: str = "task",
                          next_topic: str = "", compress: bool = False,
                          sign: bool = False, secret_key: Optional[bytes] = None) -> Dict[str, Any]:
        """Encode a separator/boundary message (token 153)."""
        return self.encode("153", {
            "separator_type": separator_type, "next_topic": next_topic
        }, compress=compress, sign=sign, secret_key=secret_key)

    def encode_attention(self, level: str = "high",
                          focus: str = "", compress: bool = False,
                          sign: bool = False, secret_key: Optional[bytes] = None) -> Dict[str, Any]:
        """Encode an attention-request message (token 151)."""
        return self.encode("151", {
            "attention_level": level, "focus": focus
        }, compress=compress, sign=sign, secret_key=secret_key)

    def encode_causal(self, cause: str, effect: str,
                      depends_on: Optional[List[int]] = None, compress: bool = False,
                      sign: bool = False, secret_key: Optional[bytes] = None) -> Dict[str, Any]:
        """Encode a causal relationship (token 104)."""
        return self.encode("104", {
            "cause": cause, "effect": effect, "depends_on": depends_on or []
        }, compress=compress, sign=sign, secret_key=secret_key)

    def encode_start_boundary(self, message_id: str,
                               direction: str = "request", compress: bool = False,
                               sign: bool = False, secret_key: Optional[bytes] = None) -> Dict[str, Any]:
        """Encode a message start boundary (token 150)."""
        return self.encode("150", {
            "message_id": message_id, "direction": direction
        }, compress=compress, sign=sign, secret_key=secret_key)

    def encode_batch(self, messages: List[Dict[str, Any]], compress: bool = False,
                      sign: bool = False, secret_key: Optional[bytes] = None) -> Dict[str, Any]:
        """Encode multiple messages into a single batch.

        Args:
            messages: List of message dicts to batch
            compress: Whether to compress each message
            sign: Whether to sign each message
            secret_key: Shared secret for signing/encryption

        Returns:
            A single batch message dict
        """
        encoded_messages = []
        for msg in messages:
            encoded = dict(msg)
            if compress and secret_key:
                pipeline_encode(encoded, secret_key=secret_key, compress=compress)
            elif compress:
                compress_message(encoded)
            elif sign and secret_key:
                sign_message(encoded, secret_key)
            encoded_messages.append(encoded)

        return batch_encode(encoded_messages)


class MessageDecoder:
    """Decodes token sequences into human-readable interpretations.

    v2 adds decompression, signature verification, decryption,
    and batch decoding support.
    """

    def __init__(self, registry: Optional[TokenRegistry] = None):
        self.registry = registry or get_registry()

    def decode(self, message: Dict[str, Any],
               secret_key: Optional[bytes] = None,
               verify: bool = False,
               decompress: bool = False) -> Dict[str, Any]:
        """Decode a full message dict into a human-readable format.

        Args:
            message: Dict with "tokens", "payload", and optional fields
            secret_key: Optional key for signature verification/decryption
            verify: Whether to verify HMAC signature
            decompress: Whether to decompress the payload

        Returns:
            Dict with interpretation, routing info, and formatted text
        """
        tokens = message.get("tokens", [])
        payload = message.get("payload", {})

        # Handle batch messages
        if isinstance(payload, dict) and payload.get(METADATA_BATCH):
            return self._decode_batch(message, secret_key)

        # Decompress if requested or if payload has compressed metadata
        if decompress or (isinstance(payload, dict) and payload.get("__compressed")):
            message = decompress_message(message)

        # Decrypt if encrypted
        if secret_key and (isinstance(payload, dict) and payload.get("__encrypted")):
            message = decrypt_message(message, secret_key)

        # Verify signature if requested
        if verify and secret_key:
            valid = verify_signature(message, secret_key)
            message["signature_verified"] = valid
            if not valid:
                raise SigningError("Signature verification failed")

        # Get routing instructions
        routing = self.registry.route_message(tokens)

        # Build interpretation for each token
        token_interpretations = []
        for token_id in tokens:
            token_data = self.registry.get_token(token_id)
            if token_data:
                token_interpretations.append({
                    "token_id": token_id,
                    "meaning": token_data.get("meaning", ""),
                    "category": token_data.get("category", ""),
                    "protocol_type": token_data.get("protocol_type", ""),
                    "context": token_data.get("context", "")
                })

        # Build the full human-readable interpretation
        interpretation = {
            "tokens": tokens,
            "routing": routing,
            "payload": message.get("payload", {}),
            "token_chain": token_interpretations,
            "summary": self._build_summary(tokens, message.get("payload", {})),
            "timestamp": message.get("timestamp"),
            "protocol_version": message.get("protocol_version", "1.0")
        }

        # Add v2 metadata
        if message.get(METADATA_SIGNED):
            interpretation["signed"] = True
        if message.get(METADATA_COMPRESSED):
            interpretation["compressed"] = True
        if message.get(METADATA_ENCRYPTED):
            interpretation["encrypted"] = True

        return interpretation

    def _decode_batch(self, batch_message: Dict[str, Any],
                       secret_key: Optional[bytes] = None) -> List[Dict[str, Any]]:
        """Decode a batch message."""
        try:
            messages = batch_decode(batch_message)
        except BatchError as e:
            raise BatchError(f"Failed to decode batch: {e}")

        # Decrypt/verify each message if key provided
        decoded = []
        for msg in messages:
            if secret_key:
                if msg.get(METADATA_ENCRYPTED):
                    msg = decrypt_message(msg, secret_key)
                if msg.get(METADATA_SIGNED):
                    verify_signature(msg, secret_key)
            decoded.append(self.decode(msg))

        return decoded

    def decode_tokens(self, token_ids: List[int]) -> Dict[str, Any]:
        """Decode just a list of token IDs (no payload)."""
        return self.decode({"tokens": token_ids, "payload": {}})

    def decode_batch(self, batch_message: Dict[str, Any],
                      secret_key: Optional[bytes] = None) -> List[Dict[str, Any]]:
        """Decode a batch message into individual decoded messages.

        Args:
            batch_message: A batch message created by batch_encode()
            secret_key: Optional key for decryption/verification

        Returns:
            List of decoded message dicts
        """
        return self._decode_batch(batch_message, secret_key)

    def _build_summary(self, tokens: List[int], payload: Dict) -> str:
        """Build a concise human-readable summary."""
        if not tokens:
            return "Empty message"

        first = self.registry.get_token(tokens[0])
        if not first:
            return f"Unknown token sequence: {tokens}"

        meaning = first.get("meaning", "Unknown")[:80]
        category = first.get("category", "unknown")

        if payload:
            payload_str = ", ".join(f"{k}={v}" for k, v in payload.items() if v)
            return f"[{category}] {meaning} | Payload: {payload_str}"

        return f"[{category}] {meaning}"

    def format_for_display(self, interpretation: Dict[str, Any]) -> str:
        """Format a decoded message for terminal display."""
        lines = []
        lines.append("=" * 60)
        lines.append(f"  TokenRelay Message (v{interpretation.get('protocol_version', '1.0')})")
        lines.append("=" * 60)

        tokens = interpretation["tokens"]
        lines.append(f"  Tokens: {tokens}")
        lines.append(f"  Action: {interpretation['routing']['action']}")
        lines.append(f"  Priority: {interpretation['routing']['priority']}")

        # Show v2 metadata
        if interpretation.get("compressed"):
            ratio = interpretation.get("compression_ratio", "N/A")
            lines.append(f"  Compressed: Yes (ratio: {ratio})")
        if interpretation.get("signed"):
            lines.append(f"  Signed: Yes")
        if interpretation.get("encrypted"):
            lines.append(f"  Encrypted: Yes")

        if interpretation.get("payload"):
            lines.append(f"  Payload: {json.dumps(interpretation['payload'], indent=4)}")

        lines.append(f"  Summary: {interpretation['summary']}")

        if interpretation.get("timestamp"):
            lines.append(f"  Timestamp: {interpretation['timestamp']}")

        lines.append("=" * 60)
        return "\n".join(lines)


# ──────────────────────────────────────────────
# Convenience functions (v1 compatible)
# ──────────────────────────────────────────────
def encode_message(message_type: str, payload: Optional[Dict] = None,
                   registry: Optional[TokenRegistry] = None) -> Dict[str, Any]:
    """Quick encode a message."""
    encoder = MessageEncoder(registry)
    return encoder.encode(message_type, payload)


def decode_message(message: Dict[str, Any],
                   registry: Optional[TokenRegistry] = None) -> Dict[str, Any]:
    """Quick decode a message."""
    decoder = MessageDecoder(registry)
    return decoder.decode(message)


def format_message(message: Dict[str, Any],
                   registry: Optional[TokenRegistry] = None) -> str:
    """Quick format a message for display."""
    decoder = MessageDecoder(registry)
    interpretation = decoder.decode(message)
    return decoder.format_for_display(interpretation)


def compress_message_wrapper(message: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience wrapper for compress_message()."""
    return compress_message(message)


def decompress_message_wrapper(message: Dict[str, Any]) -> Dict[str, Any]:
    """Convenience wrapper for decompress_message()."""
    return decompress_message(message)


# Re-export v2 functions at module level for easy access
__all__ = [
    # v1 compatibility
    "MessageEncoder", "MessageDecoder",
    "encode_message", "decode_message", "format_message",
    # v2 compression
    "compress_message", "decompress_message",
    # v2 signing
    "sign_message", "verify_signature",
    # v2 encryption
    "encrypt_message", "decrypt_message",
    # v2 batching
    "batch_encode", "batch_decode",
    # v2 benchmarks
    "measure_compression_ratio", "benchmark_compression",
    # v2 pipeline
    "pipeline_encode", "pipeline_decode",
    # v2 exceptions
    "CompressionError", "SigningError", "EncryptionError", "BatchError",
    # v2 constants
    "PROTOCOL_VERSION", "V1_VERSION",
]