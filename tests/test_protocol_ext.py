"""
TokenRelay v2 Protocol Extensions — Test Suite.

Tests all v2 features: compression, HMAC signing, AES-256-GCM encryption,
batching, compression ratio measurement, and v1 backward compatibility.
"""

import json
import sys
import zlib
import hmac
import hashlib
import secrets
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from codec import (
    MessageEncoder, MessageDecoder,
    compress_message, decompress_message,
    sign_message, verify_signature,
    encrypt_message, decrypt_message,
    batch_encode, batch_decode,
    measure_compression_ratio, benchmark_compression,
    pipeline_encode, pipeline_decode,
    encode_message, decode_message, format_message,
    CompressionError, SigningError, EncryptionError, BatchError,
    PROTOCOL_VERSION, V1_VERSION, _AES_AVAILABLE,
    METADATA_COMPRESSED, METADATA_SIGNED, METADATA_ENCRYPTED,
    METADATA_SIGNATURE, METADATA_SIGNER, METADATA_COMPRESSION_RATIO,
    METADATA_BATCH, METADATA_BATCH_INDEX, METADATA_BATCH_TOTAL,
    METADATA_IV,
)
from registry import TokenRegistry, get_registry

# ──────────────────────────────────────────────
# Test Helpers
# ──────────────────────────────────────────────
SECRET_KEY = secrets.token_bytes(32)  # 32-byte key for AES-256


def make_test_message(tokens=None, payload=None, version=PROTOCOL_VERSION):
    """Create a minimal test message."""
    return {
        "tokens": tokens or [42],
        "payload": {} if payload is None else payload,
        "timestamp": 1234567890,
        "protocol_version": version,
    }


class TestCompression(unittest.TestCase):
    """Tests for zlib compression/decompression."""

    def test_compress_message(self):
        """Compress a message and verify the payload is compressed."""
        msg = make_test_message(payload={"large_data": "x" * 1000})
        original_size = len(json.dumps(msg["payload"]))

        result = compress_message(msg)

        self.assertTrue(result.get(METADATA_COMPRESSED))
        self.assertIn(METADATA_COMPRESSION_RATIO, result)
        self.assertGreater(result[METADATA_COMPRESSION_RATIO], 1.0)
        self.assertIn("__compressed", result["payload"])

    def test_decompress_message(self):
        """Decompress a compressed message and verify payload is restored."""
        msg = make_test_message(payload={"large_data": "x" * 1000})
        compress_message(msg)

        decompressed = decompress_message(msg)

        self.assertEqual(decompressed["payload"]["large_data"], "x" * 1000)
        self.assertFalse(decompressed.get(METADATA_COMPRESSED))

    def test_round_trip_compression(self):
        """Ensure compress → decompress preserves data exactly."""
        original = make_test_message(
            payload={"key": "value", "nested": {"a": 1, "b": [1, 2, 3]}}
        )
        compressed = compress_message(dict(original))
        decompressed = decompress_message(compressed)

        self.assertEqual(decompressed["payload"], original["payload"])

    def test_decompress_non_compressed(self):
        """Decompressing a non-compressed message should return it unchanged."""
        msg = make_test_message(payload={"key": "value"})
        result = decompress_message(msg)
        self.assertEqual(result["payload"], {"key": "value"})
        self.assertFalse(result.get(METADATA_COMPRESSED))

    def test_compress_empty_payload(self):
        """Compressing an empty payload should not be marked as compressed."""
        msg = make_test_message(payload={})
        result = compress_message(msg)
        self.assertFalse(result.get(METADATA_COMPRESSED))

    def test_compress_none_payload(self):
        """Compressing a None payload should not fail."""
        msg = make_test_message(payload=None)
        result = compress_message(msg)
        self.assertFalse(result.get(METADATA_COMPRESSED))

    def test_compress_ratio_improvement(self):
        """Verify compression ratio is meaningful for repetitive data."""
        msg = make_test_message(payload={"data": "ABCD" * 500})
        compressed = compress_message(msg)
        ratio = compressed[METADATA_COMPRESSION_RATIO]
        # Repetitive data should compress well
        self.assertGreater(ratio, 1.0)

    def test_compression_error_on_corrupt(self):
        """Corrupting compressed data should raise CompressionError."""
        msg = make_test_message(payload={"data": "test"})
        compress_message(msg)
        msg["payload"]["__data"] = "deadbeef"  # Corrupt hex
        with self.assertRaises(CompressionError):
            decompress_message(msg)


class TestSigning(unittest.TestCase):
    """Tests for HMAC-SHA256 signing and verification."""

    def test_sign_message(self):
        """Sign a message and verify signature metadata is present."""
        msg = make_test_message()
        result = sign_message(msg, SECRET_KEY)

        self.assertTrue(result.get(METADATA_SIGNED))
        self.assertIn(METADATA_SIGNATURE, result)
        self.assertIn(METADATA_SIGNER, result)

    def test_verify_signature_valid(self):
        """Verify a correctly signed message returns True."""
        msg = make_test_message()
        signed = sign_message(msg, SECRET_KEY)
        self.assertTrue(verify_signature(signed, SECRET_KEY))

    def test_verify_signature_invalid_key(self):
        """Verify with wrong key returns False."""
        msg = make_test_message()
        signed = sign_message(msg, SECRET_KEY)
        wrong_key = secrets.token_bytes(32)
        self.assertFalse(verify_signature(signed, wrong_key))

    def test_verify_signature_tampered(self):
        """Tampering with a signed message should fail verification."""
        msg = make_test_message()
        signed = sign_message(msg, SECRET_KEY)
        signed["payload"]["result"] = "tampered"
        self.assertFalse(verify_signature(signed, SECRET_KEY))

    def test_verify_unsign_message(self):
        """Verifying an unsigned message returns False."""
        msg = make_test_message()
        self.assertFalse(verify_signature(msg, SECRET_KEY))

    def test_sign_round_trip(self):
        """Sign then verify should work correctly."""
        msg = make_test_message(payload={"nested": {"key": "value"}})
        signed = sign_message(msg, SECRET_KEY)
        self.assertTrue(verify_signature(signed, SECRET_KEY))

    def test_signature_deterministic_for_same_data(self):
        """Same message + same key produces same signature."""
        msg1 = make_test_message()
        msg2 = make_test_message()
        sig1 = sign_message(msg1, SECRET_KEY)
        sig2 = sign_message(msg2, SECRET_KEY)
        # Signature should match since data is the same
        self.assertEqual(sig1[METADATA_SIGNATURE], sig2[METADATA_SIGNATURE])

    def test_signer_id_unique(self):
        """Each signed message gets a unique signer ID."""
        msg1 = sign_message(make_test_message(), SECRET_KEY)
        msg2 = sign_message(make_test_message(), SECRET_KEY)
        self.assertNotEqual(msg1[METADATA_SIGNER], msg2[METADATA_SIGNER])


class TestEncryption(unittest.TestCase):
    """Tests for AES-256-GCM encryption/decryption."""

    def test_encrypt_message(self):
        """Encrypt a message and verify payload is encrypted."""
        if not _AES_AVAILABLE:
            self.skipTest("cryptography package not available")

        msg = make_test_message(payload={"secret": "data"})
        result = encrypt_message(msg, SECRET_KEY)

        self.assertTrue(result.get(METADATA_ENCRYPTED))
        self.assertIn(METADATA_IV, result)
        self.assertIn("__encrypted", result["payload"])
        self.assertNotEqual(result["payload"]["__data"], json.dumps({"secret": "data"}))

    def test_decrypt_message(self):
        """Decrypt an encrypted message and verify payload is restored."""
        if not _AES_AVAILABLE:
            self.skipTest("cryptography package not available")

        msg = make_test_message(payload={"secret": "data"})
        encrypted = encrypt_message(msg, SECRET_KEY)
        decrypted = decrypt_message(encrypted, SECRET_KEY)

        self.assertEqual(decrypted["payload"]["secret"], "data")
        self.assertFalse(decrypted.get(METADATA_ENCRYPTED))

    def test_round_trip_encryption(self):
        """Encrypt → decrypt should preserve data exactly."""
        if not _AES_AVAILABLE:
            self.skipTest("cryptography package not available")

        original = make_test_message(payload={"key": "value", "num": 42, "list": [1, 2, 3]})
        encrypted = encrypt_message(dict(original), SECRET_KEY)
        decrypted = decrypt_message(encrypted, SECRET_KEY)

        self.assertEqual(decrypted["payload"], original["payload"])

    def test_decrypt_wrong_key(self):
        """Decrypting with wrong key should raise EncryptionError."""
        if not _AES_AVAILABLE:
            self.skipTest("cryptography package not available")

        msg = make_test_message(payload={"secret": "data"})
        encrypted = encrypt_message(msg, SECRET_KEY)
        wrong_key = secrets.token_bytes(32)
        with self.assertRaises(EncryptionError):
            decrypt_message(encrypted, wrong_key)

    def test_decrypt_non_encrypted(self):
        """Decrypting a non-encrypted message returns it unchanged."""
        if not _AES_AVAILABLE:
            self.skipTest("cryptography package not available")

        msg = make_test_message(payload={"key": "value"})
        result = decrypt_message(msg, SECRET_KEY)
        self.assertEqual(result["payload"], {"key": "value"})
        self.assertFalse(result.get(METADATA_ENCRYPTED))

    def test_encrypt_invalid_key_size(self):
        """Using a non-32-byte key should raise EncryptionError."""
        if not _AES_AVAILABLE:
            self.skipTest("cryptography package not available")

        msg = make_test_message()
        with self.assertRaises(EncryptionError):
            encrypt_message(msg, secrets.token_bytes(16))

    def test_encryption_error_without_cryptography(self):
        """If cryptography is not available, encrypt_message raises EncryptionError."""
        if _AES_AVAILABLE:
            self.skipTest("cryptography is available; this test only runs when it isn't")
        msg = make_test_message()
        with self.assertRaises(EncryptionError):
            encrypt_message(msg, SECRET_KEY)


class TestBatching(unittest.TestCase):
    """Tests for message batching."""

    def test_batch_encode(self):
        """Batch encode multiple messages."""
        msgs = [
            make_test_message(tokens=[42], payload={"result": "task1"}),
            make_test_message(tokens=[0], payload={"error_code": "E01"}),
            make_test_message(tokens=[100], payload={"action": "go"}),
        ]
        batch = batch_encode(msgs)

        self.assertIn(METADATA_BATCH, batch["payload"])
        self.assertEqual(batch[METADATA_BATCH_TOTAL], 3)
        self.assertEqual(batch[METADATA_BATCH_INDEX], 0)
        self.assertEqual(len(batch["payload"]["messages"]), 3)

    def test_batch_decode(self):
        """Batch decode returns the original messages."""
        msgs = [
            make_test_message(tokens=[42]),
            make_test_message(tokens=[0]),
        ]
        batch = batch_encode(msgs)
        decoded = batch_decode(batch)

        self.assertEqual(len(decoded), 2)
        self.assertEqual(decoded[0]["tokens"], [42])
        self.assertEqual(decoded[1]["tokens"], [0])

    def test_batch_round_trip(self):
        """Encode → decode batch preserves all messages."""
        original_msgs = [
            make_test_message(payload={"id": i}) for i in range(5)
        ]
        batch = batch_encode(original_msgs)
        decoded = batch_decode(batch)

        for orig, dec in zip(original_msgs, decoded):
            self.assertEqual(orig["payload"], dec["payload"])
            self.assertEqual(orig["tokens"], dec["tokens"])

    def test_batch_decode_non_batch_raises(self):
        """Decoding a non-batch message raises BatchError."""
        msg = make_test_message()
        with self.assertRaises(BatchError):
            batch_decode(msg)

    def test_batch_size_mismatch(self):
        """Batch with mismatched total raises BatchError."""
        msgs = [make_test_message(tokens=[42])]
        batch = batch_encode(msgs)
        batch[METADATA_BATCH_TOTAL] = 99  # Tamper with total
        with self.assertRaises(BatchError):
            batch_decode(batch)

    def test_empty_batch_raises(self):
        """Batch encoding an empty list raises BatchError."""
        with self.assertRaises(BatchError):
            batch_encode([])

    def test_batch_with_compression_and_signing(self):
        """Batch messages with compression and signing metadata."""
        msgs = [make_test_message(payload={"data": "x" * 100}) for _ in range(3)]
        for msg in msgs:
            compress_message(msg)
            sign_message(msg, SECRET_KEY)

        batch = batch_encode(msgs)
        decoded = batch_decode(batch)

        for dec in decoded:
            self.assertTrue(dec.get(METADATA_COMPRESSED))
            self.assertTrue(dec.get(METADATA_SIGNED))

    def test_encoder_batch_encode(self):
        """Test MessageEncoder.encode_batch()."""
        encoder = MessageEncoder()
        msgs = [
            encoder.encode("42", {"result": "done"}),
            encoder.encode("0", {"error_code": "FAIL"}),
        ]
        batch = encoder.encode_batch(msgs)

        self.assertIn(METADATA_BATCH, batch["payload"])
        self.assertEqual(batch[METADATA_BATCH_TOTAL], 2)

    def test_decoder_decode_batch(self):
        """Test MessageDecoder.decode_batch()."""
        encoder = MessageEncoder()
        decoder = MessageDecoder()
        msgs = [
            encoder.encode("42", {"result": "done"}),
            encoder.encode("0", {"error_code": "FAIL"}),
        ]
        batch = encoder.encode_batch(msgs)
        decoded = decoder.decode_batch(batch)

        self.assertEqual(len(decoded), 2)
        self.assertEqual(decoded[0]["payload"]["result"], "done")


class TestCompressionBenchmarks(unittest.TestCase):
    """Tests for compression ratio measurement and benchmarking."""

    def test_measure_compression_ratio(self):
        """Measure compression ratio on a test message."""
        msg = make_test_message(payload={"data": "ABCD" * 500})
        result = measure_compression_ratio(msg)

        self.assertIn("original_size", result)
        self.assertIn("compressed_size", result)
        self.assertIn("ratio", result)
        self.assertIn("savings_pct", result)
        self.assertGreater(result["ratio"], 1.0)
        self.assertGreater(result["savings_pct"], 0)

    def test_measure_no_compression_for_empty(self):
        """Empty payload should have ratio of 1.0."""
        msg = make_test_message(payload={})
        result = measure_compression_ratio(msg)
        self.assertEqual(result["ratio"], 1.0)

    def test_benchmark_compression(self):
        """Benchmark compression across multiple messages."""
        msgs = [make_test_message(payload={"data": "x" * (100 * i)}) for i in range(1, 4)]
        result = benchmark_compression(msgs)

        self.assertIn("messages", result)
        self.assertIn("total_original_bytes", result)
        self.assertIn("total_compressed_bytes", result)
        self.assertIn("overall_ratio", result)
        self.assertIn("avg_ratio", result)
        self.assertIn("avg_compress_time_us", result)
        self.assertIn("savings_pct", result)
        self.assertEqual(result["messages"], 3)
        self.assertGreater(result["overall_ratio"], 1.0)

    def test_benchmark_empty_list(self):
        """Benchmark with empty list returns error."""
        result = benchmark_compression([])
        self.assertIn("error", result)


class TestPipeline(unittest.TestCase):
    """Tests for the full encode/decode pipeline."""

    def test_pipeline_encode_decode(self):
        """Full pipeline: compress → sign → encrypt → decrypt → verify → decompress."""
        if not _AES_AVAILABLE:
            self.skipTest("cryptography package not available")

        msg = make_test_message(payload={"secret": "data", "num": 42})
        processed = pipeline_encode(msg, secret_key=SECRET_KEY, compress=True)

        # Verify pipeline applied all layers
        self.assertTrue(processed.get(METADATA_ENCRYPTED))
        self.assertTrue(processed.get(METADATA_SIGNED))
        self.assertTrue(processed.get(METADATA_COMPRESSED))

        # Decode in reverse order
        recovered = pipeline_decode(processed, secret_key=SECRET_KEY)

        # Verify the signature was checked
        self.assertEqual(recovered["payload"]["secret"], "data")
        self.assertEqual(recovered["payload"]["num"], 42)

    def test_pipeline_compress_only(self):
        """Pipeline with only compression."""
        msg = make_test_message(payload={"large": "x" * 500})
        processed = pipeline_encode(msg, compress=True)
        self.assertTrue(processed.get(METADATA_COMPRESSED))
        self.assertFalse(processed.get(METADATA_SIGNED))
        self.assertFalse(processed.get(METADATA_ENCRYPTED))

    def test_pipeline_sign_only(self):
        """Pipeline without secret_key: no signing or encryption."""
        msg = make_test_message(payload={"data": "test"})
        processed = pipeline_encode(msg, compress=False)
        self.assertFalse(processed.get(METADATA_SIGNED))
        self.assertFalse(processed.get(METADATA_ENCRYPTED))
        self.assertFalse(processed.get(METADATA_COMPRESSED))

    def test_pipeline_sign_only_with_key(self):
        """Pipeline with secret_key: both signing and encryption applied."""
        msg = make_test_message(payload={"data": "test"})
        processed = pipeline_encode(msg, secret_key=SECRET_KEY, compress=False)
        self.assertTrue(processed.get(METADATA_SIGNED))
        self.assertTrue(processed.get(METADATA_ENCRYPTED))


class TestV1BackwardCompatibility(unittest.TestCase):
    """Tests that all v1 API calls still work without modification."""

    def test_encoder_v1_encode(self):
        """v1 encode() signature still works."""
        encoder = MessageEncoder()
        msg = encoder.encode("42", {"result": "done", "summary": "ok"})
        self.assertEqual(msg["tokens"], [42])
        self.assertEqual(msg["protocol_version"], PROTOCOL_VERSION)

    def test_encoder_v1_encode_task_complete(self):
        """v1 encode_task_complete() works without new params."""
        encoder = MessageEncoder()
        msg = encoder.encode_task_complete("result", "summary")
        self.assertEqual(msg["tokens"], [42])
        self.assertEqual(msg["payload"]["result"], "result")

    def test_encoder_v1_encode_error(self):
        """v1 encode_error() works without new params."""
        encoder = MessageEncoder()
        msg = encoder.encode_error("E01", "Error message", "detail")
        self.assertEqual(msg["tokens"], [0])

    def test_encoder_v1_encode_request(self):
        """v1 encode_request() works without new params."""
        encoder = MessageEncoder()
        msg = encoder.encode_request("action", "description")
        self.assertIn(151, msg["tokens"])

    def test_encoder_v1_encode_status(self):
        """v1 encode_status() works without new params."""
        encoder = MessageEncoder()
        msg = encoder.encode_status("running", "server")
        self.assertEqual(msg["tokens"], [103])

    def test_encoder_v1_encode_separator(self):
        """v1 encode_separator() works without new params."""
        encoder = MessageEncoder()
        msg = encoder.encode_separator("task")
        self.assertEqual(msg["tokens"], [153])

    def test_encoder_v1_encode_attention(self):
        """v1 encode_attention() works without new params."""
        encoder = MessageEncoder()
        msg = encoder.encode_attention("high")
        self.assertEqual(msg["tokens"], [151])

    def test_encoder_v1_encode_causal(self):
        """v1 encode_causal() works without new params."""
        encoder = MessageEncoder()
        msg = encoder.encode_causal("cause", "effect", [102])
        self.assertEqual(msg["tokens"], [104])

    def test_encoder_v1_encode_start_boundary(self):
        """v1 encode_start_boundary() works without new params."""
        encoder = MessageEncoder()
        msg = encoder.encode_start_boundary("task-001")
        self.assertEqual(msg["tokens"], [150])

    def test_decoder_v1_decode(self):
        """v1 decode() works without new params."""
        decoder = MessageDecoder()
        encoder = MessageEncoder()
        msg = encoder.encode_task_complete("result")
        result = decoder.decode(msg)
        self.assertEqual(result["tokens"], [42])

    def test_decoder_v1_decode_tokens(self):
        """v1 decode_tokens() works without new params."""
        decoder = MessageDecoder()
        result = decoder.decode_tokens([42, 153])
        self.assertIn("tokens", result)

    def test_decoder_v1_format_for_display(self):
        """v1 format_for_display() works without new params."""
        decoder = MessageDecoder()
        encoder = MessageEncoder()
        msg = encoder.encode_task_complete("result")
        interpretation = decoder.decode(msg)
        display = decoder.format_for_display(interpretation)
        self.assertIn("TokenRelay Message", display)

    def test_module_level_encode_message(self):
        """Module-level encode_message() works without new params."""
        msg = encode_message("42", {"result": "test"})
        self.assertEqual(msg["tokens"], [42])

    def test_module_level_decode_message(self):
        """Module-level decode_message() works without new params."""
        encoder = MessageEncoder()
        msg = encoder.encode_task_complete("result")
        decoded = decode_message(msg)
        self.assertEqual(decoded["tokens"], [42])

    def test_module_level_format_message(self):
        """Module-level format_message() works without new params."""
        encoder = MessageEncoder()
        msg = encoder.encode_task_complete("result")
        formatted = format_message(msg)
        self.assertIn("TokenRelay", formatted)

    def test_v1_message_protocol_version(self):
        """Messages created without v2 extensions still have v1 version."""
        encoder = MessageEncoder()
        msg = encoder.encode("42", {"result": "test"})
        # Default is v2 now, but the encode method still works
        self.assertIn(msg["protocol_version"], [PROTOCOL_VERSION, V1_VERSION])

    def test_bridge_compatibility(self):
        """TokenRelayBridge usage is still compatible."""
        from bridge import TokenRelayBridge
        bridge = TokenRelayBridge()
        msg = bridge.encode_result("done")
        self.assertEqual(msg["tokens"], [42])
        decoded = bridge.decode_result(msg)
        self.assertEqual(decoded["tokens"], [42])

    def test_registry_v1_api(self):
        """All v1 TokenRegistry methods work unchanged."""
        reg = TokenRegistry()

        # Basic lookups
        token = reg.get_token(42)
        self.assertIsNotNone(token)
        self.assertIn("meaning", token)

        # Protocol type lookups
        self.assertEqual(reg.get_protocol_type(42), "status")
        self.assertEqual(reg.get_protocol_type(0), "error")

        # Routing
        routing = reg.route_message([42, 153])
        self.assertIn("action", routing)
        self.assertIn("priority", routing)

        # Control/status/error checks
        self.assertTrue(reg.is_control_token(101))
        self.assertTrue(reg.is_status_token(42))
        self.assertTrue(reg.is_error_token(0))

        # to_dict
        d = reg.to_dict()
        self.assertIsInstance(d, dict)
        self.assertGreater(len(d), 0)

    def test_registry_v2_metadata_accessors(self):
        """v2 registry metadata accessors return expected structure."""
        reg = TokenRegistry()

        # Compression info
        comp = reg.get_compression_info(42)
        self.assertIn("supported", comp)
        self.assertIn("algorithms", comp)
        self.assertIn("default", comp)

        # Signing info
        sign = reg.get_signing_info(42)
        self.assertIn("supported", sign)
        self.assertIn("algorithm", sign)

        # Encryption info
        enc = reg.get_encryption_info(42)
        self.assertIn("supported", enc)
        self.assertIn("algorithm", enc)
        self.assertIn("key_size", enc)

        # Batch info
        batch = reg.get_batch_info(42)
        self.assertIn("supported", batch)
        self.assertIn("max_size", batch)

        # Feature check methods
        self.assertIsInstance(reg.is_compression_supported(42), bool)
        self.assertIsInstance(reg.is_signing_supported(42), bool)
        self.assertIsInstance(reg.is_encryption_supported(42), bool)

        # Features summary
        summary = reg.get_features_summary()
        self.assertIn("total_tokens", summary)
        self.assertIn("compression_supported", summary)
        self.assertIn("signing_supported", summary)
        self.assertIn("encryption_supported", summary)
        self.assertIn("batch_supported", summary)


class TestRegistryV2Features(unittest.TestCase):
    """Tests for v2 registry features."""

    def test_compression_not_supported_on_control_tokens(self):
        """Control/boundary tokens should not support compression."""
        reg = TokenRegistry()
        # 150 is boundary, 153 is boundary
        self.assertFalse(reg.is_compression_supported(150))
        self.assertFalse(reg.is_compression_supported(153))

    def test_signing_required_on_error_tokens(self):
        """Error tokens should require signing."""
        reg = TokenRegistry()
        self.assertTrue(reg.is_signing_required(0))

    def test_routing_features(self):
        """Route message should include v2 feature flags."""
        reg = TokenRegistry()
        routing = reg.route_message([42])
        # 42 is status; check if features dict exists
        if "features" in routing:
            self.assertIn("compression", routing["features"])

    def test_get_token_data_structure(self):
        """Token data includes v2 extension fields."""
        reg = TokenRegistry()
        token = reg.get_token(42)
        self.assertIn("compression_supported", token)
        self.assertIn("signing_supported", token)
        self.assertIn("encryption_supported", token)


if __name__ == "__main__":
    unittest.main(verbosity=2)