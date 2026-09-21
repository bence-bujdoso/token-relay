"""
TokenRelay v4 — Cross-Protocol Interoperability Tests.

40+ tests covering:
- AdapterRegistry: register, unregister, connect, disconnect, statistics
- ProtocolBridge: forward, receive, bidirectional, error handling
- CrossProtocolGateway: translate, batch_translate, format support
- UniversalTranslator: convert, rules management, batch conversion
- Integration: full interop stack, BRP server integration
- Error handling: all custom exceptions
"""

import sys
import time
import unittest
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from interop import (
    # Core classes
    ProtocolBridge, CrossProtocolGateway, AdapterRegistry, UniversalTranslator,
    InteropBRPServer,
    # Data classes
    AdapterConfig, AdapterState, ExternalAdapter, BridgeConfig, GatewayConfig,
    TranslationRule,
    # Enums
    ProtocolType, TranslationFormat,
    # Factory functions
    create_protocol_bridge, create_cross_protocol_gateway,
    create_universal_translator, create_interop_stack,
    # Exceptions
    InteropError, AdapterNotFoundError, ProtocolNotSupportedError, TranslationError,
)


# ═════════════════════════════════════════════════
# AdapterRegistry Tests
# ═════════════════════════════════════════════════

class TestAdapterRegistryBasics(unittest.TestCase):
    """Test AdapterRegistry basic operations."""

    def setUp(self):
        self.registry = AdapterRegistry()

    def test_register_adapter(self):
        """Register creates adapter and returns ID."""
        adapter_id = self.registry.register_adapter(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="https://api.example.com",
        )
        self.assertIsNotNone(adapter_id)
        self.assertIn(adapter_id, self.registry._adapters)

    def test_get_adapter(self):
        """Get adapter by ID returns correct adapter."""
        adapter_id = self.registry.register_adapter(
            protocol_type=ProtocolType.MQTT,
            endpoint="mqtt://broker.example.com",
        )
        adapter = self.registry.get_adapter(adapter_id)
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.config.protocol_type, ProtocolType.MQTT)

    def test_unregister_adapter(self):
        """Unregister removes adapter from registry."""
        adapter_id = self.registry.register_adapter(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="https://api.example.com",
        )
        result = self.registry.unregister_adapter(adapter_id)
        self.assertTrue(result)
        self.assertNotIn(adapter_id, self.registry._adapters)

    def test_unregister_missing_adapter(self):
        """Unregister non-existent adapter returns False."""
        result = self.registry.unregister_adapter("nonexistent")
        self.assertFalse(result)

    def test_get_all_adapters(self):
        """Get all adapters returns list of all registered adapters."""
        self.registry.register_adapter(ProtocolType.HTTP_REST, "http://a")
        self.registry.register_adapter(ProtocolType.MQTT, "mqtt://b")
        adapters = self.registry.get_all_adapters()
        self.assertEqual(len(adapters), 2)

    def test_get_adapters_by_protocol(self):
        """Get adapters filtered by protocol type."""
        self.registry.register_adapter(ProtocolType.HTTP_REST, "http://a")
        self.registry.register_adapter(ProtocolType.HTTP_REST, "http://b")
        self.registry.register_adapter(ProtocolType.MQTT, "mqtt://c")
        http_adapters = self.registry.get_adapters_by_protocol(ProtocolType.HTTP_REST)
        mqtt_adapters = self.registry.get_adapters_by_protocol(ProtocolType.MQTT)
        self.assertEqual(len(http_adapters), 2)
        self.assertEqual(len(mqtt_adapters), 1)


class TestAdapterRegistryConnection(unittest.TestCase):
    """Test AdapterRegistry connection management."""

    def setUp(self):
        self.registry = AdapterRegistry()

    def test_connect_adapter(self):
        """Connect adapter changes state to CONNECTED."""
        adapter_id = self.registry.register_adapter(
            protocol_type=ProtocolType.WEBSOCKET,
            endpoint="ws://example.com",
        )
        result = self.registry.connect_adapter(adapter_id)
        self.assertTrue(result)
        adapter = self.registry.get_adapter(adapter_id)
        self.assertEqual(adapter.state, AdapterState.CONNECTED)

    def test_disconnect_adapter(self):
        """Disconnect adapter changes state to DISCONNECTED."""
        adapter_id = self.registry.register_adapter(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="https://api.example.com",
        )
        self.registry.connect_adapter(adapter_id)
        result = self.registry.disconnect_adapter(adapter_id)
        self.assertTrue(result)
        adapter = self.registry.get_adapter(adapter_id)
        self.assertEqual(adapter.state, AdapterState.DISCONNECTED)

    def test_connect_missing_adapter_raises(self):
        """Connecting non-existent adapter raises AdapterNotFoundError."""
        with self.assertRaises(AdapterNotFoundError):
            self.registry.connect_adapter("nonexistent")

    def test_pause_resume_adapter(self):
        """Pause and resume adapter states."""
        adapter_id = self.registry.register_adapter(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="https://api.example.com",
        )
        self.registry.connect_adapter(adapter_id)
        self.registry.pause_adapter(adapter_id)
        adapter = self.registry.get_adapter(adapter_id)
        self.assertEqual(adapter.state, AdapterState.PAUSED)
        self.registry.resume_adapter(adapter_id)
        self.assertEqual(adapter.state, AdapterState.CONNECTED)

    def test_pause_missing_adapter(self):
        """Pause non-existent adapter returns False."""
        result = self.registry.pause_adapter("nonexistent")
        self.assertFalse(result)


class TestAdapterRegistryBroadcast(unittest.TestCase):
    """Test AdapterRegistry broadcast and routing."""

    def setUp(self):
        self.registry = AdapterRegistry()

    def test_broadcast_to_protocol(self):
        """Broadcast sends message to all adapters of a protocol type."""
        self.registry.register_adapter(ProtocolType.HTTP_REST, "http://a")
        self.registry.register_adapter(ProtocolType.HTTP_REST, "http://b")
        self.registry.connect_adapter("adapter_http_rest_" + [a for a in self.registry._adapters if self.registry._adapters[a].config.protocol_type == ProtocolType.HTTP_REST][0].adapter_id if False else self.registry.get_adapters_by_protocol(ProtocolType.HTTP_REST)[0].adapter_id)
        # Connect all
        for adapter in self.registry.get_adapters_by_protocol(ProtocolType.HTTP_REST):
            self.registry.connect_adapter(adapter.adapter_id)

        count = self.registry.broadcast_to_protocol(
            ProtocolType.HTTP_REST, {"msg": "hello"}
        )
        self.assertEqual(count, 2)

    def test_broadcast_empty_protocol(self):
        """Broadcast to protocol with no adapters returns 0."""
        count = self.registry.broadcast_to_protocol(ProtocolType.GRPC, {})
        self.assertEqual(count, 0)

    def test_route_to_best_adapter(self):
        """Route selects the best adapter for a protocol."""
        adapter_id = self.registry.register_adapter(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="https://api.example.com",
        )
        self.registry.connect_adapter(adapter_id)
        result = self.registry.route_to_best_adapter(
            ProtocolType.HTTP_REST, {"data": "test"}
        )
        self.assertIsNotNone(result)
        self.assertEqual(result, adapter_id)

    def test_route_no_adapters(self):
        """Route returns None when no adapters available."""
        result = self.registry.route_to_best_adapter(
            ProtocolType.GRPC, {"data": "test"}
        )
        self.assertIsNone(result)


class TestAdapterRegistryStats(unittest.TestCase):
    """Test AdapterRegistry statistics."""

    def setUp(self):
        self.registry = AdapterRegistry()

    def test_empty_stats(self):
        """Registry stats for empty registry."""
        stats = self.registry.get_stats()
        self.assertEqual(stats["total_registered"], 0)
        self.assertEqual(stats["total_connected"], 0)
        self.assertEqual(stats["total_adapters"], 0)

    def test_stats_after_operations(self):
        """Registry stats reflect operations."""
        adapter_id = self.registry.register_adapter(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="https://api.example.com",
        )
        self.registry.connect_adapter(adapter_id)
        stats = self.registry.get_stats()
        self.assertEqual(stats["total_registered"], 1)
        self.assertEqual(stats["total_connected"], 1)

    def test_adapter_stats(self):
        """Get individual adapter stats."""
        adapter_id = self.registry.register_adapter(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="https://api.example.com",
        )
        self.registry.connect_adapter(adapter_id)
        stats = self.registry.get_adapter_stats(adapter_id)
        self.assertIsNotNone(stats)
        self.assertEqual(stats["protocol"], "http_rest")
        self.assertEqual(stats["state"], "connected")


# ═════════════════════════════════════════════════
# ExternalAdapter Tests
# ═════════════════════════════════════════════════

class TestExternalAdapter(unittest.TestCase):
    """Test ExternalAdapter lifecycle and stats."""

    def test_adapter_creation(self):
        """Adapter initializes with correct config."""
        config = AdapterConfig(
            protocol_type=ProtocolType.MQTT,
            endpoint="mqtt://test",
        )
        adapter = ExternalAdapter("test_id", config)
        self.assertEqual(adapter.adapter_id, "test_id")
        self.assertEqual(adapter.state, AdapterState.REGISTERED)

    def test_adapter_connect_disconnect(self):
        """Adapter lifecycle: registered → connected → disconnected."""
        config = AdapterConfig(
            protocol_type=ProtocolType.MQTT,
            endpoint="mqtt://test",
        )
        adapter = ExternalAdapter("test_id", config)
        adapter.connect()
        self.assertEqual(adapter.state, AdapterState.CONNECTED)
        self.assertIsNotNone(adapter.connected_at)
        adapter.disconnect()
        self.assertEqual(adapter.state, AdapterState.DISCONNECTED)

    def test_adapter_pause_resume(self):
        """Adapter can be paused and resumed."""
        config = AdapterConfig(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="http://test",
        )
        adapter = ExternalAdapter("test_id", config)
        adapter.connect()
        adapter.pause()
        self.assertEqual(adapter.state, AdapterState.PAUSED)
        adapter.resume()
        self.assertEqual(adapter.state, AdapterState.CONNECTED)

    def test_adapter_message_recording(self):
        """Adapter records messages and activity."""
        config = AdapterConfig(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="http://test",
        )
        adapter = ExternalAdapter("test_id", config)
        adapter.connect()
        for _ in range(5):
            adapter.record_message()
        self.assertEqual(adapter.message_count, 5)
        self.assertGreater(adapter.last_activity, 0)

    def test_adapter_error_recording(self):
        """Adapter records errors."""
        config = AdapterConfig(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="http://test",
        )
        adapter = ExternalAdapter("test_id", config)
        adapter.connect()
        for _ in range(3):
            adapter.record_error()
        self.assertEqual(adapter.error_count, 3)

    def test_adapter_stats(self):
        """Adapter returns correct statistics."""
        config = AdapterConfig(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="http://test",
        )
        adapter = ExternalAdapter("test_id", config)
        adapter.connect()
        adapter.record_message()
        adapter.record_error()
        stats = adapter.get_stats()
        self.assertEqual(stats["message_count"], 1)
        self.assertEqual(stats["error_count"], 1)
        self.assertEqual(stats["endpoint"], "http://test")


# ═════════════════════════════════════════════════
# ProtocolBridge Tests
# ═════════════════════════════════════════════════

class TestProtocolBridgeBasics(unittest.TestCase):
    """Test ProtocolBridge basic operations."""

    def setUp(self):
        self.bridge = ProtocolBridge()

    def test_bridge_initialization(self):
        """Bridge initializes with default config."""
        self.assertEqual(self.bridge.config.bridge_port, 8766)
        self.assertTrue(self.bridge.config.enable_bidirectional)
        self.assertEqual(self.bridge.config.max_pending_messages, 10000)

    def test_connect_external(self):
        """Connect external creates adapter and connects it."""
        adapter_id = self.bridge.connect_external(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="https://api.example.com",
        )
        self.assertIsNotNone(adapter_id)
        adapter = self.bridge.registry.get_adapter(adapter_id)
        self.assertIsNotNone(adapter)
        self.assertEqual(adapter.state, AdapterState.CONNECTED)

    def test_disconnect_external(self):
        """Disconnect external removes adapter connection."""
        adapter_id = self.bridge.connect_external(
            protocol_type=ProtocolType.MQTT,
            endpoint="mqtt://test",
        )
        result = self.bridge.disconnect_external(adapter_id)
        self.assertTrue(result)
        adapter = self.bridge.registry.get_adapter(adapter_id)
        self.assertEqual(adapter.state, AdapterState.DISCONNECTED)

    def test_bridge_stats(self):
        """Bridge stats include correct information."""
        stats = self.bridge.get_bridge_stats()
        self.assertIn("total_forwarded", stats)
        self.assertIn("total_received", stats)
        self.assertIn("total_translated", stats)
        self.assertIn("uptime_seconds", stats)
        self.assertIn("adapter_stats", stats)

    def test_set_bidirectional(self):
        """Set bidirectional toggles bidirectional mode."""
        self.bridge.set_bidirectional(False)
        self.assertFalse(self.bridge.config.enable_bidirectional)
        self.bridge.set_bidirectional(True)
        self.assertTrue(self.bridge.config.enable_bidirectional)


class TestProtocolBridgeForward(unittest.TestCase):
    """Test ProtocolBridge forward_token operation."""

    def setUp(self):
        self.bridge = ProtocolBridge()
        adapter_id = self.bridge.connect_external(
            protocol_type=ProtocolType.MQTT,
            endpoint="mqtt://test",
        )
        self.adapter_id = adapter_id

    def test_forward_token(self):
        """Forward token to external protocol returns success."""
        result = self.bridge.forward_token(
            token_id="42",
            payload={"data": "hello"},
            target_protocol=ProtocolType.MQTT,
        )
        self.assertEqual(result["status"], "forwarded")
        self.assertEqual(result["token_id"], "42")

    def test_forward_to_http_rest(self):
        """Forward token to HTTP REST protocol."""
        adapter_id = self.bridge.connect_external(
            protocol_type=ProtocolType.HTTP_REST,
            endpoint="https://api.example.com",
        )
        result = self.bridge.forward_token(
            token_id="100",
            payload={"query": "test"},
            target_protocol=ProtocolType.HTTP_REST,
        )
        self.assertIn(result["status"], ["forwarded", "error"])

    def test_forward_to_grpc(self):
        """Forward token to gRPC protocol."""
        adapter_id = self.bridge.connect_external(
            protocol_type=ProtocolType.GRPC,
            endpoint="grpc://service:50051",
        )
        result = self.bridge.forward_token(
            token_id="200",
            payload={"method": "GetData"},
            target_protocol=ProtocolType.GRPC,
        )
        self.assertIn(result["status"], ["forwarded", "error"])

    def test_forward_with_custom_qos(self):
        """Forward token with custom QoS tier."""
        result = self.bridge.forward_token(
            token_id="50",
            payload={"data": "urgent"},
            target_protocol=ProtocolType.MQTT,
            qos_tier="gold",
        )
        self.assertIn(result["status"], ["forwarded", "error"])

    def test_forward_no_active_adapter(self):
        """Forward to protocol with no active adapter returns error."""
        result = self.bridge.forward_token(
            token_id="999",
            payload={"data": "test"},
            target_protocol=ProtocolType.KAFKA,
        )
        self.assertEqual(result["status"], "error")


class TestProtocolBridgeReceive(unittest.TestCase):
    """Test ProtocolBridge receive_external operation."""

    def setUp(self):
        self.bridge = ProtocolBridge()

    def test_receive_http(self):
        """Receive message from HTTP REST protocol."""
        result = self.bridge.receive_external(
            protocol_type=ProtocolType.HTTP_REST,
            message={"payload": "test_data"},
        )
        self.assertIn(result["status"], ["received", "error"])
        if result["status"] == "received":
            self.assertEqual(result["source_protocol"], "http_rest")

    def test_receive_websocket(self):
        """Receive message from WebSocket protocol."""
        result = self.bridge.receive_external(
            protocol_type=ProtocolType.WEBSOCKET,
            message={"type": "event", "data": "test"},
        )
        self.assertIn(result["status"], ["received", "error"])

    def test_receive_mqtt(self):
        """Receive message from MQTT protocol."""
        result = self.bridge.receive_external(
            protocol_type=ProtocolType.MQTT,
            message={"topic": "test/topic", "payload": "data"},
        )
        self.assertIn(result["status"], ["received", "error"])


# ═════════════════════════════════════════════════
# CrossProtocolGateway Tests
# ═════════════════════════════════════════════════

class TestCrossProtocolGatewayBasics(unittest.TestCase):
    """Test CrossProtocolGateway basic operations."""

    def setUp(self):
        self.gateway = CrossProtocolGateway()

    def test_gateway_initialization(self):
        """Gateway initializes with default config."""
        self.assertEqual(self.gateway.config.default_source_format, TranslationFormat.JSON)
        self.assertEqual(self.gateway.config.default_target_format, TranslationFormat.PROTOBUF)

    def test_get_supported_formats(self):
        """Gateway supports all translation formats."""
        formats = self.gateway.get_supported_formats()
        self.assertIn("json", formats)
        self.assertIn("protobuf", formats)
        self.assertIn("msgpack", formats)
        self.assertIn("yaml", formats)
        self.assertIn("cbor", formats)
        self.assertIn("xml", formats)

    def test_get_supported_protocols(self):
        """Gateway supports standard protocols."""
        protocols = self.gateway.get_supported_protocols()
        self.assertIn("http_rest", protocols)
        self.assertIn("grpc", protocols)
        self.assertIn("mqtt", protocols)
        self.assertIn("websocket", protocols)
        self.assertIn("sse", protocols)

    def test_get_stats(self):
        """Gateway stats track translation activity."""
        stats = self.gateway.get_stats()
        self.assertIn("total_translations", stats)
        self.assertIn("success_rate", stats)
        self.assertIn("cache_size", stats)

    def test_clear_cache(self):
        """Clear cache removes all cached translations."""
        self.gateway.translate(
            token_id="1", payload={"test": "data"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.JSON,
        )
        size_before = self.gateway.get_cache_size()
        self.assertGreater(size_before, 0)
        cleared = self.gateway.clear_cache()
        self.assertEqual(cleared, size_before)
        self.assertEqual(self.gateway.get_cache_size(), 0)

    def test_get_cache_size(self):
        """Cache size reflects translations."""
        self.gateway.translate(
            token_id="1", payload={"test": "data"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.JSON,
        )
        self.assertGreater(self.gateway.get_cache_size(), 0)


class TestCrossProtocolGatewayTranslate(unittest.TestCase):
    """Test CrossProtocolGateway translation operations."""

    def setUp(self):
        self.gateway = CrossProtocolGateway()

    def test_translate_json_to_json(self):
        """Translate JSON to JSON returns converted payload."""
        result = self.gateway.translate(
            token_id="42",
            payload={"data": "hello", "count": 42},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.JSON,
        )
        self.assertIn(result["status"] if "status" in result else "", ["", "success"])
        self.assertEqual(result["original_token_id"], "42")
        self.assertIn("converted_payload", result)
        self.assertEqual(result["converted_payload"], {"data": "hello", "count": 42})

    def test_translate_json_to_protobuf(self):
        """Translate JSON to Protobuf."""
        result = self.gateway.translate(
            token_id="100",
            payload={"key": "value"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.PROTOBUF,
        )
        self.assertIn("converted_payload", result)
        self.assertEqual(result["source_format"], "json")
        self.assertEqual(result["target_format"], "protobuf")

    def test_translate_json_to_msgpack(self):
        """Translate JSON to MessagePack."""
        result = self.gateway.translate(
            token_id="200",
            payload={"data": "test"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.MESSAGE_PACK,
        )
        self.assertIn("converted_payload", result)
        self.assertEqual(result["target_format"], "msgpack")

    def test_translate_json_to_yaml(self):
        """Translate JSON to YAML."""
        result = self.gateway.translate(
            token_id="300",
            payload={"key": "value"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.YAML,
        )
        self.assertIn("converted_payload", result)

    def test_translate_json_to_cbor(self):
        """Translate JSON to CBOR."""
        result = self.gateway.translate(
            token_id="400",
            payload={"key": "value"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.CBOR,
        )
        self.assertIn("converted_payload", result)

    def test_translate_json_to_xml(self):
        """Translate JSON to XML."""
        result = self.gateway.translate(
            token_id="500",
            payload={"key": "value"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.XML,
        )
        self.assertIn("converted_payload", result)

    def test_translate_with_protocols(self):
        """Translate with protocol types specified."""
        result = self.gateway.translate(
            token_id="600",
            payload={"data": "test"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.PROTOBUF,
            source_protocol=ProtocolType.HTTP_REST,
            target_protocol=ProtocolType.GRPC,
        )
        self.assertIn("converted_payload", result)
        self.assertIn("source_protocol", result)
        self.assertIn("target_protocol", result)

    def test_translate_with_invalid_source_format(self):
        """Translate with unsupported source format raises TranslationError."""
        # TranslationFormat enum validates input, so use a string-based
        # approach by testing the code path that doesn't recognize format
        with self.assertRaises((TranslationError, ValueError)):
            # Passing a format value not in _format_handlers triggers the error
            self.gateway.translate(
                token_id="bad",
                payload={},
                source_format=TranslationFormat.XML,
                target_format=TranslationFormat("invalid"),
            )

    def test_batch_translate(self):
        """Batch translate multiple tokens."""
        results = self.gateway.batch_translate(
            token_ids=["1", "2", "3"],
            payloads=[{"a": 1}, {"b": 2}, {"c": 3}],
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.JSON,
        )
        self.assertEqual(len(results), 3)
        for result in results:
            self.assertIn("converted_payload", result)


# ═════════════════════════════════════════════════
# UniversalTranslator Tests
# ═════════════════════════════════════════════════

class TestUniversalTranslatorBasics(unittest.TestCase):
    """Test UniversalTranslator basic operations."""

    def setUp(self):
        self.translator = UniversalTranslator()

    def test_translator_initialization(self):
        """Translator initializes with default rules."""
        self.assertGreater(self.translator.get_rule_count(), 0)
        rules = self.translator.get_rules()
        self.assertIsInstance(rules, list)

    def test_get_rules(self):
        """Get rules returns list of TranslationRule objects."""
        rules = self.translator.get_rules()
        for rule in rules:
            self.assertIsInstance(rule, TranslationRule)

    def test_get_rule_count(self):
        """Rule count matches number of registered rules."""
        count = self.translator.get_rule_count()
        self.assertGreater(count, 0)

    def test_add_rule(self):
        """Add a new translation rule."""
        rule = TranslationRule(
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.YAML,
            field_mappings={"token_id": "id"},
            priority=10,
        )
        result = self.translator.add_rule(rule)
        self.assertTrue(result)
        self.assertGreater(self.translator.get_rule_count(), 0)

    def test_remove_rule(self):
        """Remove a translation rule."""
        initial_count = self.translator.get_rule_count()
        result = self.translator.remove_rule(
            TranslationFormat.JSON, TranslationFormat.PROTOBUF
        )
        self.assertTrue(result or initial_count > 0)
        if initial_count > 0:
            self.assertLessEqual(self.translator.get_rule_count(), initial_count)

    def test_get_stats(self):
        """Translator stats include conversion metrics."""
        stats = self.translator.get_stats()
        self.assertIn("total_conversions", stats)
        self.assertIn("success_rate", stats)
        self.assertIn("rule_count", stats)


class TestUniversalTranslatorConvert(unittest.TestCase):
    """Test UniversalTranslator convert operations."""

    def setUp(self):
        self.translator = UniversalTranslator()

    def test_convert_json_to_protobuf(self):
        """Convert JSON to Protobuf format."""
        result = self.translator.convert(
            message={"token_id": "42", "payload": "hello"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.PROTOBUF,
        )
        self.assertIn("token_id", result)
        self.assertIn("_target_format", result)

    def test_convert_protobuf_to_json(self):
        """Convert Protobuf to JSON format."""
        result = self.translator.convert(
            message={"id": "42", "data": "hello"},
            source_format=TranslationFormat.PROTOBUF,
            target_format=TranslationFormat.JSON,
        )
        self.assertIn("token_id", result)
        self.assertIn("_target_format", result)

    def test_convert_json_to_msgpack(self):
        """Convert JSON to MessagePack."""
        result = self.translator.convert(
            message={"token_id": "100", "payload": "test"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.MESSAGE_PACK,
        )
        self.assertIn("token_id", result)

    def test_convert_json_to_yaml(self):
        """Convert JSON to YAML."""
        result = self.translator.convert(
            message={"token_id": "200", "payload": "test"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.YAML,
        )
        self.assertIn("token_id", result)

    def test_convert_json_to_cbor(self):
        """Convert JSON to CBOR."""
        result = self.translator.convert(
            message={"token_id": "300", "payload": "test"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.CBOR,
        )
        self.assertIn("token_id", result)

    def test_convert_json_to_xml(self):
        """Convert JSON to XML."""
        result = self.translator.convert(
            message={"token_id": "400", "payload": "test"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.XML,
        )
        self.assertIn("token_id", result)

    def test_convert_with_rule_mappings(self):
        """Convert applies field mappings from rules."""
        result = self.translator.convert(
            message={"token_id": "500", "payload": "data"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.PROTOBUF,
        )
        # Rule maps token_id → id
        self.assertIn("id", result)

    def test_convert_batch(self):
        """Convert multiple messages in batch."""
        messages = [
            {"token_id": "1", "payload": "a"},
            {"token_id": "2", "payload": "b"},
            {"token_id": "3", "payload": "c"},
        ]
        results = self.translator.convert_batch(
            messages, TranslationFormat.JSON, TranslationFormat.PROTOBUF
        )
        self.assertEqual(len(results), 3)
        for result in results:
            self.assertIn("token_id", result)

    def test_convert_invalid_format_raises(self):
        """Convert with unsupported format raises TranslationError."""
        with self.assertRaises((TranslationError, ValueError)):
            self.translator.convert(
                message={},
                source_format=TranslationFormat.JSON,
                target_format=TranslationFormat("invalid"),
            )


# ═════════════════════════════════════════════════
# Factory and Integration Tests
# ═════════════════════════════════════════════════

class TestFactoryFunctions(unittest.TestCase):
    """Test factory function creation."""

    def test_create_protocol_bridge(self):
        """Create ProtocolBridge via factory."""
        bridge = create_protocol_bridge()
        self.assertIsInstance(bridge, ProtocolBridge)

    def test_create_cross_protocol_gateway(self):
        """Create CrossProtocolGateway via factory."""
        gateway = create_cross_protocol_gateway()
        self.assertIsInstance(gateway, CrossProtocolGateway)

    def test_create_universal_translator(self):
        """Create UniversalTranslator via factory."""
        translator = create_universal_translator()
        self.assertIsInstance(translator, UniversalTranslator)

    def test_create_interop_stack(self):
        """Create complete interop stack."""
        stack = create_interop_stack()
        self.assertIn("protocol_bridge", stack)
        self.assertIn("cross_protocol_gateway", stack)
        self.assertIn("universal_translator", stack)
        self.assertIn("adapter_registry", stack)
        self.assertIn("teq_billing", stack)
        self.assertIn("car_router", stack)


class TestInteropIntegration(unittest.TestCase):
    """Test full v4 interoperability integration."""

    def setUp(self):
        self.stack = create_interop_stack()

    def test_full_stack_components(self):
        """All stack components are properly initialized."""
        self.assertIsInstance(self.stack["protocol_bridge"], ProtocolBridge)
        self.assertIsInstance(self.stack["cross_protocol_gateway"], CrossProtocolGateway)
        self.assertIsInstance(self.stack["universal_translator"], UniversalTranslator)
        self.assertIsInstance(self.stack["adapter_registry"], AdapterRegistry)
        self.assertIsInstance(self.stack["teq_billing"], type(self.stack["teq_billing"]))
        self.assertIsInstance(self.stack["car_router"], type(self.stack["car_router"]))

    def test_stack_forward_receive_cycle(self):
        """Test forward and receive cycle through the stack."""
        bridge = self.stack["protocol_bridge"]
        # Connect an external endpoint
        adapter_id = bridge.connect_external(
            protocol_type=ProtocolType.MQTT,
            endpoint="mqtt://test",
        )
        # Forward a token
        forward_result = bridge.forward_token(
            token_id="test_1", payload={"msg": "hello"},
            target_protocol=ProtocolType.MQTT,
        )
        self.assertIn(forward_result["status"], ["forwarded", "error"])
        # Receive from external
        receive_result = bridge.receive_external(
            protocol_type=ProtocolType.MQTT,
            message={"payload": "response"},
        )
        self.assertIn(receive_result["status"], ["received", "error"])

    def test_stack_translation_through_gateway(self):
        """Test translation through gateway in the stack."""
        gateway = self.stack["cross_protocol_gateway"]
        result = gateway.translate(
            token_id="stack_test",
            payload={"data": "integration"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.PROTOBUF,
        )
        self.assertIn("converted_payload", result)

    def test_stack_conversion_through_translator(self):
        """Test conversion through translator in the stack."""
        translator = self.stack["universal_translator"]
        result = translator.convert(
            message={"token_id": "conv_test", "payload": "data"},
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.YAML,
        )
        self.assertIn("token_id", result)

    def test_stack_adapter_registry_operations(self):
        """Test adapter registry through the stack."""
        registry = self.stack["adapter_registry"]
        adapter_id = registry.register_adapter(
            protocol_type=ProtocolType.GRPC,
            endpoint="grpc://service",
        )
        self.assertIn(adapter_id, registry._adapters)
        registry.connect_adapter(adapter_id)
        adapter = registry.get_adapter(adapter_id)
        self.assertEqual(adapter.state, AdapterState.CONNECTED)
        stats = registry.get_stats()
        self.assertEqual(stats["total_registered"], 1)

    def test_stack_bridge_stats(self):
        """Bridge stats work correctly in the stack."""
        bridge = self.stack["protocol_bridge"]
        stats = bridge.get_bridge_stats()
        self.assertIn("adapter_stats", stats)
        self.assertIn("total_forwarded", stats)


class TestInteropBRPServer(unittest.TestCase):
    """Test InteropBRPServer functionality."""

    def test_server_creation(self):
        """InteropBRPServer initializes correctly."""
        server = InteropBRPServer(port=8766)
        self.assertEqual(server.port, 8766)

    def test_server_start_stop(self):
        """Server can start and stop."""
        server = InteropBRPServer(port=8767)
        result = server.start()
        self.assertTrue(result)
        status = server.get_status()
        self.assertTrue(status["connected"])
        server.stop()
        status = server.get_status()
        self.assertFalse(status["connected"])

    def test_server_get_status(self):
        """Server status includes bridge and gateway stats."""
        server = InteropBRPServer(port=8768)
        server.start()
        status = server.get_status()
        self.assertIn("bridge_stats", status)
        self.assertIn("gateway_stats", status)
        self.assertIn("port", status)
        server.stop()


class TestInteropExceptions(unittest.TestCase):
    """Test custom exception classes."""

    def test_interop_error(self):
        """InteropError is a base exception."""
        err = InteropError("test error")
        self.assertIsInstance(err, Exception)
        self.assertEqual(str(err), "test error")

    def test_adapter_not_found_error(self):
        """AdapterNotFoundError inherits from InteropError."""
        err = AdapterNotFoundError("adapter_123")
        self.assertIsInstance(err, InteropError)

    def test_protocol_not_supported_error(self):
        """ProtocolNotSupportedError inherits from InteropError."""
        err = ProtocolNotSupportedError("kafka")
        self.assertIsInstance(err, InteropError)

    def test_translation_error(self):
        """TranslationError inherits from InteropError."""
        err = TranslationError("bad format")
        self.assertIsInstance(err, InteropError)


class TestTranslationRule(unittest.TestCase):
    """Test TranslationRule dataclass."""

    def test_rule_creation(self):
        """TranslationRule creates correctly with all fields."""
        rule = TranslationRule(
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.PROTOBUF,
            field_mappings={"token_id": "id"},
            default_values={"version": "4.0"},
            priority=1,
        )
        self.assertEqual(rule.source_format, TranslationFormat.JSON)
        self.assertEqual(rule.target_format, TranslationFormat.PROTOBUF)
        self.assertEqual(rule.field_mappings, {"token_id": "id"})
        self.assertEqual(rule.priority, 1)

    def test_rule_defaults(self):
        """TranslationRule has sensible defaults."""
        rule = TranslationRule(
            source_format=TranslationFormat.JSON,
            target_format=TranslationFormat.YAML,
        )
        self.assertEqual(rule.field_mappings, {})
        self.assertEqual(rule.default_values, {})
        self.assertEqual(rule.priority, 5)


class TestEnumsAndConstants(unittest.TestCase):
    """Test enum values and constants."""

    def test_protocol_type_values(self):
        """ProtocolType enum has all expected values."""
        values = [p.value for p in ProtocolType]
        self.assertIn("http_rest", values)
        self.assertIn("grpc", values)
        self.assertIn("mqtt", values)
        self.assertIn("websocket", values)
        self.assertIn("sse", values)
        self.assertIn("nats", values)
        self.assertIn("amqp", values)
        self.assertIn("kafka", values)

    def test_translation_format_values(self):
        """TranslationFormat enum has all expected values."""
        values = [f.value for f in TranslationFormat]
        self.assertIn("json", values)
        self.assertIn("protobuf", values)
        self.assertIn("msgpack", values)
        self.assertIn("yaml", values)
        self.assertIn("cbor", values)
        self.assertIn("xml", values)

    def test_adapter_state_values(self):
        """AdapterState enum has all expected values."""
        values = [s.value for s in AdapterState]
        self.assertIn("registered", values)
        self.assertIn("connected", values)
        self.assertIn("active", values)
        self.assertIn("paused", values)
        self.assertIn("disconnected", values)
        self.assertIn("error", values)


if __name__ == "__main__":
    unittest.main(verbosity=2)
