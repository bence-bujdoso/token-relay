"""Tests for the TokenRelay v2 Plugin System."""

import sys
import os
import json
from pathlib import Path

# Ensure src is on the path
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

from plugin import (
    PluginStatus, SchemaVersion, CustomTokenDefinition,
    MigrationPath, PluginManifest, Plugin, PluginRegistry,
    CorePlugin, ValidationPlugin, MigrationPlugin,
)
from registry import get_registry


# ─────────────────────────────────────────────
# SchemaVersion Tests
# ─────────────────────────────────────────────

def test_schema_version_parse():
    """SchemaVersion.from_string() parses correctly."""
    v = SchemaVersion.from_string("2.1.3")
    assert v.major == 2
    assert v.minor == 1
    assert v.patch == 3
    print("✓ test_schema_version_parse")


def test_schema_version_comparison():
    """SchemaVersion comparison operators work."""
    v1 = SchemaVersion(1, 0, 0)
    v2 = SchemaVersion(2, 0, 0)
    v3 = SchemaVersion(1, 1, 0)
    assert v1 < v2
    assert v1 < v3
    assert v2 > v1
    assert v3 > v1
    assert v1 <= SchemaVersion(1, 0, 0)
    print("✓ test_schema_version_comparison")


def test_schema_version_bump():
    """SchemaVersion bump methods work."""
    v = SchemaVersion(1, 2, 3)
    assert v.bump_major() == SchemaVersion(2, 0, 0)
    assert v.bump_minor() == SchemaVersion(1, 3, 0)
    assert v.bump_patch() == SchemaVersion(1, 2, 4)
    print("✓ test_schema_version_bump")


def test_schema_version_latest():
    """SchemaVersion.latest() returns the highest version."""
    versions = [SchemaVersion(1, 0, 0), SchemaVersion(2, 0, 0), SchemaVersion(1, 5, 0)]
    latest = SchemaVersion.latest(versions)
    assert latest == SchemaVersion(2, 0, 0)
    print("✓ test_schema_version_latest")


def test_schema_version_str():
    """SchemaVersion.__str__() returns correct format."""
    v = SchemaVersion(1, 2, 3)
    assert str(v) == "1.2.3"
    print("✓ test_schema_version_str")


# ─────────────────────────────────────────────
# CustomTokenDefinition Tests
# ─────────────────────────────────────────────

def test_custom_token_definition():
    """CustomTokenDefinition creates correctly and to_dict() works."""
    token_def = CustomTokenDefinition(
        token_id=9999,
        meaning="Custom test token",
        category="custom",
        protocol_type="data",
        routing_priority=42,
        version="1.0.0",
    )
    assert token_def.token_id == 9999
    assert token_def.meaning == "Custom test token"
    d = token_def.to_dict()
    assert d["token_id"] == 9999
    assert d["protocol_type"] == "data"
    print("✓ test_custom_token_definition")


# ─────────────────────────────────────────────
# MigrationPath Tests
# ─────────────────────────────────────────────

def test_migration_path():
    """MigrationPath creates correctly and to_dict() works."""
    migration = MigrationPath(
        from_version="1.0.0",
        to_version="2.0.0",
        description="Major version migration",
        backward_compatible=True,
    )
    assert migration.from_version == "1.0.0"
    assert migration.to_version == "2.0.0"
    d = migration.to_dict()
    assert d["from_version"] == "1.0.0"
    assert d["backward_compatible"] is True
    print("✓ test_migration_path")


# ─────────────────────────────────────────────
# PluginManifest Tests
# ─────────────────────────────────────────────

def test_plugin_manifest():
    """PluginManifest creates correctly."""
    manifest = PluginManifest(
        name="test_plugin",
        version="1.0.0",
        description="A test plugin",
        author="Test Author",
        entry_point="test.entry",
    )
    assert manifest.name == "test_plugin"
    assert manifest.status == PluginStatus.INSTALLED
    d = manifest.to_dict()
    assert d["name"] == "test_plugin"
    assert d["status"] == "installed"
    print("✓ test_plugin_manifest")


# ─────────────────────────────────────────────
# PluginRegistry Tests
# ─────────────────────────────────────────────

def test_plugin_registry_register_plugin():
    """PluginRegistry.register_plugin() registers a plugin."""
    registry = PluginRegistry()
    plugin = CorePlugin()
    result = registry.register_plugin(plugin)
    assert result is True
    assert "core_protocol" in [m.name for m in registry.list_plugins()]
    print("✓ test_plugin_registry_register_plugin")


def test_plugin_registry_unregister_plugin():
    """PluginRegistry.unregister_plugin() removes a plugin."""
    registry = PluginRegistry()
    plugin = CorePlugin()
    registry.register_plugin(plugin)
    result = registry.unregister_plugin("core_protocol")
    assert result is True
    assert registry.get_plugin("core_protocol") is None
    print("✓ test_plugin_registry_unregister_plugin")


def test_plugin_registry_get_plugin():
    """PluginRegistry.get_plugin() returns the correct plugin."""
    registry = PluginRegistry()
    plugin = CorePlugin()
    registry.register_plugin(plugin)
    retrieved = registry.get_plugin("core_protocol")
    assert retrieved is not None
    assert retrieved.manifest.name == "core_protocol"
    print("✓ test_plugin_registry_get_plugin")


def test_plugin_registry_list_plugins():
    """PluginRegistry.list_plugins() returns all plugins."""
    registry = PluginRegistry()
    registry.register_plugin(CorePlugin())
    registry.register_plugin(ValidationPlugin())
    plugins = registry.list_plugins()
    assert len(plugins) == 2
    print("✓ test_plugin_registry_list_plugins")


def test_plugin_registry_custom_token():
    """PluginRegistry.register_custom_token() adds a token."""
    registry = PluginRegistry()
    token_def = CustomTokenDefinition(
        token_id=9999, meaning="Custom", category="custom",
        protocol_type="data", routing_priority=50
    )
    result = registry.register_custom_token(token_def)
    assert result is True
    retrieved = registry.get_custom_token(9999)
    assert retrieved is not None
    assert retrieved.meaning == "Custom"
    print("✓ test_plugin_registry_custom_token")


def test_plugin_registry_register_protocol():
    """PluginRegistry.register_protocol() registers a protocol."""
    registry = PluginRegistry()
    result = registry.register_protocol("test_protocol", [42, 151, 153], version="2.0.0")
    assert result is True
    schemas = registry.get_protocol_schemas("test_protocol")
    assert "2.0.0" in schemas
    print("✓ test_plugin_registry_register_protocol")


def test_plugin_registry_schema_versioning():
    """PluginRegistry.register_schema() and get_schema() work."""
    registry = PluginRegistry()
    schema_def = {"type": "object", "properties": {"field": {"type": "string"}}}
    result = registry.register_schema("test_proto", "1.0.0", schema_def)
    assert result is True
    retrieved = registry.get_schema("test_proto", "1.0.0")
    assert retrieved == schema_def
    print("✓ test_plugin_registry_schema_versioning")


def test_plugin_registry_migration_path():
    """PluginRegistry.add_migration() and get_migrations() work."""
    registry = PluginRegistry()
    result = registry.add_migration(
        "1.0.0", "2.0.0", "Major update", backward_compatible=True
    )
    assert result is True
    migrations = registry.get_migrations("1.0.0", "2.0.0")
    assert len(migrations) == 1
    assert migrations[0].from_version == "1.0.0"
    print("✓ test_plugin_registry_migration_path")


def test_plugin_registry_migrate_message():
    """PluginRegistry.migrate_message() applies transformations."""
    registry = PluginRegistry()
    registry.add_migration("1.0.0", "2.0.0", "Test migration",
                           transform_func=lambda msg: {**msg, "migrated": True})
    message = {"tokens": [42], "payload": {}}
    result = registry.migrate_message(message, "2.0.0", "1.0.0")
    assert result["migrated"] is True
    assert result["_migrated_to"] == "2.0.0"
    print("✓ test_plugin_registry_migrate_message")


def test_plugin_registry_discover_plugins():
    """PluginRegistry.discover_plugins() finds plugin files."""
    registry = PluginRegistry()
    # Discover from the src directory itself (should find .py files)
    discovered = registry.discover_plugins(str(SRC_DIR))
    # Should find at least some modules
    assert isinstance(discovered, list)
    print(f"✓ test_plugin_registry_discover_plugins (found {len(discovered)} files)")


def test_plugin_registry_register_token_type():
    """PluginRegistry.register_token_type() creates and registers a token."""
    registry = PluginRegistry()
    token_def = registry.register_token_type(
        token_id=8888, meaning="New Token", category="custom",
        protocol_type="data", routing_priority=30
    )
    assert token_def.token_id == 8888
    assert registry.get_custom_token(8888) is not None
    print("✓ test_plugin_registry_register_token_type")


def test_plugin_registry_get_all_custom_tokens():
    """PluginRegistry.get_all_custom_tokens() returns all tokens."""
    registry = PluginRegistry()
    registry.register_custom_token(CustomTokenDefinition(
        token_id=1001, meaning="Token A", category="custom",
        protocol_type="data", routing_priority=10
    ))
    registry.register_custom_token(CustomTokenDefinition(
        token_id=1002, meaning="Token B", category="custom",
        protocol_type="data", routing_priority=20
    ))
    tokens = registry.get_all_custom_tokens()
    assert len(tokens) == 2
    print("✓ test_plugin_registry_get_all_custom_tokens")


def test_plugin_registry_all_protocols():
    """PluginRegistry.get_all_protocols() returns all protocols."""
    registry = PluginRegistry()
    registry.register_protocol("proto_a", [42], version="1.0.0")
    registry.register_protocol("proto_b", [0], version="1.0.0")
    protocols = registry.get_all_protocols()
    assert "proto_a" in protocols
    assert "proto_b" in protocols
    print("✓ test_plugin_registry_all_protocols")


def test_plugin_registry_stats():
    """PluginRegistry.get_stats() returns correct statistics."""
    registry = PluginRegistry()
    registry.register_plugin(CorePlugin())
    registry.register_plugin(ValidationPlugin())
    registry.register_custom_token(CustomTokenDefinition(
        token_id=9999, meaning="Custom", category="custom",
        protocol_type="data", routing_priority=50
    ))
    stats = registry.get_stats()
    assert stats["plugins_registered"] >= 2
    assert stats["custom_tokens"] >= 1
    print("✓ test_plugin_registry_stats")


def test_plugin_registry_to_dict():
    """PluginRegistry.to_dict() exports full state."""
    registry = PluginRegistry()
    registry.register_plugin(CorePlugin())
    d = registry.to_dict()
    assert "plugins" in d
    assert "custom_tokens" in d
    assert "protocols" in d
    assert "stats" in d
    print("✓ test_plugin_registry_to_dict")


def test_plugin_registry_register_duplicate_plugin():
    """PluginRegistry.register_plugin() returns False for duplicates."""
    registry = PluginRegistry()
    plugin = CorePlugin()
    assert registry.register_plugin(plugin) is True
    assert registry.register_plugin(plugin) is False
    print("✓ test_plugin_registry_register_duplicate_plugin")


def test_plugin_registry_unregister_nonexistent():
    """PluginRegistry.unregister_plugin() returns False for unknown plugin."""
    registry = PluginRegistry()
    result = registry.unregister_plugin("nonexistent")
    assert result is False
    print("✓ test_plugin_registry_unregister_nonexistent")


def test_plugin_registry_register_protocol_multiple_versions():
    """PluginRegistry.register_protocol() supports multiple versions."""
    registry = PluginRegistry()
    registry.register_protocol("test", [42], version="1.0.0")
    registry.register_protocol("test", [42], version="2.0.0")
    schemas = registry.get_protocol_schemas("test")
    assert "1.0.0" in schemas
    assert "2.0.0" in schemas
    assert len(schemas) == 2
    print("✓ test_plugin_registry_register_protocol_multiple_versions")


# ─────────────────────────────────────────────
# Plugin Instances Tests
# ─────────────────────────────────────────────

def test_core_plugin():
    """CorePlugin registers and unregister correctly."""
    plugin = CorePlugin()
    assert plugin.manifest.name == "core_protocol"
    assert plugin.manifest.version == "2.0.0"

    registry = PluginRegistry()
    assert plugin.register(registry) is True
    assert plugin.unregister(registry) is True
    print("✓ test_core_plugin")


def test_validation_plugin():
    """ValidationPlugin registers custom token 2000."""
    plugin = ValidationPlugin()
    registry = PluginRegistry()
    assert plugin.register(registry) is True
    token = registry.get_custom_token(2000)
    assert token is not None
    assert token.meaning == "Validation check"
    assert plugin.unregister(registry) is True
    print("✓ test_validation_plugin")


def test_migration_plugin():
    """MigrationPlugin.migrate() transforms messages."""
    plugin = MigrationPlugin()
    assert plugin.manifest.name == "migration"

    registry = PluginRegistry()
    assert plugin.register(registry) is True
    message = {"tokens": [42], "data": "test"}
    result = plugin.migrate(message, "2.0.0")
    assert result["migrated"] is True
    assert result["target"] == "2.0.0"
    print("✓ test_migration_plugin")


def test_plugin_on_message():
    """Plugin.on_message() returns default None when not overridden."""
    plugin = MigrationPlugin()
    result = plugin.on_message({"tokens": [42]})
    # MigrationPlugin doesn't override on_message, so returns None from base
    assert result is None
    print("✓ test_plugin_on_message")


def test_plugin_on_error():
    """Plugin.on_error() returns structured error dict."""
    plugin = CorePlugin()
    result = plugin.on_error("TEST_E", {"detail": "test"})
    assert result["error"] == "TEST_E"
    assert result["plugin"] == "core_protocol"
    print("✓ test_plugin_on_error")


# ─────────────────────────────────────────────
# Integration Tests
# ─────────────────────────────────────────────

def test_full_plugin_lifecycle():
    """Full lifecycle: register, use, migrate, unregister."""
    registry = PluginRegistry()

    # Register core plugin
    core = CorePlugin()
    assert registry.register_plugin(core) is True

    # Register validation plugin
    validation = ValidationPlugin()
    assert registry.register_plugin(validation) is True

    # Check plugins
    assert len(registry.list_plugins()) == 2

    # Register a custom token type
    token_def = registry.register_token_type(
        7777, "Integration Token", "custom", "data", 25
    )
    assert registry.get_custom_token(7777) is not None

    # Add a migration path
    registry.add_migration("1.0.0", "2.0.0", "Test migration")
    assert len(registry.get_migrations("1.0.0", "2.0.0")) == 1

    # Migrate a message
    migrated = registry.migrate_message(
        {"tokens": [42]}, "2.0.0", "1.0.0"
    )
    assert migrated["_migrated_to"] == "2.0.0"

    # Unregister validation plugin
    assert registry.unregister_plugin("validation") is True
    assert registry.get_plugin("validation") is None

    print("✓ test_full_plugin_lifecycle")


def test_plugin_registry_with_base_registry():
    """PluginRegistry integrates with the base TokenRegistry."""
    base = get_registry()
    registry = PluginRegistry(base)

    plugin = CorePlugin()
    registry.register_plugin(plugin)

    # The base registry should have the custom tokens from the plugin
    tokens = registry.base_registry._registry
    assert str(42) in tokens or "42" in tokens

    print("✓ test_plugin_registry_with_base_registry")


def test_schema_version_latest_static():
    """SchemaVersion.latest() with empty list returns 0.0.0."""
    latest = SchemaVersion.latest([])
    assert latest == SchemaVersion(0, 0, 0)
    print("✓ test_schema_version_latest_static")


# ─────────────────────────────────────────────
# Run All Tests
# ─────────────────────────────────────────────

def run_all_tests():
    """Run all plugin tests."""
    tests = [
        test_schema_version_parse,
        test_schema_version_comparison,
        test_schema_version_bump,
        test_schema_version_latest,
        test_schema_version_str,
        test_custom_token_definition,
        test_migration_path,
        test_plugin_manifest,
        test_plugin_registry_register_plugin,
        test_plugin_registry_unregister_plugin,
        test_plugin_registry_get_plugin,
        test_plugin_registry_list_plugins,
        test_plugin_registry_custom_token,
        test_plugin_registry_register_protocol,
        test_plugin_registry_schema_versioning,
        test_plugin_registry_migration_path,
        test_plugin_registry_migrate_message,
        test_plugin_registry_discover_plugins,
        test_plugin_registry_register_token_type,
        test_plugin_registry_get_all_custom_tokens,
        test_plugin_registry_all_protocols,
        test_plugin_registry_stats,
        test_plugin_registry_to_dict,
        test_plugin_registry_register_duplicate_plugin,
        test_plugin_registry_unregister_nonexistent,
        test_plugin_registry_register_protocol_multiple_versions,
        test_core_plugin,
        test_validation_plugin,
        test_migration_plugin,
        test_plugin_on_message,
        test_plugin_on_error,
        test_full_plugin_lifecycle,
        test_plugin_registry_with_base_registry,
        test_schema_version_latest_static,
    ]

    print("=" * 72)
    print("  TokenRelay Plugin System — Test Suite")
    print("=" * 72)
    print()

    passed = 0
    failed = 0
    errors = []

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            failed += 1
            errors.append(f"{test.__name__}: {e}")
            print(f"✗ {test.__name__}: {e}")

    print(f"\n  {passed}/{passed + failed} tests passed")
    if errors:
        print("\n  Failures:")
        for err in errors:
            print(f"    - {err}")
    print("=" * 72)
    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)