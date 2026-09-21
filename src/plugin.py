"""
TokenRelay v2 Plugin System — Protocol Registration, Schema Versioning,
Migration Paths, and Plugin Discovery.

Provides:
- PluginRegistry for registering custom token types and protocols
- Schema versioning with semantic versioning support
- Migration path support for protocol upgrades
- Plugin discovery and loading from directories and entry points
"""

import json
import os
import sys
import abc
import hashlib
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from datetime import datetime

from registry import TokenRegistry, get_registry


# ─────────────────────────────────────────────
# Types & Enums
# ─────────────────────────────────────────────

class PluginStatus(Enum):
    """Plugin lifecycle status."""
    INSTALLED = "installed"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"
    FAILED = "failed"


class SchemaVersion:
    """Semantic version for token schemas."""

    def __init__(self, major: int, minor: int, patch: int = 0):
        self.major = major
        self.minor = minor
        self.patch = patch

    def __str__(self):
        return f"{self.major}.{self.minor}.{self.patch}"

    def __eq__(self, other):
        return (self.major, self.minor, self.patch) == (other.major, other.minor, other.patch)

    def __lt__(self, other):
        return (self.major, self.minor, self.patch) < (other.major, other.minor, other.patch)

    def __le__(self, other):
        return (self.major, self.minor, self.patch) <= (other.major, other.minor, other.patch)

    def __hash__(self):
        return hash((self.major, self.minor, self.patch))

    def bump_major(self) -> "SchemaVersion":
        return SchemaVersion(self.major + 1, 0, 0)

    def bump_minor(self) -> "SchemaVersion":
        return SchemaVersion(self.major, self.minor + 1, 0)

    def bump_patch(self) -> "SchemaVersion":
        return SchemaVersion(self.major, self.minor, self.patch + 1)

    @classmethod
    def from_string(cls, s: str) -> "SchemaVersion":
        """Parse a version string like '1.2.3'."""
        parts = s.split(".")
        return cls(int(parts[0]), int(parts[1]), int(parts[2]) if len(parts) > 2 else 0)

    @classmethod
    def latest(cls, versions: List["SchemaVersion"]) -> "SchemaVersion":
        """Get the latest version from a list."""
        return max(versions) if versions else cls(0, 0, 0)


@dataclass
class CustomTokenDefinition:
    """Definition of a custom token type."""
    token_id: int
    meaning: str
    category: str = "custom"
    protocol_type: str = "data"
    routing_priority: int = 50
    payload_schema: Optional[Dict] = None
    version: str = "1.0.0"
    deprecated: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "token_id": self.token_id,
            "meaning": self.meaning,
            "category": self.category,
            "protocol_type": self.protocol_type,
            "routing_priority": self.routing_priority,
            "payload_schema": self.payload_schema or {},
            "version": self.version,
            "deprecated": self.deprecated,
            "metadata": self.metadata,
        }


@dataclass
class MigrationPath:
    """Defines a migration path between schema versions."""
    from_version: str
    to_version: str
    description: str
    transform_func: Optional[Callable] = None
    backward_compatible: bool = True
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            "from_version": self.from_version,
            "to_version": self.to_version,
            "description": self.description,
            "backward_compatible": self.backward_compatible,
            "created_at": self.created_at,
        }


@dataclass
class PluginManifest:
    """Manifest describing a plugin."""
    name: str
    version: str
    description: str
    author: str
    entry_point: str
    token_definitions: List[CustomTokenDefinition] = field(default_factory=list)
    schema_version: str = "1.0.0"
    dependencies: List[str] = field(default_factory=list)
    status: PluginStatus = PluginStatus.INSTALLED
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "author": self.author,
            "entry_point": self.entry_point,
            "schema_version": self.schema_version,
            "dependencies": self.dependencies,
            "status": self.status.value,
            "token_count": len(self.token_definitions),
            "metadata": self.metadata,
        }


# ─────────────────────────────────────────────
# Plugin Interface
# ─────────────────────────────────────────────

class Plugin(ABC):
    """Abstract base class for TokenRelay plugins."""

    @property
    @abstractmethod
    def manifest(self) -> PluginManifest:
        """Return the plugin manifest."""
        pass

    @abstractmethod
    def register(self, registry: TokenRegistry) -> bool:
        """Register the plugin's tokens and protocols with the registry."""
        pass

    @abstractmethod
    def unregister(self, registry: TokenRegistry) -> bool:
        """Unregister the plugin's tokens from the registry."""
        pass

    def on_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process an incoming message. Override for custom behavior."""
        return None

    def on_error(self, error_code: str, details: Any) -> Dict[str, Any]:
        """Handle an error condition."""
        return {"error": error_code, "plugin": self.manifest.name, "details": details}

    def migrate(self, message: Dict[str, Any],
                 target_version: str) -> Dict[str, Any]:
        """Migrate a message to a target schema version."""
        return message


# ─────────────────────────────────────────────
# Plugin Registry
# ─────────────────────────────────────────────

class PluginRegistry:
    """Registry for managing plugins, custom token types, schema versions,
    and migration paths.
    """

    def __init__(self, base_registry: Optional[TokenRegistry] = None):
        self.base_registry = base_registry or get_registry()
        self._plugins: Dict[str, PluginManifest] = {}
        self._plugin_instances: Dict[str, Plugin] = {}
        self._custom_tokens: Dict[int, CustomTokenDefinition] = {}
        self._migrations: Dict[str, List[MigrationPath]] = {}
        self._schemas: Dict[str, List[str]] = {}  # protocol -> versions
        self._lock = threading.Lock()
        self._plugin_dir: Optional[Path] = None
        self._discovered_plugins: List[str] = []

    # ── Plugin Registration ──

    def register_plugin(self, plugin: Plugin) -> bool:
        """Register a plugin instance. Returns True on success."""
        manifest = plugin.manifest
        with self._lock:
            if manifest.name in self._plugins:
                return False
            self._plugins[manifest.name] = manifest
            self._plugin_instances[manifest.name] = plugin
            # Register token definitions
            for token_def in manifest.token_definitions:
                self.register_custom_token(token_def)
            manifest.status = PluginStatus.ACTIVE
            # Register with base registry
            try:
                plugin.register(self.base_registry)
            except Exception:
                manifest.status = PluginStatus.FAILED
                return False
        return True

    def unregister_plugin(self, plugin_name: str) -> bool:
        """Unregister a plugin by name. Returns True if found and removed."""
        with self._lock:
            if plugin_name not in self._plugins:
                return False
            plugin = self._plugin_instances.get(plugin_name)
            if plugin:
                try:
                    plugin.unregister(self.base_registry)
                except Exception:
                    pass
                del self._plugin_instances[plugin_name]
            self._plugins[plugin_name].status = PluginStatus.ARCHIVED
            del self._plugins[plugin_name]

            # Remove custom tokens
            tokens_to_remove = [
                tid for tid, tdef in self._custom_tokens.items()
                if tdef.token_id > 1000  # Custom tokens use IDs > 1000
            ]
            for tid in tokens_to_remove:
                del self._custom_tokens[tid]
        return True

    def get_plugin(self, name: str) -> Optional[Plugin]:
        """Get a plugin instance by name."""
        return self._plugin_instances.get(name)

    def get_plugin_manifest(self, name: str) -> Optional[PluginManifest]:
        """Get a plugin manifest by name."""
        return self._plugins.get(name)

    def list_plugins(self) -> List[PluginManifest]:
        """List all registered plugins."""
        return list(self._plugins.values())

    # ── Custom Token Definitions ──

    def register_custom_token(self, token_def: CustomTokenDefinition) -> bool:
        """Register a custom token type definition."""
        with self._lock:
            self._custom_tokens[token_def.token_id] = token_def
            # Add to base registry
            self.base_registry._registry[str(token_def.token_id)] = {
                "meaning": token_def.meaning,
                "category": token_def.category,
                "protocol_type": token_def.protocol_type,
                "routing_priority": token_def.routing_priority,
                "payload_schema": token_def.payload_schema or {},
                "version": token_def.version,
                "deprecated": token_def.deprecated,
                "metadata": token_def.metadata,
            }
        return True

    def register_protocol(self, protocol_name: str,
                            token_ids: List[int],
                            version: str = "1.0.0") -> bool:
        """Register a new protocol with a set of token IDs.

        Args:
            protocol_name: Name of the protocol.
            token_ids: Token IDs that comprise this protocol.
            version: Schema version for the protocol.

        Returns:
            True if the protocol was registered successfully.
        """
        with self._lock:
            if protocol_name not in self._schemas:
                self._schemas[protocol_name] = []
            version_str = str(version)
            if version_str not in self._schemas[protocol_name]:
                self._schemas[protocol_name].append(version_str)
                self._schemas[protocol_name].sort(
                    key=lambda v: SchemaVersion.from_string(v)
                )
        return True

    def get_protocol_schemas(self, protocol_name: str) -> List[str]:
        """Get all schema versions for a protocol."""
        return self._schemas.get(protocol_name, [])

    def get_all_protocols(self) -> Dict[str, List[str]]:
        """Get all registered protocols and their versions."""
        return {k: list(v) for k, v in self._schemas.items()}

    # ── Schema Versioning ──

    def register_schema(self, protocol_name: str, version: str,
                          schema_def: Dict[str, Any]) -> bool:
        """Register a schema definition for a protocol version."""
        with self._lock:
            if protocol_name not in self._schemas:
                self._schemas[protocol_name] = []
            version_str = str(version)
            if version_str not in self._schemas[protocol_name]:
                self._schemas[protocol_name].append(version_str)
            # Store schema definition
            if "_schemas_detail" not in self.__dict__:
                self._schema_details: Dict[str, Dict[str, Dict]] = {}
            if protocol_name not in self._schema_details:
                self._schema_details[protocol_name] = {}
            self._schema_details[protocol_name][version_str] = schema_def
        return True

    def get_schema(self, protocol_name: str,
                    version: Optional[str] = None) -> Optional[Dict]:
        """Get a schema definition. Returns latest version if version is None."""
        if not hasattr(self, "_schema_details"):
            return None
        if protocol_name not in self._schema_details:
            return None
        details = self._schema_details[protocol_name]
        if version is None:
            versions = list(details.keys())
            if not versions:
                return None
            version = SchemaVersion.latest(
                [SchemaVersion.from_string(v) for v in versions]
            ).__str__()
        return details.get(version)

    # ── Migration Paths ──

    def add_migration(self, from_version: str, to_version: str,
                        description: str,
                        transform_func: Optional[Callable] = None,
                        backward_compatible: bool = True) -> bool:
        """Add a migration path between two schema versions."""
        with self._lock:
            key = f"{from_version}→{to_version}"
            if key not in self._migrations:
                self._migrations[key] = []
            self._migrations[key].append(MigrationPath(
                from_version=from_version,
                to_version=to_version,
                description=description,
                transform_func=transform_func,
                backward_compatible=backward_compatible,
            ))
        return True

    def get_migrations(self, from_version: str, to_version: str
                         ) -> List[MigrationPath]:
        """Get migration paths between two versions."""
        key = f"{from_version}→{to_version}"
        return self._migrations.get(key, [])

    def migrate_message(self, message: Dict[str, Any],
                         target_version: str,
                         source_version: Optional[str] = None
                         ) -> Dict[str, Any]:
        """Migrate a message to a target schema version.

        Applies registered transformation functions in order.
        """
        # Find applicable migrations
        if source_version:
            key = f"{source_version}→{target_version}"
            migrations = self.get_migrations(source_version, target_version)
        else:
            # Try to find any migration to target
            migrations = []
            for mk, mlist in self._migrations.items():
                if mk.endswith(f"→{target_version}"):
                    migrations.extend(mlist)

        result = dict(message)
        for migration in migrations:
            if migration.transform_func:
                result = migration.transform_func(result)
            result["_migrated_from"] = source_version or "unknown"
            result["_migrated_to"] = target_version
            result["_migration_path"] = migration.to_dict()
        return result

    # ── Plugin Discovery ──

    def discover_plugins(self, directory: str) -> List[str]:
        """Discover plugins in a directory by looking for plugin files.

        Scans for Python files with a Plugin subclass and returns
        their module names.
        """
        plugin_dir = Path(directory)
        self._plugin_dir = plugin_dir
        discovered = []

        if not plugin_dir.exists():
            return discovered

        for py_file in plugin_dir.glob("*.py"):
            if py_file.name.startswith("_"):
                continue
            module_name = py_file.stem
            discovered.append(module_name)

        self._discovered_plugins = discovered
        return discovered

    def load_plugin(self, module_name: str,
                      search_path: Optional[str] = None) -> Optional[Plugin]:
        """Dynamically load a plugin module by name.

        Searches for a module with a Plugin subclass and instantiates it.
        """
        search_path = search_path or str(self._plugin_dir or Path("plugins"))

        try:
            sys.path.insert(0, search_path)
            module = __import__(module_name)
            sys.path.pop(0)
        except (ImportError, ModuleNotFoundError):
            return None

        # Find Plugin subclass in the module
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if isinstance(attr, type) and issubclass(attr, Plugin) and attr is not Plugin:
                try:
                    instance = attr()
                    return instance
                except Exception:
                    continue
        return None

    def discover_and_load_all(self, directory: str) -> Dict[str, Plugin]:
        """Discover plugins in a directory and load all of them."""
        discovered = self.discover_plugins(directory)
        loaded = {}
        for name in discovered:
            plugin = self.load_plugin(name, directory)
            if plugin:
                self.register_plugin(plugin)
                loaded[name] = plugin
        return loaded

    # ── Custom Token Type Registration ──

    def register_token_type(self, token_id: int, meaning: str,
                              category: str = "custom",
                              protocol_type: str = "data",
                              routing_priority: int = 50,
                              payload_schema: Optional[Dict] = None,
                              version: str = "1.0.0") -> CustomTokenDefinition:
        """Register a custom token type and return its definition.

        Convenience method that creates and registers a CustomTokenDefinition.
        """
        token_def = CustomTokenDefinition(
            token_id=token_id,
            meaning=meaning,
            category=category,
            protocol_type=protocol_type,
            routing_priority=routing_priority,
            payload_schema=payload_schema,
            version=version,
        )
        self.register_custom_token(token_def)
        return token_def

    def get_custom_token(self, token_id: int) -> Optional[CustomTokenDefinition]:
        """Get a custom token definition by ID."""
        return self._custom_tokens.get(token_id)

    def get_all_custom_tokens(self) -> List[CustomTokenDefinition]:
        """Get all custom token definitions."""
        return list(self._custom_tokens.values())

    # ── Status & Statistics ──

    def get_stats(self) -> Dict[str, Any]:
        """Get plugin registry statistics."""
        with self._lock:
            return {
                "plugins_registered": len(self._plugins),
                "plugins_active": sum(
                    1 for p in self._plugins.values()
                    if p.status == PluginStatus.ACTIVE
                ),
                "custom_tokens": len(self._custom_tokens),
                "protocols": len(self._schemas),
                "migration_paths": sum(len(v) for v in self._migrations.values()),
                "plugin_dir": str(self._plugin_dir) if self._plugin_dir else None,
                "discovered_plugins": len(self._discovered_plugins),
            }

    def to_dict(self) -> Dict[str, Any]:
        """Export the full plugin registry state."""
        return {
            "plugins": [m.to_dict() for m in self._plugins.values()],
            "custom_tokens": [t.to_dict() for t in self._custom_tokens.values()],
            "protocols": self.get_all_protocols(),
            "stats": self.get_stats(),
        }


# ─────────────────────────────────────────────
# Standalone Plugin Implementations
# ─────────────────────────────────────────────

class CorePlugin(Plugin):
    """Plugin that registers core protocol tokens."""

    def __init__(self):
        self._manifest = PluginManifest(
            name="core_protocol",
            version="2.0.0",
            description="Core TokenRelay protocol token definitions",
            author="TokenRelay",
            entry_point="src.plugin",
            schema_version="2.0.0",
        )

    @property
    def manifest(self) -> PluginManifest:
        return self._manifest

    def register(self, registry: TokenRegistry) -> bool:
        """Register core protocol tokens with the registry."""
        core_tokens = [
            CustomTokenDefinition(42, "Task complete", "status", "status", 1,
                                   {"required": ["result"]}, "2.0.0"),
            CustomTokenDefinition(0, "Error", "error", "error", 0,
                                   {"required": ["error_code", "message"]}, "2.0.0"),
            CustomTokenDefinition(150, "Start boundary", "boundary", "boundary", 0,
                                   {"required": ["message_id"]}, "2.0.0"),
            CustomTokenDefinition(151, "Attention", "control", "control", 1,
                                   {"required": ["attention_level"]}, "2.0.0"),
            CustomTokenDefinition(153, "Separator", "boundary", "boundary", 0,
                                   {"required": ["separator_type"]}, "2.0.0"),
        ]
        for token_def in core_tokens:
            registry.register_custom_token(token_def)
        return True

    def unregister(self, registry: TokenRegistry) -> bool:
        """Unregister core protocol tokens."""
        for token_id in [42, 0, 150, 151, 153]:
            registry.base_registry._registry.pop(str(token_id), None)
        return True

    def on_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Core protocol message processing."""
        tokens = message.get("tokens", [])
        if not tokens:
            return None
        return {"processed_by": "core_plugin", "token_count": len(tokens)}


class ValidationPlugin(Plugin):
    """Plugin that adds validation capabilities."""

    def __init__(self):
        self._manifest = PluginManifest(
            name="validation",
            version="1.1.0",
            description="Token validation and schema enforcement plugin",
            author="TokenRelay",
            entry_point="src.plugin",
            schema_version="1.1.0",
            dependencies=["core_protocol"],
        )

    @property
    def manifest(self) -> PluginManifest:
        return self._manifest

    def register(self, registry: TokenRegistry) -> bool:
        """Register validation tokens."""
        registry.register_custom_token(CustomTokenDefinition(
            token_id=2000, meaning="Validation check", category="control",
            protocol_type="control", routing_priority=5,
            payload_schema={"required": ["check_type"]}, version="1.1.0"
        ))
        return True

    def unregister(self, registry: TokenRegistry) -> bool:
        registry.base_registry._registry.pop("2000", None)
        return True

    def on_message(self, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Add validation metadata to messages."""
        return {**message, "validated_by": "validation_plugin"}


class MigrationPlugin(Plugin):
    """Plugin providing protocol migration support."""

    def __init__(self):
        self._manifest = PluginManifest(
            name="migration",
            version="1.0.0",
            description="Protocol migration path support",
            author="TokenRelay",
            entry_point="src.plugin",
            schema_version="1.0.0",
            dependencies=["core_protocol"],
        )

    @property
    def manifest(self) -> PluginManifest:
        return self._manifest

    def register(self, registry: TokenRegistry) -> bool:
        return True

    def unregister(self, registry: TokenRegistry) -> bool:
        return True

    def migrate(self, message: Dict[str, Any],
                 target_version: str) -> Dict[str, Any]:
        return {"migrated": True, "target": target_version, **message}
