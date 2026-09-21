"""
TokenRelay — Extended token registry with communication protocol fields.

Extends the base meanings.json with protocol-specific metadata:
- protocol_type: How this token is used in messages (control, status, data, error)
- routing_priority: Integer for parent agent routing decisions
- payload_schema: Expected structure when this token appears in a message
- compression: Compression metadata and support
- signing: Signing metadata and support
- encryption: Encryption metadata and support
"""

import json
from pathlib import Path
from typing import Optional, Dict, Any, List

BASE_DATA_DIR = Path(__file__).resolve().parent.parent / "data"


def load_meanings() -> Dict[str, Dict]:
    """Load the base meanings.json registry."""
    base = Path("/home/columbo/ExtData/Tokenizer/data") / "meanings.json"
    with open(base) as f:
        return json.load(f)


def load_protocol_registry() -> Dict[str, Dict]:
    """Load the extended protocol registry (protocol_tokens.json)."""
    registry_path = BASE_DATA_DIR / "protocol_tokens.json"
    if registry_path.exists():
        with open(registry_path) as f:
            return json.load(f)
    return {}


# ──────────────────────────────────────────────
# v2 Extension defaults for all tokens
# ──────────────────────────────────────────────
V2_EXTENSION_DEFAULTS = {
    "compression_supported": True,
    "compression_algorithms": ["zlib"],
    "compression_default": "zlib",
    "compression_level": 9,
    "signing_supported": True,
    "signing_algorithm": "HMAC-SHA256",
    "encryption_supported": True,
    "encryption_algorithm": "AES-256-GCM",
    "encryption_key_size": 32,
    "batch_supported": True,
    "batch_max_size": 100,
}


def build_protocol_registry() -> Dict[str, Dict]:
    """Build the full protocol registry by merging base meanings with protocol fields.

    v2 adds compression, signing, and encryption metadata to each token entry.
    """
    meanings = load_meanings()
    protocol_overrides = load_protocol_registry()

    registry = {}
    for token_id, token_data in meanings.items():
        entry = dict(token_data)
        # Apply protocol-specific overrides
        if token_id in protocol_overrides:
            entry.update(protocol_overrides[token_id])
        # Add v2 extension metadata
        entry.update(_build_v2_metadata(token_id, entry))
        registry[token_id] = entry

    # Add any protocol-only tokens not in the base meanings
    for token_id, protocol_data in protocol_overrides.items():
        if token_id not in registry:
            entry = dict(protocol_data)
            entry.update(_build_v2_metadata(token_id, entry))
            registry[token_id] = entry

    return registry


def _build_v2_metadata(token_id: str, token_data: Dict) -> Dict[str, Any]:
    """Build v2 extension metadata for a token entry.

    Merges v2 defaults with any token-specific overrides.
    """
    metadata = dict(V2_EXTENSION_DEFAULTS)

    # Token-specific compression overrides (from protocol_tokens.json)
    if "compression" in token_data:
        metadata["compression_supported"] = token_data["compression"].get(
            "supported", metadata["compression_supported"]
        )
        metadata["compression_algorithms"] = token_data["compression"].get(
            "algorithms", metadata["compression_algorithms"]
        )
        metadata["compression_default"] = token_data["compression"].get(
            "default_algorithm", metadata["compression_default"]
        )

    # Token-specific signing overrides
    if "signing" in token_data:
        metadata["signing_supported"] = token_data["signing"].get(
            "supported", metadata["signing_supported"]
        )
        metadata["signing_algorithm"] = token_data["signing"].get(
            "algorithm", metadata["signing_algorithm"]
        )

    # Token-specific encryption overrides
    if "encryption" in token_data:
        metadata["encryption_supported"] = token_data["encryption"].get(
            "supported", metadata["encryption_supported"]
        )
        metadata["encryption_algorithm"] = token_data["encryption"].get(
            "algorithm", metadata["encryption_algorithm"]
        )

    # Control/boundary tokens don't carry payload data, so compression is N/A
    ptype = token_data.get("protocol_type", "")
    if ptype in ("control", "boundary"):
        metadata["compression_supported"] = False
        metadata["encryption_supported"] = False

    # Error and status tokens always support signing
    if ptype in ("error", "status"):
        metadata["signing_required"] = True

    return metadata


class TokenRegistry:
    """Manages the token registry with protocol communication fields.

    v2 adds support for compression, signing, and encryption metadata.
    """

    def __init__(self, registry: Optional[Dict[str, Dict]] = None):
        self._registry = registry or build_protocol_registry()

    def get_token(self, token_id: int) -> Optional[Dict[str, Any]]:
        """Get token data by ID. Returns None if not found."""
        return self._registry.get(str(token_id))

    def get_meaning(self, token_id: int) -> Optional[str]:
        """Get just the meaning text for a token."""
        token = self.get_token(token_id)
        return token.get("meaning") if token else None

    def get_protocol_type(self, token_id: int) -> Optional[str]:
        """Get the protocol type for routing."""
        token = self.get_token(token_id)
        return token.get("protocol_type") if token else None

    def get_routing_priority(self, token_id: int) -> int:
        """Get routing priority (lower = higher priority)."""
        token = self.get_token(token_id)
        if token and "routing_priority" in token:
            return token["routing_priority"]
        return 999  # Default lowest priority

    def get_payload_schema(self, token_id: int) -> Optional[Dict]:
        """Get the expected payload schema for this token."""
        token = self.get_token(token_id)
        return token.get("payload_schema") if token else None

    # ──────────────────────────────────────────────
    # v2 metadata accessors
    # ──────────────────────────────────────────────
    def get_compression_info(self, token_id: int) -> Dict[str, Any]:
        """Get compression metadata for a token.

        Returns dict with compression support info.
        """
        token = self.get_token(token_id)
        if not token:
            return {}
        return {
            "supported": token.get("compression_supported", False),
            "algorithms": token.get("compression_algorithms", []),
            "default": token.get("compression_default", "zlib"),
            "level": token.get("compression_level", 9),
        }

    def get_signing_info(self, token_id: int) -> Dict[str, Any]:
        """Get signing metadata for a token.

        Returns dict with signing support info.
        """
        token = self.get_token(token_id)
        if not token:
            return {}
        return {
            "supported": token.get("signing_supported", False),
            "algorithm": token.get("signing_algorithm", "HMAC-SHA256"),
            "required": token.get("signing_required", False),
        }

    def get_encryption_info(self, token_id: int) -> Dict[str, Any]:
        """Get encryption metadata for a token.

        Returns dict with encryption support info.
        """
        token = self.get_token(token_id)
        if not token:
            return {}
        return {
            "supported": token.get("encryption_supported", False),
            "algorithm": token.get("encryption_algorithm", "AES-256-GCM"),
            "key_size": token.get("encryption_key_size", 32),
        }

    def get_batch_info(self, token_id: int) -> Dict[str, Any]:
        """Get batch metadata for a token."""
        token = self.get_token(token_id)
        if not token:
            return {}
        return {
            "supported": token.get("batch_supported", True),
            "max_size": token.get("batch_max_size", 100),
        }

    def is_compression_supported(self, token_id: int) -> bool:
        """Check if token supports compression."""
        return self.get_compression_info(token_id).get("supported", False)

    def is_signing_supported(self, token_id: int) -> bool:
        """Check if token supports signing."""
        return self.get_signing_info(token_id).get("supported", False)

    def is_encryption_supported(self, token_id: int) -> bool:
        """Check if token supports encryption."""
        return self.get_encryption_info(token_id).get("supported", False)

    def is_signing_required(self, token_id: int) -> bool:
        """Check if token requires signing (e.g., error/status tokens)."""
        return self.get_signing_info(token_id).get("required", False)

    def is_control_token(self, token_id: int) -> bool:
        """Check if token is a control/token boundary marker."""
        ptype = self.get_protocol_type(token_id)
        return ptype in ("control", "boundary")

    def is_status_token(self, token_id: int) -> bool:
        """Check if token represents a task status."""
        ptype = self.get_protocol_type(token_id)
        return ptype == "status"

    def is_error_token(self, token_id: int) -> bool:
        """Check if token represents an error condition."""
        ptype = self.get_protocol_type(token_id)
        return ptype == "error"

    def register_custom_token(self, token_def: Any) -> bool:
        """Register a custom token definition in the registry."""
        self._registry[str(token_def.token_id)] = {
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

    def route_message(self, tokens: List[int]) -> Dict[str, Any]:
        """Determine how to route a message based on token IDs.

        Returns routing instructions for the parent agent.
        """
        if not tokens:
            return {"action": "unknown", "priority": 999}

        first_token = tokens[0]
        ptype = self.get_protocol_type(first_token)
        priority = self.get_routing_priority(first_token)

        routing = {
            "action": ptype or "data",
            "priority": priority,
            "first_token": first_token,
            "token_count": len(tokens),
        }

        # Add secondary routing hints from second token
        if len(tokens) > 1:
            second_type = self.get_protocol_type(tokens[1])
            routing["secondary_action"] = second_type or "data"

        # Add v2 feature flags if all tokens support them
        all_compress = all(self.is_compression_supported(t) for t in tokens)
        all_sign = all(self.is_signing_supported(t) for t in tokens)
        all_encrypt = all(self.is_encryption_supported(t) for t in tokens)

        if all_compress:
            routing["features"] = routing.get("features", {})
            routing["features"]["compression"] = True
        if all_sign:
            routing["features"] = routing.get("features", {})
            routing["features"]["signing"] = True
        if all_encrypt:
            routing["features"] = routing.get("features", {})
            routing["features"]["encryption"] = True

        return routing

    def get_broker_priority(self, token_id: int) -> int:
        """Get broker queue priority for a token.

        Maps registry routing_priority to broker priority levels.
        Returns PRIORITY_MEDIUM for unknown tokens.
        """
        priority = self.get_routing_priority(token_id)
        # Normalize: map to broker priority constants
        if priority <= 1:
            return 0  # PRIORITY_CRITICAL equivalent
        elif priority <= 5:
            return 1  # PRIORITY_HIGH equivalent
        elif priority <= 10:
            return 5  # PRIORITY_MEDIUM equivalent
        elif priority <= 50:
            return 50  # PRIORITY_LOW equivalent
        else:
            return 100  # PRIORITY_BULK equivalent

    def get_broker_prio_label(self, token_id: int) -> str:
        """Get a human-readable broker priority label for a token."""
        p = self.get_broker_priority(token_id)
        if p == 0:
            return "critical"
        elif p == 1:
            return "high"
        elif p == 5:
            return "medium"
        elif p == 50:
            return "low"
        else:
            return "bulk"

    def broker_route(self, tokens: List[int]) -> Dict[str, Any]:
        """Get routing info with broker-specific priority levels.

        Extends route_message with broker priority labels and
        queue placement hints.
        """
        routing = self.route_message(tokens)
        if not tokens:
            routing["broker_priority"] = 50
            routing["broker_priority_label"] = "medium"
            return routing

        first_token = tokens[0]
        routing["broker_priority"] = self.get_broker_priority(first_token)
        routing["broker_priority_label"] = self.get_broker_prio_label(
            first_token
        )
        return routing

    def get_broker_priority_distribution(self) -> Dict[str, int]:
        """Get count of tokens per broker priority level.

        Returns:
            Dict with keys "critical", "high", "medium", "low", "bulk"
            and their token counts.
        """
        distribution = {"critical": 0, "high": 0, "medium": 0,
                         "low": 0, "bulk": 0}
        for token_id in self._registry:
            p = self.get_broker_priority(int(token_id))
            label = self.get_broker_prio_label(int(token_id))
            distribution[label] += 1
        return distribution

    def get_queue_recommendation(self, tokens: List[int]) -> Dict[str, Any]:
        """Get broker queue recommendation for a message.

        Returns:
            Dict with enqueue priority, DLQ recommendation, and
            batch grouping hint.
        """
        routing = self.broker_route(tokens)
        broker_prio = routing["broker_priority"]

        # DLQ recommendation based on priority and type
        ptype = self.get_protocol_type(tokens[0]) if tokens else None
        if broker_prio >= 100:
            dlq_recommendation = "batch"
        elif broker_prio == 0 or ptype == "error":
            dlq_recommendation = "immediate"
        else:
            dlq_recommendation = "standard"

        # Batch grouping hint
        if broker_prio >= 50:
            batch_hint = "group_with_similar_priority"
        else:
            batch_hint = "process_individually"

        return {
            "broker_priority": broker_prio,
            "priority_label": routing["broker_priority_label"],
            "dlq_recommendation": dlq_recommendation,
            "batch_hint": batch_hint,
            "protocol_type": ptype,
        }

    def to_dict(self) -> Dict[str, Dict]:
        """Export the full registry as a dictionary."""
        return self._registry

    def get_features_summary(self) -> Dict[str, Any]:
        """Get a summary of v2 features across all tokens.

        Returns:
            Dict with counts of tokens supporting each feature
        """
        features = {
            "total_tokens": len(self._registry),
            "compression_supported": 0,
            "signing_supported": 0,
            "encryption_supported": 0,
            "batch_supported": 0,
            "signing_required": 0,
            "compression_not_supported": [],
        }

        for token_id, token_data in self._registry.items():
            if token_data.get("compression_supported"):
                features["compression_supported"] += 1
            else:
                features["compression_not_supported"].append(int(token_id))
            if token_data.get("signing_supported"):
                features["signing_supported"] += 1
            if token_data.get("encryption_supported"):
                features["encryption_supported"] += 1
            if token_data.get("batch_supported"):
                features["batch_supported"] += 1
            if token_data.get("signing_required"):
                features["signing_required"] += 1

        return features


def save_protocol_registry(registry: Dict[str, Dict], path: Optional[Path] = None):
    """Save the protocol registry to a JSON file."""
    output_path = path or (BASE_DATA_DIR / "protocol_tokens.json")
    with open(output_path, "w") as f:
        json.dump(registry, f, indent=2, ensure_ascii=False)
    return output_path


# Build the global registry instance
global_registry = TokenRegistry()


def get_registry() -> TokenRegistry:
    """Get the global token registry instance."""
    return global_registry