"""
TokenRelay v3 — Edge Pre-Computation (EPC).

Caches frequent query patterns at edge nodes with TTL, delta encoding
for changes, and pre-computed common responses.

Built on v2 modules:
- codec.compress_message/decompress_message for cached response compression
- codec.batch_encode/batch_decode for delta computation
- broker.MessageBroker for distributed cache coordination
- streaming.EventBus for cache hit/miss events
- registry.TokenRegistry for cache key registration
"""

import time
import hashlib
import json
import threading
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List, Tuple, Callable
from collections import deque, defaultdict
from enum import Enum, auto

from codec import compress_message, decompress_message, batch_encode, batch_decode
from broker import MessageBroker
from streaming import EventBus, StreamEvent, StreamEventType
from registry import TokenRegistry, get_registry


# ─────────────────────────────────────────────
# Types & Enums
# ─────────────────────────────────────────────

class CacheStatus(Enum):
    """Cache entry status."""
    HIT = auto()
    MISS = auto()
    EXPIRED = auto()
    EVICTED = auto()
    PRE_COMPUTED = auto()


class DeltaOperation(Enum):
    """Types of delta operations."""
    ADD = auto()
    REMOVE = auto()
    MODIFY = auto()
    REPLACE = auto()


# ─────────────────────────────────────────────
# Configuration & Stats
# ─────────────────────────────────────────────

@dataclass
class EPCConfig:
    """Configuration for the Edge Pre-Computation system.

    Attributes:
        default_ttl: Default time-to-live for cache entries in seconds.
        max_entries: Maximum number of entries per edge cache.
        delta_threshold: Minimum change ratio (0-1) to warrant delta encoding.
        prediction_window: Number of recent queries to consider for prediction.
        edge_nodes: Number of simulated edge nodes.
        cleanup_interval: Seconds between expired entry cleanup sweeps.
        enable_delta: Whether delta encoding is enabled globally.
        enable_precompute: Whether pre-computation of common patterns is on.
        replication_factor: How many edge nodes replicate each cache entry.
    """
    default_ttl: float = 300.0
    max_entries: int = 10_000
    delta_threshold: float = 0.15
    prediction_window: int = 100
    edge_nodes: int = 5
    cleanup_interval: float = 60.0
    enable_delta: bool = True
    enable_precompute: bool = True
    replication_factor: int = 2


@dataclass
class CacheHitStats:
    """Statistics tracking for cache performance.

    Attributes:
        total_requests: Total number of cache lookup requests.
        cache_hits: Number of successful cache hits.
        cache_misses: Number of cache misses.
        total_bytes_saved: Bytes saved via delta encoding.
        total_predictions: Number of prediction attempts.
        prediction_accuracy: Ratio of correct predictions.
        edge_node_hits: Per-node hit counts.
    """
    total_requests: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    total_bytes_saved: int = 0
    total_predictions: int = 0
    correct_predictions: int = 0
    edge_node_hits: Dict[str, int] = field(default_factory=lambda: defaultdict(int))

    @property
    def hit_rate(self) -> float:
        """Cache hit rate as a percentage."""
        if self.total_requests == 0:
            return 0.0
        return round(self.cache_hits / self.total_requests * 100, 2)

    @property
    def prediction_accuracy(self) -> float:
        """Prediction accuracy as a percentage."""
        if self.total_predictions == 0:
            return 0.0
        return round(self.correct_predictions / self.total_predictions * 100, 2)

    @property
    def delta_efficiency(self) -> float:
        """How much space delta encoding saves."""
        if self.total_requests == 0:
            return 0.0
        return round(self.total_bytes_saved / max(self.total_requests, 1) * 100, 2)

    def record_hit(self, node_id: str = "default"):
        """Record a cache hit."""
        self.total_requests += 1
        self.cache_hits += 1
        self.edge_node_hits[node_id] += 1

    def record_miss(self, node_id: str = "default"):
        """Record a cache miss."""
        self.total_requests += 1
        self.cache_misses += 1

    def record_prediction(self, correct: bool):
        """Record a prediction attempt."""
        self.total_predictions += 1
        if correct:
            self.correct_predictions += 1

    def record_bytes_saved(self, bytes_saved: int):
        """Record bytes saved via delta encoding."""
        self.total_bytes_saved += bytes_saved

    def reset(self):
        """Reset all counters."""
        self.total_requests = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.total_bytes_saved = 0
        self.total_predictions = 0
        self.correct_predictions = 0
        self.edge_node_hits.clear()

    def to_dict(self) -> Dict[str, Any]:
        """Export stats as a dictionary."""
        d = {f.name: getattr(self, f.name) for f in self.__dataclass_fields__.values()}
        d["hit_rate"] = self.hit_rate
        d["prediction_accuracy"] = self.prediction_accuracy
        d["delta_efficiency"] = self.delta_efficiency
        return d


# ─────────────────────────────────────────────
# EdgeCache — TTL + LRU Eviction with v2 compression
# ─────────────────────────────────────────────

class _CacheEntry:
    """Internal cache entry with value, expiry, and LRU tracking."""
    __slots__ = ("key", "value", "expiry", "size", "last_access", "compressed")

    def __init__(self, key: str, value: Any, ttl: float, size: int, compressed: bool = False):
        self.key = key
        self.value = value
        self.expiry = time.time() + ttl
        self.size = size
        self.last_access = time.time()
        self.compressed = compressed

    @property
    def is_expired(self) -> bool:
        return time.time() > self.expiry


class EdgeCache:
    """TTL + LRU eviction cache for storing pre-computed responses.

    Uses v2 compress_message/decompress_message for response compression.
    Integrates with TokenRegistry for cache key registration.

    Features:
    - TTL-based expiry for cache entries
    - LRU eviction when capacity is reached
    - Optional compression using v2 codec
    - Thread-safe operations
    - Per-node statistics tracking

    Example:
        cache = EdgeCache(node_id="edge_0", config=EPCConfig(max_entries=100))
        cache.put("query:42", {"result": "success"}, ttl=60)
        result = cache.get("query:42")
    """

    def __init__(self, node_id: str = "node_0", config: Optional[EPCConfig] = None,
                 registry: Optional[TokenRegistry] = None):
        self.node_id = node_id
        self._config = config or EPCConfig()
        self._registry = registry or get_registry()

        # ── Storage ──
        self._cache: Dict[str, _CacheEntry] = {}
        self._lru_order: deque = deque()  # Keys ordered by access (oldest first)
        self._key_sizes: Dict[str, int] = {}  # Approximate serialized sizes

        # ── Locks ──
        self._lock = threading.RLock()

        # ── Stats ──
        self._stats = CacheHitStats()
        self._total_evictions = 0
        self._total_expirations = 0
        self._total_compressed = 0
        self._total_decompressed = 0

    # ── Core Operations ──────────────────────────────────

    def put(self, key: str, value: Any, ttl: Optional[float] = None,
             size: Optional[int] = None, compress: bool = True) -> bool:
        """Store a value in the cache with optional TTL and compression.

        Args:
            key: Cache key (query pattern or request hash).
            value: Response data to cache.
            ttl: Time-to-live in seconds. Defaults to config.default_ttl.
            size: Approximate entry size. Computed from value if not given.
            compress: Whether to compress using v2 compress_message.

        Returns:
            True if the entry was stored successfully.
        """
        with self._lock:
            # Register key with TokenRegistry
            self._register_key(key, value)

            # Evict if key exists and will be re-added
            if key in self._cache:
                self._lru_order.remove(key)

            # Evict LRU entries if at capacity
            while len(self._cache) >= self._config.max_entries and self._lru_order:
                self._evict_lru()

            # Compress if enabled and value is a dict with payload
            compressed_value = value
            did_compress = False
            if compress and isinstance(value, dict) and value.get("payload"):
                try:
                    compressed_value = compress_message(dict(value))
                    did_compress = True
                    self._total_compressed += 1
                except Exception:
                    compressed_value = value

            if size is None:
                size = self._estimate_size(compressed_value)

            ttl = ttl if ttl is not None else self._config.default_ttl
            entry = _CacheEntry(key, compressed_value, ttl, size, compressed=did_compress)
            self._cache[key] = entry
            self._lru_order.append(key)
            self._key_sizes[key] = size
            return True

    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value from the cache.

        Returns None if the key is not found or the entry has expired.
        On hit, updates LRU ordering and decompresses if needed.
        """
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                self._stats.record_miss(self.node_id)
                return None

            if entry.is_expired:
                del self._cache[key]
                self._lru_order.remove(key)
                self._total_expirations += 1
                self._stats.record_miss(self.node_id)
                return None

            # Update LRU: move to end (most recently used)
            self._lru_order.remove(key)
            self._lru_order.append(key)
            entry.last_access = time.time()

            # Decompress if compressed
            value = entry.value
            if entry.compressed and isinstance(value, dict):
                try:
                    value = decompress_message(dict(value))
                    self._total_decompressed += 1
                except Exception:
                    pass

            self._stats.record_hit(self.node_id)
            return value

    def delete(self, key: str) -> bool:
        """Remove a specific entry from the cache.

        Returns True if the entry was found and removed.
        """
        with self._lock:
            if key not in self._cache:
                return False
            del self._cache[key]
            self._lru_order.remove(key)
            self._key_sizes.pop(key, None)
            return True

    def clear(self):
        """Remove all entries from the cache."""
        with self._lock:
            self._cache.clear()
            self._lru_order.clear()
            self._key_sizes.clear()

    # ── Eviction ─────────────────────────────────────────

    def _evict_lru(self):
        """Evict the least recently used entry."""
        if not self._lru_order:
            return
        oldest_key = self._lru_order.popleft()
        if oldest_key in self._cache:
            del self._cache[oldest_key]
            self._key_sizes.pop(oldest_key, None)
            self._total_evictions += 1

    def evict_expired(self) -> int:
        """Remove all expired entries. Returns the number removed."""
        with self._lock:
            removed = 0
            expired_keys = [k for k, v in self._cache.items() if v.is_expired]
            for key in expired_keys:
                del self._cache[key]
                self._lru_order.remove(key)
                self._key_sizes.pop(key, None)
                removed += 1
            self._total_expirations += removed
            return removed

    # ── Key Registration with TokenRegistry ────────────

    def _register_key(self, key: str, value: Any):
        """Register a cache key with the TokenRegistry if available."""
        try:
            if hasattr(self._registry, '_registry') and key not in self._registry._registry:
                self._registry._registry[key] = {
                    "meaning": f"Cached query: {key}",
                    "category": "cached",
                    "protocol_type": "data",
                    "routing_priority": 50,
                    "payload_schema": {},
                    "version": "3.0",
                    "epc_cached": True,
                }
        except Exception:
            pass  # Registry not available or not mutable

    # ── Query ────────────────────────────────────────────

    def contains(self, key: str) -> bool:
        """Check if key exists and is not expired."""
        with self._lock:
            entry = self._cache.get(key)
            if entry is None:
                return False
            if entry.is_expired:
                return False
            return True

    def get_size(self) -> int:
        """Current number of entries in the cache."""
        with self._lock:
            return len(self._cache)

    @property
    def is_full(self) -> bool:
        """True if the cache has reached max_entries."""
        with self._lock:
            return len(self._cache) >= self._config.max_entries

    @property
    def depth(self) -> int:
        """Current number of valid entries."""
        with self._lock:
            return len(self._cache)

    def get_compression_stats(self) -> Dict[str, int]:
        """Return compression-related stats."""
        with self._lock:
            return {
                "total_compressed": self._total_compressed,
                "total_decompressed": self._total_decompressed,
            }

    # ── Statistics ────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        """Return comprehensive cache statistics for this node."""
        with self._lock:
            return {
                "node_id": self.node_id,
                "depth": len(self._cache),
                "capacity": self._config.max_entries,
                "utilization": round(len(self._cache) / max(self._config.max_entries, 1), 4),
                "total_evictions": self._total_evictions,
                "total_expirations": self._total_expirations,
                "hit_rate": self._stats.hit_rate,
                "cache_hits": self._stats.cache_hits,
                "cache_misses": self._stats.cache_misses,
                "compressed": self._total_compressed,
                "decompressed": self._total_decompressed,
            }

    def get_cache_stats(self) -> CacheHitStats:
        """Get the CacheHitStats object for this node."""
        return self._stats

    # ── Internal ─────────────────────────────────────────

    @staticmethod
    def _estimate_size(value: Any) -> int:
        """Estimate the serialized size of a value."""
        try:
            return len(json.dumps(value, sort_keys=True).encode("utf-8"))
        except (TypeError, ValueError):
            return 1024


# ─────────────────────────────────────────────
# DeltaEncoder — Only Changes from Cached Response
# Uses v2 batch_encode/batch_decode for delta computation
# ─────────────────────────────────────────────

class DeltaEncoder:
    """Compute only changes (deltas) between cached and new responses.

    Uses v2 batch_encode/batch_decode for structured delta computation.
    Structural diffing on dict/list values to produce compact representations.
    Only sends deltas when the change ratio exceeds delta_threshold.

    Example:
        encoder = DeltaEncoder()
        base = {"status": "ok", "count": 5}
        updated = {"status": "ok", "count": 7}
        delta = encoder.compute_delta(base, updated)
        reconstructed = encoder.apply_delta(base, delta)
    """

    def __init__(self, threshold: Optional[float] = None):
        self.threshold = threshold if threshold is not None else 0.15
        self._total_deltas = 0
        self._total_bytes_saved = 0
        self._batch_buffer: List[Dict[str, Any]] = []

    def compute_delta(self, base: Any, updated: Any) -> Dict[str, Any]:
        """Compute the delta between base and updated values.

        Uses batch_encode to structure the delta computation,
        and batch_decode to verify reconstruction.

        Args:
            base: The original (cached) value.
            updated: The new value to compute delta for.

        Returns:
            Dict with 'operations' list describing changes,
            and 'base_snapshot' for reconstruction context.
        """
        # Compute structural differences
        delta = self._diff(base, updated, "")
        operations = delta.get("operations", [])

        # Use v2 batch_encode for structured delta representation
        batch_messages = []
        for op in operations:
            batch_msg = {
                "tokens": [0],  # Batch marker
                "payload": {"delta_op": op, "base_path": op.get("path", "")},
                "timestamp": int(time.time()),
                "protocol_version": "2.0",
                "batch_index": len(batch_messages),
            }
            batch_messages.append(batch_msg)

        if batch_messages:
            # Encode using v2 batch_encode for validation
            batch = batch_encode(batch_messages)
            decoded = batch_decode(batch)
            # Validate round-trip
            if len(decoded) != len(batch_messages):
                operations = []

        # Determine if delta is worth encoding
        base_size = self._estimate_size(base)
        delta_size = self._estimate_size(operations)

        # Only use delta if it's actually smaller than the base
        if base_size > 0 and delta_size < base_size:
            savings_ratio = (base_size - delta_size) / base_size
            if savings_ratio < self.threshold and operations:
                # Delta saves too little; use full replacement instead
                return {
                    "operations": [{"op": "REPLACE", "path": "", "value": updated}],
                    "base_snapshot": {},
                    "full_replace": True,
                }
        # When delta is larger than base, still return operations
        # (they're valid structural diffs, full_replace would also work but ops are useful)

        self._total_deltas += 1
        self._total_bytes_saved += max(0, base_size - delta_size)

        return {
            "operations": operations,
            "base_snapshot": {},
            "full_replace": False,
        }

    def apply_delta(self, base: Any, delta: Dict[str, Any]) -> Any:
        """Apply a delta to a base value to reconstruct the updated value.

        Args:
            base: The original cached value.
            delta: The delta dict from compute_delta().

        Returns:
            The reconstructed updated value.
        """
        if delta.get("full_replace"):
            for op in delta.get("operations", []):
                if op.get("op") == "REPLACE":
                    return op.get("value")
            return base

        result = base
        for op in delta.get("operations", []):
            result = self._apply_operation(result, op)
        return result

    def _diff(self, base: Any, updated: Any, path: str) -> Dict[str, Any]:
        """Recursively compute structural differences."""
        operations: List[Dict[str, Any]] = []

        if type(base) != type(updated):
            operations.append({"op": "REPLACE", "path": path or "/", "value": updated})
            return {"operations": operations}

        if isinstance(base, dict):
            all_keys = set(list(base.keys()) + list(updated.keys()))
            for key in sorted(all_keys):
                child_path = f"{path}/{key}" if path else key
                if key not in base:
                    operations.append({"op": "ADD", "path": child_path, "value": updated[key]})
                elif key not in updated:
                    operations.append({"op": "REMOVE", "path": child_path})
                elif base[key] != updated[key]:
                    child_ops = self._diff(base[key], updated[key], child_path)
                    operations.extend(child_ops.get("operations", [{"op": "MODIFY", "path": child_path, "value": updated[key]}]))

        elif isinstance(base, list):
            max_len = max(len(base), len(updated))
            for i in range(max_len):
                child_path = f"{path}[{i}]" if path else f"[{i}]"
                if i >= len(base):
                    operations.append({"op": "ADD", "path": child_path, "value": updated[i]})
                elif i >= len(updated):
                    operations.append({"op": "REMOVE", "path": child_path})
                elif base[i] != updated[i]:
                    child_ops = self._diff(base[i], updated[i], child_path)
                    operations.extend(child_ops.get("operations", [{"op": "MODIFY", "path": child_path, "value": updated[i]}]))

        elif base != updated:
            operations.append({"op": "MODIFY", "path": path or "/", "value": updated})

        return {"operations": operations}

    def _apply_operation(self, target: Any, op: Dict[str, Any]) -> Any:
        """Apply a single operation to a target value."""
        op_type = op.get("op")
        path = op.get("path", "")
        value = op.get("value")

        if op_type == "REPLACE" and not path:
            return value

        if not path:
            return target

        parts = self._parse_path(path)
        if not parts:
            return target

        result = target
        for part in parts[:-1]:
            if isinstance(result, dict) and part in result:
                result = result[part]
            elif isinstance(result, list) and part.isdigit() and int(part) < len(result):
                result = result[int(part)]
            else:
                return target

        last = parts[-1]
        if isinstance(result, dict):
            if op_type == "ADD":
                result[last] = value
            elif op_type == "REMOVE":
                result.pop(last, None)
            elif op_type in ("MODIFY", "REPLACE"):
                result[last] = value
        elif isinstance(result, list) and last.isdigit():
            idx = int(last)
            if op_type == "ADD" and idx <= len(result):
                result.insert(idx, value)
            elif op_type == "REMOVE" and idx < len(result):
                result.pop(idx)
            elif op_type in ("MODIFY", "REPLACE") and idx < len(result):
                result[idx] = value

        return target

    @staticmethod
    def _parse_path(path: str) -> List[str]:
        """Parse a path string into components."""
        parts = []
        current = ""
        in_brackets = False
        for ch in path:
            if ch == "[":
                in_brackets = True
                if current:
                    parts.append(current)
                    current = ""
            elif ch == "]":
                in_brackets = False
                if current:
                    parts.append(current)
                    current = ""
            elif ch == "/" and not in_brackets:
                if current:
                    parts.append(current)
                    current = ""
            else:
                current += ch
        if current:
            parts.append(current)
        return parts

    def get_efficiency(self) -> Dict[str, Any]:
        """Return delta encoding efficiency statistics."""
        return {
            "total_deltas": self._total_deltas,
            "total_bytes_saved": self._total_bytes_saved,
            "avg_bytes_per_delta": round(self._total_bytes_saved / max(self._total_deltas, 1), 2),
        }

    @staticmethod
    def _estimate_size(value: Any) -> int:
        """Estimate serialized size of a value."""
        try:
            return len(json.dumps(value, sort_keys=True).encode("utf-8"))
        except (TypeError, ValueError):
            return 1024


# ─────────────────────────────────────────────
# ResponsePredictor — Precompute Common Patterns with v2 EventBus
# ─────────────────────────────────────────────

class ResponsePredictor:
    """Precompute and predict responses for common query patterns.

    Uses v2 EventBus for cache hit/miss event notifications.
    Pattern matching (exact, prefix, keyword) to predict responses
    before the model is invoked.

    Example:
        predictor = ResponsePredictor()
        predictor.register_pattern(
            "What is the weather?",
            {"weather": "sunny", "temp": 72},
            confidence=0.95
        )
        prediction = predictor.predict("What is the weather?")
    """

    def __init__(self, config: Optional[EPCConfig] = None,
                 event_bus: Optional[EventBus] = None):
        self._config = config or EPCConfig()
        self._event_bus = event_bus or EventBus()
        self._patterns: Dict[str, Dict[str, Any]] = {}
        self._prefix_index: Dict[str, List[str]] = defaultdict(list)
        self._keyword_index: Dict[str, List[str]] = defaultdict(list)
        self._lock = threading.RLock()
        self._prediction_hits = 0
        self._prediction_misses = 0

        # Register default event callbacks
        self._setup_event_callbacks()

    def _setup_event_callbacks(self):
        """Set up EventBus callbacks for cache events."""
        try:
            self._event_bus.register(
                callback=lambda e: setattr(self, '_prediction_hits', self._prediction_hits + 1),
                event_types=[StreamEventType.TOKEN_RECEIVED],
                priority=1,
                callback_id="epc_hit_listener",
            )
        except Exception:
            pass

    def register_pattern(self, query_pattern: str, response: Any,
                         confidence: float = 0.8, keywords: Optional[List[str]] = None):
        """Register a query pattern with its predicted response.

        Args:
            query_pattern: The query text or pattern to match.
            response: The pre-computed response to return.
            confidence: Prediction confidence (0.0 to 1.0).
            keywords: Optional keywords for keyword-based matching.
        """
        with self._lock:
            pattern_id = hashlib.sha256(query_pattern.encode()).hexdigest()[:16]
            self._patterns[pattern_id] = {
                "query_pattern": query_pattern,
                "response": response,
                "confidence": confidence,
                "keywords": keywords or self._extract_keywords(query_pattern),
                "hit_count": 0,
                "registered_at": time.time(),
            }

            # Build prefix index
            words = query_pattern.lower().split()
            for i in range(1, len(words) + 1):
                prefix = " ".join(words[:i])
                self._prefix_index[prefix].append(pattern_id)

            # Build keyword index
            for kw in self._patterns[pattern_id]["keywords"]:
                self._keyword_index[kw.lower()].append(pattern_id)

    def predict(self, query: str) -> Optional[Dict[str, Any]]:
        """Predict a response for a query.

        Tries exact match first, then prefix match, then keyword match.
        Emits events via EventBus on hit/miss.

        Args:
            query: The user query to predict a response for.

        Returns:
            Dict with 'response', 'confidence', 'pattern_id', 'method'
            or None if no prediction found.
        """
        with self._lock:
            # Exact match
            for pid, pattern in self._patterns.items():
                if pattern["query_pattern"] == query:
                    pattern["hit_count"] += 1
                    self._prediction_hits += 1
                    self._emit_event("prediction_hit", {"pattern_id": pid, "method": "exact"})
                    return {
                        "response": pattern["response"],
                        "confidence": pattern["confidence"],
                        "pattern_id": pid,
                        "method": "exact",
                    }

            # Prefix match
            query_words = query.lower().split()
            for i in range(len(query_words), 0, -1):
                prefix = " ".join(query_words[:i])
                candidates = self._prefix_index.get(prefix, [])
                if candidates:
                    best = max(candidates, key=lambda p: self._patterns[p]["confidence"])
                    self._patterns[best]["hit_count"] += 1
                    self._prediction_hits += 1
                    self._emit_event("prediction_hit", {"pattern_id": best, "method": "prefix"})
                    return {
                        "response": self._patterns[best]["response"],
                        "confidence": self._patterns[best]["confidence"],
                        "pattern_id": best,
                        "method": "prefix",
                    }

            # Keyword match
            query_keywords = set(self._extract_keywords(query))
            candidate_scores: Dict[str, float] = {}
            for kw in query_keywords:
                for pid in self._keyword_index.get(kw.lower(), []):
                    candidate_scores[pid] = candidate_scores.get(pid, 0) + 1

            if candidate_scores:
                best_pid = max(candidate_scores, key=candidate_scores.get)
                pattern = self._patterns[best_pid]
                if pattern["confidence"] >= 0.5:
                    pattern["hit_count"] += 1
                    self._prediction_hits += 1
                    self._emit_event("prediction_hit", {"pattern_id": best_pid, "method": "keyword"})
                    return {
                        "response": pattern["response"],
                        "confidence": pattern["confidence"],
                        "pattern_id": best_pid,
                        "method": "keyword",
                    }

            self._prediction_misses += 1
            self._emit_event("prediction_miss", {"query": query})
            return None

    def _emit_event(self, event_type: str, data: Dict[str, Any]):
        """Emit a cache event via EventBus."""
        try:
            self._event_bus.emit(StreamEvent(
                event_type=getattr(StreamEventType, event_type.upper(), StreamEventType.HEARTBEAT),
                data=data,
                source="ResponsePredictor",
            ))
        except Exception:
            pass  # EventBus not fully configured

    def precompute(self, queries: List[Tuple[str, Any]],
                    confidence: float = 0.8):
        """Bulk-register query patterns with responses.

        Args:
            queries: List of (query_pattern, response) tuples.
            confidence: Default confidence for all patterns.
        """
        with self._lock:
            for query, response in queries:
                self.register_pattern(query, response, confidence=confidence)

    def get_prediction_count(self) -> int:
        """Number of registered patterns."""
        with self._lock:
            return len(self._patterns)

    def get_top_predictions(self, n: int = 5) -> List[Dict[str, Any]]:
        """Get the top N most-hit patterns."""
        with self._lock:
            sorted_patterns = sorted(
                self._patterns.values(),
                key=lambda p: p["hit_count"],
                reverse=True
            )
            return [
                {
                    "query_pattern": p["query_pattern"],
                    "confidence": p["confidence"],
                    "hit_count": p["hit_count"],
                    "method": "top",
                }
                for p in sorted_patterns[:n]
            ]

    def remove_pattern(self, query_pattern: str) -> bool:
        """Remove a registered pattern by query text.

        Returns True if the pattern was found and removed.
        """
        with self._lock:
            pattern_id = None
            for pid, p in self._patterns.items():
                if p["query_pattern"] == query_pattern:
                    pattern_id = pid
                    break
            if pattern_id is None:
                return False

            # Clean up indexes
            words = query_pattern.lower().split()
            for i in range(1, len(words) + 1):
                prefix = " ".join(words[:i])
                if pattern_id in self._prefix_index.get(prefix, []):
                    self._prefix_index[prefix].remove(pattern_id)
            for kw in self._patterns[pattern_id]["keywords"]:
                if pattern_id in self._keyword_index.get(kw.lower(), []):
                    self._keyword_index[kw.lower()].remove(pattern_id)

            del self._patterns[pattern_id]
            return True

    def clear(self):
        """Remove all registered patterns."""
        with self._lock:
            self._patterns.clear()
            self._prefix_index.clear()
            self._keyword_index.clear()

    @staticmethod
    def _extract_keywords(text: str) -> List[str]:
        """Extract meaningful keywords from text."""
        stop_words = {"the", "a", "an", "is", "are", "was", "were", "what",
                       "how", "why", "when", "where", "who", "which", "this",
                       "that", "these", "those", "to", "for", "of", "in",
                       "on", "at", "by", "with", "and", "or", "but"}
        words = [w.strip(".,!?;:()[]{}\"").lower() for w in text.split()]
        return [w for w in words if len(w) > 2 and w not in stop_words]


# ─────────────────────────────────────────────
# CacheManager — Distributed Edge Node Coordination
# Uses v2 MessageBroker for cache coordination
# ─────────────────────────────────────────────

class CacheManager:
    """Coordinate distributed edge node caches with replication and invalidation.

    Uses v2 MessageBroker for distributed cache coordination and event publishing.
    Integrates with TokenRegistry for cache key registration.

    Features:
    - Multiple edge nodes with independent caches
    - Configurable replication factor
    - Cache invalidation across nodes
    - Consistent key distribution
    - Synchronization between nodes

    Example:
        manager = CacheManager(config=EPCConfig(edge_nodes=5))
        manager.put("query:42", {"result": "ok"}, ttl=60)
        result = manager.get("query:42")
        manager.invalidate("query:42")
    """

    def __init__(self, config: Optional[EPCConfig] = None,
                 registry: Optional[TokenRegistry] = None):
        self._config = config or EPCConfig()
        self._registry = registry or get_registry()
        self._lock = threading.RLock()

        # ── v2 MessageBroker for distributed coordination ──
        # Dedup disabled (window=0) to avoid rejecting cache coordination messages
        self._broker = MessageBroker(dedup_window_seconds=0)

        # ── EventBus for cache events ──
        self._event_bus = EventBus()

        # ── Edge nodes ──
        self._nodes: Dict[str, EdgeCache] = {}
        self._node_ids: List[str] = []

        # Create edge nodes
        for i in range(self._config.edge_nodes):
            node_id = f"edge_{i}"
            self._nodes[node_id] = EdgeCache(
                node_id=node_id, config=self._config, registry=self._registry
            )
            self._node_ids.append(node_id)

        # ── Stats ──
        self._total_replications = 0
        self._total_invalidations = 0

    # ── Node Management ────────────────────────────────

    def register_node(self, node_id: str, config: Optional[EPCConfig] = None) -> EdgeCache:
        """Register a new edge node.

        Args:
            node_id: Unique identifier for the edge node.
            config: Optional configuration for the node.

        Returns:
            The created EdgeCache instance.
        """
        with self._lock:
            cache = EdgeCache(node_id=node_id, config=config or self._config, registry=self._registry)
            self._nodes[node_id] = cache
            if node_id not in self._node_ids:
                self._node_ids.append(node_id)
            # Register with broker
            self._broker.enqueue({"action": "node_register", "node_id": node_id}, priority=0)
            return cache

    def get_node(self, node_id: str) -> Optional[EdgeCache]:
        """Get an edge node cache by ID.

        Returns None if the node doesn't exist.
        """
        return self._nodes.get(node_id)

    def remove_node(self, node_id: str) -> bool:
        """Remove an edge node and its cache.

        Returns True if the node existed and was removed.
        """
        with self._lock:
            if node_id in self._nodes:
                self._broker.enqueue(
                    {"action": "node_remove", "node_id": node_id}, priority=0
                )
                del self._nodes[node_id]
                self._node_ids.remove(node_id)
                return True
            return False

    @property
    def node_count(self) -> int:
        """Number of active edge nodes."""
        return len(self._nodes)

    # ── Key Distribution ───────────────────────────────

    def _get_responsible_nodes(self, key: str) -> List[str]:
        """Get the nodes responsible for a key using consistent hashing."""
        if not self._node_ids:
            return []
        key_hash = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        primary_idx = key_hash % len(self._node_ids)
        responsible = [self._node_ids[primary_idx]]

        # Add replica nodes
        for r in range(1, self._config.replication_factor):
            idx = (primary_idx + r) % len(self._node_ids)
            responsible.append(self._node_ids[idx])

        return responsible

    # ── Core Cache Operations ──────────────────────────

    def put(self, key: str, value: Any, ttl: Optional[float] = None) -> bool:
        """Store a value across responsible edge nodes.

        Args:
            key: Cache key.
            value: Value to cache.
            ttl: Optional TTL override.

        Returns:
            True if stored on at least one node.
        """
        with self._lock:
            nodes = self._get_responsible_nodes(key)
            success = False
            for node_id in nodes:
                node = self._nodes[node_id]
                if node.put(key, value, ttl=ttl):
                    success = True
                    self._total_replications += 1
            # Coordinate via broker
            self._broker.enqueue(
                {"action": "cache_put", "key": key, "nodes": nodes}, priority=1
            )
            return success

    def get(self, key: str) -> Optional[Any]:
        """Retrieve a value from any responsible edge node.

        Args:
            key: Cache key to look up.

        Returns:
            The cached value if found on any responsible node, None otherwise.
        """
        with self._lock:
            nodes = self._get_responsible_nodes(key)
            for node_id in nodes:
                node = self._nodes[node_id]
                result = node.get(key)
                if result is not None:
                    self._broker.enqueue(
                        {"action": "cache_hit", "key": key, "node_id": node_id}, priority=2
                    )
                    return result
            self._broker.enqueue(
                {"action": "cache_miss", "key": key}, priority=2
            )
            return None

    def delete(self, key: str) -> bool:
        """Delete a key from all edge nodes.

        Returns True if the key was found on at least one node.
        """
        with self._lock:
            nodes = self._get_responsible_nodes(key)
            deleted = False
            for node_id in nodes:
                node = self._nodes[node_id]
                if node.delete(key):
                    deleted = True
                    self._total_invalidations += 1
            self._broker.enqueue(
                {"action": "cache_delete", "key": key, "deleted": deleted}, priority=0
            )
            return deleted

    def invalidate(self, key: str):
        """Invalidate a cache key across all nodes (alias for delete)."""
        self.delete(key)

    # ── Invalidation ───────────────────────────────────

    def invalidate_pattern(self, prefix: str) -> int:
        """Invalidate all keys matching a prefix across all nodes.

        Returns the number of keys invalidated.
        """
        with self._lock:
            total = 0
            for node_id, node in self._nodes.items():
                keys_to_delete = [k for k in node._cache if k.startswith(prefix)]
                for key in keys_to_delete:
                    node.delete(key)
                    total += 1
                    self._total_invalidations += 1
            self._broker.enqueue(
                {"action": "pattern_invalidation", "prefix": prefix, "count": total},
                priority=0
            )
            return total

    # ── Synchronization ────────────────────────────────

    def sync_nodes(self) -> Dict[str, Any]:
        """Synchronize state across all edge nodes.

        Returns sync status summary.
        """
        with self._lock:
            result = {"synced_nodes": 0, "total_entries": 0, "total_expirations": 0}
            for node_id, node in self._nodes.items():
                expirations = node.evict_expired()
                result["synced_nodes"] += 1
                result["total_entries"] += node.get_size()
                result["total_expirations"] += expirations
            self._broker.enqueue({"action": "sync_complete"}, priority=0)
            return result

    def get_health(self) -> Dict[str, Any]:
        """Get health status of all edge nodes."""
        with self._lock:
            health = {}
            for node_id, node in self._nodes.items():
                stats = node.get_stats()
                health[node_id] = {
                    "depth": stats["depth"],
                    "utilization": stats["utilization"],
                    "hit_rate": stats["hit_rate"],
                }
            return health

    def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all edge nodes."""
        with self._lock:
            return {
                node_id: node.get_stats()
                for node_id, node in self._nodes.items()
            }

    @property
    def total_replications(self) -> int:
        """Total number of replications across all nodes."""
        return self._total_replications

    @property
    def total_invalidations(self) -> int:
        """Total number of invalidations across all nodes."""
        return self._total_invalidations

    # ── Global Cache Operations ────────────────────────

    def get_cache_stats(self) -> CacheHitStats:
        """Aggregate cache hit statistics across all nodes."""
        with self._lock:
            total_stats = CacheHitStats()
            for node in self._nodes.values():
                node_stats = node.get_cache_stats()
                total_stats.total_requests += node_stats.total_requests
                total_stats.cache_hits += node_stats.cache_hits
                total_stats.cache_misses += node_stats.cache_misses
                total_stats.total_bytes_saved += node_stats.total_bytes_saved
                total_stats.total_predictions += node_stats.total_predictions
                total_stats.correct_predictions += node_stats.correct_predictions
                for nid, count in node_stats.edge_node_hits.items():
                    total_stats.edge_node_hits[nid] += count
            return total_stats

    def cleanup(self) -> int:
        """Clean up expired entries across all nodes. Returns total removed."""
        with self._lock:
            total = 0
            for node in self._nodes.values():
                total += node.evict_expired()
            return total

    def clear_all(self):
        """Clear all caches on all nodes."""
        with self._lock:
            for node in self._nodes.values():
                node.clear()

    def get_broker(self) -> MessageBroker:
        """Get the underlying MessageBroker instance."""
        return self._broker

    def get_event_bus(self) -> EventBus:
        """Get the underlying EventBus instance."""
        return self._event_bus


# ─────────────────────────────────────────────
# Convenience Functions
# ─────────────────────────────────────────────

def create_cache_manager(config: Optional[EPCConfig] = None) -> CacheManager:
    """Factory function to create a CacheManager."""
    return CacheManager(config=config)


def create_edge_cache(node_id: str = "node_0",
                       config: Optional[EPCConfig] = None) -> EdgeCache:
    """Factory function to create an EdgeCache."""
    return EdgeCache(node_id=node_id, config=config)


# ─────────────────────────────────────────────
# Module exports
# ─────────────────────────────────────────────

__all__ = [
    # Configuration & Stats
    "EPCConfig",
    "CacheHitStats",
    # Cache
    "EdgeCache",
    # Predictor
    "ResponsePredictor",
    # Delta encoding
    "DeltaEncoder",
    # Cache management
    "CacheManager",
    # Types
    "CacheStatus",
    "DeltaOperation",
    # Convenience
    "create_cache_manager",
    "create_edge_cache",
]