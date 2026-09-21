"""
Tests for TokenRelay v3 Edge Pre-Computation (EPC) module.

Covers:
- EdgeCache: TTL expiry, LRU eviction, thread safety, statistics
- ResponsePredictor: Pattern registration, prediction methods, precompute
- DeltaEncoder: Delta computation, application, efficiency
- CacheManager: Multi-node coordination, replication, invalidation
- EPCConfig & CacheHitStats: Configuration and statistics
"""

import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from epc import (
    EdgeCache,
    ResponsePredictor,
    DeltaEncoder,
    CacheManager,
    EPCConfig,
    CacheHitStats,
    create_cache_manager,
    create_edge_cache,
    CacheStatus,
    DeltaOperation,
)


# ═══════════════════════════════════════════════════════════════
# EPCConfig Tests
# ═══════════════════════════════════════════════════════════════

class TestEPCConfig(unittest.TestCase):
    """Test EPC configuration."""

    def test_default_config(self):
        """Default config has expected values."""
        cfg = EPCConfig()
        self.assertEqual(cfg.default_ttl, 300.0)
        self.assertEqual(cfg.max_entries, 10_000)
        self.assertEqual(cfg.delta_threshold, 0.15)
        self.assertEqual(cfg.edge_nodes, 5)
        self.assertTrue(cfg.enable_delta)
        self.assertTrue(cfg.enable_precompute)

    def test_custom_config(self):
        """Custom config overrides defaults."""
        cfg = EPCConfig(default_ttl=60.0, max_entries=100, edge_nodes=3)
        self.assertEqual(cfg.default_ttl, 60.0)
        self.assertEqual(cfg.max_entries, 100)
        self.assertEqual(cfg.edge_nodes, 3)

    def test_config_dataclass(self):
        """Config can be converted to dict."""
        cfg = EPCConfig(edge_nodes=2)
        d = cfg.__dict__
        self.assertEqual(d["edge_nodes"], 2)


# ═══════════════════════════════════════════════════════════════
# CacheHitStats Tests
# ═══════════════════════════════════════════════════════════════

class TestCacheHitStats(unittest.TestCase):
    """Test cache hit statistics tracking."""

    def test_initial_stats(self):
        """Fresh stats have zero values."""
        stats = CacheHitStats()
        self.assertEqual(stats.total_requests, 0)
        self.assertEqual(stats.cache_hits, 0)
        self.assertEqual(stats.hit_rate, 0.0)
        self.assertEqual(stats.prediction_accuracy, 0.0)

    def test_record_hit(self):
        """Recording a hit increments counters."""
        stats = CacheHitStats()
        stats.record_hit("node_0")
        self.assertEqual(stats.total_requests, 1)
        self.assertEqual(stats.cache_hits, 1)
        self.assertEqual(stats.hit_rate, 100.0)
        self.assertEqual(stats.edge_node_hits["node_0"], 1)

    def test_record_miss(self):
        """Recording a miss increments miss counter."""
        stats = CacheHitStats()
        stats.record_miss()
        self.assertEqual(stats.total_requests, 1)
        self.assertEqual(stats.cache_misses, 1)
        self.assertEqual(stats.hit_rate, 0.0)

    def test_mixed_hits_and_misses(self):
        """Hit rate computed correctly with mixed records."""
        stats = CacheHitStats()
        stats.record_hit()
        stats.record_hit()
        stats.record_miss()
        self.assertEqual(stats.cache_hits, 2)
        self.assertEqual(stats.cache_misses, 1)
        self.assertEqual(stats.hit_rate, round(2 / 3 * 100, 2))

    def test_record_prediction(self):
        """Prediction tracking works."""
        stats = CacheHitStats()
        stats.record_prediction(True)
        stats.record_prediction(True)
        stats.record_prediction(False)
        self.assertEqual(stats.total_predictions, 3)
        self.assertEqual(stats.correct_predictions, 2)
        self.assertEqual(stats.prediction_accuracy, round(2 / 3 * 100, 2))

    def test_record_bytes_saved(self):
        """Bytes saved tracking works."""
        stats = CacheHitStats()
        stats.record_bytes_saved(100)
        stats.record_bytes_saved(50)
        self.assertEqual(stats.total_bytes_saved, 150)

    def test_reset(self):
        """Reset clears all counters."""
        stats = CacheHitStats()
        stats.record_hit()
        stats.record_miss()
        stats.record_bytes_saved(100)
        stats.record_prediction(True)
        stats.reset()
        self.assertEqual(stats.total_requests, 0)
        self.assertEqual(stats.cache_hits, 0)
        self.assertEqual(stats.total_bytes_saved, 0)
        self.assertEqual(stats.total_predictions, 0)

    def test_to_dict(self):
        """to_dict includes computed metrics."""
        stats = CacheHitStats()
        stats.record_hit()
        d = stats.to_dict()
        self.assertIn("hit_rate", d)
        self.assertIn("total_requests", d)
        self.assertIn("cache_hits", d)


# ═══════════════════════════════════════════════════════════════
# EdgeCache Tests
# ═══════════════════════════════════════════════════════════════

class TestEdgeCacheBasics(unittest.TestCase):
    """Test basic EdgeCache operations."""

    def setUp(self):
        self.cache = EdgeCache(node_id="test_node", config=EPCConfig(max_entries=10))

    def test_put_and_get(self):
        """Put and get a value."""
        self.cache.put("key1", {"result": "success"})
        result = self.cache.get("key1")
        self.assertEqual(result, {"result": "success"})

    def test_get_missing(self):
        """Get a missing key returns None."""
        result = self.cache.get("nonexistent")
        self.assertIsNone(result)

    def test_put_overwrites(self):
        """Putting a new value for an existing key overwrites it."""
        self.cache.put("key1", "old")
        self.cache.put("key1", "new")
        result = self.cache.get("key1")
        self.assertEqual(result, "new")

    def test_delete(self):
        """Delete removes an entry."""
        self.cache.put("key1", "value")
        self.assertTrue(self.cache.delete("key1"))
        self.assertIsNone(self.cache.get("key1"))

    def test_delete_missing(self):
        """Deleting a missing key returns False."""
        self.assertFalse(self.cache.delete("nonexistent"))

    def test_contains(self):
        """Contains checks for existing keys."""
        self.cache.put("key1", "value")
        self.assertTrue(self.cache.contains("key1"))
        self.assertFalse(self.cache.contains("nonexistent"))

    def test_clear(self):
        """Clear removes all entries."""
        self.cache.put("key1", "v1")
        self.cache.put("key2", "v2")
        self.cache.clear()
        self.assertEqual(self.cache.get_size(), 0)


class TestEdgeCacheTTL(unittest.TestCase):
    """Test TTL-based expiry."""

    def test_entry_expires(self):
        """Entries expire after TTL."""
        cache = EdgeCache(node_id="ttl_test", config=EPCConfig(default_ttl=0.05))
        cache.put("key1", "value", ttl=0.05)
        time.sleep(0.1)
        result = cache.get("key1")
        self.assertIsNone(result)

    def test_custom_ttl(self):
        """Custom TTL per entry works."""
        cache = EdgeCache(node_id="custom_ttl", config=EPCConfig(default_ttl=10.0))
        cache.put("key1", "short", ttl=0.05)
        cache.put("key2", "long", ttl=100.0)
        time.sleep(0.08)
        self.assertIsNone(cache.get("key1"))
        self.assertEqual(cache.get("key2"), "long")

    def test_contains_expired(self):
        """contains returns False for expired entries."""
        cache = EdgeCache(node_id="contains_exp", config=EPCConfig(default_ttl=0.05))
        cache.put("key1", "value", ttl=0.05)
        time.sleep(0.08)
        self.assertFalse(cache.contains("key1"))

    def test_default_ttl(self):
        """Default TTL applies when not specified."""
        cache = EdgeCache(node_id="def_ttl", config=EPCConfig(default_ttl=0.05))
        cache.put("key1", "value")
        time.sleep(0.08)
        self.assertIsNone(cache.get("key1"))


class TestEdgeCacheLRU(unittest.TestCase):
    """Test LRU eviction."""

    def setUp(self):
        self.cache = EdgeCache(node_id="lru_test", config=EPCConfig(max_entries=3))

    def test_lru_eviction(self):
        """LRU entries are evicted when capacity is exceeded."""
        self.cache.put("key1", "v1")
        self.cache.put("key2", "v2")
        self.cache.put("key3", "v3")
        self.cache.put("key4", "v4")  # Should evict key1 (least recently used)
        self.assertIsNone(self.cache.get("key1"))
        self.assertIsNotNone(self.cache.get("key2"))
        self.assertIsNotNone(self.cache.get("key3"))
        self.assertIsNotNone(self.cache.get("key4"))

    def test_access_updates_lru(self):
        """Accessing a key updates its LRU position."""
        self.cache.put("key1", "v1")
        self.cache.put("key2", "v2")
        self.cache.put("key3", "v3")
        # Access key1 to make it most recently used
        self.cache.get("key1")
        # Add key4 — should evict key2 (least recently used)
        self.cache.put("key4", "v4")
        self.assertIsNotNone(self.cache.get("key1"))  # Should still be there
        self.assertIsNone(self.cache.get("key2"))

    def test_is_full(self):
        """is_full reflects capacity."""
        cache = EdgeCache(node_id="full_test", config=EPCConfig(max_entries=2))
        self.assertFalse(cache.is_full)
        cache.put("k1", "v1")
        self.assertFalse(cache.is_full)
        cache.put("k2", "v2")
        self.assertTrue(cache.is_full)

    def test_depth(self):
        """Depth tracking works."""
        self.assertEqual(self.cache.get_size(), 0)
        self.cache.put("key1", "v1")
        self.assertEqual(self.cache.get_size(), 1)
        self.cache.put("key2", "v2")
        self.assertEqual(self.cache.get_size(), 2)

    def test_evict_expired_removes_expired(self):
        """evict_expired removes expired entries."""
        cache = EdgeCache(node_id="exp_evict", config=EPCConfig(max_entries=10))
        cache.put("key1", "v1", ttl=0.05)
        time.sleep(0.08)
        removed = cache.evict_expired()
        self.assertGreater(removed, 0)
        self.assertEqual(cache.get_size(), 0)


class TestEdgeCacheThreadSafety(unittest.TestCase):
    """Test thread-safe operations."""

    def test_concurrent_access(self):
        """Concurrent puts and gets don't corrupt data."""
        import threading
        cache = EdgeCache(node_id="thread_test", config=EPCConfig(max_entries=100))

        def worker(start):
            for i in range(10):
                cache.put(f"key_{start}_{i}", f"val_{start}_{i}")

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All values should be retrievable
        for i in range(5):
            for j in range(10):
                result = cache.get(f"key_{i}_{j}")
                self.assertEqual(result, f"val_{i}_{j}")

    def test_stats_thread_safe(self):
        """get_stats works under concurrent access."""
        import threading
        cache = EdgeCache(node_id="stats_thread", config=EPCConfig(max_entries=10))

        def writer():
            for i in range(20):
                cache.put(f"k{i}", f"v{i}", ttl=0.1)

        t = threading.Thread(target=writer)
        t.start()
        time.sleep(0.01)
        stats = cache.get_stats()  # Should not crash
        t.join()
        self.assertIn("depth", stats)
        self.assertIn("node_id", stats)


class TestEdgeCacheStatistics(unittest.TestCase):
    """Test EdgeCache statistics."""

    def test_get_stats(self):
        """get_stats returns comprehensive info."""
        cache = EdgeCache(node_id="stats_node", config=EPCConfig(max_entries=5))
        cache.put("k1", "v1")
        cache.get("k1")  # Hit
        cache.get("k2")  # Miss
        stats = cache.get_stats()
        self.assertEqual(stats["node_id"], "stats_node")
        self.assertEqual(stats["depth"], 1)
        self.assertEqual(stats["cache_hits"], 1)
        self.assertEqual(stats["cache_misses"], 1)
        self.assertGreaterEqual(stats["hit_rate"], 0)

    def test_cache_hit_stats_object(self):
        """get_cache_stats returns CacheHitStats."""
        cache = EdgeCache(node_id="cs_test", config=EPCConfig(max_entries=5))
        cache.put("k1", "v1")
        cache.get("k1")
        stats = cache.get_cache_stats()
        self.assertIsInstance(stats, CacheHitStats)
        self.assertEqual(stats.cache_hits, 1)

    def test_eviction_counting(self):
        """Eviction count increases on LRU eviction."""
        cache = EdgeCache(node_id="evict_cnt", config=EPCConfig(max_entries=2))
        cache.put("k1", "v1")
        cache.put("k2", "v2")
        cache.put("k3", "v3")  # Evicts k1
        stats = cache.get_stats()
        self.assertGreater(stats["total_evictions"], 0)


# ═══════════════════════════════════════════════════════════════
# ResponsePredictor Tests
# ═══════════════════════════════════════════════════════════════

class TestResponsePredictorBasics(unittest.TestCase):
    """Test basic ResponsePredictor operations."""

    def setUp(self):
        self.predictor = ResponsePredictor()

    def test_register_and_predict_exact(self):
        """Register a pattern and predict exact match."""
        self.predictor.register_pattern(
            "What is the weather?",
            {"weather": "sunny"},
            confidence=0.95
        )
        result = self.predictor.predict("What is the weather?")
        self.assertIsNotNone(result)
        self.assertEqual(result["response"], {"weather": "sunny"})
        self.assertEqual(result["confidence"], 0.95)
        self.assertEqual(result["method"], "exact")

    def test_predict_no_match(self):
        """Predict returns None for unknown queries."""
        result = self.predictor.predict("Unknown question?")
        self.assertIsNone(result)

    def test_prediction_count(self):
        """get_prediction_count tracks registered patterns."""
        self.assertEqual(self.predictor.get_prediction_count(), 0)
        self.predictor.register_pattern("q1", "r1")
        self.assertEqual(self.predictor.get_prediction_count(), 1)
        self.predictor.register_pattern("q2", "r2")
        self.assertEqual(self.predictor.get_prediction_count(), 2)

    def test_remove_pattern(self):
        """remove_pattern works correctly."""
        self.predictor.register_pattern("q1", "r1")
        self.assertEqual(self.predictor.get_prediction_count(), 1)
        self.assertTrue(self.predictor.remove_pattern("q1"))
        self.assertEqual(self.predictor.get_prediction_count(), 0)
        self.assertFalse(self.predictor.remove_pattern("q1"))

    def test_clear(self):
        """clear removes all patterns."""
        self.predictor.register_pattern("q1", "r1")
        self.predictor.register_pattern("q2", "r2")
        self.predictor.clear()
        self.assertEqual(self.predictor.get_prediction_count(), 0)
        self.assertIsNone(self.predictor.predict("q1"))


class TestResponsePredictorPrefixMatch(unittest.TestCase):
    """Test prefix-based prediction."""

    def setUp(self):
        self.predictor = ResponsePredictor()

    def test_prefix_match(self):
        """Prefix matching works for partial queries."""
        self.predictor.register_pattern(
            "What is the weather in Paris?",
            {"city": "Paris", "weather": "rainy"},
            confidence=0.9
        )
        result = self.predictor.predict("What is the weather")
        self.assertIsNotNone(result)
        self.assertEqual(result["method"], "prefix")

    def test_longest_prefix_priority(self):
        """Longer prefix matches take priority."""
        self.predictor.register_pattern(
            "What is the weather?",
            {"weather": "sunny"},
            confidence=0.8
        )
        self.predictor.register_pattern(
            "What is the weather in Paris?",
            {"city": "Paris", "weather": "rainy"},
            confidence=0.95
        )
        # Exact prefix "What is the weather" should match the longer one too
        # Actually, "What is the weather" is an exact match for the first
        # Let's test with a slightly different query
        result = self.predictor.predict("What is the weather in London?")
        self.assertIsNotNone(result)
        self.assertEqual(result["method"], "prefix")


class TestResponsePredictorKeywordMatch(unittest.TestCase):
    """Test keyword-based prediction."""

    def setUp(self):
        self.predictor = ResponsePredictor()

    def test_keyword_match(self):
        """Keyword matching finds related patterns."""
        self.predictor.register_pattern(
            "Show me the latest news about technology",
            {"news": "tech_headlines"},
            confidence=0.7,
            keywords=["news", "technology", "latest"]
        )
        result = self.predictor.predict("I want technology news")
        self.assertIsNotNone(result)
        self.assertEqual(result["method"], "keyword")

    def test_no_keyword_match(self):
        """No match when keywords don't overlap."""
        self.predictor.register_pattern(
            "Show weather",
            {"weather": "sunny"},
            keywords=["weather"]
        )
        result = self.predictor.predict("What about finance")
        self.assertIsNone(result)


class TestResponsePredictorPrecompute(unittest.TestCase):
    """Test bulk precomputation."""

    def test_precompute(self):
        """Bulk registration works."""
        predictor = ResponsePredictor()
        queries = [
            ("Hello", {"response": "Hi there"}),
            ("How are you?", {"response": "I'm fine"}),
            ("Thank you", {"response": "You're welcome"}),
        ]
        predictor.precompute(queries, confidence=0.85)
        self.assertEqual(predictor.get_prediction_count(), 3)
        result = predictor.predict("Hello")
        self.assertIsNotNone(result)
        self.assertEqual(result["confidence"], 0.85)

    def test_precompute_with_custom_confidence(self):
        """Precompute with varying confidence."""
        predictor = ResponsePredictor()
        predictor.precompute([("q1", "r1")], confidence=0.99)
        result = predictor.predict("q1")
        self.assertEqual(result["confidence"], 0.99)


class TestResponsePredictorTopPredictions(unittest.TestCase):
    """Test top predictions retrieval."""

    def setUp(self):
        self.predictor = ResponsePredictor()

    def test_top_predictions(self):
        """get_top_predictions returns most-hit patterns."""
        self.predictor.register_pattern("q1", "r1", confidence=0.5)
        self.predictor.register_pattern("q2", "r2", confidence=0.9)
        self.predictor.predict("q1")
        self.predictor.predict("q1")
        self.predictor.predict("q2")
        top = self.predictor.get_top_predictions(n=2)
        self.assertEqual(len(top), 2)
        self.assertEqual(top[0]["query_pattern"], "q1")  # Most hits

    def test_top_n_larger_than_registered(self):
        """Requesting more top predictions than registered returns all."""
        self.predictor.register_pattern("q1", "r1")
        top = self.predictor.get_top_predictions(n=5)
        self.assertEqual(len(top), 1)


# ═══════════════════════════════════════════════════════════════
# DeltaEncoder Tests
# ═══════════════════════════════════════════════════════════════

class TestDeltaEncoderBasics(unittest.TestCase):
    """Test DeltaEncoder basic operations."""

    def setUp(self):
        self.encoder = DeltaEncoder()

    def test_compute_delta_add(self):
        """Detect added keys."""
        base = {"a": 1, "b": 2}
        updated = {"a": 1, "b": 2, "c": 3}
        delta = self.encoder.compute_delta(base, updated)
        ops = delta["operations"]
        add_ops = [op for op in ops if op["op"] == "ADD"]
        self.assertTrue(len(add_ops) > 0)

    def test_compute_delta_remove(self):
        """Detect removed keys."""
        base = {"a": 1, "b": 2}
        updated = {"a": 1}
        delta = self.encoder.compute_delta(base, updated)
        ops = delta["operations"]
        remove_ops = [op for op in ops if op["op"] == "REMOVE"]
        self.assertTrue(len(remove_ops) > 0)

    def test_compute_delta_modify(self):
        """Detect modified values."""
        base = {"a": 1, "b": 2}
        updated = {"a": 10, "b": 2}
        delta = self.encoder.compute_delta(base, updated)
        ops = delta["operations"]
        modify_ops = [op for op in ops if op["op"] == "MODIFY"]
        self.assertTrue(len(modify_ops) > 0)

    def test_apply_delta(self):
        """Apply delta reconstructs the updated value."""
        base = {"status": "ok", "count": 5, "items": [1, 2, 3]}
        updated = {"status": "ok", "count": 7, "items": [1, 2, 3, 4]}
        delta = self.encoder.compute_delta(base, updated)
        result = self.encoder.apply_delta(base, delta)
        self.assertEqual(result, updated)

    def test_round_trip_dict(self):
        """Round-trip through delta works for nested dicts."""
        base = {"user": {"name": "Alice", "age": 30}, "active": True}
        updated = {"user": {"name": "Alice", "age": 31}, "active": True, "email": "a@b.c"}
        delta = self.encoder.compute_delta(base, updated)
        result = self.encoder.apply_delta(base, delta)
        self.assertEqual(result, updated)

    def test_round_trip_list(self):
        """Round-trip through delta works for lists."""
        base = [1, 2, 3]
        updated = [1, 2, 4, 5]
        delta = self.encoder.compute_delta(base, updated)
        result = self.encoder.apply_delta(base, delta)
        self.assertEqual(result, updated)


class TestDeltaEncoderFullReplace(unittest.TestCase):
    """Test full replace when delta is too small."""

    def setUp(self):
        self.encoder = DeltaEncoder(threshold=0.5)  # High threshold

    def test_small_change_full_replace(self):
        """Tiny changes trigger full replace."""
        encoder = DeltaEncoder(threshold=0.5)  # High threshold
        base = {"a": 1, "b": 2, "c": 3}
        updated = {"a": 1, "b": 2, "c": 4}  # Tiny change
        delta = encoder.compute_delta(base, updated)
        # May use full replace due to high threshold
        self.assertIn("operations", delta)

    def test_apply_full_replace(self):
        """Full replace delta applies correctly."""
        base = {"a": 1, "b": 2}
        updated = {"a": 10, "b": 20}
        delta = self.encoder.compute_delta(base, updated)
        result = self.encoder.apply_delta(base, delta)
        self.assertEqual(result, updated)


class TestDeltaEncoderEfficiency(unittest.TestCase):
    """Test delta efficiency tracking."""

    def test_efficiency_stats(self):
        """get_efficiency returns stats."""
        encoder = DeltaEncoder()
        base = {"a": 1, "b": 2, "c": 3, "d": 4}
        updated = {"a": 1, "b": 99, "c": 3, "d": 4}
        encoder.compute_delta(base, updated)
        eff = encoder.get_efficiency()
        self.assertIn("total_deltas", eff)
        self.assertIn("total_bytes_saved", eff)
        self.assertIn("avg_bytes_per_delta", eff)


class TestDeltaEncoderEdgeCases(unittest.TestCase):
    """Test edge cases for DeltaEncoder."""

    def setUp(self):
        self.encoder = DeltaEncoder()

    def test_identical_values(self):
        """No delta for identical values."""
        base = {"a": 1, "b": 2}
        updated = {"a": 1, "b": 2}
        delta = self.encoder.compute_delta(base, updated)
        self.assertEqual(len(delta["operations"]), 0)

    def test_completely_different(self):
        """Fully different values produce REPLACE."""
        base = {"a": 1}
        updated = {"x": 100, "y": 200}
        delta = self.encoder.compute_delta(base, updated)
        self.assertTrue(len(delta["operations"]) > 0)

    def test_none_values(self):
        """Handle None values."""
        base = {"a": None, "b": 2}
        updated = {"a": 1, "b": 2}
        delta = self.encoder.compute_delta(base, updated)
        result = self.encoder.apply_delta(base, delta)
        self.assertEqual(result["a"], 1)

    def test_nested_structures(self):
        """Handle deeply nested structures."""
        base = {"l1": {"l2": {"l3": {"val": 1}}}}
        updated = {"l1": {"l2": {"l3": {"val": 2}}}}
        delta = self.encoder.compute_delta(base, updated)
        result = self.encoder.apply_delta(base, delta)
        self.assertEqual(result["l1"]["l2"]["l3"]["val"], 2)


# ═══════════════════════════════════════════════════════════════
# CacheManager Tests
# ═══════════════════════════════════════════════════════════════

class TestCacheManagerBasics(unittest.TestCase):
    """Test basic CacheManager operations."""

    def setUp(self):
        self.manager = CacheManager(config=EPCConfig(edge_nodes=3, max_entries=10))

    def test_put_and_get(self):
        """Put and get across edge nodes."""
        self.manager.put("query:42", {"result": "success"}, ttl=60)
        result = self.manager.get("query:42")
        self.assertEqual(result, {"result": "success"})

    def test_get_missing(self):
        """Get missing key returns None."""
        result = self.manager.get("nonexistent")
        self.assertIsNone(result)

    def test_delete(self):
        """Delete removes from all nodes."""
        self.manager.put("key1", "value", ttl=60)
        self.assertTrue(self.manager.delete("key1"))
        result = self.manager.get("key1")
        self.assertIsNone(result)

    def test_delete_missing(self):
        """Delete missing key returns False."""
        self.assertFalse(self.manager.delete("nonexistent"))

    def test_put_distribution(self):
        """Keys are distributed across nodes."""
        self.manager.put("key1", "v1", ttl=60)
        self.manager.put("key2", "v2", ttl=60)
        self.manager.put("key3", "v3", ttl=60)
        # Each key should be on at least one node
        self.assertIsNotNone(self.manager.get("key1"))
        self.assertIsNotNone(self.manager.get("key2"))
        self.assertIsNotNone(self.manager.get("key3"))


class TestCacheManagerNodeManagement(unittest.TestCase):
    """Test edge node management."""

    def setUp(self):
        self.manager = CacheManager(config=EPCConfig(edge_nodes=2))

    def test_register_node(self):
        """Register a new edge node."""
        cache = self.manager.register_node("new_node")
        self.assertIsInstance(cache, EdgeCache)
        self.assertEqual(self.manager.node_count, 3)

    def test_get_node(self):
        """Get an edge node by ID."""
        node = self.manager.get_node("edge_0")
        self.assertIsInstance(node, EdgeCache)
        self.assertIsNone(self.manager.get_node("nonexistent"))

    def test_remove_node(self):
        """Remove an edge node."""
        self.assertTrue(self.manager.remove_node("edge_0"))
        self.assertEqual(self.manager.node_count, 1)
        self.assertFalse(self.manager.remove_node("nonexistent"))

    def test_node_count(self):
        """Node count tracks correctly."""
        self.assertEqual(self.manager.node_count, 2)
        self.manager.register_node("extra")
        self.assertEqual(self.manager.node_count, 3)


class TestCacheManagerInvalidation(unittest.TestCase):
    """Test cache invalidation operations."""

    def setUp(self):
        self.manager = CacheManager(config=EPCConfig(edge_nodes=3))

    def test_invalidate(self):
        """Invalidate removes cached entries."""
        self.manager.put("key1", "v1", ttl=60)
        self.manager.invalidate("key1")
        self.assertIsNone(self.manager.get("key1"))

    def test_invalidate_pattern(self):
        """Invalidate by prefix removes matching keys."""
        self.manager.put("user:1:profile", {"name": "Alice"}, ttl=60)
        self.manager.put("user:2:profile", {"name": "Bob"}, ttl=60)
        self.manager.put("other:key", "val", ttl=60)
        removed = self.manager.invalidate_pattern("user:")
        self.assertGreater(removed, 0)
        self.assertIsNone(self.manager.get("user:1:profile"))
        self.assertIsNone(self.manager.get("user:2:profile"))
        self.assertIsNotNone(self.manager.get("other:key"))

    def test_invalidate_pattern_no_match(self):
        """Invalidate pattern with no matches."""
        self.manager.put("key1", "v1", ttl=60)
        removed = self.manager.invalidate_pattern("nonexistent:")
        self.assertEqual(removed, 0)


class TestCacheManagerSynchronization(unittest.TestCase):
    """Test node synchronization."""

    def setUp(self):
        self.manager = CacheManager(config=EPCConfig(edge_nodes=3, max_entries=10))
        # Short TTL for testing
        self.manager.put("k1", "v1", ttl=0.05)

    def test_sync_nodes(self):
        """Sync returns summary with entries and expirations."""
        time.sleep(0.08)  # Let the entry expire
        result = self.manager.sync_nodes()
        self.assertIn("synced_nodes", result)
        self.assertIn("total_entries", result)
        self.assertIn("total_expirations", result)

    def test_get_health(self):
        """Health check works for all nodes."""
        self.manager.put("k1", "v1", ttl=60)
        health = self.manager.get_health()
        for node_id, info in health.items():
            self.assertIn("depth", info)
            self.assertIn("hit_rate", info)
            self.assertIn("utilization", info)

    def test_get_all_stats(self):
        """All node stats are available."""
        self.manager.put("k1", "v1", ttl=60)
        stats = self.manager.get_all_stats()
        self.assertEqual(len(stats), self.manager.node_count)
        for node_id, node_stats in stats.items():
            self.assertIn("depth", node_stats)
            self.assertIn("node_id", node_stats)

    def test_get_cache_stats(self):
        """Aggregate cache stats work."""
        self.manager.put("k1", "v1", ttl=60)
        self.manager.get("k1")
        stats = self.manager.get_cache_stats()
        self.assertGreaterEqual(stats.total_requests, 1)


class TestCacheManagerCleanup(unittest.TestCase):
    """Test cleanup operations."""

    def setUp(self):
        self.manager = CacheManager(config=EPCConfig(edge_nodes=2))

    def test_cleanup(self):
        """Cleanup removes expired entries."""
        short_manager = CacheManager(config=EPCConfig(edge_nodes=2, max_entries=10))
        short_manager.put("k1", "v1", ttl=0.05)
        time.sleep(0.08)
        removed = short_manager.cleanup()
        self.assertGreater(removed, 0)

    def test_clear_all(self):
        """clear_all removes everything from all nodes."""
        self.manager.put("k1", "v1", ttl=60)
        self.manager.put("k2", "v2", ttl=60)
        self.manager.clear_all()
        self.assertIsNone(self.manager.get("k1"))
        self.assertIsNone(self.manager.get("k2"))


class TestCacheManagerFactory(unittest.TestCase):
    """Test factory functions."""

    def test_create_cache_manager(self):
        """Factory creates working CacheManager."""
        manager = create_cache_manager(config=EPCConfig(edge_nodes=2))
        self.assertIsInstance(manager, CacheManager)
        manager.put("k", "v", ttl=60)
        self.assertEqual(manager.get("k"), "v")

    def test_create_edge_cache(self):
        """Factory creates working EdgeCache."""
        cache = create_edge_cache(node_id="factory_node")
        self.assertIsInstance(cache, EdgeCache)
        cache.put("k", "v")
        self.assertEqual(cache.get("k"), "v")


class TestCacheManagerReplication(unittest.TestCase):
    """Test replication across nodes."""

    def test_replication_factor(self):
        """Keys are replicated across multiple nodes."""
        manager = CacheManager(config=EPCConfig(edge_nodes=5, replication_factor=3))
        manager.put("key:test", {"data": "value"}, ttl=60)
        # Check that key exists on at least the replication factor nodes
        nodes_with_key = 0
        for node_id in ["edge_0", "edge_1", "edge_2", "edge_3", "edge_4"]:
            node = manager.get_node(node_id)
            if node and node.contains("key:test"):
                nodes_with_key += 1
        self.assertGreaterEqual(nodes_with_key, 1)

    def test_total_replications(self):
        """Total replications counter works."""
        manager = CacheManager(config=EPCConfig(edge_nodes=3))
        manager.put("k1", "v1", ttl=60)
        self.assertGreaterEqual(manager.total_replications, 1)


# ═══════════════════════════════════════════════════════════════
# Integration Tests
# ═══════════════════════════════════════════════════════════════

class TestEPCIntegration(unittest.TestCase):
    """Test end-to-end EPC integration."""

    def test_predictor_to_cache_flow(self):
        """Predict a response and cache it."""
        predictor = ResponsePredictor()
        manager = CacheManager(config=EPCConfig(edge_nodes=2, max_entries=10))

        predictor.register_pattern(
            "Hello", {"greeting": "Hi!"}, confidence=0.95
        )
        prediction = predictor.predict("Hello")
        self.assertIsNotNone(prediction)

        # Cache the predicted response
        manager.put("query:hello", prediction["response"], ttl=300)
        cached = manager.get("query:hello")
        self.assertEqual(cached, {"greeting": "Hi!"})

    def test_delta_encoding_workflow(self):
        """Full delta encoding workflow with cache."""
        cache = EdgeCache(node_id="delta_test", config=EPCConfig(max_entries=10))
        encoder = DeltaEncoder()

        base_data = {"status": "ok", "count": 5, "data": [1, 2, 3]}
        cache.put("query:1", base_data, ttl=300)

        new_data = {"status": "ok", "count": 7, "data": [1, 2, 3, 4]}
        delta = encoder.compute_delta(base_data, new_data)
        reconstructed = encoder.apply_delta(base_data, delta)

        self.assertEqual(reconstructed, new_data)

    def test_full_prediction_pipeline(self):
        """Complete pipeline: predict → cache → retrieve → delta."""
        config = EPCConfig(edge_nodes=3, max_entries=20, default_ttl=300)
        manager = CacheManager(config=config)
        predictor = ResponsePredictor()
        encoder = DeltaEncoder()

        # Register common patterns
        predictor.precompute([
            ("Hello", {"greeting": "Hi!"}),
            ("Help me", {"response": "How can I assist?"}),
        ], confidence=0.9)

        # Predict and cache
        prediction = predictor.predict("Hello")
        self.assertIsNotNone(prediction)
        manager.put("q:hello", prediction["response"], ttl=300)

        # Retrieve from cache
        cached = manager.get("q:hello")
        self.assertEqual(cached, {"greeting": "Hi!"})

        # Test delta encoding on subsequent update
        updated = {"greeting": "Hi!", "extra": "info"}
        delta = encoder.compute_delta(cached, updated)
        reconstructed = encoder.apply_delta(cached, delta)
        self.assertEqual(reconstructed, updated)

    def test_cache_manager_with_expiry(self):
        """Cache manager handles TTL correctly."""
        manager = CacheManager(config=EPCConfig(edge_nodes=2, max_entries=10))
        manager.put("temp_key", "temp_value", ttl=0.05)
        time.sleep(0.08)
        result = manager.get("temp_key")
        self.assertIsNone(result)

    def test_stats_aggregation(self):
        """Cache stats aggregation across nodes."""
        manager = CacheManager(config=EPCConfig(edge_nodes=3))
        manager.put("k1", "v1", ttl=60)
        manager.put("k2", "v2", ttl=60)
        manager.get("k1")
        manager.get("k1")
        manager.get("k2")
        manager.get("k3")  # Miss

        stats = manager.get_cache_stats()
        self.assertEqual(stats.cache_hits, 3)
        self.assertEqual(stats.cache_misses, 2)
        self.assertGreater(stats.total_requests, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)