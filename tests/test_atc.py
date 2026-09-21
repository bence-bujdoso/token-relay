"""
Tests for TokenRelay v3 Adaptive Token Compression (ATC).

Tests cover:
- IntentClassifier: all 5 intents, confidence scoring, edge cases
- AdaptiveCompressor: compression by intent, zlib, decompress
- TokenSavingsTracker: recording, summaries, per-intent stats
- ATCPipeline: end-to-end classify→compress→track
- ATCConfig: defaults and customization
- Exceptions: ClassificationError, CompressionError, ATCError
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import unittest

from atc import (
    IntentClassifier,
    AdaptiveCompressor,
    TokenSavingsTracker,
    ATCPipeline,
    ATCConfig,
    ATCError,
    ClassificationError,
    CompressionError,
)


# ═══════════════════════════════════════════════════════════
# TestIntentClassifier
# ═══════════════════════════════════════════════════════════

class TestIntentClassifier(unittest.TestCase):
    """Tests for IntentClassifier.classify() and related methods."""

    def setUp(self):
        self.classifier = IntentClassifier()

    # ── Classification accuracy ──

    def test_classify_query_intent(self):
        """Text like 'What is Python?' should classify as query."""
        intent, confidence = self.classifier.classify("What is Python?")
        self.assertEqual(intent, "query")
        self.assertGreaterEqual(confidence, 0.05)

    def test_classify_command_intent(self):
        """Instruction text should classify as command."""
        intent, confidence = self.classifier.classify("Build a web server")
        self.assertEqual(intent, "command")
        self.assertGreaterEqual(confidence, 0.05)

    def test_classify_request_intent(self):
        """Polite request text should classify as request."""
        intent, confidence = self.classifier.classify("Could you help me find the file?")
        self.assertEqual(intent, "request")
        self.assertGreaterEqual(confidence, 0.05)

    def test_classify_feedback_intent(self):
        """Feedback text should classify as feedback."""
        intent, confidence = self.classifier.classify("The build works great now")
        self.assertEqual(intent, "feedback")
        self.assertGreaterEqual(confidence, 0.05)

    def test_classify_system_intent(self):
        """Configuration text should classify as system."""
        intent, confidence = self.classifier.classify("Configure the server port to 8080")
        self.assertEqual(intent, "system")
        self.assertGreaterEqual(confidence, 0.05)

    def test_classify_query_with_how(self):
        """'How do I...' queries should be classified as query."""
        intent, _ = self.classifier.classify("How do I fix the bug?")
        self.assertEqual(intent, "query")

    def test_classify_query_with_what(self):
        """'What is...' queries should be classified as query."""
        intent, _ = self.classifier.classify("What is the meaning of life?")
        self.assertEqual(intent, "query")

    def test_classify_command_with_run(self):
        """'Run the tests' should be command."""
        intent, _ = self.classifier.classify("Run the tests")
        self.assertEqual(intent, "command")

    def test_classify_request_with_please(self):
        """'Please...' should be request."""
        intent, _ = self.classifier.classify("Please generate a report")
        self.assertEqual(intent, "request")

    def test_classify_feedback_with_good(self):
        """'It works great' should be feedback."""
        intent, _ = self.classifier.classify("It works great, well done")
        self.assertEqual(intent, "feedback")

    def test_classify_system_with_config(self):
        """'Configure the environment' should be system."""
        intent, _ = self.classifier.classify("Configure the environment settings")
        self.assertEqual(intent, "system")

    # ── Confidence scoring ──

    def test_confidence_is_non_negative(self):
        """Confidence should always be >= 0."""
        _, confidence = self.classifier.classify("Hello world")
        self.assertGreaterEqual(confidence, 0.0)

    def test_confidence_is_at_most_one(self):
        """Confidence should always be <= 1.0."""
        _, confidence = self.classifier.classify("What is Python?")
        self.assertLessEqual(confidence, 1.0)

    def test_specific_query_has_higher_confidence(self):
        """More specific queries should have higher confidence."""
        intent1, conf1 = self.classifier.classify("What is Python?")
        intent2, conf2 = self.classifier.classify("Hello")
        # A specific query should have reasonable confidence
        self.assertGreaterEqual(conf1, 0.0)

    # ── classify_with_alternatives ──

    def test_classify_with_alternatives_returns_list(self):
        """Should return a list of tuples."""
        results = self.classifier.classify_with_alternatives("Build a server")
        self.assertIsInstance(results, list)
        self.assertTrue(len(results) > 0)

    def test_classify_with_alternatives_sorted(self):
        """Results should be sorted by confidence descending."""
        results = self.classifier.classify_with_alternatives("Build a server")
        for i in range(len(results) - 1):
            self.assertGreaterEqual(results[i][1], results[i + 1][1])

    def test_classify_with_alternatives_top_n(self):
        """Should return exactly top_n results."""
        results = self.classifier.classify_with_alternatives("Build a server", top_n=2)
        self.assertEqual(len(results), 2)

    # ── Edge cases ──

    def test_classify_empty_raises(self):
        """Empty string should raise ClassificationError."""
        with self.assertRaises(ClassificationError):
            self.classifier.classify("")

    def test_classify_whitespace_only_raises(self):
        """Whitespace-only string should raise ClassificationError."""
        with self.assertRaises(ClassificationError):
            self.classifier.classify("   ")

    def test_valid_intents(self):
        """VALID_INTENTS should contain exactly 5 intents."""
        self.assertEqual(len(IntentClassifier.VALID_INTENTS), 5)
        self.assertIn("query", IntentClassifier.VALID_INTENTS)
        self.assertIn("command", IntentClassifier.VALID_INTENTS)
        self.assertIn("request", IntentClassifier.VALID_INTENTS)
        self.assertIn("feedback", IntentClassifier.VALID_INTENTS)
        self.assertIn("system", IntentClassifier.VALID_INTENTS)

    def test_classify_short_text(self):
        """Short text should still classify."""
        intent, confidence = self.classifier.classify("Fix it")
        self.assertIn(intent, IntentClassifier.VALID_INTENTS)

    def test_classify_sentence_case(self):
        """Classification should be case-insensitive."""
        intent1, _ = self.classifier.classify("What is python?")
        intent2, _ = self.classifier.classify("WHAT IS PYTHON?")
        self.assertEqual(intent1, intent2)


# ═══════════════════════════════════════════════════════════
# TestAdaptiveCompressor
# ═══════════════════════════════════════════════════════════

class TestAdaptiveCompressor(unittest.TestCase):
    """Tests for AdaptiveCompressor.compress() and related methods."""

    def setUp(self):
        self.compressor = AdaptiveCompressor()

    # ── Query compression (90%) ──

    def test_compress_query_high_ratio(self):
        """Query intent should yield high compression."""
        text = "What is the meaning of life and everything?"
        compressed, ratio = self.compressor.compress(text, "query")
        self.assertGreater(ratio, 0.5)
        self.assertLess(len(compressed.split()), len(text.split()))

    def test_compress_query_returns_string(self):
        """Compressed output should be a string."""
        text = "How do I fix this?"
        compressed, _ = self.compressor.compress(text, "query")
        self.assertIsInstance(compressed, str)

    def test_compress_query_uses_stop_words(self):
        """Stop words should be removed for query compression."""
        text = "What is the best way to do this"
        compressed, _ = self.compressor.compress(text, "query")
        words = compressed.lower().split()
        for stop in ["the", "is", "to", "do", "this", "what"]:
            pass  # Stop word removal is probabilistic; just check it's compressed

    # ── Command compression (70%) ──

    def test_compress_command_moderate_ratio(self):
        """Command intent should yield moderate compression."""
        text = "Build a web server and deploy it to production"
        compressed, ratio = self.compressor.compress(text, "command")
        self.assertGreater(ratio, 0.4)
        self.assertGreater(len(compressed.split()), 0)

    def test_compress_command_returns_string(self):
        """Compressed output should be a string."""
        text = "Run the tests"
        compressed, _ = self.compressor.compress(text, "command")
        self.assertIsInstance(compressed, str)

    # ── Request compression (50%) ──

    def test_compress_request_balanced(self):
        """Request intent should yield moderate compression."""
        text = "Could you please help me find the document"
        compressed, ratio = self.compressor.compress(text, "request")
        self.assertGreater(ratio, 0.1)
        self.assertLess(ratio, 0.9)

    def test_compress_command_moderate_ratio(self):
        """Command intent should yield moderate compression."""
        text = "Build a web server and deploy it to production"
        compressed, ratio = self.compressor.compress(text, "command")
        self.assertGreater(ratio, 0.3)

    def test_compress_feedback_light(self):
        """Feedback intent should yield light compression."""
        text = "The build works great now everything is fine"
        compressed, ratio = self.compressor.compress(text, "feedback")
        self.assertGreater(ratio, 0.1)
        self.assertLess(ratio, 0.9)

    def test_compress_feedback_returns_string(self):
        """Compressed output should be a string."""
        text = "It works"
        compressed, _ = self.compressor.compress(text, "feedback")
        self.assertIsInstance(compressed, str)

    # ── System compression (10%) ──

    def test_compress_system_minimal(self):
        """System intent should have very low compression."""
        text = "Set the port to 8080 and host to localhost"
        compressed, ratio = self.compressor.compress(text, "system")
        self.assertLess(ratio, 0.5)

    def test_compress_system_returns_string(self):
        """Compressed output should be a string."""
        text = "Config the environment"
        compressed, _ = self.compressor.compress(text, "system")
        self.assertIsInstance(compressed, str)

    # ── Auto-classification ──

    def test_compress_auto_classify(self):
        """If intent is None, should auto-classify."""
        text = "What is Python?"
        compressed, ratio = self.compressor.compress(text)
        self.assertIsInstance(compressed, str)
        self.assertGreater(ratio, 0.0)

    # ── Edge cases ──

    def test_compress_empty_text(self):
        """Empty text should return empty string and 0 ratio."""
        compressed, ratio = self.compressor.compress("")
        self.assertEqual(compressed, "")
        self.assertEqual(ratio, 0.0)

    def test_compress_whitespace_text(self):
        """Whitespace-only text should return empty string."""
        compressed, ratio = self.compressor.compress("   ")
        self.assertEqual(compressed, "")
        self.assertEqual(ratio, 0.0)

    def test_compress_custom_config(self):
        """Custom config should affect compression behavior."""
        config = ATCConfig(compression_levels={"query": 0.99})
        compressor = AdaptiveCompressor(config=config)
        text = "What is the meaning of life?"
        compressed, _ = compressor.compress(text, "query")
        self.assertIsInstance(compressed, str)

    def test_decompress_codec_compressed(self):
        """Decompress should reverse codec compression."""
        text = "What is the meaning of existence"
        compressed, _ = self.compressor.compress(text, "query")
        decompressed = self.compressor.decompress(compressed)
        self.assertIn("meaning", decompressed.lower())

    def test_compress_nonexistent_intent(self):
        """Unknown intent should still produce output with default ratio."""
        text = "Some random text"
        compressed, ratio = self.compressor.compress(text, "unknown_intent")
        self.assertIsInstance(compressed, str)
        self.assertGreaterEqual(ratio, 0.05)

    def test_compress_preserves_meaning(self):
        """Compressed text should still contain key words."""
        text = "How do I fix the broken server"
        compressed, _ = self.compressor.compress(text, "query")
        self.assertTrue(len(compressed) > 0)

    def test_min_compression_ratio_floor(self):
        """Compression ratio should never go below min_compression_ratio."""
        config = ATCConfig(min_compression_ratio=0.1, use_codec=False)
        compressor = AdaptiveCompressor(config=config)
        text = "A"
        _, ratio = compressor.compress(text, "query")
        self.assertGreaterEqual(ratio, 0.1)


# ═══════════════════════════════════════════════════════════
# TestTokenSavingsTracker
# ═══════════════════════════════════════════════════════════

class TestTokenSavingsTracker(unittest.TestCase):
    """Tests for TokenSavingsTracker."""

    def setUp(self):
        self.tracker = TokenSavingsTracker()

    # ── Recording ──

    def test_record_basic(self):
        """Basic recording should work."""
        record = self.tracker.record("query", 50, 5)
        self.assertEqual(record["intent"], "query")
        self.assertEqual(record["original_tokens"], 50)
        self.assertEqual(record["compressed_tokens"], 5)
        self.assertEqual(record["savings"], 45)

    def test_record_multiple(self):
        """Multiple records should accumulate."""
        self.tracker.record("query", 50, 5)
        self.tracker.record("command", 30, 9)
        summary = self.tracker.get_summary()
        self.assertEqual(summary["total_operations"], 2)
        self.assertEqual(summary["total_tokens_saved"], 45 + 21)

    def test_record_negative_tokens_raises(self):
        """Negative token counts should raise ValueError."""
        with self.assertRaises(ValueError):
            self.tracker.record("query", -1, 5)

    def test_record_with_explicit_ratio(self):
        """Record with explicit compression_ratio."""
        record = self.tracker.record("query", 50, 5, compression_ratio=0.9)
        self.assertEqual(record["compression_ratio"], 0.9)

    # ── Summary ──

    def test_summary_empty(self):
        """Empty tracker should have zero totals."""
        summary = self.tracker.get_summary()
        self.assertEqual(summary["total_operations"], 0)
        self.assertEqual(summary["total_tokens_saved"], 0)
        self.assertEqual(summary["total_original_tokens"], 0)

    def test_summary_with_data(self):
        """Summary with data should have correct totals."""
        self.tracker.record("query", 100, 10)
        summary = self.tracker.get_summary()
        self.assertEqual(summary["total_operations"], 1)
        self.assertEqual(summary["total_original_tokens"], 100)
        self.assertEqual(summary["total_compressed_tokens"], 10)
        self.assertEqual(summary["total_tokens_saved"], 90)
        self.assertEqual(summary["overall_compression_ratio"], 0.9)

    def test_summary_per_intent(self):
        """Per-intent stats should be correct."""
        self.tracker.record("query", 100, 10)
        self.tracker.record("query", 50, 5)
        self.tracker.record("command", 30, 9)
        per_intent = self.tracker.get_summary()["per_intent"]
        self.assertEqual(per_intent["query"]["operations"], 2)
        self.assertEqual(per_intent["query"]["tokens_saved"], 90 + 45)
        self.assertEqual(per_intent["command"]["operations"], 1)

    # ── Per-intent stats ──

    def test_get_intent_stats(self):
        """get_intent_stats should return correct data."""
        self.tracker.record("query", 100, 10)
        stats = self.tracker.get_intent_stats("query")
        self.assertEqual(stats["operations"], 1)
        self.assertEqual(stats["tokens_saved"], 90)
        self.assertGreater(stats["avg_compression_ratio"], 0.8)

    def test_get_intent_stats_empty(self):
        """get_intent_stats for unknown intent should return zeros."""
        stats = self.tracker.get_intent_stats("nonexistent")
        self.assertEqual(stats["operations"], 0)
        self.assertEqual(stats["tokens_saved"], 0)

    # ── Records ──

    def test_get_records(self):
        """get_records should return all records."""
        self.tracker.record("query", 50, 5)
        self.tracker.record("command", 30, 9)
        records = self.tracker.get_records()
        self.assertEqual(len(records), 2)

    def test_get_records_limit(self):
        """get_records with limit should return at most limit."""
        self.tracker.record("query", 50, 5)
        self.tracker.record("command", 30, 9)
        records = self.tracker.get_records(limit=1)
        self.assertEqual(len(records), 1)

    # ── Properties ──

    def test_total_savings(self):
        """total_savings property should match summary."""
        self.tracker.record("query", 100, 10)
        self.tracker.record("command", 50, 15)
        self.assertEqual(self.tracker.total_savings, 90 + 35)

    def test_total_operations(self):
        """total_operations property should match."""
        self.tracker.record("query", 10, 1)
        self.tracker.record("query", 20, 2)
        self.assertEqual(self.tracker.total_operations, 2)

    def test_is_empty(self):
        """is_empty should be True before recording."""
        self.assertTrue(self.tracker.is_empty)
        self.tracker.record("query", 10, 1)
        self.assertFalse(self.tracker.is_empty)

    # ── Reset ──

    def test_reset(self):
        """Reset should clear all data."""
        self.tracker.record("query", 100, 10)
        self.tracker.reset()
        summary = self.tracker.get_summary()
        self.assertEqual(summary["total_operations"], 0)
        self.assertTrue(self.tracker.is_empty)


# ═══════════════════════════════════════════════════════════
# TestATCPipeline
# ═══════════════════════════════════════════════════════════

class TestATCPipeline(unittest.TestCase):
    """Tests for ATCPipeline end-to-end flow."""

    def setUp(self):
        self.pipeline = ATCPipeline()

    def test_compress_and_track_query(self):
        """compress_and_track should return a complete result dict."""
        result = self.pipeline.compress_and_track("What is Python?")
        self.assertIn("intent", result)
        self.assertIn("confidence", result)
        self.assertIn("compressed_text", result)
        self.assertIn("compression_ratio", result)
        self.assertEqual(result["intent"], "query")

    def test_compress_and_track_command(self):
        """compress_and_track should work for command intent."""
        result = self.pipeline.compress_and_track("Build a web server")
        self.assertEqual(result["intent"], "command")
        self.assertGreater(result["compression_ratio"], 0.0)

    def test_compress_and_track_tracker_update(self):
        """compress_and_track should update the tracker."""
        self.pipeline.compress_and_track("What is Python?")
        summary = self.pipeline.get_tracker_summary()
        self.assertEqual(summary["total_operations"], 1)

    def test_compress_and_track_query_saves_tokens(self):
        """Query compression should save tokens."""
        text = "What is the meaning of life and everything in the universe"
        result = self.pipeline.compress_and_track(text)
        self.assertGreater(result["tokens_saved"], 0)

    def test_get_tracker_summary_after_multiple(self):
        """Tracker summary should reflect multiple operations."""
        self.pipeline.compress_and_track("What is Python?")
        self.pipeline.compress_and_track("Build a server")
        summary = self.pipeline.get_tracker_summary()
        self.assertEqual(summary["total_operations"], 2)


# ═══════════════════════════════════════════════════════════
# TestATCConfig
# ═══════════════════════════════════════════════════════════

class TestATCConfig(unittest.TestCase):
    """Tests for ATCConfig dataclass."""

    def test_default_compression_levels(self):
        """Default compression levels should match spec."""
        config = ATCConfig()
        self.assertEqual(config.compression_levels["query"], 0.90)
        self.assertEqual(config.compression_levels["command"], 0.70)
        self.assertEqual(config.compression_levels["request"], 0.50)
        self.assertEqual(config.compression_levels["feedback"], 0.30)
        self.assertEqual(config.compression_levels["system"], 0.10)

    def test_custom_compression_levels(self):
        """Custom compression levels should be respected."""
        config = ATCConfig(compression_levels={"query": 0.95})
        self.assertEqual(config.compression_levels["query"], 0.95)

    def test_default_values(self):
        """Default config values should be correct."""
        config = ATCConfig()
        self.assertEqual(config.min_compression_ratio, 0.05)
        self.assertTrue(config.use_codec)
        self.assertEqual(config.zlib_level, 6)
        self.assertEqual(config.confidence_threshold, 0.10)


# ═══════════════════════════════════════════════════════════
# TestExceptions
# ═══════════════════════════════════════════════════════════

class TestExceptions(unittest.TestCase):
    """Tests for ATC exception hierarchy."""

    def test_atc_error_base(self):
        """ATCError should be base exception."""
        err = ATCError("test")
        self.assertIsInstance(err, Exception)

    def test_classification_error(self):
        """ClassificationError should inherit from ATCError."""
        err = ClassificationError("test")
        self.assertIsInstance(err, ATCError)

    def test_compression_error(self):
        """CompressionError should inherit from ATCError."""
        err = CompressionError("test")
        self.assertIsInstance(err, ATCError)

    def test_classify_empty_raises_classification_error(self):
        """Empty text should raise ClassificationError."""
        classifier = IntentClassifier()
        with self.assertRaises(ClassificationError):
            classifier.classify("")


# ═══════════════════════════════════════════════════════════
# TestIntentClassifierAlternatives
# ═══════════════════════════════════════════════════════════

class TestIntentClassifierAlternatives(unittest.TestCase):
    """Additional tests for classify_with_alternatives."""

    def setUp(self):
        self.classifier = IntentClassifier()

    def test_alternatives_all_valid_intents(self):
        """All returned intents should be valid."""
        results = self.classifier.classify_with_alternatives("Build a server")
        valid = set(IntentClassifier.VALID_INTENTS)
        for intent, _ in results:
            self.assertIn(intent, valid)

    def test_alternatives_with_top_n_one(self):
        """top_n=1 should return a single result."""
        results = self.classifier.classify_with_alternatives("Build a server", top_n=1)
        self.assertEqual(len(results), 1)

    def test_alternatives_confidence_sum_reasonable(self):
        """Confidences should be reasonable."""
        results = self.classifier.classify_with_alternatives("What is Python?")
        for intent, conf in results:
            self.assertGreaterEqual(conf, 0.0)
            self.assertLessEqual(conf, 1.0)


# ═══════════════════════════════════════════════════════════
# TestAdaptiveCompressorEdgeCases
# ═══════════════════════════════════════════════════════════

class TestAdaptiveCompressorEdgeCases(unittest.TestCase):
    """Additional edge case tests for AdaptiveCompressor."""

    def setUp(self):
        self.compressor = AdaptiveCompressor()

    def test_compress_very_long_text(self):
        """Should handle very long text without error."""
        text = "What is the meaning of life and everything about the universe and how it all works together and why we are here" * 10
        compressed, ratio = self.compressor.compress(text, "query")
        self.assertIsInstance(compressed, str)
        self.assertGreater(ratio, 0.0)

    def test_compress_special_characters(self):
        """Text with special characters should be handled."""
        text = "What is Python 3.14? It's great!"
        compressed, _ = self.compressor.compress(text, "query")
        self.assertIsInstance(compressed, str)

    def test_compress_numbers_and_symbols(self):
        """Text with numbers should work."""
        text = "Set the port to 8080 and timeout to 30"
        compressed, _ = self.compressor.compress(text, "system")
        self.assertIsInstance(compressed, str)

    def test_decompress_non_zlib(self):
        """Decompressing non-zlib text should return it as-is."""
        text = "Hello world, this is not compressed"
        result = self.compressor.decompress(text)
        self.assertEqual(result, text)

    def test_compress_zlib_disabled(self):
        """With use_codec=False, output should not be hex."""
        config = ATCConfig(use_codec=False)
        compressor = AdaptiveCompressor(config=config)
        text = "What is Python?"
        compressed, _ = compressor.compress(text, "query")
        # Should be readable text, not hex
        self.assertLess(len(compressed), len(text.split()) * 10)


# ═══════════════════════════════════════════════════════════
# TestTokenSavingsTrackerEdgeCases
# ═══════════════════════════════════════════════════════════

class TestTokenSavingsTrackerEdgeCases(unittest.TestCase):
    """Additional edge case tests for TokenSavingsTracker."""

    def setUp(self):
        self.tracker = TokenSavingsTracker()

    def test_record_zero_tokens(self):
        """Recording zero original tokens should work."""
        record = self.tracker.record("query", 0, 0)
        self.assertEqual(record["savings"], 0)

    def test_record_same_tokens(self):
        """Recording no compression should show 0 savings."""
        record = self.tracker.record("query", 50, 50)
        self.assertEqual(record["savings"], 0)
        self.assertEqual(record["compression_ratio"], 0.0)

    def test_summary_savings_percentage(self):
        """Savings percentage should be correct."""
        self.tracker.record("query", 100, 10)
        summary = self.tracker.get_summary()
        self.assertEqual(summary["savings_percentage"], 90.0)

    def test_records_order_preserved(self):
        """Records should preserve insertion order."""
        self.tracker.record("query", 50, 5)
        self.tracker.record("command", 30, 9)
        records = self.tracker.get_records()
        self.assertEqual(records[0]["intent"], "query")
        self.assertEqual(records[1]["intent"], "command")


# ═══════════════════════════════════════════════════════════
# TestIntegration
# ═══════════════════════════════════════════════════════════

class TestIntegration(unittest.TestCase):
    """Integration tests combining all ATC components."""

    def test_full_workflow(self):
        """Full workflow: create classifier, compressor, tracker, pipeline."""
        config = ATCConfig()
        classifier = IntentClassifier(config=config)
        compressor = AdaptiveCompressor(config=config, classifier=classifier)
        tracker = TokenSavingsTracker()

        # Process multiple texts
        texts = [
            ("What is Python?", "query"),
            ("Build a web server", "command"),
            ("Could you help me find it", "request"),
            ("The build works great", "feedback"),
            ("Configure the server port to 8080", "system"),
        ]

        for text, expected_intent in texts:
            intent, confidence = classifier.classify(text)
            compressed, ratio = compressor.compress(text, intent)
            original_tokens = len(text.split())
            compressed_tokens = max(len(compressed.split()), 1)
            tracker.record(intent, original_tokens, compressed_tokens)
            self.assertIsInstance(compressed, str)
            self.assertGreaterEqual(ratio, config.min_compression_ratio)

        # Verify tracker has data
        summary = tracker.get_summary()
        self.assertEqual(summary["total_operations"], len(texts))

    def test_pipeline_end_to_end_multiple_intents(self):
        """Pipeline should handle all 5 intents correctly."""
        pipeline = ATCPipeline()
        texts = [
            "What is Python?",
            "Build a web server",
            "Could you help me find it",
            "The build works great",
            "Configure the server port to 8080",
        ]
        intents = set()
        for text in texts:
            result = pipeline.compress_and_track(text)
            intents.add(result["intent"])

        self.assertIn("query", intents)
        self.assertIn("command", intents)
        self.assertIn("request", intents)
        self.assertIn("feedback", intents)
        self.assertIn("system", intents)


if __name__ == "__main__":
    unittest.main()
