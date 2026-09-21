"""
TokenRelay v3 — Adaptive Token Compression (ATC).

Classifies user queries into 5 intents (query, command, request, feedback, system)
and applies dynamic compression levels to minimize token consumption.

Built on TokenRelay v2 modules:
- codec.compress_message/decompress_message for zlib-based compression
- codec.sign_message/verify_signature for integrity (optional)

Features:
- IntentClassifier: keyword/pattern-based text classification with confidence
- AdaptiveCompressor: intent-aware compression using v2 codec
- TokenSavingsTracker: cumulative savings tracking
- ATCConfig: configuration dataclass

Compression targets by intent:
  query    → 90% (quick question, aggressive stripping)
  command  → 70% (instruction, moderate stripping)
  request  → 50% (task, preserve detail)
  feedback → 30% (response, minimal stripping)
  system   → 10% (config, near-original)
"""

import re
import time
import math
import json
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

# ── v2 codec imports ──
from codec import compress_message, decompress_message


# ──────────────────────────────────────────────
# ATC Exceptions
# ──────────────────────────────────────────────

class ATCError(Exception):
    """Base exception for ATC module."""
    pass


class ClassificationError(ATCError):
    """Raised when intent classification fails."""
    pass


class CompressionError(ATCError):
    """Raised when compression fails."""
    pass


# ──────────────────────────────────────────────
# Intent keywords — weighted for classification
# ──────────────────────────────────────────────

_INTENT_KEYWORDS: Dict[str, List[Tuple[str, float]]] = {
    "query": [
        ("what", 0.15), ("how", 0.15), ("why", 0.10), ("when", 0.08),
        ("where", 0.08), ("who", 0.08), ("which", 0.07), ("is", 0.05),
        ("are", 0.05), ("can", 0.05), ("does", 0.04), ("do", 0.03),
        ("explain", 0.10), ("tell", 0.08), ("describe", 0.06),
        ("define", 0.06), ("meaning", 0.05), ("difference", 0.05),
        ("difference between", 0.05), ("why is", 0.04), ("how to", 0.04),
    ],
    "command": [
        ("run", 0.10), ("execute", 0.12), ("build", 0.10), ("create", 0.10),
        ("generate", 0.10), ("deploy", 0.10), ("install", 0.08), ("set", 0.07),
        ("configure", 0.08), ("start", 0.07), ("stop", 0.07), ("restart", 0.06),
        ("delete", 0.07), ("remove", 0.06), ("update", 0.06), ("init", 0.05),
        ("clean", 0.05), ("compile", 0.08), ("write", 0.05), ("push", 0.05),
        ("pull", 0.05), ("merge", 0.05), ("commit", 0.05), ("test", 0.04),
        ("fix", 0.05), ("debug", 0.05), ("refactor", 0.06), ("optimize", 0.05),
        ("implement", 0.06), ("add", 0.04), ("enable", 0.04), ("disable", 0.04),
        ("convert", 0.05), ("format", 0.04), ("migrate", 0.05),
    ],
    "request": [
        ("please", 0.08), ("could you", 0.10), ("would you", 0.08),
        ("i need", 0.10), ("i want", 0.08), ("help me", 0.08),
        ("assist", 0.07), ("require", 0.07), ("need", 0.06), ("want", 0.05),
        ("request", 0.08), ("ask", 0.06), ("suggest", 0.05), ("recommend", 0.05),
        ("provide", 0.06), ("send", 0.05), ("show", 0.05), ("give", 0.05),
        ("find", 0.05), ("search", 0.05), ("look", 0.04), ("retrieve", 0.05),
        ("fetch", 0.05), ("get", 0.04), ("produce", 0.04), ("generate a", 0.04),
    ],
    "feedback": [
        ("good", 0.08), ("bad", 0.08), ("great", 0.08), ("excellent", 0.06),
        ("terrible", 0.06), ("working", 0.07), ("works", 0.06), ("broken", 0.06),
        ("fixed", 0.06), ("issue", 0.06), ("problem", 0.06), ("error", 0.05),
        ("bug", 0.06), ("fail", 0.05), ("failed", 0.05), ("success", 0.05),
        ("worked", 0.05), ("perfect", 0.05), ("okay", 0.04), ("fine", 0.04),
        ("better", 0.05), ("worse", 0.05), ("improve", 0.04), ("improved", 0.04),
        ("slow", 0.04), ("fast", 0.04), ("crash", 0.05), ("crashed", 0.04),
        ("tutorial", 0.04), ("guide", 0.04), ("learned", 0.04),
    ],
    "system": [
        ("config", 0.15), ("configuration", 0.15), ("settings", 0.10),
        ("environment", 0.10), ("env", 0.08), ("path", 0.07), ("port", 0.07),
        ("host", 0.07), ("server", 0.07), ("version", 0.08), ("upgrade", 0.07),
        ("permission", 0.07), ("access", 0.06), ("credential", 0.07),
        ("token", 0.07), ("key", 0.06), ("debug", 0.05), ("log", 0.06),
        ("monitor", 0.06), ("status", 0.06), ("parameter", 0.07), ("flag", 0.06),
        ("option", 0.06), ("preference", 0.06), ("profile", 0.06),
        ("database", 0.06), ("cache", 0.06), ("memory", 0.06),
        ("storage", 0.06), ("restart", 0.06), ("uninstall", 0.07),
        ("configure", 0.06), ("system", 0.12), ("install", 0.05),
    ],
}

# ──────────────────────────────────────────────
# Stop words to strip during compression
# ──────────────────────────────────────────────

_STOP_WORDS = frozenset([
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "as", "is", "was", "are", "were", "been",
    "be", "have", "has", "had", "do", "does", "did", "will", "would",
    "could", "should", "may", "might", "shall", "can", "need", "dare",
    "it", "its", "he", "she", "they", "them", "their", "his", "her",
    "we", "us", "our", "you", "your", "i", "me", "my", "this", "that",
    "these", "those", "which", "who", "whom", "what", "where", "when",
    "how", "not", "no", "nor", "so", "if", "then", "than", "too",
    "very", "just", "about", "above", "after", "again", "all", "also",
    "am", "any", "because", "before", "below", "between", "both",
    "each", "few", "further", "here", "there", "once", "only", "own",
    "same", "some", "such", "up", "down", "out", "off", "over", "under",
    "into", "through", "during", "while", "until", "against",
    "s", "t", "d", "m", "re", "ve", "ll", "don", "doesn", "didn",
    "won", "wouldn", "couldn", "shouldn", "isn", "aren", "wasn",
    "weren", "haven", "hasn", "hadn", "mustn", "okay",
])

_QUERY_FILLERS = frozenset(["basically", "essentially", "actually", "really", "simply", "just", "literally"])


@dataclass
class ATCConfig:
    """Configuration for the Adaptive Token Compression system.

    Attributes:
        compression_levels: Mapping of intent to target compression ratio (0.0-1.0).
        min_compression_ratio: Minimum achievable compression ratio (safety floor).
        use_codec: Whether to use v2 codec for compression.
        zlib_level: zlib compression level passed to codec (1-9).
        confidence_threshold: Minimum confidence score to accept a classification.
        preserve_capitalized: Whether to preserve words starting with capitals.
        strip_stop_words: Whether to remove stop words during compression.
    """
    compression_levels: Dict[str, float] = field(default_factory=lambda: {
        "query": 0.90,
        "command": 0.70,
        "request": 0.50,
        "feedback": 0.30,
        "system": 0.10,
    })
    min_compression_ratio: float = 0.05
    use_codec: bool = True
    zlib_level: int = 6
    confidence_threshold: float = 0.10
    preserve_capitalized: bool = True
    strip_stop_words: bool = True


class IntentClassifier:
    """Classifies user text into one of 5 intents with a confidence score.

    Uses a weighted keyword matching approach.

    Example:
        classifier = IntentClassifier()
        intent, confidence = classifier.classify("How do I fix the bug?")
        # intent = "query", confidence ≈ 0.30
    """

    VALID_INTENTS = ("query", "command", "request", "feedback", "system")

    def __init__(self, config: Optional[ATCConfig] = None):
        self.config = config or ATCConfig()
        self._compiled_patterns: Dict[str, List[Tuple[re.Pattern, float]]] = {}
        self._build_patterns()

    def _build_patterns(self):
        """Pre-compile keyword patterns for efficient matching."""
        for intent, keywords in _INTENT_KEYWORDS.items():
            patterns = []
            for word, weight in keywords:
                pattern = re.compile(re.escape(word), re.IGNORECASE)
                patterns.append((pattern, weight))
            self._compiled_patterns[intent] = patterns

    def classify(self, text: str) -> Tuple[str, float]:
        """Classify text into an intent and return the confidence score.

        Raises:
            ClassificationError: If text is empty or cannot be classified.
        """
        if not text or not text.strip():
            raise ClassificationError("Cannot classify empty text")

        text_lower = text.strip().lower()
        scores: Dict[str, float] = {}

        for intent in self.VALID_INTENTS:
            score = self._score_text(text_lower, intent)
            scores[intent] = score

        total = sum(scores.values())
        if total == 0:
            best_intent = max(scores, key=scores.get)
            return best_intent, 0.05

        max_score = max(scores.values())
        confidence = max_score / total if total > 0 else 0.0
        best_intent = max(scores, key=scores.get)

        if confidence < self.config.confidence_threshold:
            confidence = self.config.confidence_threshold

        return best_intent, round(confidence, 4)

    def _score_text(self, text: str, intent: str) -> float:
        """Score text for a specific intent using keyword matching."""
        patterns = self._compiled_patterns.get(intent, [])
        score = 0.0

        for pattern, weight in patterns:
            matches = pattern.findall(text)
            if matches:
                score += weight * len(matches)

        # Boost for multi-word phrase matches
        multi_word = [w for w, _ in _INTENT_KEYWORDS.get(intent, []) if " " in w]
        for phrase in multi_word:
            if phrase.lower() in text:
                for w, wt in _INTENT_KEYWORDS[intent]:
                    if w == phrase:
                        score += wt * 0.5
                        break

        word_count = max(len(text.split()), 1)
        normalized = score / math.log(word_count + 1)
        return round(normalized, 4)

    def classify_with_alternatives(self, text: str, top_n: int = 3
                                    ) -> List[Tuple[str, float]]:
        """Classify text and return top-N intents sorted by confidence."""
        if not text or not text.strip():
            return [("query", 0.05)]

        text_lower = text.strip().lower()
        scores: Dict[str, float] = {}

        for intent in self.VALID_INTENTS:
            scores[intent] = self._score_text(text_lower, intent)

        sorted_intents = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        total = sum(s[1] for s in sorted_intents)

        results = []
        for intent, score in sorted_intents[:top_n]:
            confidence = (score / total) if total > 0 else 0.05
            results.append((intent, round(confidence, 4)))

        return results


class AdaptiveCompressor:
    """Applies dynamic text compression based on intent classification.

    Uses v2 codec (compress_message/decompress_message) for zlib compression.
    System intent skips compression entirely — only normalizes whitespace.

    Example:
        compressor = AdaptiveCompressor()
        result = compressor.compress("How do I fix the bug?", "query")
    """

    def __init__(self, config: Optional[ATCConfig] = None,
                 classifier: Optional[IntentClassifier] = None):
        self.config = config or ATCConfig()
        self.classifier = classifier or IntentClassifier(config=self.config)

    def compress(self, text: str, intent: Optional[str] = None
                 ) -> Tuple[str, float]:
        """Compress text according to the specified intent's compression level.

        System intent skips codec compression (only normalizes whitespace).
        All other intents apply text summarization then v2 codec compression.
        """
        if not text or not text.strip():
            return "", 0.0

        if intent is None:
            intent, _ = self.classifier.classify(text)

        target_ratio = self.config.compression_levels.get(intent, 0.5)
        original_word_count = len(text.split())

        # System intent: minimal compression — normalize whitespace only, no codec
        if intent == "system":
            compressed_text = self._normalize_whitespace(text)
            # Calculate character-level savings ratio for system
            char_savings = 1.0 - (len(compressed_text) / max(len(text), 1))
            ratio = round(max(char_savings, self.config.min_compression_ratio), 4)
            return compressed_text, ratio

        # Apply intent-specific text compression
        compressed_text = self._apply_compression(text, intent, target_ratio)

        # Apply v2 codec compression (zlib via compress_message)
        if self.config.use_codec:
            compressed_text = self._codec_compress(compressed_text)

        compressed_word_count = max(len(compressed_text.split()), 1)
        compression_ratio = round(
            1.0 - (compressed_word_count / original_word_count), 4
        )
        compression_ratio = max(compression_ratio, self.config.min_compression_ratio)

        return compressed_text, compression_ratio

    def _apply_compression(self, text: str, intent: str,
                           target_ratio: float) -> str:
        """Apply intent-specific text summarization (pre-codec)."""
        words = text.split()
        if intent == "query":
            return self._aggressive_compress(words, target_ratio)
        if intent == "command":
            return self._moderate_compress(words, target_ratio)
        if intent == "request":
            return self._balanced_compress(words, target_ratio)
        if intent == "feedback":
            return self._light_compress(words, target_ratio)
        return self._normalize_whitespace(text)

    def _aggressive_compress(self, words: List[str],
                               target_ratio: float) -> str:
        """Aggressive compression for queries — keep keywords only."""
        if self.config.strip_stop_words:
            filtered = [w for w in words if w.lower() not in _STOP_WORDS
                        and w.lower() not in _QUERY_FILLERS]
        else:
            filtered = words

        if self.config.preserve_capitalized:
            capitalized = [w for w in filtered if w and w[0].isupper()]
            if capitalized and len(capitalized) < max(len(filtered) * 0.3, 1):
                filtered = capitalized + [w for w in filtered if w not in capitalized]

        target_count = max(int(len(words) * (1.0 - target_ratio)), 1)
        if len(filtered) > target_count:
            filtered = self._prioritize_words(filtered, target_count)

        return " ".join(filtered) if filtered else " ".join(words[:target_count])

    def _moderate_compress(self, words: List[str],
                               target_ratio: float) -> str:
        """Moderate compression for commands — strip stop words."""
        if self.config.strip_stop_words:
            filtered = [w for w in words if w.lower() not in _STOP_WORDS]
        else:
            filtered = words

        target_count = max(int(len(words) * (1.0 - target_ratio)), 1)
        if len(filtered) > target_count:
            filtered = self._prioritize_words(filtered, target_count)

        return " ".join(filtered) if filtered else " ".join(words[:target_count])

    def _balanced_compress(self, words: List[str],
                               target_ratio: float) -> str:
        """Balanced compression for requests — strip politeness."""
        polite_words = {"please", "could", "you", "would", "i", "me", "my", "can"}
        if self.config.strip_stop_words:
            filtered = [w for w in words
                        if w.lower() not in _STOP_WORDS and w.lower() not in polite_words]
        else:
            filtered = [w for w in words if w.lower() not in polite_words]

        target_count = max(int(len(words) * (1.0 - target_ratio)), 1)
        if len(filtered) > target_count:
            filtered = self._prioritize_words(filtered, target_count)

        return " ".join(filtered) if filtered else " ".join(words[:target_count])

    def _light_compress(self, words: List[str],
                           target_ratio: float) -> str:
        """Light compression for feedback — minimal restructuring."""
        if self.config.strip_stop_words:
            filtered = [w for w in words if w.lower() not in _STOP_WORDS]
        else:
            filtered = words

        target_count = max(int(len(words) * (1.0 - target_ratio)), 1)
        if len(filtered) > target_count:
            filtered = self._prioritize_words(filtered, target_count)

        return " ".join(filtered) if filtered else " ".join(words[:target_count])

    def _normalize_whitespace(self, text: str) -> str:
        """Minimal compression — just normalize whitespace."""
        return re.sub(r'\s+', ' ', text.strip())

    def _codec_compress(self, text: str) -> str:
        """Apply v2 codec compression to text using compress_message.

        Args:
            text: Text to compress.

        Returns:
            Hex-encoded zlib-compressed string from codec.
        """
        try:
            msg = {"payload": {"__text": text}}
            compressed_msg = compress_message(msg)
            payload = compressed_msg.get("payload", {})
            if isinstance(payload, dict) and payload.get("__compressed"):
                return payload["__data"]
            return text
        except Exception as e:
            raise CompressionError(f"Codec compression failed: {e}")

    def decompress(self, compressed_text: str) -> str:
        """Decompress a codec-compressed text string back to original.

        Uses decompress_message from v2 codec.
        Falls back to returning as-is if not codec-compressed.
        """
        try:
            msg = {"payload": {"__compressed": True, "__data": compressed_text}}
            decompressed_msg = decompress_message(msg)
            payload = decompressed_msg.get("payload", {})
            if isinstance(payload, dict) and "__text" in payload:
                return payload["__text"]
            if isinstance(payload, dict):
                return json.dumps(payload)
            if isinstance(payload, str):
                return payload
            return str(payload)
        except Exception:
            return compressed_text

    def _prioritize_words(self, words: List[str],
                             target_count: int) -> List[str]:
        """Select the most meaningful words to keep."""
        if target_count >= len(words):
            return words

        def word_score(w: str) -> float:
            s = 0.0
            if w and w[0].isupper():
                s += 2.0
            if len(w) > 6:
                s += 1.0
            elif len(w) > 4:
                s += 0.5
            if len(w) <= 3 and w.lower() not in {"no", "not"}:
                s -= 0.5
            return s

        scored = [(word_score(w), i, w) for i, w in enumerate(words)]
        scored.sort(key=lambda x: x[0], reverse=True)
        kept_indices = sorted(set(x[1] for x in scored[:target_count]))
        return [words[i] for i in kept_indices]


class TokenSavingsTracker:
    """Tracks cumulative token savings from compression operations."""

    def __init__(self):
        self._records: List[Dict[str, Any]] = []
        self._intent_stats: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "original_tokens": 0, "compressed_tokens": 0,
                "savings": 0, "operations": 0,
            }
        )

    def record(self, intent: str, original_tokens: int,
               compressed_tokens: int,
               compression_ratio: Optional[float] = None) -> Dict[str, Any]:
        """Record a compression operation."""
        if original_tokens < 0 or compressed_tokens < 0:
            raise ValueError("Token counts must be non-negative")

        if compression_ratio is None:
            compression_ratio = (
                (original_tokens - compressed_tokens) / max(original_tokens, 1)
            )

        savings = max(original_tokens - compressed_tokens, 0)

        record = {
            "intent": intent,
            "original_tokens": original_tokens,
            "compressed_tokens": compressed_tokens,
            "savings": savings,
            "compression_ratio": round(compression_ratio, 4),
            "timestamp": time.time(),
        }

        self._records.append(record)

        stats = self._intent_stats[intent]
        stats["original_tokens"] += original_tokens
        stats["compressed_tokens"] += compressed_tokens
        stats["savings"] += savings
        stats["operations"] += 1

        return record

    def get_summary(self) -> Dict[str, Any]:
        """Get overall compression statistics."""
        total_original = sum(r["original_tokens"] for r in self._records)
        total_compressed = sum(r["compressed_tokens"] for r in self._records)
        total_savings = sum(r["savings"] for r in self._records)
        total_ops = len(self._records)

        overall_ratio = (
            (total_original - total_compressed) / max(total_original, 1)
        )

        return {
            "total_operations": total_ops,
            "total_original_tokens": total_original,
            "total_compressed_tokens": total_compressed,
            "total_tokens_saved": total_savings,
            "overall_compression_ratio": round(overall_ratio, 4),
            "savings_percentage": round(overall_ratio * 100, 2),
            "per_intent": {
                intent: {
                    "operations": stats["operations"],
                    "original_tokens": stats["original_tokens"],
                    "compressed_tokens": stats["compressed_tokens"],
                    "tokens_saved": stats["savings"],
                    "avg_compression_ratio": round(
                        (stats["original_tokens"] - stats["compressed_tokens"])
                        / max(stats["original_tokens"], 1), 4
                    ) if stats["original_tokens"] > 0 else 0.0,
                }
                for intent, stats in self._intent_stats.items()
            },
        }

    def get_intent_stats(self, intent: str) -> Dict[str, Any]:
        """Get statistics for a specific intent."""
        stats = self._intent_stats.get(intent)
        if not stats or stats["operations"] == 0:
            return {
                "operations": 0, "original_tokens": 0,
                "compressed_tokens": 0, "tokens_saved": 0,
                "avg_compression_ratio": 0.0,
            }
        return {
            "operations": stats["operations"],
            "original_tokens": stats["original_tokens"],
            "compressed_tokens": stats["compressed_tokens"],
            "tokens_saved": stats["savings"],
            "avg_compression_ratio": round(
                (stats["original_tokens"] - stats["compressed_tokens"])
                / max(stats["original_tokens"], 1), 4
            ),
        }

    def reset(self):
        """Reset all tracking data."""
        self._records.clear()
        self._intent_stats.clear()

    def get_records(self, limit: Optional[int] = None
                     ) -> List[Dict[str, Any]]:
        """Get all recorded compression operations."""
        if limit is not None:
            return self._records[-limit:]
        return list(self._records)

    @property
    def total_savings(self) -> int:
        return sum(r["savings"] for r in self._records)

    @property
    def total_operations(self) -> int:
        return len(self._records)

    @property
    def is_empty(self) -> bool:
        return len(self._records) == 0


class ATCPipeline:
    """Convenience class combining all ATC components."""

    def __init__(self, config: Optional[ATCConfig] = None):
        self.config = config or ATCConfig()
        self.classifier = IntentClassifier(config=self.config)
        self.compressor = AdaptiveCompressor(config=self.config,
                                              classifier=self.classifier)
        self.tracker = TokenSavingsTracker()

    def compress_and_track(self, text: str) -> Dict[str, Any]:
        """Classify, compress, and track a single text input."""
        intent, confidence = self.classifier.classify(text)
        original_tokens = len(text.split())
        compressed_text, compression_ratio = self.compressor.compress(text, intent)
        compressed_tokens = max(len(compressed_text.split()), 1)

        self.tracker.record(intent, original_tokens, compressed_tokens,
                             compression_ratio)

        return {
            "intent": intent,
            "confidence": confidence,
            "compressed_text": compressed_text,
            "compression_ratio": compression_ratio,
            "original_tokens": original_tokens,
            "compressed_tokens": compressed_tokens,
            "tokens_saved": original_tokens - compressed_tokens,
        }

    def get_tracker_summary(self) -> Dict[str, Any]:
        """Get the tracker's summary statistics."""
        return self.tracker.get_summary()
