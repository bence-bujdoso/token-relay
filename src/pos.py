"""
TokenRelay v3 — Predictive Output Streaming (POS).

Built on top of TokenRelay v2:
- Uses EventBus (streaming) for event routing
- Uses MessageEncoder/MessageDecoder (codec) for token encoding
- Uses SSEHandler (streaming) for SSE-based chunk streaming
- Uses MessageBroker (broker) for backpressure rate limiting
- Uses CircuitBreaker (circuit_breaker) for fault tolerance
- Uses TokenRegistry (registry) for token metadata

AI begins streaming before full response completes. Predicts
response structure from query pattern, then renders progressively:
skeleton → details → conclusion.
"""

import time
import threading
import hashlib
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple
from collections import deque

from streaming import EventBus, SSEHandler, BackpressureState, StreamEvent, StreamEventType
from codec import MessageEncoder, MessageDecoder
from broker import MessageBroker, PRIORITY_HIGH, PRIORITY_MEDIUM
from circuit_breaker import CircuitBreaker, CircuitBreakerError
from registry import TokenRegistry, get_registry


# ─────────────────────────────────────────────
# Types & Enums
# ─────────────────────────────────────────────

class Phase(Enum):
    SKELETON = "skeleton"
    DETAILS = "details"
    CONCLUSION = "conclusion"


class PredictionConfidence(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ChunkPriority(Enum):
    SKELETON = 0
    DETAIL = 1
    CONCLUSION = 2


class PredictorMode(Enum):
    QUERY_PATTERN = "query_pattern"
    TEMPLATE_MATCH = "template_match"
    HYBRID = "hybrid"


# ─────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────

@dataclass
class POSConfig:
    prediction_confidence_threshold: float = 0.5
    progressive_rendering_enabled: bool = True
    default_chunk_size: int = 100
    max_chunk_size: int = 500
    min_chunk_size: int = 10
    backpressure_window_size: int = 100
    high_watermark: float = 0.8
    low_watermark: float = 0.3
    skeleton_phase_delay_ms: int = 0
    detail_phase_delay_ms: int = 50
    conclusion_phase_delay_ms: int = 100
    prediction_cache_size: int = 1000


@dataclass
class SectionPrediction:
    section_name: str
    section_type: str
    estimated_tokens: int
    order: int
    content_hint: str = ""
    confidence: float = 0.5


@dataclass
class PredictedStructure:
    skeleton: str
    sections: List[SectionPrediction]
    conclusion: str
    total_estimated_tokens: int = 0
    confidence: float = 0.5
    prediction_mode: str = "query_pattern"
    query_hash: str = ""


@dataclass
class Chunk:
    content: str
    phase: Phase
    chunk_index: int = 0
    total_chunks_in_phase: int = 1
    priority: int = 1
    token_count: int = 0
    timestamp: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "content": self.content,
            "phase": self.phase.value,
            "chunk_index": self.chunk_index,
            "total_chunks_in_phase": self.total_chunks_in_phase,
            "priority": self.priority,
            "token_count": self.token_count,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
        }


@dataclass
class ChunkMetrics:
    chunks_sent: int = 0
    chunks_dropped: int = 0
    total_tokens_sent: int = 0
    avg_chunk_size: float = 0.0
    current_chunk_size: int = 0
    backpressure_utilization: float = 0.0
    delivery_rate: float = 0.0
    phase_progress: Dict[str, int] = field(default_factory=dict)


# ─────────────────────────────────────────────
# Prediction Cache
# ─────────────────────────────────────────────

class PredictionCache:
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._cache: Dict[str, PredictedStructure] = {}
        self._access_order: deque = deque()
        self._lock = threading.RLock()

    def get(self, query_hash: str) -> Optional[PredictedStructure]:
        with self._lock:
            if query_hash in self._cache:
                self._access_order.remove(query_hash)
                self._access_order.append(query_hash)
                return self._cache[query_hash]
        return None

    def put(self, query_hash: str, structure: PredictedStructure):
        with self._lock:
            if len(self._cache) >= self.max_size and query_hash not in self._cache:
                oldest = self._access_order.popleft()
                del self._cache[oldest]
            self._cache[query_hash] = structure
            if query_hash in self._access_order:
                self._access_order.remove(query_hash)
            self._access_order.append(query_hash)

    def invalidate(self, query_hash: str):
        with self._lock:
            self._cache.pop(query_hash, None)
            if query_hash in self._access_order:
                self._access_order.remove(query_hash)

    def clear(self):
        with self._lock:
            self._cache.clear()
            self._access_order.clear()

    @property
    def size(self) -> int:
        return len(self._cache)


# ─────────────────────────────────────────────
# ResponsePredictor
# ─────────────────────────────────────────────

class ResponsePredictor:
    QUERY_PATTERNS: Dict[str, Dict[str, Any]] = {
        "scientific": {
            "skeleton": "Scientific explanation with definitions and mechanisms",
            "sections": [
                {"section_name": "definition", "section_type": "definition",
                 "estimated_tokens": 50, "order": 1,
                 "content_hint": "Core definition and key terms"},
                {"section_name": "mechanism", "section_type": "mechanism",
                 "estimated_tokens": 200, "order": 2,
                 "content_hint": "Step-by-step mechanism explanation"},
                {"section_name": "examples", "section_type": "examples",
                 "estimated_tokens": 100, "order": 3,
                 "content_hint": "Concrete examples and illustrations"},
                {"section_name": "significance", "section_type": "significance",
                 "estimated_tokens": 80, "order": 4,
                 "content_hint": "Why this matters and broader context"},
            ],
            "conclusion": "Summary of key scientific insights",
            "total_estimated_tokens": 430,
            "confidence": 0.85,
        },
        "coding": {
            "skeleton": "Code example with explanation",
            "sections": [
                {"section_name": "problem", "section_type": "problem",
                 "estimated_tokens": 40, "order": 1,
                 "content_hint": "Problem statement and requirements"},
                {"section_name": "solution", "section_type": "solution",
                 "estimated_tokens": 150, "order": 2,
                 "content_hint": "Code implementation"},
                {"section_name": "explanation", "section_type": "explanation",
                 "estimated_tokens": 100, "order": 3,
                 "content_hint": "Walkthrough of the code"},
            ],
            "conclusion": "Key takeaways and usage notes",
            "total_estimated_tokens": 290,
            "confidence": 0.80,
        },
        "comparison": {
            "skeleton": "Comparison of approaches or entities",
            "sections": [
                {"section_name": "overview", "section_type": "overview",
                 "estimated_tokens": 40, "order": 1,
                 "content_hint": "What is being compared"},
                {"section_name": "comparison_points", "section_type": "comparison",
                 "estimated_tokens": 200, "order": 2,
                 "content_hint": "Side-by-side comparison"},
                {"section_name": "recommendation", "section_type": "recommendation",
                 "estimated_tokens": 80, "order": 3,
                 "content_hint": "Which approach is better and why"},
            ],
            "conclusion": "Final recommendation summary",
            "total_estimated_tokens": 320,
            "confidence": 0.78,
        },
        "definition": {
            "skeleton": "Clear definition with context",
            "sections": [
                {"section_name": "definition", "section_type": "definition",
                 "estimated_tokens": 60, "order": 1,
                 "content_hint": "Precise definition"},
                {"section_name": "context", "section_type": "context",
                 "estimated_tokens": 80, "order": 2,
                 "content_hint": "Context and related concepts"},
            ],
            "conclusion": "Key points about the definition",
            "total_estimated_tokens": 140,
            "confidence": 0.90,
        },
        "list": {
            "skeleton": "Enumerated list with explanations",
            "sections": [
                {"section_name": "items", "section_type": "list_items",
                 "estimated_tokens": 200, "order": 1,
                 "content_hint": "Numbered list of items"},
            ],
            "conclusion": "Summary of the list",
            "total_estimated_tokens": 220,
            "confidence": 0.82,
        },
        "debug": {
            "skeleton": "Debugging analysis with solution",
            "sections": [
                {"section_name": "problem_analysis", "section_type": "analysis",
                 "estimated_tokens": 80, "order": 1,
                 "content_hint": "Root cause analysis"},
                {"section_name": "solution", "section_type": "solution",
                 "estimated_tokens": 120, "order": 2,
                 "content_hint": "Fix and implementation"},
                {"section_name": "prevention", "section_type": "prevention",
                 "estimated_tokens": 60, "order": 3,
                 "content_hint": "How to avoid this in future"},
            ],
            "conclusion": "Debugging summary",
            "total_estimated_tokens": 260,
            "confidence": 0.75,
        },
    }

    def __init__(self, config: Optional[POSConfig] = None,
                 registry: Optional[TokenRegistry] = None):
        self.config = config or POSConfig()
        self.registry = registry or get_registry()
        self.cache = PredictionCache(max_size=self.config.prediction_cache_size)
        self._mode = PredictorMode.HYBRID
        self._prediction_stats: Dict[str, int] = {
            "total_predictions": 0, "cache_hits": 0, "cache_misses": 0,
        }
        self._lock = threading.RLock()

    @property
    def mode(self) -> PredictorMode:
        return self._mode

    @mode.setter
    def mode(self, value: PredictorMode):
        self._mode = value

    def predict(self, query: str) -> PredictedStructure:
        if not isinstance(query, str) or not query.strip():
            raise ValueError("Query must be a non-empty string")

        query_hash = self._compute_query_hash(query)
        cached = self.cache.get(query_hash)
        if cached is not None:
            with self._lock:
                self._prediction_stats["cache_hits"] += 1
            return cached

        with self._lock:
            self._prediction_stats["cache_misses"] += 1
            self._prediction_stats["total_predictions"] += 1

        category = self._classify_query(query)
        pattern = self.QUERY_PATTERNS.get(category, self._get_default_pattern(category))

        sections = []
        for sec_data in pattern["sections"]:
            sections.append(SectionPrediction(
                section_name=sec_data["section_name"],
                section_type=sec_data["section_type"],
                estimated_tokens=sec_data["estimated_tokens"],
                order=sec_data["order"],
                content_hint=sec_data.get("content_hint", ""),
                confidence=pattern["confidence"],
            ))

        structure = PredictedStructure(
            skeleton=pattern["skeleton"],
            sections=sections,
            conclusion=pattern["conclusion"],
            total_estimated_tokens=pattern["total_estimated_tokens"],
            confidence=pattern["confidence"],
            prediction_mode=self._mode.value,
            query_hash=query_hash,
        )
        self.cache.put(query_hash, structure)
        return structure

    def predict_with_confidence(self, query: str) -> Tuple[PredictedStructure, float]:
        structure = self.predict(query)
        return structure, structure.confidence

    def get_prediction_stats(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._prediction_stats)

    def reset_stats(self):
        with self._lock:
            self._prediction_stats = {"total_predictions": 0, "cache_hits": 0, "cache_misses": 0}

    def _classify_query(self, query: str) -> str:
        query_lower = query.lower()
        checks = {
            "scientific": ["how does", "what is", "explain", "mechanism", "process", "原理", "科学"],
            "coding": ["write code", "implement", "function", "def ", "class ", "编程", "代码"],
            "comparison": ["compare", "vs", "versus", "difference", "which is better", "对比"],
            "definition": ["define", "what is", "meaning of", "术语", "定义"],
            "list": ["list", "enumerate", "steps to", "how to", "列出", "步骤"],
            "debug": ["debug", "fix error", "why doesn't", "error", "bug", "调试"],
        }
        for category, keywords in checks.items():
            for kw in keywords:
                if kw.lower() in query_lower:
                    return category
        return "definition" if len(query.split()) <= 10 else "scientific"

    def _get_default_pattern(self, category: str) -> Dict[str, Any]:
        return {
            "skeleton": f"Response covering {category}",
            "sections": [
                {"section_name": "introduction", "section_type": "introduction",
                 "estimated_tokens": 50, "order": 1, "content_hint": "Introduction"},
                {"section_name": "body", "section_type": "body",
                 "estimated_tokens": 150, "order": 2, "content_hint": "Main content"},
            ],
            "conclusion": "Conclusion",
            "total_estimated_tokens": 200,
            "confidence": 0.4,
        }

    @staticmethod
    def _compute_query_hash(query: str) -> str:
        return hashlib.sha256(query.strip().encode("utf-8")).hexdigest()[:16]


# ─────────────────────────────────────────────
# BackpressureController
# ─────────────────────────────────────────────

class BackpressureController:
    def __init__(self, config: Optional[POSConfig] = None,
                 broker: Optional[MessageBroker] = None):
        self.config = config or POSConfig()
        self.broker = broker or MessageBroker(max_depth=10000)
        self._window_size = self.config.backpressure_window_size
        self._high_watermark = self.config.high_watermark
        self._low_watermark = self.config.low_watermark
        self._backpressure_state = BackpressureState(
            window_size=self.config.backpressure_window_size,
            high_watermark=self.config.high_watermark,
            low_watermark=self.config.low_watermark,
        )
        self._sent_timestamps: deque = deque()
        self._chunk_sizes: deque = deque()
        self._lock = threading.RLock()
        self._total_sent = 0
        self._total_dropped = 0
        self._current_chunk_size = self.config.default_chunk_size
        self._rate_multiplier = 1.0
        self._callbacks: List[Callable[[Dict[str, Any]], None]] = []

    def record_chunk_sent(self, chunk_size: int, dropped: bool = False):
        now = time.time()
        with self._lock:
            if dropped:
                self._total_dropped += 1
            else:
                self._sent_timestamps.append(now)
                self._chunk_sizes.append(chunk_size)
                self._total_sent += chunk_size
                self._backpressure_state.record_accept()
                self._trim_window(now)

    def record_chunks_batched(self, count: int, total_tokens: int):
        now = time.time()
        with self._lock:
            for _ in range(count):
                self._sent_timestamps.append(now)
            self._chunk_sizes.append(total_tokens)
            self._total_sent += total_tokens
            self._backpressure_state.record_accept()
            self._trim_window(now)

    def get_current_rate(self) -> float:
        now = time.time()
        with self._lock:
            self._trim_window(now)
            if not self._sent_timestamps:
                return 0.0
            window_start = now - self._window_size
            recent_tokens = sum(
                s for t, s in zip(self._sent_timestamps, self._chunk_sizes)
                if t >= window_start
            )
            recent_count = sum(1 for t in self._sent_timestamps if t >= window_start)
            if recent_count == 0:
                return 0.0
            return recent_tokens / self._window_size

    def get_adjusted_rate(self) -> float:
        utilization = self.get_utilization()
        if utilization >= self._high_watermark:
            self._rate_multiplier = max(0.1, self._rate_multiplier * 0.7)
        elif utilization <= self._low_watermark:
            self._rate_multiplier = min(2.0, self._rate_multiplier * 1.2)
        else:
            if self._rate_multiplier > 1.0:
                self._rate_multiplier = max(1.0, self._rate_multiplier * 0.95)
            elif self._rate_multiplier < 1.0:
                self._rate_multiplier = min(1.0, self._rate_multiplier * 1.05)
        return self._rate_multiplier

    def get_utilization(self) -> float:
        now = time.time()
        with self._lock:
            self._trim_window(now)
            return len(self._sent_timestamps) / max(self._window_size, 1)

    def get_backpressure_state(self) -> Dict[str, Any]:
        now = time.time()
        with self._lock:
            self._trim_window(now)
            return {
                "utilization": round(self.get_utilization(), 3),
                "rate_multiplier": round(self._rate_multiplier, 3),
                "current_chunk_size": self._current_chunk_size,
                "total_sent": self._total_sent,
                "total_dropped": self._total_dropped,
                "window_size": self._window_size,
                "high_watermark": self._high_watermark,
                "low_watermark": self._low_watermark,
                "current_rate_per_sec": round(self.get_current_rate(), 1),
                "adjusted_rate_per_sec": round(self.get_current_rate() * self._rate_multiplier, 1),
                "broker_queue_depth": self.broker.depth,
            }

    def adjust_chunk_size(self) -> int:
        utilization = self.get_utilization()
        multiplier = self.get_adjusted_rate()
        base = self.config.default_chunk_size
        if utilization >= self._high_watermark:
            new_size = int(base * 0.3 * multiplier)
        elif utilization >= (self._high_watermark + self._low_watermark) / 2:
            new_size = int(base * 0.6 * multiplier)
        else:
            new_size = int(base * multiplier)
        self._current_chunk_size = max(
            self.config.min_chunk_size, min(self.config.max_chunk_size, new_size)
        )
        return self._current_chunk_size

    def register_callback(self, callback: Callable[[Dict[str, Any]], None]):
        self._callbacks.append(callback)

    def _trim_window(self, now: float):
        cutoff = now - self._window_size
        while self._sent_timestamps and self._sent_timestamps[0] < cutoff:
            self._sent_timestamps.popleft()
            if self._chunk_sizes:
                self._chunk_sizes.popleft()

    @property
    def total_sent(self) -> int:
        return self._total_sent

    @property
    def total_dropped(self) -> int:
        return self._total_dropped


# ─────────────────────────────────────────────
# ChunkManager
# ─────────────────────────────────────────────

class ChunkManager:
    def __init__(self, config: Optional[POSConfig] = None,
                 backpressure: Optional[BackpressureController] = None):
        self.config = config or POSConfig()
        self.backpressure = backpressure or BackpressureController(self.config)
        self._chunk_history: deque = deque(maxlen=1000)
        self._phase_chunk_sizes: Dict[Phase, int] = {
            Phase.SKELETON: self.config.default_chunk_size,
            Phase.DETAILS: self.config.default_chunk_size,
            Phase.CONCLUSION: self.config.default_chunk_size,
        }
        self._lock = threading.RLock()
        self._event_bus = EventBus()
        self._encoder = MessageEncoder()
        self._decoder = MessageDecoder()

    def get_chunk_size(self, phase: Phase,
                       custom_backpressure: Optional[Dict[str, Any]] = None) -> int:
        with self._lock:
            phase_multipliers = {Phase.SKELETON: 0.5, Phase.DETAILS: 1.0, Phase.CONCLUSION: 0.7}
            base_size = self.config.default_chunk_size
            multiplier = phase_multipliers.get(phase, 1.0)
            if custom_backpressure:
                utilization = custom_backpressure.get("utilization", 0.0)
                rate_mult = custom_backpressure.get("rate_multiplier", 1.0)
            else:
                state = self.backpressure.get_backpressure_state()
                utilization = state["utilization"]
                rate_mult = state["rate_multiplier"]
            if utilization >= self.config.high_watermark:
                size = int(base_size * multiplier * 0.3 * rate_mult)
            elif utilization >= (self.config.high_watermark + self.config.low_watermark) / 2:
                size = int(base_size * multiplier * 0.6 * rate_mult)
            else:
                size = int(base_size * multiplier * rate_mult)
            size = max(self.config.min_chunk_size, min(self.config.max_chunk_size, size))
            self._phase_chunk_sizes[phase] = size
            self._chunk_history.append({"phase": phase.value, "size": size, "timestamp": time.time()})
            return size

    def create_chunk(self, content: str, phase: Phase,
                      chunk_index: int = 0, total_in_phase: int = 1,
                      priority: Optional[int] = None,
                      metadata: Optional[Dict[str, Any]] = None) -> Chunk:
        if priority is None:
            priority_map = {Phase.SKELETON: ChunkPriority.SKELETON.value, Phase.DETAILS: ChunkPriority.DETAIL.value, Phase.CONCLUSION: ChunkPriority.CONCLUSION.value}
            priority = priority_map.get(phase, 1)
        encoded = self._encoder.encode(str(priority), {"content": content})
        chunk = Chunk(
            content=content, phase=phase, chunk_index=chunk_index,
            total_chunks_in_phase=total_in_phase, priority=priority,
            token_count=len(content),
            metadata={**{"encoded_tokens": encoded["tokens"]}, **(metadata or {})},
        )
        return chunk

    def split_content(self, content: str, phase: Phase) -> List[Chunk]:
        chunk_size = self.get_chunk_size(phase)
        chunks = []
        pos = 0
        chunk_index = 0
        while pos < len(content):
            end = pos + chunk_size
            if end < len(content):
                break_point = self._find_break_point(content, pos, end)
                chunk_text = content[pos:break_point].strip()
            else:
                chunk_text = content[pos:].strip()
            if chunk_text:
                chunk = self.create_chunk(content=chunk_text, phase=phase,
                                          chunk_index=chunk_index,
                                          total_in_phase=max(1, (len(content) // chunk_size) + 1))
                chunks.append(chunk)
            pos += len(chunk_text)
            chunk_index += 1
        return chunks

    def merge_chunks(self, chunks: List[Chunk]) -> List[Chunk]:
        if not chunks:
            return []
        merged = [chunks[0]]
        for chunk in chunks[1:]:
            last = merged[-1]
            if (last.phase == chunk.phase and last.priority == chunk.priority
                    and last.token_count + chunk.token_count <= self.config.max_chunk_size):
                merged_content = last.content + " " + chunk.content
                merged[-1] = Chunk(
                    content=merged_content, phase=last.phase,
                    chunk_index=last.chunk_index, total_chunks_in_phase=last.total_chunks_in_phase,
                    priority=last.priority,
                    token_count=last.token_count + chunk.token_count,
                    timestamp=last.timestamp,
                    metadata={**last.metadata, **chunk.metadata},
                )
            else:
                merged.append(chunk)
        return merged

    def get_phase_chunks(self, phase: Phase) -> int:
        return self._phase_chunk_sizes.get(phase, self.config.default_chunk_size)

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            history_list = list(self._chunk_history)
            avg_size = (sum(h["size"] for h in history_list) / len(history_list) if history_list else 0.0)
            return {
                "phase_chunk_sizes": {p.value: s for p, s in self._phase_chunk_sizes.items()},
                "history_size": len(history_list), "avg_chunk_size": round(avg_size, 1),
                "backpressure_state": self.backpressure.get_backpressure_state(),
            }

    def _find_break_point(self, content: str, start: int, end: int) -> int:
        if end >= len(content):
            return len(content)
        for i in range(end - 1, max(int(start + (end - start) * 0.8) - 1, start), -1):
            if content[i] in ' \n.。!！?？':
                return i + 1
        return end


# ─────────────────────────────────────────────
# ProgressiveRenderer
# ─────────────────────────────────────────────

class ProgressiveRenderer:
    def __init__(self, config: Optional[POSConfig] = None,
                 chunk_manager: Optional[ChunkManager] = None,
                 backpressure: Optional[BackpressureController] = None):
        self.config = config or POSConfig()
        self.chunk_manager = chunk_manager or ChunkManager(config, backpressure)
        self.backpressure = backpressure or self.chunk_manager.backpressure
        self._event_bus = EventBus()
        # SSEHandler referenced for SSE streaming support
        self._circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=10)
        self._lock = threading.RLock()
        self._rendering = False
        self._total_chunks_rendered = 0

    def register_callback(self, phase: str, callback: Callable[[Chunk], None]):
        valid_phases = ["skeleton", "details", "conclusion", "chunk", "phase_complete"]
        if phase not in valid_phases:
            raise ValueError(f"Unknown phase: {phase}. Must be one of {valid_phases}")
        with self._lock:
            if phase not in self._event_bus._callbacks:
                self._event_bus._callbacks[phase] = []
            from streaming import EventCallback
            self._event_bus._callbacks[phase].append(
                EventCallback(callback, event_types=[], priority=0)
            )

    def render(self, structure: PredictedStructure) -> Iterator[Chunk]:
        self._rendering = True
        self._total_chunks_rendered = 0
        try:
            yield from self._render_phase(structure, Phase.SKELETON, "skeleton", [structure.skeleton])
            detail_texts = self._get_detail_texts(structure)
            yield from self._render_phase(structure, Phase.DETAILS, "details", detail_texts)
            yield from self._render_phase(structure, Phase.CONCLUSION, "conclusion", [structure.conclusion])
        finally:
            self._rendering = False
            self._emit_phase_complete(Phase.CONCLUSION)

    def render_async(self, structure: PredictedStructure, callback: Callable[[Chunk], None]):
        for chunk in self.render(structure):
            callback(chunk)
            self._emit_chunk_event(chunk)

    def _render_phase(self, structure: PredictedStructure, phase: Phase,
                      phase_name: str, texts: List[str]) -> Iterator[Chunk]:
        delay = self._get_phase_delay(phase)
        if delay > 0:
            time.sleep(delay / 1000)
        chunk_size = self.chunk_manager.get_chunk_size(phase)
        for idx, text in enumerate(texts):
            if not self._rendering:
                break
            chunks = self.chunk_manager.split_content(text, phase)
            for chunk in chunks:
                if not self._rendering:
                    break
                if not self._check_backpressure():
                    continue
                self._total_chunks_rendered += 1
                self._emit_callback(phase_name, chunk)
                self._emit_chunk_event(chunk)
                yield chunk
                self.backpressure.record_chunk_sent(len(chunk.content))
                self.backpressure.adjust_chunk_size()
            self._emit_callback(f"{phase_name}_phase", Chunk(
                content=f"[PHASE_COMPLETE:{phase_name}]", phase=phase,
                chunk_index=idx + 1, total_chunks_in_phase=len(texts),
                priority=ChunkPriority.CONCLUSION.value if phase == Phase.CONCLUSION else ChunkPriority.SKELETON.value,
            ))

    def _get_detail_texts(self, structure: PredictedStructure) -> List[str]:
        texts = []
        for section in sorted(structure.sections, key=lambda s: s.order):
            hint = section.content_hint or f"Details about {section.section_name}"
            text = f"[{section.section_name.upper()}] {hint}. " * max(1, section.estimated_tokens // 10)
            texts.append(text.strip())
        return texts

    def _check_backpressure(self) -> bool:
        state = self.backpressure.get_backpressure_state()
        if state["utilization"] >= self.config.high_watermark:
            time.sleep(0.01)
            return False
        return True

    def _get_phase_delay(self, phase: Phase) -> int:
        delay_map = {Phase.SKELETON: self.config.skeleton_phase_delay_ms, Phase.DETAILS: self.config.detail_phase_delay_ms, Phase.CONCLUSION: self.config.conclusion_phase_delay_ms}
        return delay_map.get(phase, 0)

    def _emit_callback(self, phase: str, chunk: Chunk):
        with self._lock:
            for cb in self._event_bus._callbacks.get(phase, []):
                try:
                    cb.callback(chunk)
                    cb.call_count += 1
                    cb.last_called = time.time()
                except Exception:
                    pass

    def _emit_chunk_event(self, chunk: Chunk):
        with self._lock:
            for cb in self._event_bus._callbacks.get("chunk", []):
                try:
                    cb.callback(chunk)
                except Exception:
                    pass

    def _emit_phase_complete(self, phase: Phase):
        with self._lock:
            for cb in self._event_bus._callbacks.get("phase_complete", []):
                try:
                    cb.callback(Chunk(content=f"[PHASE_COMPLETE:{phase.value}]", phase=phase, chunk_index=0, total_chunks_in_phase=1, priority=0))
                except Exception:
                    pass

    def get_render_stats(self) -> Dict[str, Any]:
        return {
            "total_chunks_rendered": self._total_chunks_rendered,
            "currently_rendering": self._rendering,
            "chunk_manager_metrics": self.chunk_manager.get_metrics(),
            "backpressure_state": self.backpressure.get_backpressure_state(),
            "circuit_breaker_state": self._circuit_breaker.state_name,
        }

    def reset(self):
        self._rendering = False
        self._total_chunks_rendered = 0


# ─────────────────────────────────────────────
# Convenience Functions
# ─────────────────────────────────────────────

def create_pos_system(config: Optional[POSConfig] = None) -> Dict[str, Any]:
    cfg = config or POSConfig()
    backpressure = BackpressureController(cfg)
    chunk_manager = ChunkManager(cfg, backpressure)
    predictor = ResponsePredictor(cfg)
    renderer = ProgressiveRenderer(cfg, chunk_manager, backpressure)
    return {"predictor": predictor, "renderer": renderer, "chunk_manager": chunk_manager, "backpressure_controller": backpressure, "config": cfg}


def stream_response(query: str, config: Optional[POSConfig] = None) -> Iterator[Chunk]:
    system = create_pos_system(config)
    structure = system["predictor"].predict(query)
    yield from system["renderer"].render(structure)


# ─────────────────────────────────────────────
# Exceptions
# ─────────────────────────────────────────────

class POSPredictionError(Exception):
    pass


class POSRenderingError(Exception):
    pass


class POSBackpressureError(Exception):
    pass


__all__ = [
    "POSConfig", "SectionPrediction", "PredictedStructure", "Chunk", "ChunkMetrics",
    "Phase", "PredictionConfidence", "ChunkPriority", "PredictorMode",
    "ResponsePredictor", "ProgressiveRenderer", "ChunkManager", "BackpressureController",
    "PredictionCache", "POSPredictionError", "POSRenderingError", "POSBackpressureError",
    "create_pos_system", "stream_response",
]
