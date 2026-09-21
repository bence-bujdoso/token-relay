"""
TokenRelay v3 — Context-Aware Routing (CAR).

Integrates with existing v2 infrastructure:
- broker.MessageBroker — endpoint routing and message queuing
- codec.MessageEncoder/MessageDecoder — message formatting
- registry.TokenRegistry — endpoint registration and metadata
- circuit_breaker.CircuitBreaker — endpoint health monitoring

Provides:
- ComplexityAnalyzer: Query → complexity score (1-10)
- ModelSelector: Select best endpoint using MessageBroker
- PerformanceTracker: Per-endpoint stats via broker + circuit breaker
- RouteOptimizer: Historical adjustment using CircuitBreaker
- CARConfig: Configuration for all CAR components
"""

import time
import math
import statistics
from enum import Enum, auto
from dataclasses import dataclass, field, asdict
from typing import Optional, Dict, Any, List, Tuple
from collections import deque, defaultdict

from broker import MessageBroker, PRIORITY_CRITICAL, PRIORITY_HIGH, \
    PRIORITY_MEDIUM, PRIORITY_LOW
from codec import MessageEncoder, MessageDecoder
from registry import TokenRegistry, get_registry
from circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerError, \
    create_breaker


# ─────────────────────────────────────────────
# Enums & Constants
# ─────────────────────────────────────────────

class ComplexityLevel(Enum):
    """Complexity classification levels."""
    TRIVIAL = 1      # Simple keyword lookup
    SIMPLE = 2       # Short factual question
    MODERATE = 4     # Multi-step reasoning
    COMPLEX = 7      # Deep analysis required
    EXPERT = 10      # Expert-level reasoning


class UserPreference(Enum):
    """User's preferred trade-off axis."""
    SPEED = "speed"       # Prefer fastest response
    QUALITY = "quality"   # Prefer best quality output
    BALANCED = "balanced" # Default balanced approach


class EndpointStatus(Enum):
    """Current health status of an endpoint."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    OVERLOADED = "overloaded"
    DOWN = "down"


# ─────────────────────────────────────────────
# CARConfig
# ─────────────────────────────────────────────

@dataclass
class CARConfig:
    """Configuration for the Context-Aware Routing system.

    Attributes:
        max_complexity: Maximum complexity score (default 10).
        min_complexity: Minimum complexity score (default 1).
        default_endpoint: Fallback endpoint when none match.
        performance_window_size: Number of recent samples to track.
        degradation_latency_ms: Latency threshold (ms) for degraded status.
        overload_latency_ms: Latency threshold (ms) for overloaded status.
        success_rate_floor: Minimum success rate before endpoint is down.
        half_life_seconds: Decay rate for historical performance weighting.
    """
    max_complexity: int = 10
    min_complexity: int = 1
    default_endpoint: str = "default"
    performance_window_size: int = 100
    degradation_latency_ms: float = 500.0
    overload_latency_ms: float = 2000.0
    success_rate_floor: float = 0.5
    half_life_seconds: float = 300.0

    load_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "low": 0.3,
        "medium": 0.6,
        "high": 0.8,
    })


# ─────────────────────────────────────────────
# ComplexityAnalyzer
# ─────────────────────────────────────────────

class ComplexityAnalyzer:
    """Analyzes query text and produces a complexity score (1-10)."""

    def __init__(self, config: Optional[CARConfig] = None):
        self.config = config or CARConfig()
        self._tech_indicators = {
            "algorithm", "complexity", "recursion", "database", "encryption",
            "compression", "distributed", "concurrency", "optimization",
            "authentication", "authorization", "serialization", "threading",
            "benchmark", "latency", "throughput", "scalability",
        }
        self._multi_hop_indicators = {
            "compare", "contrast", "analyze", "evaluate", "discuss",
            "explain why", "how does", "what are the implications",
            "break down", "step by step", "derive", "prove",
        }

    def analyze(self, query: str) -> int:
        if not isinstance(query, str) or not query.strip():
            raise ValueError("Query must be a non-empty string")
        query_lower = query.strip().lower()
        score = 1
        word_count = len(query.split())
        if word_count > 50: score += 3
        elif word_count > 20: score += 2
        elif word_count > 10: score += 1
        tech_hits = sum(1 for term in self._tech_indicators if term in query_lower)
        score += min(tech_hits, 3)
        special_chars = sum(1 for c in query if c in '<>{[()]}=!&|;:,')
        code_indicators = sum(1 for kw in ["function", "def ", "class ", "import ", "return ", "if __name__"] if kw in query_lower)
        score += min(special_chars // 5 + code_indicators, 2)
        multi_hits = sum(1 for indicator in self._multi_hop_indicators if indicator in query_lower)
        score += min(multi_hits, 3)
        if query_lower.startswith("how") or query_lower.startswith("why"): score += 1
        if query_lower.startswith("what are") or "implications" in query_lower: score += 1
        structured_hints = sum(1 for hint in ["json", "schema", "format", "output as", "table"] if hint in query_lower)
        score += min(structured_hints, 1)
        return max(self.config.min_complexity, min(self.config.max_complexity, score))

    def classify(self, query: str) -> ComplexityLevel:
        score = self.analyze(query)
        if score <= 2: return ComplexityLevel.TRIVIAL
        elif score <= 4: return ComplexityLevel.SIMPLE
        elif score <= 6: return ComplexityLevel.MODERATE
        elif score <= 8: return ComplexityLevel.COMPLEX
        else: return ComplexityLevel.EXPERT

    def batch_analyze(self, queries: List[str]) -> List[Dict[str, Any]]:
        results = []
        for q in queries:
            try:
                score = self.analyze(q)
                classification = self.classify(q)
                results.append({"query": q, "score": score, "classification": classification.value})
            except ValueError:
                results.append({"query": q, "score": 0, "classification": "invalid"})
        return results


# ─────────────────────────────────────────────
# PerformanceTracker
# ─────────────────────────────────────────────

class PerformanceTracker:
    """Tracks per-endpoint performance statistics using v2 infrastructure."""

    def __init__(self, config: Optional[CARConfig] = None,
                 broker: Optional[MessageBroker] = None,
                 registry: Optional[TokenRegistry] = None):
        self.config = config or CARConfig()
        self.broker = broker
        self.registry = registry or get_registry()
        self._history: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=self.config.performance_window_size)
        )
        self._totals: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"success": 0, "failure": 0, "total": 0}
        )
        self._breakers: Dict[str, CircuitBreaker] = {}

    def _get_breaker(self, endpoint: str) -> CircuitBreaker:
        if endpoint not in self._breakers:
            self._breakers[endpoint] = create_breaker(
                name=f"car_{endpoint}", failure_threshold=5, recovery_timeout=30,
            )
        return self._breakers[endpoint]

    def record(self, endpoint: str, latency_ms: float, success: bool) -> None:
        self._history[endpoint].append((time.time(), latency_ms, success))
        self._totals[endpoint]["total"] += 1
        if success:
            self._totals[endpoint]["success"] += 1
        else:
            self._totals[endpoint]["failure"] += 1
        breaker = self._get_breaker(endpoint)
        if success:
            try: breaker.call(lambda: None)
            except Exception: pass
        else:
            try: breaker.call(lambda: (_ for _ in ()).throw(ValueError("endpoint failure")))
            except Exception: pass

    def get_stats(self, endpoint: str) -> Dict[str, Any]:
        history = self._history.get(endpoint)
        totals = self._totals.get(endpoint, {"success": 0, "failure": 0, "total": 0})
        if not history or totals["total"] == 0:
            return {
                "endpoint": endpoint, "avg_latency_ms": 0.0, "p95_latency_ms": 0.0,
                "p99_latency_ms": 0.0, "success_rate": 0.0, "total_requests": 0,
                "recent_samples": 0, "status": EndpointStatus.DOWN.value,
            }
        latencies = [h[1] for h in history]
        successes = sum(1 for h in history if h[2])
        total = len(history)
        avg_latency = statistics.mean(latencies)
        sorted_latencies = sorted(latencies)
        p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)] if len(sorted_latencies) > 1 else sorted_latencies[0]
        p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)] if len(sorted_latencies) > 1 else sorted_latencies[0]
        success_rate = successes / total if total > 0 else 0.0
        breaker = self._get_breaker(endpoint)
        cb_state = breaker.state
        if cb_state == CircuitState.OPEN:
            status = EndpointStatus.DOWN.value
        elif avg_latency > self.config.overload_latency_ms:
            status = EndpointStatus.OVERLOADED.value
        elif avg_latency > self.config.degradation_latency_ms:
            status = EndpointStatus.DEGRADED.value
        elif success_rate < self.config.success_rate_floor:
            status = EndpointStatus.DOWN.value
        else:
            status = EndpointStatus.HEALTHY.value
        return {
            "endpoint": endpoint, "avg_latency_ms": round(avg_latency, 2),
            "p95_latency_ms": round(p95, 2), "p99_latency_ms": round(p99, 2),
            "success_rate": round(success_rate, 4), "total_requests": totals["total"],
            "recent_samples": total, "status": status,
        }

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        return {ep: self.get_stats(ep) for ep in self._history}

    def get_best_endpoint(self, exclude: Optional[List[str]] = None) -> Optional[str]:
        exclude = exclude or []
        candidates = {}
        for ep in self._history:
            if ep in exclude: continue
            stats = self.get_stats(ep)
            if stats["total_requests"] > 0 and stats["status"] != EndpointStatus.DOWN.value:
                candidates[ep] = stats
        if not candidates: return None
        best = None; best_score = -1.0
        for ep, stats in candidates.items():
            score = stats["success_rate"] * 1000 / max(stats["avg_latency_ms"], 0.001)
            if score > best_score: best_score = score; best = ep
        return best

    def is_healthy(self, endpoint: str) -> bool:
        stats = self.get_stats(endpoint)
        return stats["status"] in (EndpointStatus.HEALTHY.value, EndpointStatus.DEGRADED.value)

    def get_success_rate(self, endpoint: str) -> float:
        return self.get_stats(endpoint).get("success_rate", 0.0)

    def get_avg_latency(self, endpoint: str) -> float:
        return self.get_stats(endpoint).get("avg_latency_ms", 0.0)

    def get_breaker_state(self, endpoint: str) -> CircuitState:
        return self._get_breaker(endpoint).state

    def reset(self, endpoint: Optional[str] = None) -> None:
        if endpoint:
            self._history.pop(endpoint, None)
            self._totals.pop(endpoint, None)
            self._breakers.pop(endpoint, None)
        else:
            self._history.clear()
            self._totals.clear()
            self._breakers.clear()


# ─────────────────────────────────────────────
# ModelSelector
# ─────────────────────────────────────────────

class ModelSelector:
    """Selects the optimal model endpoint using MessageBroker."""

    DEFAULT_PROFILES: Dict[str, Dict[str, Any]] = {
        "fast": {"latency_ms": 50.0, "quality_score": 0.5, "max_complexity": 3, "cost_per_token": 0.001, "broker_priority": PRIORITY_HIGH},
        "balanced": {"latency_ms": 150.0, "quality_score": 0.75, "max_complexity": 6, "cost_per_token": 0.005, "broker_priority": PRIORITY_MEDIUM},
        "quality": {"latency_ms": 500.0, "quality_score": 0.95, "max_complexity": 10, "cost_per_token": 0.01, "broker_priority": PRIORITY_LOW},
        "expert": {"latency_ms": 2000.0, "quality_score": 0.99, "max_complexity": 10, "cost_per_token": 0.05, "broker_priority": PRIORITY_LOW},
    }

    def __init__(self, config: Optional[CARConfig] = None,
                 broker: Optional[MessageBroker] = None,
                 registry: Optional[TokenRegistry] = None,
                 profiles: Optional[Dict[str, Dict[str, Any]]] = None):
        self.config = config or CARConfig()
        self.broker = broker or MessageBroker()
        self.registry = registry or get_registry()
        self.profiles = profiles if profiles is not None else dict(self.DEFAULT_PROFILES)
        self._encoder = MessageEncoder(self.registry)
        self._decoder = MessageDecoder(self.registry)

    def select(self, complexity: int, user_pref: UserPreference,
               load: float, performance_tracker: Optional[PerformanceTracker] = None,
               exclude: Optional[List[str]] = None) -> str:
        complexity = max(1, min(10, complexity))
        load = max(0.0, min(1.0, load))
        exclude = exclude or []
        candidates = {
            name: profile for name, profile in self.profiles.items()
            if profile["max_complexity"] >= complexity and name not in exclude
        }
        if not candidates:
            return self.config.default_endpoint
        best_endpoint = None; best_score = -float('inf')
        for name, profile in candidates.items():
            if performance_tracker:
                breaker_state = performance_tracker.get_breaker_state(name)
                if breaker_state == CircuitState.OPEN:
                    continue
            score = self._score_endpoint(name, profile, complexity, user_pref, load, performance_tracker)
            if score > best_score:
                best_score = score
                best_endpoint = name
        return best_endpoint or self.config.default_endpoint

    def _score_endpoint(self, name: str, profile: Dict[str, Any],
                          complexity: int, user_pref: UserPreference,
                          load: float,
                          performance_tracker: Optional[PerformanceTracker]) -> float:
        score = 0.0
        quality_match = profile["quality_score"]
        if user_pref == UserPreference.QUALITY:
            score += quality_match * 10
        elif user_pref == UserPreference.SPEED:
            score += (1 - quality_match) * 5
        else:
            score += quality_match * 5 + (1 - quality_match) * 5
        complexity_headroom = profile["max_complexity"] - complexity
        if complexity_headroom >= 0:
            score += complexity_headroom * 0.5
        else:
            score -= 5
        broker_prio = profile.get("broker_priority", PRIORITY_MEDIUM)
        score += (10 - broker_prio) * 0.5
        effective_latency = profile["latency_ms"] * (1 + load * 0.5)
        score += max(0, 500 - effective_latency) / 100
        if performance_tracker:
            stats = performance_tracker.get_stats(name)
            if stats["total_requests"] > 0:
                if stats["status"] == EndpointStatus.DOWN.value: score -= 100
                elif stats["status"] == EndpointStatus.OVERLOADED.value: score -= 50
                elif stats["status"] == EndpointStatus.DEGRADED.value: score -= 20
                score += stats["success_rate"] * 10
                score -= stats["avg_latency_ms"] / 100
        return score

    def route_via_broker(self, message: Dict[str, Any], endpoint: str) -> bool:
        priority = self.profiles.get(endpoint, {}).get("broker_priority", PRIORITY_MEDIUM)
        try:
            self.broker.enqueue(message, priority=priority)
            return True
        except Exception:
            return False

    def add_profile(self, name: str, profile: Dict[str, Any]) -> None:
        required = {"latency_ms", "quality_score", "max_complexity", "cost_per_token"}
        if not required.issubset(profile.keys()):
            raise ValueError(f"Profile must contain {required}")
        self.profiles[name] = profile

    def get_profiles(self) -> Dict[str, Dict[str, Any]]:
        return dict(self.profiles)

    def get_profile(self, name: str) -> Optional[Dict[str, Any]]:
        return self.profiles.get(name)


# ─────────────────────────────────────────────
# RouteOptimizer
# ─────────────────────────────────────────────

class RouteOptimizer:
    """Optimizes routing decisions using CircuitBreaker for health monitoring."""

    def __init__(self, config: Optional[CARConfig] = None):
        self.config = config or CARConfig()
        self._ewma_scores: Dict[str, float] = defaultdict(lambda: 50.0)
        self._adjustments: Dict[str, float] = defaultdict(lambda: 1.0)
        self._trends: Dict[str, deque] = defaultdict(lambda: deque(maxlen=20))
        self._breakers: Dict[str, CircuitBreaker] = {}

    def _get_breaker(self, endpoint: str) -> CircuitBreaker:
        if endpoint not in self._breakers:
            self._breakers[endpoint] = create_breaker(
                name=f"route_{endpoint}", failure_threshold=3, recovery_timeout=15,
            )
        return self._breakers[endpoint]

    def update(self, endpoint: str, latency_ms: float, success: bool) -> None:
        alpha = self._compute_alpha()
        latency_score = max(0, 100 - (latency_ms / self.config.overload_latency_ms) * 100)
        success_bonus = 20 if success else -30
        old_score = self._ewma_scores[endpoint]
        new_score = alpha * (latency_score + success_bonus) + (1 - alpha) * old_score
        self._ewma_scores[endpoint] = new_score
        self._trends[endpoint].append(1 if success else 0)
        breaker = self._get_breaker(endpoint)
        if success:
            try: breaker.call(lambda: None)
            except Exception: pass
        else:
            try: breaker.call(lambda: (_ for _ in ()).throw(ValueError("routing failure")))
            except Exception: pass
        self._adjustments[endpoint] = self._compute_adjustment(endpoint)

    def compute_weights(self, endpoints: List[str]) -> Dict[str, float]:
        if not endpoints: return {}
        raw_weights = {}
        for ep in endpoints:
            score = self._ewma_scores.get(ep, 50.0)
            adjustment = self._adjustments.get(ep, 1.0)
            breaker = self._breakers.get(ep)
            if breaker and breaker.state == CircuitState.OPEN:
                raw_weights[ep] = 0.001
            else:
                raw_weights[ep] = max(score * adjustment, 0.01)
        total = sum(raw_weights.values())
        if total <= 0:
            return {ep: 1.0 / len(endpoints) for ep in endpoints}
        weights = {ep: w / total for ep, w in raw_weights.items()}
        for ep in endpoints:
            trend = self._trends.get(ep)
            if trend and len(trend) >= 5:
                recent = list(trend)[-5:]
                recent_success = sum(recent) / len(recent)
                if recent_success < 0.5:
                    weights[ep] *= 0.5
        total = sum(weights.values())
        if total > 0:
            weights = {ep: w / total for ep, w in weights.items()}
        return weights

    def get_best_endpoint(self, endpoints: List[str]) -> Optional[str]:
        weights = self.compute_weights(endpoints)
        if not weights: return None
        return max(weights, key=weights.get)

    def _compute_alpha(self) -> float:
        return 0.3

    def _compute_adjustment(self, endpoint: str) -> float:
        trend = self._trends.get(endpoint)
        if not trend or len(trend) < 3: return 1.0
        success_rate = sum(trend) / len(trend)
        if success_rate >= 0.9: return 1.2
        elif success_rate >= 0.7: return 1.0
        elif success_rate >= 0.5: return 0.8
        else: return 0.4

    def get_recommendations(self, endpoints: List[str], n: int = 2) -> List[Dict[str, Any]]:
        weights = self.compute_weights(endpoints)
        recommendations = []
        for ep in endpoints:
            weight = weights.get(ep, 0.0)
            ewma = self._ewma_scores.get(ep, 50.0)
            breaker = self._breakers.get(ep)
            cb_state = breaker.state.name if breaker else "CLOSED"
            reason_map = {"CLOSED": "Healthy - routing normally", "HALF_OPEN": "Recovering - monitor closely", "OPEN": "Failed - avoid routing"}
            reason = reason_map.get(cb_state, "Good historical performance" if ewma > 60 else ("Needs monitoring" if ewma > 40 else "Poor performance - avoid"))
            recommendations.append({"endpoint": ep, "weight": round(weight, 4), "ewma_score": round(ewma, 2), "breaker_state": cb_state, "reason": reason})
        recommendations.sort(key=lambda x: x["weight"], reverse=True)
        return recommendations[:n]

    def reset(self) -> None:
        self._ewma_scores.clear()
        self._adjustments.clear()
        self._trends.clear()
        self._breakers.clear()


# ─────────────────────────────────────────────
# CARRouter
# ─────────────────────────────────────────────

class CARRouter:
    """High-level Context-Aware Routing orchestrator using v2 infrastructure."""

    def __init__(self, config: Optional[CARConfig] = None,
                 broker: Optional[MessageBroker] = None,
                 registry: Optional[TokenRegistry] = None):
        self.config = config or CARConfig()
        self.registry = registry or get_registry()
        self.broker = broker or MessageBroker()
        self._encoder = MessageEncoder(self.registry)
        self._decoder = MessageDecoder(self.registry)
        self.complexity_analyzer = ComplexityAnalyzer(self.config)
        self.model_selector = ModelSelector(self.config, broker=self.broker, registry=self.registry)
        self.performance_tracker = PerformanceTracker(self.config, broker=self.broker, registry=self.registry)
        self.route_optimizer = RouteOptimizer(self.config)
        self._routing_history: List[Dict[str, Any]] = []

    def route(self, query: str, user_pref: UserPreference = UserPreference.BALANCED,
              load: float = 0.5) -> Dict[str, Any]:
        complexity = self.complexity_analyzer.analyze(query)
        classification = self.complexity_analyzer.classify(query)
        endpoint = self.model_selector.select(
            complexity=complexity, user_pref=user_pref, load=load,
            performance_tracker=self.performance_tracker,
        )
        all_endpoints = list(self.model_selector.profiles.keys())
        recommendations = self.route_optimizer.get_recommendations(all_endpoints)
        message = self._encoder.encode(message_type="100", payload={"query": query, "complexity": complexity}, compress=True)
        routing_record = {
            "query": query, "complexity": complexity,
            "classification": classification.value, "user_pref": user_pref.value,
            "load": load, "endpoint": endpoint, "timestamp": time.time(),
        }
        self._routing_history.append(routing_record)
        return {
            "endpoint": endpoint, "complexity": complexity,
            "classification": classification.value, "reasoning": recommendations,
            "user_preference": user_pref.value, "system_load": load,
            "message": message,
        }

    def record_outcome(self, endpoint: str, latency_ms: float, success: bool) -> None:
        self.performance_tracker.record(endpoint, latency_ms, success)
        self.route_optimizer.update(endpoint, latency_ms, success)

    def get_routing_stats(self) -> Dict[str, Any]:
        return {
            "total_routes": len(self._routing_history),
            "performance_stats": self.performance_tracker.get_all_stats(),
            "optimizer_recommendations": self.route_optimizer.get_recommendations(
                list(self.model_selector.profiles.keys())
            ),
            "best_endpoint": self.performance_tracker.get_best_endpoint(),
        }

    def reset(self) -> None:
        self.complexity_analyzer = ComplexityAnalyzer(self.config)
        self.performance_tracker.reset()
        self.route_optimizer.reset()
        self._routing_history.clear()


# ─────────────────────────────────────────────
# Convenience functions
# ─────────────────────────────────────────────

def create_router(config: Optional[CARConfig] = None,
                  broker: Optional[MessageBroker] = None,
                  registry: Optional[TokenRegistry] = None) -> CARRouter:
    return CARRouter(config=config, broker=broker, registry=registry)


def route_query(query: str, user_pref: str = "balanced",
                load: float = 0.5) -> Dict[str, Any]:
    pref_map = {"speed": UserPreference.SPEED, "quality": UserPreference.QUALITY, "balanced": UserPreference.BALANCED}
    router = CARRouter()
    return router.route(query, pref_map.get(user_pref.lower(), UserPreference.BALANCED), load)
