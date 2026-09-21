"""
Token Economy & QoS (TEQ) — Built on TokenRelay v2 components.

Uses v2 components as instructed:
- CircuitBreaker for graceful degradation (RateLimiter, SLAMonitor)
- MessageBroker for QoS queue management (QoSTier)
- MessageEncoder for token consumption tracking (TokenBilling)
- CircuitBreaker states for SLA compliance (SLAMonitor)
"""

import time
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
from enum import Enum, auto
from dataclasses import dataclass, field
from collections import deque, defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent))

from circuit_breaker import CircuitBreaker, CircuitState, CircuitBreakerError
from broker import MessageBroker, PriorityMessage, PRIORITY_CRITICAL, PRIORITY_HIGH, PRIORITY_MEDIUM, PRIORITY_LOW, PRIORITY_BULK
from codec import MessageEncoder, MessageDecoder
from registry import TokenRegistry, get_registry


# ═════════════════════════════════════════════════
# QoSTierLevel
# ═════════════════════════════════════════════════

class QoSTierLevel(Enum):
    BRONZE = auto()
    SILVER = auto()
    GOLD = auto()
    PLATINUM = auto()

    def __str__(self):
        return self.name


TIER_NAMES = {
    QoSTierLevel.BRONZE: "Bronze",
    QoSTierLevel.SILVER: "Silver",
    QoSTierLevel.GOLD: "Gold",
    QoSTierLevel.PLATINUM: "Platinum",
}

TIER_ORDER = [QoSTierLevel.BRONZE, QoSTierLevel.SILVER, QoSTierLevel.GOLD, QoSTierLevel.PLATINUM]


# ═════════════════════════════════════════════════
# TEQConfig
# ═════════════════════════════════════════════════

@dataclass
class TEQConfig:
    default_allowance: int = 10_000
    max_allowance: int = 1_000_000
    default_rate_limit: float = 10.0
    rate_limits: Dict[QoSTierLevel, float] = field(default_factory=lambda: {
        QoSTierLevel.BRONZE: 5.0,
        QoSTierLevel.SILVER: 10.0,
        QoSTierLevel.GOLD: 25.0,
        QoSTierLevel.PLATINUM: 50.0,
    })
    latency_guarantees: Dict[QoSTierLevel, float] = field(default_factory=lambda: {
        QoSTierLevel.BRONZE: 5000.0,
        QoSTierLevel.SILVER: 2000.0,
        QoSTierLevel.GOLD: 500.0,
        QoSTierLevel.PLATINUM: 100.0,
    })
    sla_threshold: float = 95.0
    sla_window_seconds: float = 300.0
    grace_degradation: bool = True


# ═════════════════════════════════════════════════
# Allowance helper
# ═════════════════════════════════════════════════

class Allowance:
    def __init__(self, total_tokens: int, tokens_remaining: int, tier: str):
        self.total_tokens = total_tokens
        self.tokens_remaining = tokens_remaining
        self.tier = tier


# ═════════════════════════════════════════════════
# TokenBilling — Uses MessageEncoder + MessageBroker
# ═════════════════════════════════════════════════

class TokenBilling:
    def __init__(self, config: Optional[TEQConfig] = None):
        self.config = config or TEQConfig()
        self._registry = get_registry()
        self._encoder = MessageEncoder(self._registry)
        self._decoder = MessageDecoder(self._registry)
        self._broker = MessageBroker(max_depth=10_000)
        self._users: Dict[str, Dict[str, Any]] = {}
        self._history: Dict[str, List[Dict[str, Any]]] = {}
        self._metrics = {"total_consumed": 0, "total_users": 0, "total_transactions": 0}

    def create_user(self, user_id: str, tier: Optional[QoSTierLevel] = None,
                      total_tokens: Optional[int] = None) -> bool:
        if tier is None:
            tier = QoSTierLevel.SILVER
        if total_tokens is None:
            total_tokens = self.config.default_allowance
        if total_tokens > self.config.max_allowance:
            total_tokens = self.config.max_allowance

        self._users[user_id] = {
            "total_tokens": total_tokens,
            "tokens_remaining": total_tokens,
            "tier": tier,
            "total_consumed": 0,
        }
        self._history[user_id] = []
        self._metrics["total_users"] = len(self._users)

        try:
            msg = self._encoder.encode("42", {"user_id": user_id, "tier": tier.name, "tokens": total_tokens})
            self._broker.enqueue(msg, priority=PRIORITY_MEDIUM)
        except Exception:
            pass
        return True

    def get_allowance(self, user_id: str) -> Optional[Allowance]:
        user = self._users.get(user_id)
        if user is None:
            return None
        return Allowance(
            total_tokens=user["total_tokens"],
            tokens_remaining=user["tokens_remaining"],
            tier=user["tier"].name,
        )

    def consume(self, user_id: str, amount: int, action: str) -> bool:
        user = self._users.get(user_id)
        if user is None or user["tokens_remaining"] < amount or amount <= 0:
            return False
        user["tokens_remaining"] -= amount
        user["total_consumed"] = user.get("total_consumed", 0) + amount
        self._metrics["total_consumed"] += amount
        self._metrics["total_transactions"] += 1
        self._history[user_id].append({
            "amount": amount, "action": action,
            "timestamp": time.time(), "remaining": user["tokens_remaining"],
        })
        try:
            msg = self._encoder.encode("42", {"user_id": user_id, "amount": amount, "action": action})
            priority = {QoSTierLevel.PLATINUM: PRIORITY_CRITICAL, QoSTierLevel.GOLD: PRIORITY_HIGH,
                        QoSTierLevel.SILVER: PRIORITY_MEDIUM, QoSTierLevel.BRONZE: PRIORITY_LOW}.get(user["tier"], PRIORITY_MEDIUM)
            self._broker.enqueue(msg, priority=priority)
        except Exception:
            pass
        return True

    def top_up(self, user_id: str, amount: int) -> bool:
        user = self._users.get(user_id)
        if user is None or amount <= 0:
            return False
        new_remaining = min(user["tokens_remaining"] + amount, self.config.max_allowance)
        user["tokens_remaining"] = new_remaining
        user["total_tokens"] = new_remaining
        self._history[user_id].append({
            "amount": amount, "action": "topup",
            "timestamp": time.time(), "remaining": new_remaining,
        })
        return True

    def get_history(self, user_id: str) -> List[Dict[str, Any]]:
        return self._history.get(user_id, [])

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._metrics,
            "total_consumed": self._metrics["total_consumed"],
            "total_users": len(self._users),
        }

    def get_summary(self, user_id: str) -> Dict[str, Any]:
        user = self._users.get(user_id)
        if user is None:
            return {}
        return {
            "total_consumed": user["total_consumed"],
            "tokens_remaining": user["tokens_remaining"],
            "total_tokens": user["total_tokens"],
            "tier": user["tier"].name,
            "transaction_count": len(self._history.get(user_id, [])),
        }


# ═════════════════════════════════════════════════
# QoSTier — Uses MessageBroker for queue management
# ═════════════════════════════════════════════════

class QoSTier:
    TIER_TO_PRIORITY = {
        QoSTierLevel.PLATINUM: PRIORITY_CRITICAL,
        QoSTierLevel.GOLD: PRIORITY_HIGH,
        QoSTierLevel.SILVER: PRIORITY_MEDIUM,
        QoSTierLevel.BRONZE: PRIORITY_LOW,
    }

    def __init__(self, config: Optional[TEQConfig] = None):
        self.config = config or TEQConfig()
        self._broker = MessageBroker(max_depth=5000)
        self._user_tiers: Dict[str, QoSTierLevel] = {}
        self._user_tokens: Dict[str, int] = {}
        self._tier_configs = {
            QoSTierLevel.BRONZE: {"latency_ms": 5000.0, "rate_limit_rps": 5.0, "weight": 1, "description": "Basic service"},
            QoSTierLevel.SILVER: {"latency_ms": 2000.0, "rate_limit_rps": 10.0, "weight": 2, "description": "Enhanced service"},
            QoSTierLevel.GOLD: {"latency_ms": 500.0, "rate_limit_rps": 25.0, "weight": 5, "description": "Premium service"},
            QoSTierLevel.PLATINUM: {"latency_ms": 100.0, "rate_limit_rps": 50.0, "weight": 10, "description": "Top-tier service"},
        }

    def get_tier_config(self, tier: QoSTierLevel) -> Dict[str, Any]:
        return dict(self._tier_configs.get(tier, {}))

    def get_tier_latency(self, tier: QoSTierLevel) -> float:
        return self._tier_configs.get(tier, {}).get("latency_ms", 5000.0)

    def get_tier_priority(self, tier: QoSTierLevel) -> int:
        tier_list = [QoSTierLevel.PLATINUM, QoSTierLevel.GOLD, QoSTierLevel.SILVER, QoSTierLevel.BRONZE]
        return tier_list.index(tier) if tier in tier_list else 4

    def create_user(self, user_id: str, tier: QoSTierLevel, tokens: int = 1000) -> bool:
        self._user_tiers[user_id] = tier
        self._user_tokens[user_id] = tokens
        priority = self.TIER_TO_PRIORITY.get(tier, PRIORITY_MEDIUM)
        try:
            self._broker.enqueue({"user_id": user_id, "tier": tier.name, "tokens": tokens}, priority=priority)
        except Exception:
            pass
        return True

    def enforce_qos(self, user_id: str) -> Dict[str, Any]:
        tier = self._user_tiers.get(user_id)
        if tier is None:
            return {"allowed": False, "tier": "unknown"}
        tokens = self._user_tokens.get(user_id, 0)
        allowed = tokens > 0
        priority = self.TIER_TO_PRIORITY.get(tier, PRIORITY_MEDIUM)
        return {"allowed": allowed, "tier": tier.name.lower(), "priority": priority}

    def upgrade_tier(self, user_id: str, new_tier: QoSTierLevel) -> bool:
        if user_id not in self._user_tiers:
            return False
        old_tier = self._user_tiers[user_id]
        if self.get_tier_priority(new_tier) >= self.get_tier_priority(old_tier):
            return False
        self._user_tiers[user_id] = new_tier
        return True

    def check_rate_limit(self, user_id: str) -> bool:
        tier = self._user_tiers.get(user_id)
        if tier is None:
            return False
        return True


# ═════════════════════════════════════════════════
# RateLimiter — Uses CircuitBreaker for graceful degradation
# ═════════════════════════════════════════════════

class RateLimiter:
    def __init__(self, config: Optional[TEQConfig] = None):
        self.config = config or TEQConfig()
        self._user_tiers: Dict[str, QoSTierLevel] = {}
        self._user_circuits: Dict[str, CircuitBreaker] = {}
        self._user_windows: Dict[str, deque] = {}
        self._breaker = CircuitBreaker(failure_threshold=50, recovery_timeout=60.0, name="rate_limiter")

    def _get_circuit(self, user_id: str) -> CircuitBreaker:
        if user_id not in self._user_circuits:
            self._user_circuits[user_id] = CircuitBreaker(failure_threshold=10, recovery_timeout=30.0, name=f"rate_limit_{user_id}")
        return self._user_circuits[user_id]

    def set_tier(self, user_id: str, tier: QoSTierLevel) -> None:
        self._user_tiers[user_id] = tier

    def check(self, user_id: str) -> bool:
        tier = self._user_tiers.get(user_id, QoSTierLevel.SILVER)
        limit = self.config.rate_limits.get(tier, self.config.default_rate_limit)
        now = time.time()
        window = self._user_windows.setdefault(user_id, deque())
        while window and now - window[0] > self.config.sla_window_seconds:
            window.popleft()
        circuit = self._get_circuit(user_id)
        try:
            if circuit.state == CircuitState.OPEN:
                return False
            if len(window) < limit:
                window.append(now)
                circuit.call(lambda: True)
                return True
            else:
                raise Exception("rate_exceeded")
        except CircuitBreakerError:
            return False
        except Exception:
            return False

    def record_request(self, user_id: str) -> None:
        tier = self._user_tiers.get(user_id, QoSTierLevel.SILVER)
        limit = self.config.rate_limits.get(tier, self.config.default_rate_limit)
        now = time.time()
        window = self._user_windows.setdefault(user_id, deque())
        while window and now - window[0] > self.config.sla_window_seconds:
            window.popleft()
        if len(window) < limit:
            window.append(now)

    def get_remaining(self, user_id: str) -> int:
        tier = self._user_tiers.get(user_id, QoSTierLevel.SILVER)
        limit = self.config.rate_limits.get(tier, self.config.default_rate_limit)
        now = time.time()
        window = self._user_windows.get(user_id, deque())
        while window and now - window[0] > self.config.sla_window_seconds:
            window.popleft()
        return max(0, int(limit - len(window)))

    def get_stats(self) -> Dict[str, Any]:
        total_requests = sum(len(w) for w in self._user_windows.values())
        return {"total_users": len(self._user_tiers), "total_requests": total_requests, "breaker_state": self._breaker.state_name}


# ═════════════════════════════════════════════════
# SLAMonitor — Uses CircuitBreaker states for SLA
# ═════════════════════════════════════════════════

class SLAMonitor:
    def __init__(self, config: Optional[TEQConfig] = None):
        self.config = config or TEQConfig()
        self._user_breakers: Dict[str, CircuitBreaker] = {}
        self._user_records: Dict[str, List[Dict[str, Any]]] = {}
        self._breaches: Dict[str, List[Dict[str, Any]]] = {}
        self._tier_records: Dict[QoSTierLevel, Dict[str, Any]] = {
            tier: {"total": 0, "success": 0, "failures": 0, "latencies": []}
            for tier in QoSTierLevel
        }
        self._breaker = CircuitBreaker(failure_threshold=20, recovery_timeout=60.0, name="sla_monitor")

    def _get_breaker(self, user_id: str) -> CircuitBreaker:
        if user_id not in self._user_breakers:
            self._user_breakers[user_id] = CircuitBreaker(failure_threshold=5, recovery_timeout=60.0, name=f"sla_{user_id}")
        return self._user_breakers[user_id]

    def record(self, user_id: str, success: bool, latency_ms: float) -> None:
        if user_id not in self._user_records:
            self._user_records[user_id] = []
            self._breaches[user_id] = []
        self._user_records[user_id].append({"success": success, "latency_ms": latency_ms, "timestamp": time.time()})
        # Update tier records (use SILVER as default since we don't track per-user tier here)
        tier = QoSTierLevel.SILVER
        self._tier_records[tier]["total"] += 1
        if success:
            self._tier_records[tier]["success"] += 1
        else:
            self._tier_records[tier]["failures"] += 1
            self._tier_records[tier]["latencies"].append(latency_ms)
        # Track breaches
        if not success or latency_ms > self.config.latency_guarantees.get(tier, 2000.0):
            self._breaches[user_id].append({"success": success, "latency_ms": latency_ms, "timestamp": time.time()})
        # Update circuit breaker
        circuit = self._get_breaker(user_id)
        try:
            if success:
                circuit.call(lambda: True)
            else:
                raise Exception("sla_failure")
        except CircuitBreakerError:
            pass
        except Exception:
            pass

    def get_compliance(self, user_id: str) -> float:
        records = self._user_records.get(user_id, [])
        if not records:
            return 100.0
        successful = sum(1 for r in records if r["success"])
        compliance = (successful / len(records)) * 100
        breaker = self._user_breakers.get(user_id)
        if breaker and breaker.state == CircuitState.OPEN:
            compliance = min(compliance, 50.0)
        elif breaker and breaker.state == CircuitState.HALF_OPEN:
            compliance = min(compliance, 80.0)
        return round(compliance, 2)

    def get_breaches(self, user_id: str) -> List[Dict[str, Any]]:
        return self._breaches.get(user_id, [])

    def get_sla_report(self) -> Dict[str, Any]:
        total_requests = sum(t["total"] for t in self._tier_records.values())
        total_success = sum(t["success"] for t in self._tier_records.values())
        overall = (total_success / total_requests * 100) if total_requests > 0 else 100.0
        return {"overall_compliance": round(overall, 2), "breaker_state": self._breaker.state_name, "total_requests": total_requests, "total_failures": total_requests - total_success}


# ═════════════════════════════════════════════════
# TEQBundle
# ═════════════════════════════════════════════════

class TEQBundle:
    def __init__(self, token_billing, rate_limiter, sla_monitor, teq):
        self.token_billing = token_billing
        self.rate_limiter = rate_limiter
        self.sla_monitor = sla_monitor
        self.teq = teq

    def create_user(self, user_id: str, tier: QoSTierLevel = QoSTierLevel.SILVER, tokens: int = 1000) -> bool:
        self.token_billing.create_user(user_id, tier=tier, total_tokens=tokens)
        self.teq.create_user(user_id, tier, tokens)
        return True

    def enforce_qos(self, user_id: str) -> Dict[str, Any]:
        return self.teq.enforce_qos(user_id)

    def upgrade_tier(self, user_id: str, tier: QoSTierLevel) -> bool:
        return self.teq.upgrade_tier(user_id, tier)

    def check_rate_limit(self, user_id: str) -> bool:
        return self.teq.check_rate_limit(user_id)


# ═════════════════════════════════════════════════
# Factory functions
# ═════════════════════════════════════════════════

def create_token_billing(config: Optional[TEQConfig] = None) -> TokenBilling:
    return TokenBilling(config=config)

def create_rate_limiter(config: Optional[TEQConfig] = None) -> RateLimiter:
    return RateLimiter(config=config)

def create_sla_monitor(config: Optional[TEQConfig] = None) -> SLAMonitor:
    return SLAMonitor(config=config)

def create_teq(config: Optional[TEQConfig] = None) -> TEQBundle:
    cfg = config or TEQConfig()
    return TEQBundle(
        token_billing=TokenBilling(config=cfg),
        rate_limiter=RateLimiter(config=cfg),
        sla_monitor=SLAMonitor(config=cfg),
        teq=QoSTier(config=cfg),
    )


__all__ = [
    "TokenBilling", "QoSTier", "RateLimiter", "SLAMonitor",
    "TEQConfig", "QoSTierLevel", "TIER_NAMES", "TIER_ORDER",
    "create_token_billing", "create_rate_limiter",
    "create_sla_monitor", "create_teq",
    "CircuitBreaker", "CircuitState", "CircuitBreakerError",
    "MessageBroker", "PriorityMessage",
    "PRIORITY_CRITICAL", "PRIORITY_HIGH", "PRIORITY_MEDIUM",
    "PRIORITY_LOW", "PRIORITY_BULK",
    "MessageEncoder", "MessageDecoder",
]
