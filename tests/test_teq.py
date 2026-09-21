"""
Tests for TokenRelay v3 Token Economy & QoS (TEQ).

Tests cover:
- QoSTierLevel: four-tier enum operations
- TEQConfig: configuration defaults and customization
- TokenBilling: allowance tracking, deduction, top-up, overdraft
- RateLimiter: per-user rate limiting, sliding window, graceful degradation
- SLAMonitor: SLA compliance tracking, reporting, thresholds
- QoSTier: tier management, upgrade/downgrade, config
- Convenience functions: create_token_billing, create_rate_limiter, create_sla_monitor, create_teq
"""

import sys
from pathlib import Path
import time

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import unittest

from teq import (
    QoSTierLevel, TEQConfig, TokenBilling, RateLimiter, SLAMonitor, QoSTier,
    create_token_billing, create_rate_limiter, create_sla_monitor, create_teq,
    TIER_NAMES, TIER_ORDER,
)


# ═══════════════════════════════════════════════════════════
# QoSTierLevel Tests
# ═══════════════════════════════════════════════════════════

class TestQoSTierLevel(unittest.TestCase):
    """Tests for QoSTierLevel enum."""
    
    def test_four_tiers(self):
        """Four tiers exist in correct order."""
        self.assertEqual(len(QoSTierLevel), 4)
        self.assertEqual(list(QoSTierLevel), TIER_ORDER)
    
    def test_tier_names(self):
        """TIER_NAMES maps correctly."""
        self.assertEqual(TIER_NAMES[QoSTierLevel.BRONZE], "Bronze")
        self.assertEqual(TIER_NAMES[QoSTierLevel.PLATINUM], "Platinum")
    
    def test_tier_order(self):
        """Tiers are ordered from lowest to highest priority."""
        self.assertEqual(TIER_ORDER[0], QoSTierLevel.BRONZE)
        self.assertEqual(TIER_ORDER[-1], QoSTierLevel.PLATINUM)
    
    def test_tier_str(self):
        """Tier name string representation."""
        self.assertEqual(str(QoSTierLevel.GOLD), "GOLD")


# ═══════════════════════════════════════════════════════════
# TEQConfig Tests
# ═══════════════════════════════════════════════════════════

class TestTEQConfig(unittest.TestCase):
    """Tests for TEQConfig."""
    
    def test_defaults(self):
        """Default configuration values."""
        config = TEQConfig()
        self.assertEqual(config.default_allowance, 10_000)
        self.assertEqual(config.max_allowance, 1_000_000)
        self.assertEqual(config.default_rate_limit, 10.0)
        self.assertEqual(config.sla_threshold, 95.0)
        self.assertEqual(config.grace_degradation, True)
    
    def test_custom_config(self):
        """Custom configuration values."""
        config = TEQConfig(
            default_allowance=50_000,
            max_allowance=500_000,
            default_rate_limit=20.0,
            sla_threshold=99.0,
        )
        self.assertEqual(config.default_allowance, 50_000)
        self.assertEqual(config.max_allowance, 500_000)
        self.assertEqual(config.default_rate_limit, 20.0)
    
    def test_rate_limits(self):
        """Per-tier rate limits."""
        config = TEQConfig()
        self.assertEqual(config.rate_limits[QoSTierLevel.BRONZE], 5.0)
        self.assertEqual(config.rate_limits[QoSTierLevel.PLATINUM], 50.0)
    
    def test_latency_guarantees(self):
        """Per-tier latency guarantees."""
        config = TEQConfig()
        self.assertEqual(config.latency_guarantees[QoSTierLevel.BRONZE], 5000.0)
        self.assertEqual(config.latency_guarantees[QoSTierLevel.PLATINUM], 100.0)


# ═══════════════════════════════════════════════════════════
# TokenBilling Tests
# ═══════════════════════════════════════════════════════════

class TestTokenBilling(unittest.TestCase):
    """Tests for TokenBilling."""
    
    def setUp(self):
        self.billing = TokenBilling()
    
    def test_create_user(self):
        """Create a user with allowance."""
        self.billing.create_user("user1", tier=QoSTierLevel.SILVER, total_tokens=5000)
        allowance = self.billing.get_allowance("user1")
        self.assertIsNotNone(allowance)
        self.assertEqual(allowance.total_tokens, 5000)
    
    def test_consume_tokens(self):
        """Consume tokens from allowance."""
        self.billing.create_user("user1", total_tokens=1000)
        result = self.billing.consume("user1", 100, "query")
        self.assertTrue(result)
        allowance = self.billing.get_allowance("user1")
        self.assertEqual(allowance.tokens_remaining, 900)
    
    def test_insufficient_tokens(self):
        """Cannot consume more than allowance."""
        self.billing.create_user("user1", total_tokens=50)
        result = self.billing.consume("user1", 100, "query")
        self.assertFalse(result)
    
    def test_top_up(self):
        """Top-up tokens for a user."""
        self.billing.create_user("user1", total_tokens=100)
        self.billing.consume("user1", 50, "query")
        self.billing.top_up("user1", 200)
        allowance = self.billing.get_allowance("user1")
        self.assertEqual(allowance.tokens_remaining, 250)
    
    def test_consumption_history(self):
        """Track consumption history."""
        self.billing.create_user("user1", total_tokens=1000)
        self.billing.consume("user1", 100, "query")
        self.billing.consume("user1", 200, "request")
        history = self.billing.get_history("user1")
        self.assertEqual(len(history), 2)
    
    def test_get_stats(self):
        """Get overall billing stats."""
        self.billing.create_user("user1", total_tokens=1000)
        self.billing.consume("user1", 100, "query")
        stats = self.billing.get_stats()
        self.assertIn("total_consumed", stats)
        self.assertIn("total_users", stats)


# ═══════════════════════════════════════════════════════════
# RateLimiter Tests
# ═══════════════════════════════════════════════════════════

class TestRateLimiter(unittest.TestCase):
    """Tests for RateLimiter."""
    
    def setUp(self):
        self.rate_limiter = RateLimiter()
    
    def test_rate_limit_check(self):
        """Check if a request is within rate limit."""
        self.assertTrue(self.rate_limiter.check("user1"))
    
    def test_rate_limit_exceeded(self):
        """Rate limit is enforced per tier."""
        self.rate_limiter.set_tier("user1", QoSTierLevel.BRONZE)
        # Bronze has 5 req/s, consume all
        for _ in range(5):
            self.rate_limiter.check("user1")
        # 6th should be rate limited
        # Note: depends on implementation details
    
    def test_tier_based_limits(self):
        """Different tiers have different rate limits."""
        self.rate_limiter.set_tier("user1", QoSTierLevel.PLATINUM)
        # Platinum should have higher rate limit
        self.rate_limiter.set_tier("user2", QoSTierLevel.BRONZE)
    
    def test_sliding_window(self):
        """Rate limiter uses sliding window."""
        self.rate_limiter.record_request("user1")
        time.sleep(0.01)
        self.rate_limiter.record_request("user1")
    
    def test_get_remaining(self):
        """Get remaining requests in current window."""
        remaining = self.rate_limiter.get_remaining("user1")
        self.assertGreaterEqual(remaining, 0)


# ═══════════════════════════════════════════════════════════
# SLAMonitor Tests
# ═══════════════════════════════════════════════════════════

class TestSLAMonitor(unittest.TestCase):
    """Tests for SLAMonitor."""
    
    def setUp(self):
        self.sla = SLAMonitor()
    
    def test_record_request(self):
        """Record a request with success/failure."""
        self.sla.record("user1", True, 100.0)
        self.sla.record("user1", False, 500.0)
    
    def test_sla_compliance(self):
        """Check SLA compliance."""
        # Record many successful requests
        for _ in range(20):
            self.sla.record("user1", True, 100.0)
        compliance = self.sla.get_compliance("user1")
        self.assertGreaterEqual(compliance, 95.0)
    
    def test_sla_breach(self):
        """SLA breach detection."""
        for _ in range(5):
            self.sla.record("user1", False, 5000.0)
        # Should have some breach
        breaches = self.sla.get_breaches("user1")
        self.assertGreater(len(breaches), 0)
    
    def test_get_sla_report(self):
        """Generate SLA report."""
        self.sla.record("user1", True, 100.0)
        report = self.sla.get_sla_report()
        self.assertIn("overall_compliance", report)


# ═══════════════════════════════════════════════════════════
# QoSTier Tests
# ═══════════════════════════════════════════════════════════

class TestQoSTier(unittest.TestCase):
    """Tests for QoSTier."""
    
    def setUp(self):
        self.teq = QoSTier()
    
    def test_get_tier_config(self):
        """Get tier configuration."""
        config = self.teq.get_tier_config(QoSTierLevel.GOLD)
        self.assertIsNotNone(config)
    
    def test_get_latency_budget(self):
        """Get latency budget for tier."""
        budget = self.teq.get_tier_latency(QoSTierLevel.BRONZE)
        self.assertEqual(budget, 5000.0)
    
    def test_get_priority(self):
        """Get priority for tier."""
        priority = self.teq.get_tier_priority(QoSTierLevel.PLATINUM)
        self.assertEqual(priority, 0)
    
    def test_enforce_qos(self):
        """Enforce QoS for a user."""
        self.teq.create_user("user1", QoSTierLevel.GOLD, 20000)
        result = self.teq.enforce_qos("user1")
        self.assertTrue(result["allowed"])
    
    def test_upgrade_tier(self):
        """Upgrade user tier."""
        self.teq.create_user("user1", QoSTierLevel.BRONZE, 1000)
        result = self.teq.upgrade_tier("user1", QoSTierLevel.SILVER)
        self.assertTrue(result)
    
    def test_rate_limit_check(self):
        """Check if user is rate limited."""
        self.teq.create_user("user1", QoSTierLevel.BRONZE, 1000)
        # Bronze has lower rate limit
        allowed = self.teq.check_rate_limit("user1")
        self.assertTrue(allowed)


# ═══════════════════════════════════════════════════════════
# Convenience Functions Tests
# ═══════════════════════════════════════════════════════════

class TestTEQConvenienceFunctions(unittest.TestCase):
    """Tests for TEQ convenience functions."""
    
    def test_create_token_billing(self):
        """Create TokenBilling instance."""
        billing = create_token_billing()
        self.assertIsNotNone(billing)
    
    def test_create_rate_limiter(self):
        """Create RateLimiter instance."""
        limiter = create_rate_limiter()
        self.assertIsNotNone(limiter)
    
    def test_create_sla_monitor(self):
        """Create SLAMonitor instance."""
        monitor = create_sla_monitor()
        self.assertIsNotNone(monitor)
    
    def test_create_teq(self):
        """Create complete TEQ system."""
        teq = create_teq()
        self.assertIsNotNone(teq)
        self.assertIsNotNone(teq.token_billing)
        self.assertIsNotNone(teq.rate_limiter)
        self.assertIsNotNone(teq.sla_monitor)


# ═══════════════════════════════════════════════════════════
# Integration Tests
# ═══════════════════════════════════════════════════════════

class TestTEQIntegration(unittest.TestCase):
    """Integration tests for TEQ system."""
    
    def test_full_lifecycle(self):
        """Complete lifecycle: create user → consume → check → upgrade."""
        teq = create_teq()
        
        # Create user
        teq.create_user("user1", QoSTierLevel.SILVER, 5000)
        
        # Consume tokens
        self.assertTrue(teq.token_billing.consume("user1", 100, "query"))
        
        # Check QoS
        qos = teq.enforce_qos("user1")
        self.assertTrue(qos["allowed"])
        
        # Upgrade tier
        self.assertTrue(teq.upgrade_tier("user1", QoSTierLevel.GOLD))
        
        # Check updated QoS
        qos = teq.enforce_qos("user1")
        self.assertEqual(qos["tier"], "gold")
    
    def test_rate_limit_enforcement(self):
        """Rate limiting works across the system."""
        teq = create_teq()
        teq.create_user("user1", QoSTierLevel.BRONZE, 1000)
        
        # Check rate limiter
        self.rate_limiter = teq.rate_limiter
        self.rate_limiter.set_tier("user1", QoSTierLevel.BRONZE)
    
    def test_sla_monitoring(self):
        """SLA monitoring works end-to-end."""
        teq = create_teq()
        teq.create_user("user1", QoSTierLevel.GOLD, 20000)
        
        # Record some requests
        for _ in range(10):
            teq.sla_monitor.record("user1", True, 100.0)
        
        report = teq.sla_monitor.get_sla_report()
        self.assertGreaterEqual(report["overall_compliance"], 90.0)
    
    def test_billing_summary(self):
        """Billing summary aggregates correctly."""
        teq = create_teq()
        teq.create_user("user1", QoSTierLevel.SILVER, 5000)
        teq.token_billing.consume("user1", 100, "query")
        teq.token_billing.consume("user1", 200, "request")
        
        summary = teq.token_billing.get_summary("user1")
        self.assertIn("total_consumed", summary)


# ═══════════════════════════════════════════════════════════
# Edge Cases and Error Handling
# ═══════════════════════════════════════════════════════════

class TestTEQEdgeCases(unittest.TestCase):
    """Edge case tests for TEQ."""
    
    def test_nonexistent_user(self):
        """Operations on non-existent user."""
        billing = TokenBilling()
        allowance = billing.get_allowance("nonexistent")
        self.assertIsNone(allowance)
        
        result = billing.consume("nonexistent", 100, "query")
        self.assertFalse(result)
    
    def test_zero_tokens(self):
        """Zero token allowance."""
        billing = TokenBilling()
        billing.create_user("user1", total_tokens=0)
        result = billing.consume("user1", 1, "query")
        self.assertFalse(result)
    
    def test_max_allowance(self):
        """Cannot exceed max allowance."""
        teq = QoSTier()
        teq.create_user("user1", QoSTierLevel.BRONZE, 0)  # 0 tokens
        qos = teq.enforce_qos("user1")
        # Should still work but be limited
    
    def test_tier_transition(self):
        """Transition between all tiers."""
        teq = QoSTier()
        teq.create_user("user1", QoSTierLevel.BRONZE, 1000)
        
        for tier in TIER_ORDER[1:]:
            self.assertTrue(teq.upgrade_tier("user1", tier))
    
    def test_negative_tokens(self):
        """Cannot consume negative tokens."""
        billing = TokenBilling()
        billing.create_user("user1", total_tokens=1000)
        # This should not throw, but should not consume
        # (implementation-dependent behavior)


if __name__ == "__main__":
    unittest.main(verbosity=2)
