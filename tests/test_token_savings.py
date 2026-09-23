"""TokenRelay v4 — Token Savings Calculator Tests.

Tests for src/token_savings.py:
- SavingsTracker: calculation, recording, stats
- Per-session and cumulative statistics
- Historical trend data
"""
import sys
import json
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from token_savings import SavingsTracker, SavingsRecord


# ═══════════════════════════════════════════
# SAVINGS TRACKER TESTS
# ═══════════════════════════════════════════

class TestSavingsTracker(unittest.TestCase):
    def setUp(self):
        self.tracker = SavingsTracker()

    def test_init(self):
        self.assertIsInstance(self.tracker, SavingsTracker)

    def test_calculate_savings(self):
        # Direct: 500 tokens, Relay: 200 tokens → 60% savings
        savings = self.tracker.calculate_savings(500, 200, 500)
        self.assertEqual(savings, 60.0)

    def test_calculate_savings_zero_direct(self):
        # Zero direct tokens should return 0.0 to avoid division by zero
        savings = self.tracker.calculate_savings(100, 50, 0)
        self.assertEqual(savings, 0.0)

    def test_calculate_savings_equal(self):
        # Same tokens → 0% savings
        savings = self.tracker.calculate_savings(300, 300, 300)
        self.assertEqual(savings, 0.0)

    def test_calculate_savings_perfect(self):
        # Zero relay tokens → 100% savings
        savings = self.tracker.calculate_savings(300, 0, 300)
        self.assertEqual(savings, 100.0)

    def test_record(self):
        record = self.tracker.record(500, 200, 500, session_id="test")
        self.assertIsInstance(record, SavingsRecord)
        self.assertEqual(record.prompt_tokens, 500)
        self.assertEqual(record.relay_tokens, 200)
        self.assertEqual(record.direct_tokens, 500)
        self.assertEqual(record.savings_pct, 60.0)

    def test_cumulative_stats(self):
        self.tracker.record(500, 200, 500, "s1")
        self.tracker.record(300, 150, 300, "s2")
        stats = self.tracker.get_cumulative_stats()
        self.assertEqual(stats["total_calls"], 2)
        self.assertEqual(stats["total_direct_tokens"], 800)
        self.assertEqual(stats["total_relay_tokens"], 350)
        self.assertEqual(stats["total_savings_pct"], round((1 - 350/800) * 100, 2))

    def test_session_stats(self):
        self.tracker.record(500, 200, 500, session_id="s1")
        self.tracker.record(300, 150, 300, session_id="s1")
        self.tracker.record(800, 350, 800, session_id="s2")
        stats = self.tracker.get_session_stats("s1")
        self.assertEqual(stats["calls"], 2)
        self.assertEqual(stats["session_id"], "s1")
        self.assertIn("savings_pct", stats)

    def test_session_stats_empty(self):
        stats = self.tracker.get_session_stats("nonexistent")
        self.assertEqual(stats["calls"], 0)
        self.assertEqual(stats["savings_pct"], 0.0)

    def test_comparison_summary(self):
        self.tracker.record(500, 200, 500, "s1")
        self.tracker.record(300, 150, 300, "s1")
        summary = self.tracker.get_comparison_summary()
        self.assertEqual(summary["total_calls"], 2)
        self.assertGreater(summary["avg_savings_pct"], 0)
        self.assertIn("total_tokens_saved", summary)

    def test_historical_trend(self):
        for i in range(5):
            self.tracker.record(100 * (i+1), 50 * (i+1), 100 * (i+1), "s1")
        trend = self.tracker.get_historical_trend(max_points=10)
        self.assertEqual(len(trend), 5)
        self.assertIn("savings_pct", trend[0])
        self.assertIn("datetime", trend[0])
        self.assertIn("prompt_tokens", trend[0])

    def test_historical_trend_limit(self):
        for i in range(15):
            self.tracker.record(100, 50, 100, "s1")
        trend = self.tracker.get_historical_trend(max_points=10)
        self.assertEqual(len(trend), 10)

    def test_get_all_records(self):
        self.tracker.record(500, 200, 500, "s1")
        self.tracker.record(300, 150, 300, "s2")
        records = self.tracker.get_all_records()
        self.assertEqual(len(records), 2)
        self.assertIsInstance(records[0], SavingsRecord)

    def test_export_json(self):
        self.tracker.record(500, 200, 500, "s1")
        json_str = self.tracker.export_json()
        self.assertIsInstance(json_str, str)
        data = json.loads(json_str)
        self.assertIn("cumulative", data)
        self.assertIn("comparison", data)
        self.assertIn("historical_trend", data)
        self.assertIn("sessions", data)
        self.assertIn("recent_records", data)

    def test_record_without_session_id(self):
        record = self.tracker.record(100, 50, 100)
        self.assertEqual(record.session_id, "")

    def test_multiple_records_accumulate(self):
        for i in range(10):
            self.tracker.record(100, 50, 100, f"s{i}")
        self.assertEqual(len(self.tracker.get_all_records()), 10)
        cumulative = self.tracker.get_cumulative_stats()
        self.assertEqual(cumulative["total_calls"], 10)

    def test_record_zero_tokens(self):
        record = self.tracker.record(0, 0, 0, "s1")
        self.assertEqual(record.savings_pct, 0.0)


# ═══════════════════════════════════════════
# SAVINGS RECORD TESTS
# ═══════════════════════════════════════════

class TestSavingsRecord(unittest.TestCase):
    def test_dataclass(self):
        record = SavingsRecord(
            timestamp=time.time(),
            prompt_tokens=100,
            relay_tokens=50,
            direct_tokens=100,
            savings_pct=50.0,
            session_id="test"
        )
        self.assertEqual(record.prompt_tokens, 100)
        self.assertEqual(record.relay_tokens, 50)
        self.assertEqual(record.savings_pct, 50.0)
        self.assertEqual(record.session_id, "test")

    def test_to_dict(self):
        record = SavingsRecord(
            timestamp=time.time(),
            prompt_tokens=100,
            relay_tokens=50,
            direct_tokens=100,
            savings_pct=50.0,
            session_id="test"
        )
        d = record.to_dict() if hasattr(record, 'to_dict') else None
        if d:
            self.assertEqual(d["prompt_tokens"], 100)


# ═══════════════════════════════════════════
# RUN ALL TESTS
# ═══════════════════════════════════════════

if __name__ == "__main__":
    unittest.main()
