"""Tests for the Webhook System (src/webhook.py)."""
import sys
import os
import unittest
import json
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from webhook import WebhookManager, WebhookEvent, WebhookConfig, get_webhook_manager


class TestWebhookManager(unittest.TestCase):
    """Test WebhookManager functionality."""

    def setUp(self):
        self.manager = WebhookManager("/tmp/test_webhooks.db")

    def test_register_webhook(self):
        wid = self.manager.register_webhook(
            url="http://example.com/webhook",
            events=["task_complete", "task_failed"],
            secret="mysecret"
        )
        self.assertIsInstance(wid, str)
        self.assertGreater(len(wid), 0)

    def test_list_webhooks(self):
        self.manager.register_webhook("http://ex1.com", ["task_complete"], "s1")
        self.manager.register_webhook("http://ex2.com", ["task_failed"], "s2")
        webhooks = self.manager.list_webhooks()
        self.assertGreaterEqual(len(webhooks), 2)

    def test_get_webhook(self):
        wid = self.manager.register_webhook("http://example.com", ["task_complete"])
        wh = self.manager.get_webhook(wid)
        self.assertIsNotNone(wh)
        self.assertEqual(wh["url"], "http://example.com")

    def test_get_nonexistent_webhook(self):
        wh = self.manager.get_webhook("nonexistent")
        self.assertIsNone(wh)

    def test_delete_webhook(self):
        wid = self.manager.register_webhook("http://example.com", ["task_complete"])
        deleted = self.manager.delete_webhook(wid)
        self.assertTrue(deleted)
        wh = self.manager.get_webhook(wid)
        self.assertIsNone(wh)

    def test_delete_nonexistent_webhook(self):
        deleted = self.manager.delete_webhook("nonexistent")
        self.assertFalse(deleted)

    def test_toggle_webhook(self):
        wid = self.manager.register_webhook("http://example.com", ["task_complete"])
        toggled = self.manager.toggle_webhook(wid, False)
        self.assertTrue(toggled)
        wh = self.manager.get_webhook(wid)
        self.assertFalse(wh["active"])

    def test_toggle_webhook_active(self):
        wid = self.manager.register_webhook("http://example.com", ["task_complete"])
        self.manager.toggle_webhook(wid, False)
        toggled = self.manager.toggle_webhook(wid, True)
        self.assertTrue(toggled)
        wh = self.manager.get_webhook(wid)
        self.assertTrue(wh["active"])

    def test_deliver_webhook_not_found(self):
        result = self.manager.deliver_sync("nonexistent", {"event": "task_complete"})
        self.assertFalse(result["success"])
        self.assertIn("not found", result["error"])

    def test_deliver_webhook_inactive(self):
        wid = self.manager.register_webhook("http://example.com", ["task_complete"])
        self.manager.toggle_webhook(wid, False)
        result = self.manager.deliver_sync(wid, {"event": "task_complete"})
        self.assertFalse(result["success"])
        self.assertIn("inactive", result["error"])

    def test_deliver_event_not_subscribed(self):
        wid = self.manager.register_webhook("http://example.com", ["task_complete"])
        result = self.manager.deliver_sync(wid, {"event": "task_failed", "data": "test"})
        self.assertFalse(result["success"])
        self.assertIn("not subscribed", result["error"])

    def test_hmac_signature(self):
        payload = json.dumps({"event": "test"})
        sig = self.manager._compute_hmac(payload, "secret")
        self.assertIsInstance(sig, str)
        self.assertEqual(len(sig), 64)  # SHA256 hex

    def test_hmac_signature_consistency(self):
        payload = json.dumps({"event": "test"})
        sig1 = self.manager._compute_hmac(payload, "secret")
        sig2 = self.manager._compute_hmac(payload, "secret")
        self.assertEqual(sig1, sig2)

    def test_webhook_stats(self):
        self.manager.register_webhook("http://ex1.com", ["task_complete"], "s1")
        self.manager.register_webhook("http://ex2.com", ["task_failed"], "s2")
        stats = self.manager.get_stats()
        # The manager may have other webhooks from previous tests in the same DB
        self.assertGreaterEqual(stats["total_webhooks"], 2)
        self.assertGreaterEqual(stats["active"], 2)

    def test_register_webhook_optional_fields(self):
        wid = self.manager.register_webhook("http://example.com", ["task_complete"])
        wh = self.manager.get_webhook(wid)
        self.assertEqual(wh["secret"], "")
        self.assertTrue(wh["active"])

    def test_deliver_with_retries(self):
        """Test that deliver_sync attempts retries on failure."""
        wid = self.manager.register_webhook(
            "http://localhost:99999/unreachable",
            ["task_complete"],
            "secret"
        )
        result = self.manager.deliver_sync(wid, {"event": "task_complete"})
        self.assertFalse(result["success"])
        self.assertIn("attempts", result)
        self.assertEqual(result["attempts"], 3)


class TestWebhookEventEnum(unittest.TestCase):
    """Test WebhookEvent enum."""

    def test_enum_values(self):
        self.assertEqual(WebhookEvent.TASK_COMPLETE.value, "task_complete")
        self.assertEqual(WebhookEvent.TASK_FAILED.value, "task_failed")
        self.assertEqual(WebhookEvent.SWARM_CONSENSUS_REACHED.value, "swarm_consensus_reached")
        self.assertEqual(WebhookEvent.BENCHMARK_COMPLETE.value, "benchmark_complete")

    def test_enum_members(self):
        members = list(WebhookEvent)
        self.assertEqual(len(members), 4)


class TestWebhookConfig(unittest.TestCase):
    """Test WebhookConfig dataclass."""

    def test_create_config(self):
        config = WebhookConfig(url="http://example.com", events=["task_complete"])
        self.assertEqual(config.url, "http://example.com")
        self.assertEqual(config.events, ["task_complete"])
        self.assertTrue(config.active)
        self.assertNotEqual(config.webhook_id, "")


class TestWebhookManagerSingleton(unittest.TestCase):
    """Test get_webhook_manager singleton."""

    def test_singleton(self):
        m1 = get_webhook_manager("/tmp/test_webhook_singleton.db")
        m2 = get_webhook_manager("/tmp/test_webhook_singleton.db")
        self.assertIs(m1, m2)


if __name__ == '__main__':
    unittest.main()
