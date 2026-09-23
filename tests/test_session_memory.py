"""TokenRelay v4 — Session Memory Tests.

Tests for src/session_memory.py:
- SessionManager: create, store, retrieve, delete sessions
- TTL-based expiry
- Session resumption
- SQLite persistence
"""
import sys
import json
import time
import tempfile
import os
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from session_memory import SessionManager, Session


# ═══════════════════════════════════════════
# SESSION MANAGER TESTS
# ═══════════════════════════════════════════

class TestSessionManager(unittest.TestCase):
    def setUp(self):
        # Use a temp database for each test
        self.db_fd, self.db_path = tempfile.mkstemp(suffix='.db')
        self.manager = SessionManager(db_path=self.db_path, default_ttl=86400)

    def tearDown(self):
        os.close(self.db_fd)
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)

    def test_init(self):
        self.assertIsInstance(self.manager, SessionManager)
        self.assertEqual(self.manager.default_ttl, 86400)

    def test_create_session(self):
        session = self.manager.create_session()
        self.assertIsInstance(session, Session)
        self.assertIsNotNone(session.id)
        sessions = self.manager.get_all_sessions()
        self.assertTrue(any(s["id"] == session.id for s in sessions))

    def test_create_session_with_ttl(self):
        session = self.manager.create_session(ttl=3600)
        self.assertEqual(session.expires_at - session.created_at, 3600)

    def test_create_session_with_metadata(self):
        session = self.manager.create_session(metadata={"purpose": "test", "user": "alice"})
        self.assertEqual(session.metadata["purpose"], "test")
        self.assertEqual(session.metadata["user"], "alice")

    def test_store_and_retrieve_message(self):
        session = self.manager.create_session()
        result = self.manager.store_message(session.id, "user", "Hello, world!")
        self.assertTrue(result)
        history = self.manager.retrieve_history(session.id)
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["role"], "user")
        self.assertEqual(history[0]["content"], "Hello, world!")

    def test_store_multiple_messages(self):
        session = self.manager.create_session()
        self.manager.store_message(session.id, "user", "First message")
        self.manager.store_message(session.id, "assistant", "Second message")
        history = self.manager.retrieve_history(session.id)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["role"], "user")
        self.assertEqual(history[1]["role"], "assistant")

    def test_retrieve_history_expired(self):
        """Expired sessions should return empty history."""
        manager = SessionManager(db_path=self.db_path, default_ttl=1)
        session = manager.create_session(ttl=1)
        manager.store_message(session.id, "user", "test")
        time.sleep(1.1)
        history = manager.retrieve_history(session.id)
        self.assertEqual(len(history), 0)

    def test_get_session(self):
        session = self.manager.create_session(metadata={"purpose": "test"})
        self.manager.store_message(session.id, "user", "Hello")
        retrieved = self.manager.get_session(session.id)
        self.assertIsInstance(retrieved, Session)
        self.assertEqual(retrieved.id, session.id)
        self.assertEqual(len(retrieved.messages), 1)

    def test_get_session_expired(self):
        manager = SessionManager(db_path=self.db_path, default_ttl=1)
        session = manager.create_session(ttl=1)
        time.sleep(1.1)
        retrieved = manager.get_session(session.id)
        self.assertIsNone(retrieved)

    def test_get_session_not_found(self):
        retrieved = self.manager.get_session("nonexistent-id")
        self.assertIsNone(retrieved)

    def test_delete_session(self):
        session = self.manager.create_session()
        self.manager.store_message(session.id, "user", "Hello")
        result = self.manager.delete_session(session.id)
        self.assertTrue(result)
        self.assertIsNone(self.manager.get_session(session.id))
        self.assertEqual(len(self.manager.get_all_sessions()), 0)

    def test_delete_session_not_found(self):
        result = self.manager.delete_session("nonexistent")
        self.assertFalse(result)

    def test_extend_ttl(self):
        session = self.manager.create_session()
        session_id = session.id
        result = self.manager.extend_ttl(session.id, additional_seconds=3600)
        self.assertTrue(result)
        updated = self.manager.get_session(session_id)
        self.assertIsNotNone(updated)

    def test_extend_ttl_expired(self):
        manager = SessionManager(db_path=self.db_path, default_ttl=1)
        session = manager.create_session(ttl=1)
        time.sleep(1.1)
        result = manager.extend_ttl(session.id)
        self.assertFalse(result)

    def test_extend_ttl_not_found(self):
        result = self.manager.extend_ttl("nonexistent")
        self.assertFalse(result)

    def test_resume_session(self):
        session = self.manager.create_session()
        self.manager.store_message(session.id, "user", "Hello")
        resumed = self.manager.resume_session(session.id)
        self.assertIsInstance(resumed, Session)
        self.assertEqual(len(resumed.messages), 1)

    def test_resume_session_expired(self):
        manager = SessionManager(db_path=self.db_path, default_ttl=1)
        session = manager.create_session(ttl=1)
        time.sleep(1.1)
        resumed = manager.resume_session(session.id)
        self.assertIsNone(resumed)

    def test_cleanup_expired(self):
        manager = SessionManager(db_path=self.db_path, default_ttl=1)
        session = manager.create_session(ttl=1)
        manager.store_message(session.id, "user", "test")
        time.sleep(1.1)
        count = manager.cleanup_expired()
        self.assertGreaterEqual(count, 1)
        self.assertEqual(len(manager.get_all_sessions()), 0)

    def test_get_all_sessions(self):
        for i in range(3):
            session = self.manager.create_session(metadata={"index": i})
            self.manager.store_message(session.id, "user", f"Message {i}")
        sessions = self.manager.get_all_sessions()
        self.assertEqual(len(sessions), 3)

    def test_get_stats(self):
        session = self.manager.create_session()
        self.manager.store_message(session.id, "user", "Hello")
        stats = self.manager.get_stats()
        self.assertEqual(stats["total_sessions"], 1)
        self.assertEqual(stats["active_sessions"], 1)
        self.assertEqual(stats["total_messages"], 1)
        self.assertGreater(stats["db_size_bytes"], 0)

    def test_default_ttl(self):
        """Test that default TTL is 24 hours (86400 seconds)."""
        self.assertEqual(self.manager.default_ttl, 86400)

    def test_session_persistence_across_instances(self):
        """Test that sessions persist when creating a new SessionManager with same db."""
        session = self.manager.create_session(metadata={"persist": True})
        session_id = session.id
        self.manager.store_message(session.id, "user", "Hello world")

        # Create a new manager with the same database
        manager2 = SessionManager(db_path=self.db_path, default_ttl=86400)
        retrieved = manager2.get_session(session_id)
        self.assertIsInstance(retrieved, Session)
        self.assertEqual(len(retrieved.messages), 1)
        self.assertEqual(retrieved.messages[0]["content"], "Hello world")

    def test_export_json(self):
        session = self.manager.create_session(metadata={"purpose": "test"})
        self.manager.store_message(session.id, "user", "Hello")
        json_str = self.manager.export_json()
        self.assertIsInstance(json_str, str)
        data = json.loads(json_str)
        self.assertIn("sessions", data)
        self.assertIn("stats", data)
        self.assertIn("export_timestamp", data)

    def test_session_id_unique(self):
        """Test that each session gets a unique ID."""
        ids = set()
        for _ in range(10):
            session = self.manager.create_session()
            self.assertNotIn(session.id, ids)
            ids.add(session.id)
        self.assertEqual(len(ids), 10)


# ═══════════════════════════════════════════
# RUN ALL TESTS
# ═══════════════════════════════════════════

if __name__ == "__main__":
    unittest.main()
