"""TokenRelay v4 — Webhook System.

Provides WebhookManager, WebhookEvent enum, webhook delivery with HMAC
signatures, and retry with exponential backoff. Persists webhooks in SQLite.
"""
import enum
import hashlib
import hmac
import json
import sqlite3
import time
import uuid
import threading
import urllib.request
import urllib.error
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any, Callable
from datetime import datetime


class WebhookEvent(enum.Enum):
    """Webhook event types."""
    TASK_COMPLETE = "task_complete"
    TASK_FAILED = "task_failed"
    SWARM_CONSENSUS_REACHED = "swarm_consensus_reached"
    BENCHMARK_COMPLETE = "benchmark_complete"


@dataclass
class WebhookConfig:
    """Configuration for a webhook endpoint."""
    url: str
    events: List[str] = field(default_factory=list)
    secret: str = ""
    active: bool = True
    created_at: float = field(default_factory=time.time)
    webhook_id: str = ""

    def __post_init__(self):
        if not self.webhook_id:
            self.webhook_id = str(uuid.uuid4())


class WebhookManager:
    """Manage webhooks: register, list, delete, and deliver events.

    Persists webhooks in SQLite (Python stdlib sqlite3).
    Delivers async HTTP POST with HMAC signature (sha256).
    Retries with exponential backoff (3 retries, 1s/2s/4s delays).
    """

    def __init__(self, db_path: str = "/tmp/tokenrelay_webhooks.db"):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        """Initialize the SQLite database with the webhooks table."""
        with self._lock:
            conn = sqlite3.connect(self._db_path)
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS webhooks (
                        webhook_id TEXT PRIMARY KEY,
                        url TEXT NOT NULL,
                        events TEXT NOT NULL,
                        secret TEXT NOT NULL DEFAULT '',
                        active INTEGER NOT NULL DEFAULT 1,
                        created_at REAL NOT NULL
                    )
                """)
                conn.commit()
            finally:
                conn.close()

    def _conn(self):
        """Get a new SQLite connection."""
        return sqlite3.connect(self._db_path)

    def register_webhook(
        self,
        url: str,
        events: List[str],
        secret: str = "",
        webhook_id: Optional[str] = None,
    ) -> str:
        """Register a new webhook. Returns the webhook ID."""
        wid = webhook_id or str(uuid.uuid4())
        events_json = json.dumps(events) if isinstance(events, list) else str(events)
        with self._lock:
            conn = self._conn()
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO webhooks (webhook_id, url, events, secret, active, created_at) VALUES (?, ?, ?, ?, ?, ?)",
                    (wid, url, events_json, secret, 1, time.time())
                )
                conn.commit()
            finally:
                conn.close()
        return wid

    def list_webhooks(self) -> List[Dict[str, Any]]:
        """List all webhooks."""
        with self._lock:
            conn = self._conn()
            try:
                rows = conn.execute("SELECT * FROM webhooks ORDER BY created_at DESC").fetchall()
                return [
                    {
                        "webhook_id": r[0],
                        "url": r[1],
                        "events": json.loads(r[2]) if isinstance(r[2], str) and r[2].startswith("[") else [r[2]],
                        "secret": r[3],
                        "active": bool(r[4]),
                        "created_at": r[5],
                    }
                    for r in rows
                ]
            finally:
                conn.close()

    def get_webhook(self, webhook_id: str) -> Optional[Dict[str, Any]]:
        """Get a single webhook by ID."""
        with self._lock:
            conn = self._conn()
            try:
                row = conn.execute("SELECT * FROM webhooks WHERE webhook_id = ?", (webhook_id,)).fetchone()
                if not row:
                    return None
                return {
                    "webhook_id": row[0],
                    "url": row[1],
                    "events": json.loads(row[2]) if isinstance(row[2], str) and row[2].startswith("[") else [row[2]],
                    "secret": row[3],
                    "active": bool(row[4]),
                    "created_at": row[5],
                }
            finally:
                conn.close()

    def delete_webhook(self, webhook_id: str) -> bool:
        """Delete a webhook by ID. Returns True if found and deleted."""
        with self._lock:
            conn = self._conn()
            try:
                cursor = conn.execute("DELETE FROM webhooks WHERE webhook_id = ?", (webhook_id,))
                conn.commit()
                return cursor.rowcount > 0
            finally:
                conn.close()

    def toggle_webhook(self, webhook_id: str, active: bool) -> bool:
        """Toggle webhook active state."""
        with self._lock:
            conn = self._conn()
            try:
                conn.execute("UPDATE webhooks SET active = ? WHERE webhook_id = ?", (1 if active else 0, webhook_id))
                conn.commit()
                return True
            finally:
                conn.close()

    def _compute_hmac(self, payload: str, secret: str) -> str:
        """Compute HMAC-SHA256 signature for a payload."""
        sig = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
        return sig

    async def deliver(self, webhook_id: str, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Deliver an event to a webhook endpoint via async HTTP POST with HMAC.

        Retries with exponential backoff (3 retries, 1s/2s/4s delays).
        Returns delivery result dict.
        """
        webhook = self.get_webhook(webhook_id)
        if not webhook:
            return {"success": False, "error": f"Webhook {webhook_id} not found"}
        if not webhook["active"]:
            return {"success": False, "error": f"Webhook {webhook_id} is inactive"}

        url = webhook["url"]
        secret = webhook["secret"]
        event_type = event_data.get("event", "unknown")

        # Check if event type matches webhook subscriptions
        if event_type not in webhook["events"] and "*" not in webhook["events"]:
            return {"success": False, "error": f"Event {event_type} not subscribed by webhook"}

        payload = json.dumps(event_data)
        signature = self._compute_hmac(payload, secret)
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": f"sha256={signature}",
            "X-Webhook-Event": event_type,
            "X-Webhook-Id": webhook_id,
        }

        retries = 3
        delays = [1, 2, 4]
        last_error = None

        for attempt in range(retries):
            try:
                req = urllib.request.Request(url, data=payload.encode(), headers=headers, method="POST")
                resp = urllib.request.urlopen(req, timeout=30)
                body = resp.read().decode()
                resp.close()
                return {
                    "success": True,
                    "webhook_id": webhook_id,
                    "event": event_type,
                    "status_code": resp.status if hasattr(resp, 'status') else 200,
                    "attempt": attempt + 1,
                    "response": body,
                }
            except urllib.error.URLError as e:
                last_error = str(e)
                if attempt < retries - 1:
                    time.sleep(delays[attempt])
            except Exception as e:
                last_error = str(e)
                if attempt < retries - 1:
                    time.sleep(delays[attempt])

        return {
            "success": False,
            "webhook_id": webhook_id,
            "event": event_type,
            "error": last_error,
            "attempts": retries,
        }

    def deliver_sync(self, webhook_id: str, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Synchronous version of deliver for use in the server handler."""
        webhook = self.get_webhook(webhook_id)
        if not webhook:
            return {"success": False, "error": f"Webhook {webhook_id} not found"}
        if not webhook["active"]:
            return {"success": False, "error": f"Webhook {webhook_id} is inactive"}

        url = webhook["url"]
        secret = webhook["secret"]
        event_type = event_data.get("event", "unknown")

        if event_type not in webhook["events"] and "*" not in webhook["events"]:
            return {"success": False, "error": f"Event {event_type} not subscribed"}

        payload = json.dumps(event_data)
        signature = self._compute_hmac(payload, secret)
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Signature": f"sha256={signature}",
            "X-Webhook-Event": event_type,
            "X-Webhook-Id": webhook_id,
        }

        retries = 3
        delays = [1, 2, 4]
        last_error = None
        for attempt in range(retries):
            try:
                req = urllib.request.Request(url, data=payload.encode(), headers=headers, method="POST")
                resp = urllib.request.urlopen(req, timeout=30)
                body = resp.read().decode()
                resp.close()
                return {
                    "success": True,
                    "webhook_id": webhook_id,
                    "event": event_type,
                    "status_code": 200,
                    "attempt": attempt + 1,
                    "response": body,
                }
            except Exception as e:
                last_error = str(e)
                if attempt < retries - 1:
                    time.sleep(delays[attempt])

        return {
            "success": False,
            "webhook_id": webhook_id,
            "event": event_type,
            "error": last_error,
            "attempts": retries,
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get webhook statistics."""
        with self._lock:
            conn = self._conn()
            try:
                total = conn.execute("SELECT COUNT(*) FROM webhooks").fetchone()[0]
                active = conn.execute("SELECT COUNT(*) FROM webhooks WHERE active = 1").fetchone()[0]
                inactive = total - active
                return {"total_webhooks": total, "active": active, "inactive": inactive}
            finally:
                conn.close()


# Module-level singleton for convenience
_default_manager: Optional[WebhookManager] = None

def get_webhook_manager(db_path: str = "/tmp/tokenrelay_webhooks.db") -> WebhookManager:
    """Get or create the default webhook manager singleton."""
    global _default_manager
    if _default_manager is None:
        _default_manager = WebhookManager(db_path)
    return _default_manager
