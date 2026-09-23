"""TokenRelay v4 — Session Memory & Conversation History.

Persistent conversation storage using Python standard library sqlite3.
Provides TTL-based expiry and session resumption support.
"""
import json
import sqlite3
import time
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field, asdict
from pathlib import Path


@dataclass
class Session:
    """Represents a conversation session."""
    id: str
    created_at: float
    expires_at: float
    messages: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "messages": self.messages,
            "metadata": self.metadata,
        }


class SessionManager:
    """Persistent conversation storage using SQLite (stdlib)."""

    def __init__(self, db_path: str = None, default_ttl: int = 86400):
        """Initialize SessionManager.

        Args:
            db_path: Path to SQLite database file. Defaults to ~/.hermes/profiles/token-translator/sessions.db
            default_ttl: Default TTL in seconds (default 24h = 86400).
        """
        if db_path is None:
            db_path = str(Path.home() / ".hermes" / "profiles" / "token-translator" / "sessions.db")
        self.db_path = db_path
        self.default_ttl = int(default_ttl)
        self._lock = threading.RLock()  # Reentrant to allow nested _is_valid calls
        self._init_db()

    def _init_db(self):
        """Initialize the SQLite database schema."""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS sessions (
                        id TEXT PRIMARY KEY,
                        created_at REAL NOT NULL,
                        expires_at REAL NOT NULL,
                        metadata TEXT NOT NULL DEFAULT '{}',
                        message_count INTEGER NOT NULL DEFAULT 0
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        session_id TEXT NOT NULL,
                        role TEXT NOT NULL,
                        content TEXT NOT NULL,
                        timestamp REAL NOT NULL,
                        FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
                    )
                """)
                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_messages_session
                    ON messages(session_id, timestamp)
                """)
                conn.commit()
            finally:
                conn.close()

    def create_session(self, name: str = "", ttl: int = None, metadata: Dict[str, Any] = None) -> Session:
        """Create a new session.

        Args:
            ttl: Time-to-live in seconds. Defaults to self.default_ttl (24h).
            metadata: Optional metadata dict for the session.
        Returns:
            The newly created Session object.
        """
        if ttl is None:
            ttl = self.default_ttl or 86400
        ttl = int(ttl)
        if metadata is None:
            metadata = {}

        session_id = str(uuid.uuid4())
        now = time.time()
        expires_at = now + ttl

        session = Session(
            id=session_id,
            created_at=now,
            expires_at=expires_at,
            messages=[],
            metadata=metadata,
        )

        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(
                    "INSERT INTO sessions (id, created_at, expires_at, metadata) VALUES (?, ?, ?, ?)",
                    (session_id, now, expires_at, json.dumps(metadata))
                )
                conn.commit()
            finally:
                conn.close()

        return session

    @staticmethod
    def _session_id(session_id) -> str:
        """Extract string session ID from a Session object or string."""
        return session_id.id if hasattr(session_id, 'id') else session_id

    def store_message(self, session_id, role: str, content: str) -> bool:
        """Store a message in a session.

        Args:
            session_id: The session ID string or Session object.
            role: The role of the message (e.g., 'user', 'assistant', 'system').
            content: The message content.
        Returns:
            True if the message was stored, False if session is expired/not found.
        """
        sid = self._session_id(session_id)
        with self._lock:
            if not self._is_valid(sid):
                return False
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(
                    "INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, ?, ?, ?)",
                    (sid, role, content, time.time())
                )
                conn.execute(
                    "UPDATE sessions SET message_count = message_count + 1 WHERE id = ?",
                    (sid,)
                )
                conn.commit()
                return True
            finally:
                conn.close()

    def retrieve_history(self, session_id) -> List[Dict[str, Any]]:
        """Retrieve full conversation history for a session.

        Args:
            session_id: The session ID string or Session object.
        Returns:
            List of message dicts sorted by timestamp, or empty list if expired/not found.
        """
        sid = self._session_id(session_id)
        with self._lock:
            if not self._is_valid(sid):
                return []
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.execute(
                    "SELECT role, content, timestamp FROM messages WHERE session_id = ? ORDER BY timestamp ASC",
                    (sid,)
                )
                messages = []
                for role, content, timestamp in cursor.fetchall():
                    messages.append({
                        "role": role,
                        "content": content,
                        "timestamp": timestamp,
                    })
                return messages
            finally:
                conn.close()

    def get_session(self, session_id) -> Optional[Session]:
        """Get a Session object by ID, or None if expired/not found.

        Returns None if the session has expired (TTL-based).
        """
        sid = self._session_id(session_id)
        with self._lock:
            if not self._is_valid(sid):
                return None
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.execute(
                    "SELECT id, created_at, expires_at, metadata FROM sessions WHERE id = ?",
                    (sid,)
                )
                row = cursor.fetchone()
                if not row:
                    return None
                session = Session(
                    id=row[0],
                    created_at=row[1],
                    expires_at=row[2],
                    metadata=json.loads(row[3]),
                    messages=self.retrieve_history(sid),
                )
                return session
            finally:
                conn.close()

    def _is_valid(self, session_id) -> bool:
        """Check if a session exists and hasn't expired.

        Also auto-cleans expired sessions.
        """
        sid = self._session_id(session_id)
        now = time.time()
        conn = sqlite3.connect(self.db_path)
        try:
            cursor = conn.execute(
                "SELECT expires_at FROM sessions WHERE id = ?",
                (sid,)
            )
            row = cursor.fetchone()
            if row is None:
                return False
            if now > row[0]:
                conn.execute("DELETE FROM messages WHERE session_id = ?", (sid,))
                conn.execute("DELETE FROM sessions WHERE id = ?", (sid,))
                conn.commit()
                return False
            return True
        finally:
            conn.close()

    def extend_ttl(self, session_id, additional_seconds: int = None) -> bool:
        """Extend a session's TTL.

        Args:
            session_id: The session to extend (Session object or string).
            additional_seconds: Seconds to add. Defaults to self.default_ttl.
        Returns:
            True if extended, False if session not found/expired.
        """
        if additional_seconds is None:
            additional_seconds = self.default_ttl
        sid = self._session_id(session_id)

        with self._lock:
            now = time.time()
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.execute(
                    "SELECT expires_at FROM sessions WHERE id = ?",
                    (sid,)
                )
                row = cursor.fetchone()
                if row is None or now > row[0]:
                    return False
                new_expires = now + additional_seconds
                conn.execute(
                    "UPDATE sessions SET expires_at = ? WHERE id = ?",
                    (new_expires, sid)
                )
                conn.commit()
                return True
            finally:
                conn.close()

    def delete_session(self, session_id) -> bool:
        """Delete a session and all its messages.

        Returns True if the session existed and was deleted.
        """
        sid = self._session_id(session_id)
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.execute(
                    "DELETE FROM sessions WHERE id = ?",
                    (sid,)
                )
                conn.execute("DELETE FROM messages WHERE session_id = ?", (sid,))
                conn.commit()
                return cursor.rowcount > 0
            finally:
                conn.close()

    def get_all_sessions(self) -> List[Dict[str, Any]]:
        """Get all non-expired sessions with basic info."""
        return self.list_sessions()

    def list_sessions(self) -> List[Dict[str, Any]]:
        """Get all non-expired sessions with basic info."""
        now = time.time()
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.execute(
                    "SELECT id, created_at, expires_at, metadata, message_count FROM sessions WHERE expires_at > ? ORDER BY created_at DESC",
                    (now,)
                )
                sessions = []
                for row in cursor.fetchall():
                    sessions.append({
                        "id": row[0],
                        "created_at": row[1],
                        "expires_at": row[2],
                        "metadata": json.loads(row[3]),
                        "message_count": row[4],
                    })
                return sessions
            finally:
                conn.close()

    def resume_session(self, session_id) -> Optional[Session]:
        """Resume a session for continuation.

        Validates the session, updates activity timestamp, and returns
        the Session object with its message history. Returns None if
        the session is expired or doesn't exist.
        """
        sid = self._session_id(session_id)
        if not self._is_valid(sid):
            return None

        session = self.get_session(session_id)
        if session:
            # Extend TTL on resume
            self.extend_ttl(session_id)
        return session

    def cleanup_expired(self) -> int:
        """Remove all expired sessions and their messages.

        Returns the number of expired sessions removed.
        """
        now = time.time()
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                cursor = conn.execute(
                    "DELETE FROM messages WHERE session_id IN (SELECT id FROM sessions WHERE expires_at < ?)",
                    (now,)
                )
                count = conn.execute(
                    "DELETE FROM sessions WHERE expires_at < ?", (now,)
                ).rowcount
                conn.commit()
                return count
            finally:
                conn.close()

    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        now = time.time()
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            try:
                total_sessions = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
                active_sessions = conn.execute(
                    "SELECT COUNT(*) FROM sessions WHERE expires_at > ?", (now,)
                ).fetchone()[0]
                expired_sessions = total_sessions - active_sessions
                total_messages = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
                db_size = Path(self.db_path).stat().st_size if Path(self.db_path).exists() else 0
                return {
                    "total_sessions": total_sessions,
                    "active_sessions": active_sessions,
                    "expired_sessions": expired_sessions,
                    "total_messages": total_messages,
                    "db_size_bytes": db_size,
                    "default_ttl_seconds": self.default_ttl,
                }
            finally:
                conn.close()

    def export_json(self) -> str:
        """Export all session data as JSON."""
        sessions = []
        for session_info in self.get_all_sessions():
            session = self.get_session(session_info["id"])
            if session:
                sessions.append(session.to_dict())
        data = {
            "sessions": sessions,
            "stats": self.get_stats(),
            "export_timestamp": datetime.now().isoformat(),
        }
        return json.dumps(data, indent=2, default=str)



