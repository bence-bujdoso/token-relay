"""
TokenRelay v2 — Message Broker with Priority Queue, Deduplication, and DLQ.

Provides:
- MessageBroker: priority-queue-based message routing with dedup, ordering,
  depth monitoring, and dead letter queue support.
- Priority levels mapped from TokenRegistry routing_priority.
"""

import heapq
import time
import hashlib
import json
from typing import Optional, Dict, Any, List, Tuple
from collections import defaultdict, deque
from pathlib import Path

from registry import TokenRegistry, get_registry


# ─────────────────────────────────────────────
# States & Constants
# ─────────────────────────────────────────────

class QueueFullError(Exception):
    """Raised when the broker's queue depth limit is exceeded."""
    pass


class DuplicateMessageError(Exception):
    """Raised when a deduplicated message is rejected."""
    pass


class DLQError(Exception):
    """Raised when a message cannot be placed in the dead letter queue."""
    pass


# ─────────────────────────────────────────────
# Priority Levels
# ─────────────────────────────────────────────

PRIORITY_CRITICAL = 0
PRIORITY_HIGH = 1
PRIORITY_MEDIUM = 5
PRIORITY_LOW = 50
PRIORITY_BULK = 100


# ─────────────────────────────────────────────
# MessageBroker
# ─────────────────────────────────────────────

class MessageBroker:
    """A priority-queue message broker with deduplication, ordering,
    depth monitoring, and dead letter queue support.

    Features:
    - Priority queue (lower number = higher priority, uses heapq)
    - Message deduplication by content hash
    - FIFO ordering guaranteed within the same priority level
    - Queue depth monitoring with configurable max depth
    - Dead letter queue (DLQ) for messages that exceed retry limits

    Example:
        broker = MessageBroker(max_depth=1000)
        msg = {"tokens": [42], "payload": {"result": "ok"}}
        broker.enqueue(msg, priority=PRIORITY_HIGH)
        msg = broker.dequeue()
    """

    def __init__(self, registry: Optional[TokenRegistry] = None,
                 max_depth: int = 10_000,
                 dlq_max_depth: int = 10_000,
                 dedup_window_seconds: float = 300.0):
        """Initialize the MessageBroker.

        Args:
            registry: TokenRegistry instance for priority lookup.
                Defaults to global registry.
            max_depth: Maximum number of messages in the main queue.
            dlq_max_depth: Maximum number of messages in the DLQ.
            dedup_window_seconds: Time window (seconds) for dedup tracking.
        """
        self.registry = registry or get_registry()
        self.max_depth = max_depth
        self.dlq_max_depth = dlq_max_depth
        self.dedup_window_seconds = dedup_window_seconds

        # ── Priority queue: list of (priority, sequence, message) ──
        # heapq sorts by tuple order: priority first, then sequence for FIFO
        self._queue: List[Tuple[int, int, Dict[str, Any]]] = []
        self._sequence_counter = 0  # Monotonic counter for FIFO ordering

        # ── Dead letter queue ──
        self._dlq: List[Tuple[float, Dict[str, Any]]] = []  # (timestamp, message)

        # ── Deduplication tracking ──
        self._seen_hashes: Dict[str, float] = {}  # hash → timestamp

        # ── Depth monitoring ──
        self._depth_history: deque = deque(maxlen=100)  # Last 100 depth samples
        self._total_enqueued = 0
        self._total_dequeued = 0
        self._total_dropped = 0
        self._total_dlq = 0

        # ── Metrics ──
        self._metrics = {
            "messages_enqueued": 0,
            "messages_dequeued": 0,
            "messages_dropped": 0,
            "messages_dlq": 0,
            "duplicates_rejected": 0,
            "queue_peak_depth": 0,
        }

    # ── Core Queue Operations ──────────────────────────────────

    def enqueue(self, message: Dict[str, Any],
                priority: Optional[int] = None) -> bool:
        """Enqueue a message with an optional priority.

        If priority is not given, the registry's routing_priority is used
        based on the first token in the message.

        Args:
            message: Message dict with "tokens", "payload", etc.
            priority: Integer priority (lower = higher priority).
                If None, inferred from the message tokens via the registry.

        Returns:
            True if enqueued successfully.

        Raises:
            QueueFullError: If the queue is at max depth.
            DuplicateMessageError: If the message content is a duplicate
                within the dedup window.
        """
        # Check queue depth
        if len(self._queue) >= self.max_depth:
            self._metrics["messages_dropped"] += 1
            self._total_dropped += 1
            raise QueueFullError(
                f"Queue full ({len(self._queue)}/{self.max_depth}). "
                f"Consider increasing max_depth or checking DLQ."
            )

        # Deduplication check
        msg_hash = self._compute_hash(message)
        now = time.time()
        if msg_hash in self._seen_hashes:
            if now - self._seen_hashes[msg_hash] < self.dedup_window_seconds:
                self._metrics["duplicates_rejected"] += 1
                self._total_dropped += 1
                raise DuplicateMessageError(
                    f"Duplicate message rejected (hash: {msg_hash[:8]}...) "
                    f"within dedup window ({self.dedup_window_seconds}s)"
                )
        self._seen_hashes[msg_hash] = now

        # Determine priority
        if priority is None:
            priority = self._infer_priority(message)

        # Enqueue with monotonic sequence for FIFO within same priority
        self._sequence_counter += 1
        heapq.heappush(self._queue, (priority, self._sequence_counter, message))

        # Update metrics
        self._metrics["messages_enqueued"] += 1
        self._total_enqueued += 1
        self._depth_history.append(len(self._queue))
        if len(self._queue) > self._metrics["queue_peak_depth"]:
            self._metrics["queue_peak_depth"] = len(self._queue)

        return True

    def dequeue(self) -> Optional[Dict[str, Any]]:
        """Dequeue the highest-priority message.

        Returns:
            The message dict, or None if the queue is empty.
        """
        if not self._queue:
            return None

        priority, seq, message = heapq.heappop(self._queue)
        self._metrics["messages_dequeued"] += 1
        self._total_dequeued += 1
        return message

    def peek(self) -> Optional[Dict[str, Any]]:
        """Peek at the highest-priority message without removing it.

        Returns:
            The message dict, or None if the queue is empty.
        """
        if not self._queue:
            return None
        return self._queue[0][2]

    # ── Dead Letter Queue ──────────────────────────────────────

    def send_to_dlq(self, message: Dict[str, Any],
                    reason: str = "") -> bool:
        """Send a message to the dead letter queue.

        Args:
            message: The failed message to quarantine.
            reason: Description of why the message failed.

        Returns:
            True if placed in DLQ successfully.

        Raises:
            DLQError: If the DLQ is full.
        """
        if len(self._dlq) >= self.dlq_max_depth:
            raise DLQError(
                f"DLQ full ({len(self._dlq)}/{self.dlq_max_depth}). "
                f"Cannot quarantine message."
            )

        enriched = dict(message)
        enriched["_dlq_reason"] = reason
        enriched["_dlq_timestamp"] = time.time()

        self._dlq.append((time.time(), enriched))
        self._metrics["messages_dlq"] += 1
        self._total_dlq += 1
        return True

    def receive_from_dlq(self) -> Optional[Dict[str, Any]]:
        """Retrieve the oldest message from the DLQ.

        Returns:
            The DLQ message dict, or None if DLQ is empty.
        """
        if not self._dlq:
            return None
        timestamp, message = self._dlq.pop(0)
        return message

    def clear_dlq(self) -> int:
        """Clear the dead letter queue.

        Returns:
            The number of messages cleared.
        """
        count = len(self._dlq)
        self._dlq.clear()
        return count

    # ── Priority & Routing ─────────────────────────────────────

    def enqueue_with_priority(self, message: Dict[str, Any]) -> bool:
        """Enqueue using the registry's routing_priority for the message.

        Looks up the first token in the message to determine priority.
        Falls back to PRIORITY_MEDIUM if no token found.

        Args:
            message: Message dict with "tokens" key.

        Returns:
            True if enqueued.
        """
        priority = self._infer_priority(message)
        return self.enqueue(message, priority=priority)

    def _infer_priority(self, message: Dict[str, Any]) -> int:
        """Infer queue priority from the message tokens via the registry.

        Args:
            message: Message dict with "tokens" key.

        Returns:
            Integer priority (lower = higher priority).
        """
        tokens = message.get("tokens", [])
        if not tokens:
            return PRIORITY_MEDIUM

        first_token = tokens[0]
        try:
            priority = self.registry.get_routing_priority(first_token)
            return int(priority)
        except Exception:
            return PRIORITY_MEDIUM

    def route_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Get routing information for a message using the registry.

        Returns:
            Dict with action, priority, and routing hints.
        """
        tokens = message.get("tokens", [])
        return self.registry.route_message(tokens)

    # ── Queue Monitoring ───────────────────────────────────────

    @property
    def depth(self) -> int:
        """Current number of messages in the queue."""
        return len(self._queue)

    @property
    def dlq_depth(self) -> int:
        """Current number of messages in the dead letter queue."""
        return len(self._dlq)

    @property
    def is_empty(self) -> bool:
        """True if the main queue is empty."""
        return len(self._queue) == 0

    @property
    def is_full(self) -> bool:
        """True if the main queue has reached max_depth."""
        return len(self._queue) >= self.max_depth

    @property
    def depth_ratio(self) -> float:
        """Current queue depth as a ratio of max_depth (0.0 to 1.0+)."""
        return self.depth / self.max_depth if self.max_depth > 0 else 0.0

    def get_depth_history(self) -> List[int]:
        """Return the recorded depth samples (most recent last)."""
        return list(self._depth_history)

    def get_health(self) -> Dict[str, Any]:
        """Get queue health status.

        Returns:
            Dict with depth, capacity, ratio, and alert status.
        """
        ratio = self.depth_ratio
        if ratio >= 1.0:
            status = "critical"
        elif ratio >= 0.8:
            status = "warning"
        elif ratio >= 0.5:
            status = "moderate"
        else:
            status = "healthy"

        return {
            "status": status,
            "depth": self.depth,
            "max_depth": self.max_depth,
            "depth_ratio": round(ratio, 3),
            "dlq_depth": self.dlq_depth,
            "peak_depth": self._metrics["queue_peak_depth"],
        }

    # ── Deduplication ──────────────────────────────────────────

    def is_duplicate(self, message: Dict[str, Any]) -> bool:
        """Check if a message is a duplicate within the dedup window."""
        msg_hash = self._compute_hash(message)
        now = time.time()
        if msg_hash in self._seen_hashes:
            return now - self._seen_hashes[msg_hash] < self.dedup_window_seconds
        return False

    def clear_dedup_cache(self):
        """Clear the deduplication hash cache."""
        self._seen_hashes.clear()

    @staticmethod
    def _compute_hash(message: Dict[str, Any]) -> str:
        """Compute a content hash for deduplication.

        Uses a stable JSON representation to ensure consistent hashing.
        """
        try:
            content = json.dumps(message, sort_keys=True,
                                 separators=(",", ":")).encode("utf-8")
        except (TypeError, ValueError):
            content = str(message).encode("utf-8")
        return hashlib.sha256(content).hexdigest()

    # ── Batch Operations ───────────────────────────────────────

    def enqueue_batch(self, messages: List[Dict[str, Any]],
                      priority: Optional[int] = None) -> Dict[str, int]:
        """Enqueue multiple messages efficiently.

        Args:
            messages: List of message dicts.
            priority: Optional fixed priority for all messages.

        Returns:
            Dict with counts: enqueued, rejected, duplicates.
        """
        result = {"enqueued": 0, "rejected": 0, "duplicates": 0}
        for msg in messages:
            try:
                if self.is_duplicate(msg):
                    result["duplicates"] += 1
                    self._metrics["duplicates_rejected"] += 1
                    continue
                self.enqueue(msg, priority=priority)
                result["enqueued"] += 1
            except DuplicateMessageError:
                result["duplicates"] += 1
                result["rejected"] += 1
            except QueueFullError:
                result["rejected"] += 1
        return result

    def drain(self) -> List[Dict[str, Any]]:
        """Drain all messages from the queue in priority order.

        Returns:
            List of all messages sorted by priority (highest first).
        """
        messages = []
        while self._queue:
            _, _, msg = heapq.heappop(self._queue)
            messages.append(msg)
        return messages

    # ── Metrics ────────────────────────────────────────────────

    def get_metrics(self) -> Dict[str, Any]:
        """Return comprehensive broker metrics."""
        return {
            **self._metrics,
            "total_enqueued": self._total_enqueued,
            "total_dequeued": self._total_dequeued,
            "total_dropped": self._total_dropped,
            "total_dlq": self._total_dlq,
            "current_depth": self.depth,
            "dlq_depth": self.dlq_depth,
            "dedup_cache_size": len(self._seen_hashes),
            "pending_dedup_entries": len(
                [v for v in self._seen_hashes.values()
                 if time.time() - v < self.dedup_window_seconds]
            ),
        }

    def reset_metrics(self):
        """Reset all metrics counters and clear the queue."""
        self._metrics = {
            "messages_enqueued": 0,
            "messages_dequeued": 0,
            "messages_dropped": 0,
            "messages_dlq": 0,
            "duplicates_rejected": 0,
            "queue_peak_depth": 0,
        }
        self._total_enqueued = 0
        self._total_dequeued = 0
        self._total_dropped = 0
        self._total_dlq = 0
        self._sequence_counter = 0
        # Clear the queue and dedup cache
        self._queue.clear()
        self._seen_hashes.clear()


# ─────────────────────────────────────────────
# Convenience Functions
# ─────────────────────────────────────────────

def create_broker(registry: Optional[TokenRegistry] = None,
                  **kwargs) -> MessageBroker:
    """Factory function to create a MessageBroker."""
    return MessageBroker(registry=registry, **kwargs)


class PriorityMessage:
    """Wrapper for a message with an explicit priority and timestamp.

    Useful when the priority is known at construction time.
    """

    def __init__(self, message: Dict[str, Any], priority: int,
                 timestamp: Optional[float] = None):
        self.message = message
        self.priority = priority
        self.timestamp = timestamp or time.time()

    def to_tuple(self) -> Tuple[int, int, Dict[str, Any]]:
        """Convert to the internal queue tuple format."""
        return (self.priority, int(self.timestamp * 1_000_000), self.message)
