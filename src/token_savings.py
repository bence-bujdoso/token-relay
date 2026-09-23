"""TokenRelay v4 — Token Savings Calculator.

Compares token usage vs direct LLM call baseline. Tracks per-session and
cumulative statistics. Uses Python standard library only (stdlib).
"""
import json
import time
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class SavingsRecord:
    """A single savings measurement."""
    timestamp: float
    prompt_tokens: int
    relay_tokens: int
    direct_tokens: int
    savings_pct: float
    session_id: str = ""


class SavingsTracker:
    """Compares token usage vs direct LLM call baseline."""

    def __init__(self):
        self._records: deque = deque(maxlen=10000)
        self._session_data: Dict[str, List[SavingsRecord]] = defaultdict(list)
        self._cumulative = {
            "total_prompt_tokens": 0,
            "total_relay_tokens": 0,
            "total_direct_tokens": 0,
            "total_savings": 0,
            "total_calls": 0,
            "total_savings_pct": 0.0,
        }

    def calculate_savings(self, prompt_tokens: int, relay_tokens: int, direct_tokens: int) -> float:
        """Calculate percentage saved vs direct LLM call.

        savings_pct = (1 - relay_tokens / direct_tokens) * 100
        Returns 0.0 if direct_tokens is 0 to avoid division by zero.
        """
        if direct_tokens <= 0:
            return 0.0
        return round((1 - relay_tokens / direct_tokens) * 100, 2)

    def record(
        self,
        prompt_tokens: int,
        relay_tokens: int,
        direct_tokens: int,
        session_id: str = "",
    ) -> SavingsRecord:
        """Record a savings measurement and return the SavingsRecord."""
        savings_pct = self.calculate_savings(prompt_tokens, relay_tokens, direct_tokens)
        record = SavingsRecord(
            timestamp=time.time(),
            prompt_tokens=prompt_tokens,
            relay_tokens=relay_tokens,
            direct_tokens=direct_tokens,
            savings_pct=savings_pct,
            session_id=session_id,
        )
        self._records.append(record)
        self._session_data[session_id].append(record)

        # Update cumulative stats
        self._cumulative["total_prompt_tokens"] += prompt_tokens
        self._cumulative["total_relay_tokens"] += relay_tokens
        self._cumulative["total_direct_tokens"] += direct_tokens
        self._cumulative["total_savings"] += savings_pct
        self._cumulative["total_calls"] += 1
        if self._cumulative["total_calls"] > 0:
            self._cumulative["total_savings_pct"] = round(
                (1 - self._cumulative["total_relay_tokens"] / max(self._cumulative["total_direct_tokens"], 1)) * 100, 2
            )
        return record

    def get_session_stats(self, session_id: str) -> Dict[str, Any]:
        """Get per-session statistics."""
        records = self._session_data.get(session_id, [])
        if not records:
            return {"session_id": session_id, "calls": 0, "savings_pct": 0.0}
        total_prompt = sum(r.prompt_tokens for r in records)
        total_relay = sum(r.relay_tokens for r in records)
        total_direct = sum(r.direct_tokens for r in records)
        savings_pct = self.calculate_savings(total_prompt, total_relay, total_direct)
        return {
            "session_id": session_id,
            "calls": len(records),
            "total_prompt_tokens": total_prompt,
            "total_relay_tokens": total_relay,
            "total_direct_tokens": total_direct,
            "savings_pct": savings_pct,
            "avg_savings_pct": round(sum(r.savings_pct for r in records) / len(records), 2),
            "first_call": records[0].timestamp,
            "last_call": records[-1].timestamp,
        }

    def get_cumulative_stats(self) -> Dict[str, Any]:
        """Get cumulative statistics across all sessions."""
        return dict(self._cumulative)

    def get_all_records(self) -> List[SavingsRecord]:
        """Get all recorded savings."""
        return list(self._records)

    def get_historical_trend(self, max_points: int = 100) -> List[Dict[str, Any]]:
        """Get historical trend data for charting."""
        records = list(self._records)[-max_points:]
        trend = []
        for r in records:
            trend.append({
                "timestamp": r.timestamp,
                "datetime": datetime.fromtimestamp(r.timestamp).isoformat(),
                "prompt_tokens": r.prompt_tokens,
                "relay_tokens": r.relay_tokens,
                "direct_tokens": r.direct_tokens,
                "savings_pct": r.savings_pct,
            })
        return trend

    def get_comparison_summary(self) -> Dict[str, Any]:
        """Get summary comparing traditional vs relay."""
        if not self._records:
            return {"total_calls": 0}
        total_prompt = sum(r.prompt_tokens for r in self._records)
        total_relay = sum(r.relay_tokens for r in self._records)
        total_direct = sum(r.direct_tokens for r in self._records)
        return {
            "total_calls": len(self._records),
            "avg_savings_pct": round(sum(r.savings_pct for r in self._records) / len(self._records), 2),
            "total_prompt_tokens": total_prompt,
            "total_relay_tokens": total_relay,
            "total_direct_tokens": total_direct,
            "total_tokens_saved": total_direct - total_relay,
            "best_savings_pct": max(r.savings_pct for r in self._records),
            "worst_savings_pct": min(r.savings_pct for r in self._records),
        }

    def export_json(self) -> str:
        """Export all savings data as JSON."""
        data = {
            "cumulative": self.get_cumulative_stats(),
            "comparison": self.get_comparison_summary(),
            "historical_trend": self.get_historical_trend(),
            "sessions": {
                sid: self.get_session_stats(sid)
                for sid in self._session_data.keys()
            },
            "recent_records": [
                {
                    "timestamp": r.timestamp,
                    "prompt_tokens": r.prompt_tokens,
                    "relay_tokens": r.relay_tokens,
                    "direct_tokens": r.direct_tokens,
                    "savings_pct": r.savings_pct,
                    "session_id": r.session_id,
                }
                for r in list(self._records)[-50:]
            ],
        }
        return json.dumps(data, indent=2, default=str)

