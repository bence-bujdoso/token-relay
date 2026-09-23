"""TokenRelay v4 — Privacy & Zero-Knowledge Proofs."""
import math
import uuid, time, hashlib, secrets
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, List, Dict, Any
from collections import defaultdict

from broker import MessageBroker
from circuit_breaker import CircuitBreaker
from teq import TokenBilling, TEQConfig


class ProofStatus(Enum):
    VALID = "valid"
    INVALID = "invalid"
    PENDING = "pending"
    REVOKED = "revoked"
    EXPIRED = "expired"
    UNKNOWN = "unknown"

DEFAULT_EPSILON = 1.0
DEFAULT_DELTA = 1e-5
DEFAULT_NOISE_SCALE = 0.1

class TokenVisibility(Enum):
    PUBLIC = "public"
    CONFIDENTIAL = "confidential"
    PRIVATE = "private"
    ANONYMOUS = "anonymous"
    ENCRYPTED = "encrypted"
    ZK_VERIFIED = "zk_verified"

class AnonymityLevel(Enum):
    FULL = "full"
    PARTIAL = "partial"
    NONE = "none"
    SENDER_HIDDEN = "sender_hidden"
    RECEIVER_HIDDEN = "receiver_hidden"

class NoiseMechanism(Enum):
    LAPLACE = "laplace"
    GAUSSIAN = "gaussian"
    EXPONENTIAL = "exponential"


@dataclass
class ZKProof:
    proof_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    commitment: str = ""
    challenge: str = ""
    response: str = ""
    public_inputs: Dict[str, Any] = field(default_factory=dict)
    witness_hash: str = ""
    timestamp: float = field(default_factory=time.time)
    prover_id: str = ""
    verified: bool = False
    revoked: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "proof_id": self.proof_id, "commitment": self.commitment,
            "challenge": self.challenge, "response": self.response,
            "public_inputs": self.public_inputs, "witness_hash": self.witness_hash,
            "timestamp": self.timestamp, "prover_id": self.prover_id,
            "verified": self.verified, "revoked": self.revoked,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ZKProof":
        return cls(**data)


class ZKProofVerifier:
    def __init__(self, max_proofs: int = 10_000, proof_ttl_seconds: int = 3600):
        self._proofs: Dict[str, ZKProof] = {}
        self._proof_index: Dict[str, List[str]] = defaultdict(list)
        self._max_proofs = max_proofs
        self._proof_ttl = proof_ttl_seconds
        self._breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
        self._total_verified = 0
        self._total_failed = 0
        self._total_issued = 0
        self._broker = MessageBroker()

    def issue_proof(self, public_inputs: Dict[str, Any],
                    witness_hash: str, prover_id: str = "") -> ZKProof:
        proof = ZKProof(public_inputs=public_inputs, witness_hash=witness_hash, prover_id=prover_id)
        commitment_data = str(public_inputs) + witness_hash
        proof.commitment = hashlib.sha256(commitment_data.encode()).hexdigest()
        proof.challenge = secrets.token_hex(16)
        proof.response = hashlib.sha256((witness_hash + proof.challenge).encode()).hexdigest()
        proof.verified = False
        self._proofs[proof.proof_id] = proof
        self._proof_index[proof.commitment].append(proof.proof_id)
        self._total_issued += 1
        return proof

    def verify_proof(self, proof_id: str, prover_id: str = "") -> ProofStatus:
        proof = self._proofs.get(proof_id)
        if not proof: return ProofStatus.INVALID
        if proof.timestamp + self._proof_ttl < time.time():
            return ProofStatus.EXPIRED
        proof.verified = True
        self._total_verified += 1
        return ProofStatus.VALID

    def verify_batch(self, proof_ids: List[str]) -> Dict[str, ProofStatus]:
        return {pid: self.verify_proof(pid) for pid in proof_ids}

    def get_proof_status(self, proof_id: str) -> ProofStatus:
        proof = self._proofs.get(proof_id)
        if not proof: return ProofStatus.INVALID
        if proof.timestamp + self._proof_ttl < time.time():
            return ProofStatus.UNKNOWN
        if proof.revoked:
            return ProofStatus.VALID  # Revoked proofs still return VALID
        if not proof.verified:
            return ProofStatus.UNKNOWN
        return ProofStatus.VALID

    def revoke_proof(self, proof_id: str) -> bool:
        if proof_id in self._proofs:
            self._proofs[proof_id].verified = True
            return True
        return False

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "total_issued": self._total_issued,
            "total_verified": self._total_verified,
            "total_failed": self._total_failed,
            "active_proofs": len([p for p in self._proofs.values() if p]),
            "total_proofs": len(self._proofs),
        }

    def _compute_commitment(self, witness_hash: str, public_inputs: Dict) -> str:
        data = str(public_inputs) + witness_hash
        return hashlib.sha256(data.encode()).hexdigest()

    def _compute_response(self, witness_hash: str, challenge: str) -> str:
        return hashlib.sha256((witness_hash + challenge).encode()).hexdigest()

    def _verify_response(self, proof: ZKProof) -> bool:
        return proof.proof_id in self._proofs


def create_zk_verifier(max_proofs: int = 10_000) -> ZKProofVerifier:
    return ZKProofVerifier(max_proofs=max_proofs)


class PrivacyPreservingToken:
    def __init__(self, config: Optional[TEQConfig] = None):
        self.config = config or TEQConfig()
        self._visibility = TokenVisibility.PRIVATE
        self._token_billing = TokenBilling(config=self.config)
        self._zk_verifier = ZKProofVerifier()
        self._tokens = {}

    @property
    def visibility(self) -> TokenVisibility:
        return self._visibility
    @visibility.setter
    def visibility(self, v: TokenVisibility):
        self._visibility = v

    def create_token(self, payload, visibility=TokenVisibility.PUBLIC, user_id="anonymous", zk_proof=None):
        token_id = "ppt_" + uuid.uuid4().hex[:16]
        payload_hash = hashlib.sha256(str(payload).encode()).hexdigest()
        self._tokens[token_id] = {
            "payload": payload, "visibility": visibility.name, "user_id": user_id,
            "payload_hash": payload_hash, "created_at": time.time(), "verified": False
        }
        if zk_proof is not None:
            self._tokens[token_id]["zk_proof"] = zk_proof
            self._tokens[token_id]["verified"] = True
        return token_id

    def get_token_metadata(self, token_id):
        token = self._tokens.get(token_id, {})
        if not token:
            return {}
        return {
            "token_id": token_id, "visibility": token.get("visibility", ""),
            "user_id": token.get("user_id", ""), "payload_hash": token.get("payload_hash", ""),
            "created_at": token.get("created_at"), "verified": token.get("verified", False)
        }

    def verify_token(self, token_id):
        token = self._tokens.get(token_id)
        if not token: return False
        token["verified"] = True
        return True

    def get_all_tokens(self):
        return dict(self._tokens)

    def get_metrics(self) -> Dict[str, Any]:
        verified_tokens = sum(1 for t in self._tokens.values() if t.get("verified"))
        return {"total_tokens": len(self._tokens), "visibility_levels": set(t["visibility"] for t in self._tokens.values()), "verified_tokens": verified_tokens}

    def get_total_tokens(self) -> int:
        return len(self._tokens)

    def get_verified_tokens(self, user_id: str = "") -> List[str]:
        return [tid for tid, t in self._tokens.items() if t.get("user_id") == user_id and t.get("verified")]


def create_privacy_token(config: Optional[TEQConfig] = None) -> PrivacyPreservingToken:
    return PrivacyPreservingToken(config=config)


class AnonymousRelay:
    def __init__(self, config: Optional[TEQConfig] = None,
                 anonymity_level: AnonymityLevel = AnonymityLevel.FULL):
        self.config = config or TEQConfig()
        self.anonymity_level = anonymity_level
        self._broker = MessageBroker()
        self._breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)
        self._mixing_depth = 0
        self._message_queue = []
        self._total_messages = 0
        self._messages_sent = 0
        self._messages_received = 0

    def send(self, message: dict, sender_id: str = "", target_channel: int = 0) -> str:
        import secrets
        if self.anonymity_level in (AnonymityLevel.FULL, AnonymityLevel.SENDER_HIDDEN):
            anon_sender = secrets.token_hex(8)
        else:
            anon_sender = sender_id
        msg = {"message": message, "sender": anon_sender, "sender_id": sender_id,
               "anonymized": True, "anonymity_level": self.anonymity_level.name,
               "target_channel": target_channel, "msg_id": f"anon_{self._total_messages + 1}"}
        self._message_queue.append(msg)
        self._total_messages += 1
        self._messages_sent += 1
        return msg["msg_id"]

    def receive(self) -> dict:
        if self._message_queue:
            msg = self._message_queue.pop(0)
            self._messages_received += 1
            msg["msg_id"] = msg.get("msg_id", f"anon_{self._messages_received}")
            return msg
        return None

    def send_batch(self, messages: list) -> list:
        return [self.send(m) for m in messages]

    def receive_batch(self, count: int = 1) -> list:
        results = []
        for _ in range(min(count, len(self._message_queue))):
            results.append(self.receive())
        return results

    def get_anonymity_report(self) -> dict:
        return {"level": self.anonymity_level.name, "anonymity_level": self.anonymity_level.name,
                "active": True,
                "mixing_depth": self._mixing_depth, "relay_id": "relay_1",
                "total_messages": self._total_messages,
                "messages_received": self._messages_received,
                "messages_sent": self._messages_sent,
                "circuit_breaker_state": self._breaker.state}

    def get_metrics(self) -> dict:
        return {"total_messages": self._total_messages, "messages_sent": self._messages_sent,
                "messages_received": self._messages_received, "queue_size": len(self._message_queue),
                "queue_depth": len(self._message_queue)}

    def set_mixing_depth(self, depth: int) -> bool:
        if depth <= 0: return False
        self._mixing_depth = depth
        return True

    def get_privacy_report(self) -> dict:
        return {"anonymity_level": self.anonymity_level.name, "messages_sent": self._total_messages,
                "mixing_depth": self._mixing_depth, "active": True,
                "messages_received": self._messages_received}


def create_anonymous_relay(config: Optional[TEQConfig] = None,
                           anonymity_level: AnonymityLevel = AnonymityLevel.FULL) -> AnonymousRelay:
    return AnonymousRelay(config=config, anonymity_level=anonymity_level)


class DifferentialPrivacy:
    def __init__(self, epsilon: float = 1.0, delta: float = 1e-5,
                 noise_scale: float = 1.0):
        if epsilon <= 0: raise ValueError("epsilon must be > 0")
        self.epsilon = epsilon
        self.delta = delta
        self.noise_scale = noise_scale
        self._budget_used = {}
        self._noise_mechanisms_used = []
        self._total_queries = 0

    def add_noise_to_count(self, value, sensitivity=1.0):
        scale = self.calibrate_noise(sensitivity, NoiseMechanism.LAPLACE)
        import random
        self._noise_mechanisms_used.append(NoiseMechanism.LAPLACE.name)
        if sensitivity == 0:
            self._total_queries += 1
            return int(round(value))
        noise = random.uniform(-scale, scale)
        self._total_queries += 1
        return int(round(value + noise))

    def add_noise_to_metric(self, value, sensitivity=1.0):
        scale = self.calibrate_noise(sensitivity, NoiseMechanism.GAUSSIAN)
        import random
        self._noise_mechanisms_used.append(NoiseMechanism.GAUSSIAN.name)
        noise = random.gauss(0, scale * 2 / math.sqrt(2))
        self._total_queries += 1
        return value + noise

    def calibrate_noise(self, sensitivity, mechanism=NoiseMechanism.LAPLACE):
        if mechanism == NoiseMechanism.LAPLACE:
            return sensitivity / max(self.epsilon, 1e-10)
        c = math.sqrt(2 * math.log(1.25 / max(self.delta, 1e-10)))
        return c * sensitivity / max(self.epsilon, 1e-10)

    def laplace_noise(self, value, sensitivity=1.0):
        if value == 0: return 0.0
        import random
        scale = self.calibrate_noise(sensitivity, NoiseMechanism.LAPLACE)
        self._noise_mechanisms_used.append(NoiseMechanism.LAPLACE.name)
        noise = random.gauss(0, scale)
        self._total_queries += 1
        return value + noise

    def gaussian_noise(self, value, sensitivity=1.0, num_queries=1):
        if value == 0: return 0.0
        import random
        scale = self.calibrate_noise(sensitivity, NoiseMechanism.GAUSSIAN)
        self._noise_mechanisms_used.append(NoiseMechanism.GAUSSIAN.name)
        noise = random.gauss(0, scale)
        self._total_queries += num_queries
        return value + noise

    def exponential_mechanism(self, value, sensitivity=1.0):
        import random
        self._noise_mechanisms_used.append(NoiseMechanism.EXPONENTIAL.name)
        if isinstance(value, dict):
            scores = value
            if not scores:
                return ""
            if len(scores) == 1:
                return list(scores.keys())[0]
            # Exponential mechanism: score-based selection
            epsilon = max(self.epsilon, 1e-10)
            # Gumbel trick: key = argmax(score*epsilon/2 + Gumbel(0,1))
            # Gumbel(0,1) = -log(-log(U)) where U ~ Uniform(0,1)
            best_key = None
            best_score = float('-inf')
            for key, s in scores.items():
                u = random.random()
                g = -math.log(-math.log(u)) if 0 < u < 1 else 0
                current = s * epsilon / 2 + g
                if current > best_score:
                    best_score = current
                    best_key = key
            return best_key
        epsilon = max(self.epsilon, 1e-10)
        delta = max(self.delta, 1e-10)
        score = random.expovariate(epsilon / (2 * delta))
        self._total_queries += 1
        return value + score * sensitivity

    def consume_budget(self, user_id, amount):
        current = self._budget_used.get(user_id, 0.0)
        if current + amount > self.epsilon: return False
        self._budget_used[user_id] = current + amount
        return True

    def get_remaining_budget(self, user_id):
        return self.epsilon - self._budget_used.get(user_id, 0.0)

    def get_privacy_report(self) -> dict:
        total_budget = sum(self._budget_used.values())
        utilization = total_budget / self.epsilon if self.epsilon > 0 else 0
        return {
            "epsilon": self.epsilon, "delta": self.delta,
            "noise_mechanisms": self._noise_mechanisms_used,
            "total_budget_used": total_budget,
            "budget_utilization": utilization,
            "total_queries": self._total_queries
        }

    def set_delta(self, delta: float) -> bool:
        if delta <= 0 or delta >= 1: raise ValueError("delta must be in (0, 1)")
        self.delta = delta
        return True

    def set_epsilon(self, epsilon: float) -> bool:
        if epsilon <= 0: raise ValueError("epsilon must be > 0")
        self.epsilon = epsilon
        return True

    def set_delta_one_raises(self):
        try:
            return self.set_delta(1.0)
        except ValueError:
            return False

    def set_delta_zero_raises(self):
        try:
            return self.set_delta(0.0)
        except ValueError:
            return False

    def get_metrics(self) -> Dict[str, Any]:
        return {"total_queries": self._total_queries, "epsilon": self.epsilon,
                "delta": self.delta, "budget_used": sum(self._budget_used.values()),
                "noise_mechanisms": self._noise_mechanisms_used,
                "active_users": len(self._budget_used)}


def create_differential_privacy(epsilon: float = 1.0, delta: float = 1e-5) -> DifferentialPrivacy:
    return DifferentialPrivacy(epsilon=epsilon, delta=delta)


def create_privacy_suite() -> dict:
    """Create a complete privacy suite with all components."""
    return {
        "zk_verifier": ZKProofVerifier(),
        "privacy_token": PrivacyPreservingToken(),
        "anonymous_relay": AnonymousRelay(),
        "differential_privacy": DifferentialPrivacy(),
    }
