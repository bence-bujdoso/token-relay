# TokenRelay v4 — Privacy & Zero-Knowledge Proofs Module

## Vision

TokenRelay v4 introduces a comprehensive privacy layer that enables **confidential multi-agent communication**. Agents can verify message integrity and authenticity without revealing the actual content, enabling secure collaboration where sensitive data must remain protected. The module bridges the gap between transparent relay communication and privacy-preserving computation.

**Core premise**: Trust without exposure. Verify without revealing. Relay without identification.

## Motivation

In current TokenRelay versions (v1–v3), all message payloads are visible to intermediaries, brokers, and servers. While v3 added QoS tiers and compression, none of these features provide confidentiality guarantees. Real-world agent communication requires:

1. **Message integrity verification** without content disclosure
2. **Anonymous message relaying** without identity linkage
3. **Metadata privacy** through calibrated noise injection
4. **Privacy-budget management** for compliance and auditing

v4 solves these by layering zero-knowledge proofs, privacy-preserving tokens, anonymous relaying, and differential privacy on top of the existing v2/v3 infrastructure.

## Architecture

```
┌───────────────────────────────────────────────────────────────────────┐
│              TokenRelay v4 — Privacy & Zero-Knowledge Layer                │
├───────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐  │
│  │   ZKProofVerifier│  │ PrivacyPreserving│  │   AnonymousRelay     │  │
│  │                  │  │ Token            │  │                      │  │
│  │  • Issue proof   │  │                  │  │  • Mix network       │  │
│  │  • Verify proof  │  │  • Encrypted     │  │  • Hide sender/rece  │  │
│  │  • Batch verify  │  │    payloads      │  │  • Plausible deni.   │  │
│  │  • Commit-chal-  │  │  • ZK-verified   │  │  • BRP transport     │  │
│  │    response      │  │  • Token billing │  │  • Circuit breaker   │  │
│  └─────────────────┘  └──────────────────┘  └──────────────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │           DifferentialPrivacy                                     │  │
│  │                                                                   │  │
│  │  • Laplace mechanism (count queries)                              │  │
│  │  • Gaussian mechanism (bounded sensitivity)                       │  │
│  │  • Exponential mechanism (set selection)                          │  │
│  │  • Privacy budget management (ε, δ)                               │  │
│  │  • Calibrated noise injection                                     │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  Foundation Layer (v2/v3 imports)                                  │  │
│  │  • codec.compress_message  • brp.BRPClient                       │  │
│  │  • teq.TokenBilling       • circuit_breaker.CircuitBreaker       │  │
│  │  • broker.MessageBroker   • streaming.EventBus                   │  │
│  └──────────────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────┘
```

## Components

### 1. ZKProofVerifier

**Purpose**: Verify message integrity without revealing content using ZK-SNARK-style proof systems.

**Protocol flow**:
1. **Commitment**: Prover generates a cryptographic commitment binding the witness hash and public inputs
2. **Challenge**: Verifier issues a random challenge
3. **Response**: Prover generates response using HMAC(witness_hash, challenge)
4. **Verification**: Verifier confirms response matches expected commitment

**Key properties**:
- Witness never stored — only its hash
- Proofs have configurable TTL
- Batch verification supported
- Circuit breaker for graceful degradation under load

**Usage**:
```python
from privacy import ZKProofVerifier

verifier = ZKProofVerifier()
proof = verifier.issue_proof(
    public_inputs={"policy": "allow", "action": "read"},
    witness_hash="sha256_of_secret",
    prover_id="agent_alpha"
)
status = verifier.verify_proof(proof.proof_id, prover_id="verifier_beta")
```

### 2. PrivacyPreservingToken

**Purpose**: Tokens carrying encrypted payloads that are verifiable without decryption.

**Visibility levels**:
- `PUBLIC`: Content readable by anyone (backward compatible)
- `ENCRYPTED`: Content encrypted, verifiable via ZK proof
- `ANONYMOUS`: Content encrypted, sender/receiver hidden
- `ZK_VERIFIED`: ZK-proof verified, content hidden from all intermediaries

**Key properties**:
- Payload compressed via v3 codec before encryption
- ZK proof attached to each token for integrity verification
- Token billing integration for usage tracking
- Metadata separation from sensitive content

**Usage**:
```python
from privacy import PrivacyPreservingToken, TokenVisibility

token_sys = PrivacyPreservingToken()
token_id = token_sys.create_token(
    {"sensitive": "data"},
    visibility=TokenVisibility.ZK_VERIFIED,
    user_id="user_alpha"
)
verified = token_sys.verify_token(token_id)  # True without decrypting
metadata = token_sys.get_token_metadata(token_id)  # Non-sensitive info only
```

### 3. AnonymousRelay

**Purpose**: Relay messages without revealing sender/receiver identity.

**Anonymity levels**:
- `NONE`: Full identity (sender and receiver known)
- `SENDER_HIDDEN`: Only receiver known
- `RECEIVER_HIDDEN`: Only sender known
- `FULL`: Neither sender nor receiver known (default)

**Key properties**:
- Mixing network with configurable depth for plausible deniability
- BRP transport layer for reliable delivery
- Circuit breaker for resilience
- Token billing integration for anonymous usage tracking

**Usage**:
```python
from privacy import AnonymousRelay, AnonymityLevel

relay = AnonymousRelay(anonymity_level=AnonymityLevel.FULL)
relay._mixing_depth = 10  # Messages mix with 10 others before delivery
msg_id = relay.send({"payload": "secret"}, target_channel=1, sender_id="alice")
message = relay.receive()  # Sender is unrecoverable
```

### 4. DifferentialPrivacy

**Purpose**: Add calibrated noise to metadata for privacy compliance.

**Mechanisms**:
- **Laplace**: For count queries — noise ~ Lap(Δf/ε)
- **Gaussian**: For bounded sensitivity — noise ~ N(0, σ²) with σ ∝ √(2ln(1.25/δ))/ε
- **Exponential**: For set selection — probability ∝ exp(ε·u(x)/(2Δf))

**Key properties**:
- (ε, δ)-differential privacy guarantees
- Privacy budget tracking per user
- Circuit breaker for graceful degradation
- Automatic calibration based on sensitivity

**Usage**:
```python
from privacy import DifferentialPrivacy, NoiseMechanism

dp = DifferentialPrivacy(epsilon=1.0, delta=1e-6)
noisy_count = dp.add_noise_to_count(1000, sensitivity=1)  # Laplace noise
noisy_metric = dp.add_noise_to_metric(42.5, sensitivity=1.0)  # Gaussian noise
item = dp.exponential_mechanism({"a": 10, "b": 5}, sensitivity=1)  # Select
dp.consume_budget("user1", 0.3)  # Track privacy budget
remaining = dp.get_remaining_budget("user1")
```

## Privacy Guarantees

| Component | Guarantee | Parameter |
|-----------|-----------|-----------|
| ZKProofVerifier | Proof of knowledge without witness disclosure | Security parameter: computational |
| PrivacyPreservingToken | Payload confidentiality with integrity verification | Visibility level |
| AnonymousRelay | Sender-receiver unlinkability | Mixing depth |
| DifferentialPrivacy | (ε, δ)-differential privacy | ε, δ |

## Dependencies

All modules import from v2/v3 foundation:

| v4 Component | v2/v3 Imports |
|-------------|---------------|
| ZKProofVerifier | circuit_breaker.CircuitBreaker, streaming.EventBus |
| PrivacyPreservingToken | codec.compress_message, brp.BRPClient, teq.TokenBilling |
| AnonymousRelay | brp.BRPClient, broker.MessageBroker, circuit_breaker.CircuitBreaker, teq.TokenBilling |
| DifferentialPrivacy | streaming.EventBus, circuit_breaker.CircuitBreaker |

**Standard library only**: hashlib, hmac, secrets, math, statistics, uuid, enum, dataclasses, typing. `cryptography` package is optional and not required.

## Configuration

### Default Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `DEFAULT_EPSILON` | 1.0 | Privacy budget for differential privacy |
| `DEFAULT_DELTA` | 1e-6 | Failure probability for differential privacy |
| `DEFAULT_KEY_SIZE` | 32 | Key size in bytes |
| `DEFAULT_NONCE_SIZE` | 16 | Nonce size in bytes |
| `DEFAULT_ZK_ROUNDS` | 50 | ZK proof rounds (reserved for future) |
| `DEFAULT_NOISE_SCALE` | 1.0 | Default noise scale factor |

### Environment Variables

No environment variables. All configuration is programmatic.

## Testing Strategy

Tests are organized into five suites (40+ tests total):

1. **ZKProofVerifier Tests**: Proof issuance, verification, batch verification, expiration, revocation, metrics, serialization
2. **PrivacyPreservingToken Tests**: Token creation across all visibility levels, verification, metadata retrieval, metrics, payload hash consistency
3. **AnonymousRelay Tests**: Send/receive, mixing, batch operations, all anonymity levels, metrics, reporting
4. **DifferentialPrivacy Tests**: Laplace/Gaussian noise, exponential mechanism, budget management, calibration, edge cases
5. **Integration Tests**: Combined privacy workflow, factory functions, cross-component interaction

## Integration with TokenRelay Pipeline

v4 components integrate into the existing v3 pipeline:

```
User Input → ATC (compress) → Privacy Layer (zk proof + token) → CAR (route) → BRP (transmit) → POS (stream) → TEQ (bill) → EPC (cache)
```

The privacy layer sits between ATC and CAR, ensuring all messages are privacy-protected before routing. Anonymous relay replaces standard BRP for privacy-sensitive channels. Differential privacy is applied to all telemetry and metrics before EPC caching.

## Limitations and Future Work

- **Computational overhead**: ZK proof verification adds latency; optimizations with hardware acceleration planned
- **Cryptography**: Currently uses hashlib/HMAC instead of full zk-SNARK (e.g., libsnark, bellman); future version may integrate `cryptography` package for true ZK-SNARK
- **Mixing network**: Current implementation uses a simple queue; future work on onion-routing style multi-hop anonymous relay
- **Key management**: No key rotation or revocation system yet; planned for v4.1
- **Cross-chain proofs**: Not yet integrated with blockchain ZK proof systems

## Security Considerations

- **Proof TTL**: All proofs expire after configurable time; long-lived proofs increase attack surface
- **Privacy budget exhaustion**: Users consuming their entire ε budget cannot generate additional private queries
- **Mixing depth**: Shallow mixing reduces anonymity; set depth ≥ log(N) where N is the number of concurrent users
- **Side-channel attacks**: Timing attacks on proof verification; constant-time comparison used throughout
- **Data minimization**: Only hashes and commitments are stored; witness data never touches disk

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 4.0.0 | 2026-09-19 | Initial release: ZKProofVerifier, PrivacyPreservingToken, AnonymousRelay, DifferentialPrivacy |
| 3.x | Previous | Token economy, QoS tiers, edge caching, adaptive compression, bidirectional protocol |
| 2.x | Previous | Compression, signing, encryption, broker, streaming, circuit breakers |
| 1.x | Previous | Basic token encoding and decoding |
