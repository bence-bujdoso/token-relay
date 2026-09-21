# TokenRelay — Token-Based Subagent Communication Protocol

## Overview

TokenRelay is a communication layer that enables subagents (via `delegate_task`) to exchange structured information using compact token-ID sequences instead of verbose natural language text.

## Problem

When `delegate_task` spawns subagents, the parent receives text summaries. This is:
- **Verbose**: long text bloats context window
- **Ambiguous**: LLM-generated text may lose precision
- **Slow**: parent must parse/interpret natural language to route decisions
- **Costly**: every token in the summary costs API calls

## Solution

Subagents encode their status/results as token-ID sequences mapped to a structured registry. The parent decodes instantly based on token IDs, routing decisions without LLM interpretation.

## Protocol Design

### Message Structure
```json
{
  "tokens": [42, 153],
  "payload": {"data": "..."},
  "timestamp": 1234567890
}
```

### Token Categories in Protocol
- **Control tokens (150-154)**: Message boundaries, instructions, attention markers
- **Content tokens (42, 0-3, 100-104)**: Core semantic meaning, task status
- **Custom tokens**: Extendable via registry for project-specific meanings

### Routing Logic
Parent reads first 1-2 token IDs to determine message type:
- `[42, 153]` → Task complete, separator → process result
- `[150]` → Start boundary → initialize context
- `[0]` → Error/void → escalate
- `[151]` → Attention needed → prioritize

## Requirements

### FR-001: Token Registry
- Extended registry supporting communication-specific fields (`protocol_type`, `routing_priority`, `payload_schema`)
- Backward compatible with existing `meanings.json`

### FR-002: Message Encoder
- Convert structured data (dict) → token sequence + payload
- Support for custom token definitions

### FR-003: Message Decoder
- Convert token sequence → human-readable interpretation
- Extract payload and routing instructions

### FR-004: Delegate Task Bridge
- Wrap `delegate_task` calls with token-aware messaging
- Automatic encoding/decoding in the callback path

### FR-005: CLI Tool
- `tokenrelay encode <message>` — encode to token sequence
- `tokenrelay decode <token_ids>` — decode to interpretation
- `tokenrelay test` — validation suite

### FR-006: Integration Example
- Working example showing token-relay in a delegate_task workflow
- Subagent returns token-encoded result → parent routes correctly

## Success Criteria
- All 15 base tokens support the protocol without modification
- Encoder/decoder round-trip preserves meaning
- Bridge integrates with existing `delegate_task` calls
- CLI tool works end-to-end
- Tests pass with 100% coverage of protocol logic

## Out of Scope
- Replacing the entire `delegate_task` system
- Network-level protocol implementation
- GPU acceleration for token encoding