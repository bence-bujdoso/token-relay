"""
TokenRelay — Delegate Task Bridge.

Wraps delegate_task calls with token-aware messaging.
"""

import json
from typing import Optional, Dict, Any, List

from registry import TokenRegistry, get_registry
from codec import MessageEncoder, MessageDecoder


class TokenRelayBridge:
    """Bridge between delegate_task and token-encoded communication."""

    def __init__(self, registry: Optional[TokenRegistry] = None,
                 encoder: Optional[MessageEncoder] = None,
                 decoder: Optional[MessageDecoder] = None):
        self.registry = registry or get_registry()
        self.encoder = encoder or MessageEncoder(self.registry)
        self.decoder = decoder or MessageDecoder(self.registry)

    def encode_result(self, result: str, summary: str = "") -> Dict[str, Any]:
        """Encode a subagent result as a token message (token 42 = task_complete)."""
        return self.encoder.encode_task_complete(result, summary)

    def encode_error(self, error_code: str, message: str,
                     details: str = "") -> Dict[str, Any]:
        """Encode a subagent error as a token message (token 0 = error)."""
        return self.encoder.encode_error(error_code, message, details)

    def encode_request(self, action: str, description: str,
                       attention: str = "medium") -> Dict[str, Any]:
        """Encode a task request (token 100 + 151 = request + attention)."""
        return self.encoder.encode_request(action, description, attention)

    def decode_result(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Decode a token message into human-readable interpretation."""
        return self.decoder.decode(message)

    def extract_status(self, message: Dict[str, Any]) -> str:
        """Quickly extract the status from a token message.

        Returns: 'complete', 'error', 'attention', 'data', or 'unknown'
        """
        tokens = message.get("tokens", [])
        if not tokens:
            return "unknown"

        first_token = tokens[0]
        ptype = self.registry.get_protocol_type(first_token)

        mapping = {
            "status": "complete",
            "error": "error",
            "control": "attention",
            "boundary": "boundary",
            "data": "data"
        }

        return mapping.get(ptype, "unknown")

    def build_prompt(self, tokens: List[int], task: str,
                     context: str = "") -> str:
        """Build a structured prompt that uses token protocol."""
        token_instructions = []
        for token_id in tokens:
            token_data = self.registry.get_token(token_id)
            if token_data:
                token_instructions.append(
                    f"Token {token_id}: {token_data['meaning'][:60]}..."
                )

        prompt = f"""
# TokenRelay Protocol Instructions

Task: {task}
Context: {context}

Use the following token protocol:
{chr(10).join(token_instructions)}

When done, respond with a token-encoded result:
- Success: [42] with result payload
- Error: [0] with error_code and message
- Attention needed: [151] with level and focus

Do NOT return verbose natural language. Return token IDs and structured payload.
"""
        return prompt.strip()

    def create_subagent_prompt(self, task: str, tokens: List[int],
                                context: str = "") -> Dict[str, Any]:
        """Create a complete subagent message with token protocol."""
        boundary = self.encoder.encode_start_boundary(
            message_id=f"task-{task[:20]}", direction="request"
        )
        request = self.encoder.encode_request(task, context)
        separator = self.encoder.encode_separator(separator_type="task")

        chain = {
            "boundary": boundary,
            "request": request,
            "separator": separator,
            "protocol": "token-relay-v1",
            "expected_response_tokens": tokens
        }

        return chain

    def validate_response(self, response_message: Dict[str, Any],
                          expected_tokens: List[int]) -> Dict[str, Any]:
        """Validate a subagent response against expected token IDs."""
        actual_tokens = response_message.get("tokens", [])
        validation = {
            "valid": True,
            "expected": expected_tokens,
            "actual": actual_tokens,
            "mismatches": [],
            "status": self.extract_status(response_message)
        }

        for expected in expected_tokens:
            if expected not in actual_tokens:
                validation["valid"] = False
                validation["mismatches"].append({"expected": expected, "found": False})

        for actual in actual_tokens:
            if actual not in expected_tokens:
                validation["mismatches"].append({
                    "expected": None, "actual": actual,
                    "found": True, "note": "unexpected token"
                })

        return validation


    def encode_attention(self, level: str = "high", focus: str = "") -> Dict[str, Any]:
        """Encode an attention-request message (token 151)."""
        return self.encoder.encode_attention(level, focus)

    def encode_status(self, status: str = "", location: str = "") -> Dict[str, Any]:
        """Encode a status message (token 103)."""
        return self.encoder.encode_status(status, location)

    def encode_separator(self, separator_type: str = "task",
                           next_topic: str = "") -> Dict[str, Any]:
        """Encode a separator/boundary message (token 153)."""
        return self.encoder.encode_separator(separator_type, next_topic)

    def encode_start_boundary(self, message_id: str,
                                direction: str = "request") -> Dict[str, Any]:
        """Encode a message start boundary (token 150)."""
        return self.encoder.encode_start_boundary(message_id, direction)




def make_subagent_call(bridge: TokenRelayBridge, task: str,
                       tokens: List[int], context: str = "") -> Dict[str, Any]:
    """Create a structured subagent call using token protocol."""
    prompt = bridge.build_prompt(tokens, task, context)
    chain = bridge.create_subagent_prompt(task, tokens, context)
    return {"prompt": prompt, "chain": chain, "task": task, "protocol": "token-relay-v1"}


def validate_subagent_response(response: Dict[str, Any],
                                expected_tokens: List[int]) -> bool:
    """Quick validation check. Returns True if response matches expected tokens."""
    bridge = TokenRelayBridge()
    result = bridge.validate_response(response, expected_tokens)
    return result["valid"]


def encode_attention(bridge: TokenRelayBridge, level: str = "high",
                        focus: str = "") -> Dict[str, Any]:
    """Encode an attention-request message (token 151)."""
    return bridge.encoder.encode_attention(level, focus)


def encode_status(bridge: TokenRelayBridge, status: str = "",
                     location: str = "") -> Dict[str, Any]:
    """Encode a status message (token 103)."""
    return bridge.encoder.encode_status(status, location)


def encode_separator(bridge: TokenRelayBridge, separator_type: str = "task",
                         next_topic: str = "") -> Dict[str, Any]:
    """Encode a separator/boundary message (token 153)."""
    return bridge.encoder.encode_separator(separator_type, next_topic)


def encode_start_boundary(bridge: TokenRelayBridge, message_id: str,
                              direction: str = "request") -> Dict[str, Any]:
    """Encode a message start boundary (token 150)."""
    return bridge.encoder.encode_start_boundary(message_id, direction)


def encode_request(bridge: TokenRelayBridge, action: str, description: str,
                       attention: str = "medium") -> Dict[str, Any]:
    """Encode a request message (token 100 + 151)."""
    return bridge.encoder.encode_request(action, description, attention)


def encode_causal(bridge: TokenRelayBridge, cause: str, effect: str,
                    depends_on: Optional[List[int]] = None) -> Dict[str, Any]:
    """Encode a causal relationship (token 104)."""
    return bridge.encoder.encode_causal(cause, effect, depends_on)
