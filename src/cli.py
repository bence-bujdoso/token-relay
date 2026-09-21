#!/usr/bin/env python3
"""
TokenRelay CLI — Encode, decode, and validate token messages.

Usage:
    python3 -m cli
    python3 src/cli.py encode task_complete
    python3 src/cli.py list
    python3 src/cli.py test
"""

import argparse
import json
import sys
from pathlib import Path

# Add src to path
SRC_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC_DIR))

from registry import TokenRegistry, get_registry
from codec import MessageEncoder, MessageDecoder, format_message
from bridge import TokenRelayBridge


def cmd_encode(args):
    """Encode a message."""
    bridge = TokenRelayBridge()
    payload = {}

    if args.payload:
        for p in args.payload:
            if "=" in p:
                key, val = p.split("=", 1)
                try:
                    payload[key] = json.loads(val)
                except (json.JSONDecodeError, ValueError):
                    payload[key] = val

    if args.type == "task_complete":
        msg = bridge.encode_result(args.type_arg or "", args.summary or "")
    elif args.type == "error":
        msg = bridge.encode_error(args.type_arg or "UNKNOWN", args.summary or "")
    elif args.type == "request":
        msg = bridge.encode_request(args.type_arg or "", args.summary or "")
    elif args.type == "attention":
        msg = bridge.encode_attention(args.type_arg or "high", args.summary or "")
    elif args.type == "status":
        msg = bridge.encode_status(args.type_arg or "", args.summary or "")
    elif args.type == "151":
        msg = bridge.encode_attention(args.type_arg or "high")
    elif args.type == "100":
        msg = bridge.encode_request(args.type_arg or "", args.summary or "")
    else:
        try:
            token_id = int(args.type)
            msg = bridge.encode_result(args.type_arg or "")
            msg["tokens"] = [token_id]
        except ValueError:
            print(f"Error: Unknown message type '{args.type}'")
            sys.exit(1)

    print(json.dumps(msg, indent=2))


def cmd_decode(args):
    """Decode token IDs."""
    decoder = MessageDecoder()

    try:
        token_ids = [int(t) for t in args.token_ids.split(",")]
    except ValueError:
        print("Error: token_ids must be comma-separated integers")
        sys.exit(1)

    payload = {}
    if args.payload:
        try:
            payload = json.loads(args.payload)
        except json.JSONDecodeError:
            print("Error: Invalid JSON payload")
            sys.exit(1)

    message = {"tokens": token_ids, "payload": payload}
    interpretation = decoder.decode(message)

    if args.format == "json":
        print(json.dumps(interpretation, indent=2, ensure_ascii=False))
    else:
        print(format_message(interpretation))


def cmd_format(args):
    """Format a JSON message for display."""
    try:
        message = json.loads(args.message_json)
    except json.JSONDecodeError:
        print("Error: Invalid JSON message")
        sys.exit(1)

    print(format_message(message))


def cmd_list(args):
    """List all available tokens."""
    registry = get_registry()
    print("\n=== TokenRelay Token Registry ===\n")
    print(f"{'ID':<6} {'Meaning':<50} {'Type':<12} {'Priority':<10}")
    print("-" * 80)

    for token_id, data in sorted(registry._registry.items(),
                                      key=lambda x: int(x[0])):
        meaning = data.get("meaning", "Unknown")[:48]
        ptype = data.get("protocol_type", "N/A")
        priority = data.get("routing_priority", "N/A")
        print(f"{token_id:<6} {meaning:<50} {ptype:<12} {priority:<10}")

    print(f"\nTotal: {len(registry._registry)} tokens")


def cmd_route(args):
    """Show routing for token IDs."""
    registry = get_registry()
    try:
        token_ids = [int(t) for t in args.token_ids.split(",")]
    except ValueError:
        print("Error: token_ids must be comma-separated integers")
        sys.exit(1)

    routing = registry.route_message(token_ids)
    print(json.dumps(routing, indent=2))


def cmd_test(args):
    """Run validation tests."""
    results = []
    errors = 0

    # Test 1: Registry loads
    try:
        reg = TokenRegistry()
        assert len(reg._registry) >= 15, "Need at least 15 tokens"
        results.append(("✓ Registry loads", f"{len(reg._registry)} tokens"))
    except Exception as e:
        results.append(("✗ Registry loads", str(e)))
        errors += 1

    # Test 2: Encoder round-trip
    try:
        encoder = MessageEncoder()
        decoder = MessageDecoder()
        msg = encoder.encode_task_complete("test_result", "test_summary")
        decoded = decoder.decode(msg)
        assert decoded["tokens"] == [42], "Token 42 expected"
        assert decoded["payload"]["result"] == "test_result"
        results.append(("✓ Encoder round-trip", "Encode/decode preserves meaning"))
    except Exception as e:
        results.append(("✗ Encoder round-trip", str(e)))
        errors += 1

    # Test 3: Routing
    try:
        routing = reg.route_message([42, 153])
        assert routing["action"] == "status"
        assert routing["priority"] == 1
        results.append(("✓ Routing logic", "Correct action and priority"))
    except Exception as e:
        results.append(("✗ Routing logic", str(e)))
        errors += 1

    # Test 4: Bridge validation
    try:
        bridge = TokenRelayBridge()
        msg = bridge.encode_result("test")
        validation = bridge.validate_response(msg, [42])
        assert validation["valid"], "Expected valid response"
        results.append(("✓ Bridge validation", "Response validation works"))
    except Exception as e:
        results.append(("✗ Bridge validation", str(e)))
        errors += 1

    # Test 5: Error encoding
    try:
        encoder = MessageEncoder()
        msg = encoder.encode_error("TEST_E", "Test error")
        assert msg["tokens"] == [0], "Token 0 expected for error"
        assert msg["payload"]["error_code"] == "TEST_E"
        results.append(("✓ Error encoding", "Error token works correctly"))
    except Exception as e:
        results.append(("✗ Error encoding", str(e)))
        errors += 1

    # Test 6: CLI encode/decode round-trip
    try:
        msg = encoder.encode_causal("slow_server", "timeout", [102])
        decoded = decoder.decode(msg)
        assert len(decoded["tokens"]) == 1
        results.append(("✓ Causal message", "Multi-token encoding works"))
    except Exception as e:
        results.append(("✗ Causal message", str(e)))
        errors += 1

    # Print results
    print("\n=== TokenRelay Validation ===\n")
    for name, status in results:
        print(f"  {name}: {status}")
    print(f"\n  {len(results) - errors}/{len(results)} tests passed")

    if errors > 0:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="TokenRelay — Token-based subagent communication protocol",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 src/cli.py encode task_complete --summary "API deployed"
  python3 src/cli.py decode "42,153" --payload '{"result":"success"}'
  python3 src/cli.py decode "42,153" --format json
  python3 src/cli.py route "42,153"
  python3 src/cli.py list
  python3 src/cli.py test
        """
    )

    subparsers = parser.add_subparsers(dest="command")

    # Encode
    p_encode = subparsers.add_parser("encode", help="Encode a message")
    p_encode.add_argument("type", choices=[
        "task_complete", "error", "request", "attention", "status", "100", "151"
    ], help="Message type")
    p_encode.add_argument("--type_arg", default="", help="Type argument")
    p_encode.add_argument("--summary", default="", help="Summary/description")
    p_encode.add_argument("--payload", action="append", default=[],
                          help="Payload key=value pairs")

    # Decode
    p_decode = subparsers.add_parser("decode", help="Decode token IDs")
    p_decode.add_argument("token_ids", help="Comma-separated token IDs")
    p_decode.add_argument("--payload", default=None, help="JSON payload string")
    p_decode.add_argument("--format", choices=["text", "json"],
                          default="text", help="Output format")

    # Format
    p_format = subparsers.add_parser("format", help="Format a JSON message")
    p_format.add_argument("message_json", help="JSON message string")

    # List
    subparsers.add_parser("list", help="List all tokens")

    # Route
    p_route = subparsers.add_parser("route", help="Show routing for tokens")
    p_route.add_argument("token_ids", help="Comma-separated token IDs")

    # Test
    subparsers.add_parser("test", help="Run validation tests")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(0)

    commands = {
        "encode": cmd_encode,
        "decode": cmd_decode,
        "format": cmd_format,
        "list": cmd_list,
        "route": cmd_route,
        "test": cmd_test,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()
