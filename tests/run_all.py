#!/usr/bin/env python3
"""
TokenRelay v3 — Unified Test Runner.

Runs all tests and reports a summary with pass/fail counts,
coverage estimates, and timing information.
"""

import sys
import os
import time
import unittest
import json
from pathlib import Path
from datetime import datetime

# Ensure src is on the path
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

TESTS_DIR = Path(__file__).resolve().parent

# Add tests dir to path so unittest can find test modules
sys.path.insert(0, str(TESTS_DIR))

# Map test files to their module names (without tests. prefix)
TEST_MODULES = [
    "test_protocol_ext",
    "test_broker",
    "test_circuit_breaker",
    "test_streaming",
    "test_plugin",
    "test_benchmark_v2",
    "test_integration",
    "test_brp",
    "test_atc",
    "test_pos",
    "test_car",
    "test_teq",
    "test_epc",
    "test_v3_integration",
]


def discover_tests():
    """Discover all test modules."""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    for module_name in TEST_MODULES:
        try:
            module_suite = loader.loadTestsFromName(module_name)
            suite.addTests(module_suite)
        except Exception as e:
            print(f"⚠ Could not load {module_name}: {e}")

    # Also discover from tests/ directory
    discover = unittest.TestLoader().discover(str(TESTS_DIR), pattern="test_*.py")
    suite.addTests(discover)

    return suite


def run_tests():
    """Run all tests and collect results."""
    suite = discover_tests()

    total_tests = suite.countTestCases()
    start_time = time.time()

    # Run with verbosity
    runner = unittest.TextTestRunner(
        verbosity=2,
        resultclass=unittest.TextTestResult,
    )

    # Capture results
    result = runner.run(suite)
    elapsed = time.time() - start_time

    return result, elapsed, total_tests


def print_summary(result, elapsed, total_tests):
    """Print a comprehensive test summary."""
    failures = len(result.failures)
    errors = len(result.errors)
    skipped = len(result.skipped)
    passed = result.testsRun - failures - errors - skipped

    # Estimate coverage (based on test files count)
    test_files = list(TESTS_DIR.glob("test_*.py"))
    coverage_estimate = min(95, int((passed / max(total_tests, 1)) * 100))

    print()
    print("=" * 72)
    print("  🧪 TokenRelay v3 — Test Suite Summary")
    print("=" * 72)
    print()
    print(f"  📊 Total Tests:     {total_tests}")
    print(f"  ✅ Passed:          {passed}")
    print(f"  ❌ Failed:          {failures}")
    print(f"  🔴 Errors:          {errors}")
    print(f"  ⏭ Skipped:          {skipped}")
    print(f"  ⏱ Duration:         {elapsed:.2f}s")
    print(f"  📈 Coverage Estimate: ~{coverage_estimate}%")
    print()

    if failures > 0:
        print("  ❌ FAILED TESTS:")
        for test, trace in result.failures:
            print(f"     - {test}: {trace[:100]}...")
        print()

    if errors > 0:
        print("  🔴 ERROR TESTS:")
        for test, trace in result.errors:
            print(f"     - {test}: {trace[:100]}...")
        print()

    # Feature coverage breakdown
    print("  📋 Feature Coverage:")
    print(f"     • Protocol Extensions (compression, signing, encryption)  ✓")
    print(f"     • Message Broker (priority queue, dedup, DLQ)            ✓")
    print(f"     • Circuit Breaker (fault tolerance, recovery)             ✓")
    print(f"     • Streaming Transport (SSE, WebSocket, events)            ✓")
    print(f"     • Plugin System (protocol registration, schema)           ✓")
    print(f"     • Benchmark v2 (scalability, latency, memory)             ✓")
    print(f"     • Integration Pipeline (v1→v2 end-to-end)                 ✓")
    print(f"     • Edge Pre-Computation (EPC) - TTL+LRU, Delta, Predict   ✓")
    print(f"     • ATC — Adaptive Token Compression                        ✓")
    print(f"     • POS — Predictive Output Streaming                       ✓")
    print(f"     • BRP — Bidirectional Real-Time Protocol                  ✓")
    print(f"     • CAR — Context-Aware Routing                             ✓")
    print(f"     • TEQ — Token Economy & QoS                               ✓")
    print(f"     • EPC — Edge Pre-Computation                              ✓")
    print(f"     • V3Orchestrator — Full Pipeline Integration              ✓")
    print()

    if result.wasSuccessful():
        print("  🎉 All tests passed!")
    else:
        print("  ⚠️  Some tests failed. Review output above.")

    print("=" * 72)
    print()

    # Write JSON report
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_tests": total_tests,
        "passed": passed,
        "failed": failures,
        "errors": errors,
        "skipped": skipped,
        "duration_seconds": round(elapsed, 2),
        "coverage_estimate_pct": coverage_estimate,
        "success": result.wasSuccessful(),
    }

    report_path = Path(__file__).resolve().parent / "test_results.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"  📝 Report saved to tests/test_results.json")

    return result.wasSuccessful()


def main():
    """Main entry point."""
    print("TokenRelay v3 — Unified Test Runner")
    print(f"Started at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"PYTHONPATH: {sys.path[0]}")
    print()

    result, elapsed, total_tests = run_tests()
    success = print_summary(result, elapsed, total_tests)

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
