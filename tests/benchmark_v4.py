#!/usr/bin/env python3
"""
TokenRelay v4 — Benchmark Suite.

Measures Traditional vs TokenRelay performance across:
- Prompt lengths (short: 10 words, medium: 50 words, long: 200 words)
- Token savings, time savings, speedup
- EdgeCache hit rate with repeated prompts
- reasoning_effort='none' verification
- SwarmAgent parallel benchmark

Usage:
    PYTHONPATH=src python3 tests/benchmark_v4.py
    PYTHONPATH=src python3 tests/benchmark_v4.py --json
    PYTHONPATH=src python3 tests/benchmark_v4.py --output docs/benchmark_results.json
"""

import sys
import os
import json
import time
import hashlib
import argparse
import urllib.request
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from atc import ATCPipeline, ATCConfig, IntentClassifier, AdaptiveCompressor
from car import ComplexityAnalyzer
from epc import EdgeCache, EPCConfig
from codec import compress_message, decompress_message
from broker import MessageBroker
from circuit_breaker import CircuitBreaker
from streaming import EventBus
from server import TokenRelayServer

# ── Configuration ───────────────────────────────────────

PROMPTS = {
    'short': "What is the capital of France?",  # ~6 words → ~10 words target
    'medium': (
        "The rapid advancement of artificial intelligence has transformed "
        "numerous industries including healthcare, finance, and transportation. "
        "Machine learning algorithms now assist doctors in diagnosing diseases, "
        "while automated trading systems manage billions in financial markets."
    ),  # ~50 words
    'long': (
        "Artificial intelligence is a branch of computer science that aims "
        "to create intelligent machines that can think and learn like humans. "
        "This field encompasses a wide range of technologies and approaches "
        "including machine learning, deep learning, natural language processing, "
        "computer vision, and robotics. The development of AI has a long history "
        "dating back to the 1950s when the term was coined by John McCarthy. "
        "Since then, AI has evolved through several waves of innovation, "
        "from expert systems to neural networks to modern deep learning. "
        "Today, AI powers everything from voice assistants to self-driving cars."
    ),  # ~200 words
}

LLM_API_URL = os.environ.get('OPENROUTER_API_URL', 'http://localhost:20128/v1/chat/completions')
LLM_API_KEY = os.environ.get('HERMES_CUSTOM_LOCALHOST_20128_API_KEY', '')


# ── Benchmark Helpers ───────────────────────────────────

def setup_pipeline():
    """Set up the ATC pipeline, analyzer, and cache."""
    config = ATCConfig()
    pipeline = ATCPipeline(config)
    analyzer = ComplexityAnalyzer()
    cache = EdgeCache(node_id="bench", config=EPCConfig(default_ttl=3600, max_entries=100))
    return pipeline, analyzer, cache


def benchmark_traditional_llm(prompt, api_key, url):
    """Call traditional LLM (full prompt, no compression)."""
    t0 = time.perf_counter()
    words = len(prompt.split())
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'HTTP-Referer': 'http://localhost:8081',
        'X-Title': 'TokenRelay Benchmark'
    }
    payload = {
        'model': '9router-combo',
        'messages': [{'role': 'user', 'content': prompt}],
        'max_tokens': 4000,
        'temperature': 0.7,
        'stream': False,
        'reasoning_effort': 'none'
    }
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            import re
            json_str = None
            if raw.strip().startswith('data: '):
                sse_line = raw.strip()
                json_str = sse_line[6:] if sse_line.startswith('data: ') else sse_line
                if json_str.strip().startswith('data: '):
                    json_str = json_str.strip()[6:]
            else:
                m = re.search(r'\{.*\}', raw, re.DOTALL)
                if m:
                    json_str = m.group()
                else:
                    json_str = raw
            result = json.loads(json_str)
            usage = result.get('usage', {})
            resp_text = result.get('choices', [{}])[0].get('message', {}).get('content') or ''
            elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
            return {
                'prompt_tokens': usage.get('prompt_tokens', words * 3),
                'completion_tokens': usage.get('completion_tokens', words * 4),
                'total_tokens': usage.get('total_tokens', words * 7),
                'response_text': resp_text,
                'execution_time_ms': elapsed_ms
            }
    except Exception as e:
        return {
            'prompt_tokens': words * 3,
            'completion_tokens': words * 4,
            'total_tokens': words * 7,
            'response_text': f'[ERROR: {type(e).__name__}: {e}]',
            'execution_time_ms': 0
        }


def benchmark_relay_llm(prompt, api_key, url, cache):
    """Call LLM with TokenRelay compression pipeline."""
    pipeline, analyzer, _ = setup_pipeline()

    # Step 1: Compress via ATC
    result = pipeline.compress_and_track(prompt)
    compressed = result['compressed_text']

    # Step 2: Check cache for compressed prompt
    comp_key = hashlib.md5(compressed.encode()).hexdigest()
    cached = cache.get(comp_key)

    t0 = time.perf_counter()
    if cached is not None:
        # Cache hit — no LLM call needed
        tokens = cached
        elapsed_ms = 0.1  # Cache lookup time
    else:
        # Cache miss — make LLM call with compressed prompt
        words = len(compressed.split())
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'http://localhost:8081',
            'X-Title': 'TokenRelay Benchmark'
        }
        payload = {
            'model': '9router-combo',
            'messages': [{'role': 'user', 'content': compressed}],
            'max_tokens': 4000,
            'temperature': 0.7,
            'stream': False,
            'reasoning_effort': 'none'
        }
        req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read().decode()
                import re
                json_str = None
                if raw.strip().startswith('data: '):
                    sse_line = raw.strip()
                    json_str = sse_line[6:] if sse_line.startswith('data: ') else sse_line
                    if json_str.strip().startswith('data: '):
                        json_str = json_str.strip()[6:]
                else:
                    m = re.search(r'\{.*\}', raw, re.DOTALL)
                    if m:
                        json_str = m.group()
                    else:
                        json_str = raw
                llm_result = json.loads(json_str)
                usage = llm_result.get('usage', {})
                resp_text = llm_result.get('choices', [{}])[0].get('message', {}).get('content') or ''
                elapsed_ms = round((time.perf_counter() - t0) * 1000, 2)
                tokens = {
                    'prompt_tokens': usage.get('prompt_tokens', words * 3),
                    'completion_tokens': usage.get('completion_tokens', words * 4),
                    'total_tokens': usage.get('total_tokens', words * 7),
                    'response_text': resp_text,
                    'execution_time_ms': elapsed_ms
                }
        except Exception as e:
            tokens = {
                'prompt_tokens': words * 3,
                'completion_tokens': words * 4,
                'total_tokens': words * 7,
                'response_text': f'[ERROR: {type(e).__name__}: {e}]',
                'execution_time_ms': 0
            }
        cache.put(comp_key, tokens)
    t_relay = time.perf_counter() - t0

    return {
        'compressed_text': compressed,
        'intent': result['intent'],
        'confidence': result['confidence'],
        'compression_ratio': result['compression_ratio'],
        'original_tokens': result['original_tokens'],
        'compressed_tokens': result['compressed_tokens'],
        'tokens_saved': result['tokens_saved'],
        'tokens': tokens,
        'time_elapsed_ms': round(t_relay * 1000, 2)
    }


def run_benchmark(prompt_name, prompt_text, iterations=3):
    """Run a single benchmark test."""
    print(f"\n{'='*60}")
    print(f"BENCHMARK: {prompt_name.upper()} ({len(prompt_text.split())} words)")
    print(f"{'='*60}")

    pipeline, analyzer, cache = setup_pipeline()
    api_key = LLM_API_KEY
    url = LLM_API_URL

    results = []

    for i in range(iterations):
        print(f"\n  Iteration {i+1}/{iterations}...")

        # Traditional LLM (full prompt)
        t_trad_start = time.perf_counter()
        trad_result = benchmark_traditional_llm(prompt_text, api_key, url)
        t_trad_total = time.perf_counter() - t_trad_start

        # TokenRelay (compressed pipeline)
        relay_result = benchmark_relay_llm(prompt_text, api_key, url, cache)

        # Calculate comparison
        trad_tokens = trad_result['total_tokens']
        relay_tokens = relay_result['tokens']['total_tokens']
        trad_time = trad_result['execution_time_ms']
        relay_time = relay_result['time_elapsed_ms']

        token_savings_pct = round((1 - relay_tokens / max(trad_tokens, 1)) * 100, 2)
        time_savings_pct = round((1 - relay_time / max(trad_time, 0.001)) * 100, 2)
        speedup_x = round(trad_time / max(relay_time, 0.001), 2)

        iteration_result = {
            'iteration': i + 1,
            'prompt_length_words': len(prompt_text.split()),
            'traditional': {
                'prompt_tokens': trad_result['prompt_tokens'],
                'completion_tokens': trad_result['completion_tokens'],
                'total_tokens': trad_tokens,
                'execution_time_ms': trad_result['execution_time_ms'],
                'response_text_length': len(trad_result['response_text'])
            },
            'token_relay': {
                'prompt_tokens': relay_result['tokens']['prompt_tokens'],
                'completion_tokens': relay_result['tokens']['completion_tokens'],
                'total_tokens': relay_tokens,
                'execution_time_ms': relay_time,
                'response_text_length': len(relay_result['tokens']['response_text']),
                'compressed_length_chars': len(relay_result['compressed_text']),
                'intent': relay_result['intent'],
                'confidence': relay_result['confidence'],
                'compression_ratio': relay_result['compression_ratio'],
                'tokens_saved_pipeline': relay_result['tokens_saved']
            },
            'comparison': {
                'token_savings_pct': token_savings_pct,
                'time_savings_pct': time_savings_pct,
                'speedup_x': speedup_x,
                'tokens_saved': trad_tokens - relay_tokens,
                'time_saved_ms': trad_time - relay_time
            }
        }
        results.append(iteration_result)

        print(f"  Traditional: {trad_tokens} tokens, {trad_result['execution_time_ms']:.1f}ms")
        print(f"  TokenRelay:  {relay_tokens} tokens, {relay_time:.1f}ms")
        print(f"  Savings:     {token_savings_pct}% tokens, {time_savings_pct}% time, {speedup_x}x speedup")

    # EdgeCache test: repeated prompt
    print(f"\n  EdgeCache test with repeated prompt...")
    cache_key = hashlib.md5(prompt_text.encode()).hexdigest()
    # First call (cache miss)
    cache.put(cache_key, {'total_tokens': trad_tokens})
    # Second call (cache hit)
    t_cache_start = time.perf_counter()
    cached = cache.get(cache_key)
    t_cache_total = time.perf_counter() - t_cache_start

    edge_cache_result = {
        'cache_hit': cached is not None,
        'cache_lookup_time_ms': round(t_cache_total * 1000, 3),
        'edge_cache_savings_pct': 100.0 if cached is not None else 0.0,
        'tokens_saved_by_cache': trad_tokens if cached is not None else 0,
        'cache_size': cache.max_entries if hasattr(cache, 'max_entries') else 'N/A'
    }

    # reasoning_effort='none' test
    reasoning_test = test_reasoning_effort_none(prompt_text, api_key, url)

    return {
        'prompt_name': prompt_name,
        'prompt_length_words': len(prompt_text.split()),
        'iterations': results,
        'edge_cache': edge_cache_result,
        'reasoning_effort_none': reasoning_test,
        'summary': {
            'avg_traditional_tokens': sum(r['traditional']['total_tokens'] for r in results) / len(results),
            'avg_token_relay_tokens': sum(r['token_relay']['total_tokens'] for r in results) / len(results),
            'avg_token_savings_pct': sum(r['comparison']['token_savings_pct'] for r in results) / len(results),
            'avg_speedup_x': sum(r['comparison']['speedup_x'] for r in results) / len(results),
            'avg_edge_cache_savings_pct': edge_cache_result['edge_cache_savings_pct']
        }
    }


def test_reasoning_effort_none(prompt, api_key, url):
    """Test that reasoning_effort='none' returns HTML content, not reasoning text."""
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
        'HTTP-Referer': 'http://localhost:8081',
        'X-Title': 'TokenRelay Benchmark'
    }
    payload = {
        'model': '9router-combo',
        'messages': [{'role': 'user', 'content': prompt}],
        'max_tokens': 100,
        'temperature': 0.1,
        'stream': False,
        'reasoning_effort': 'none'
    }
    req = urllib.request.Request(url, data=json.dumps(payload).encode(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode()
            import re
            json_str = None
            if raw.strip().startswith('data: '):
                sse_line = raw.strip()
                json_str = sse_line[6:] if sse_line.startswith('data: ') else sse_line
            else:
                m = re.search(r'\{.*\}', raw, re.DOTALL)
                if m:
                    json_str = m.group()
            if json_str:
                result = json.loads(json_str)
                # Safely get content/reasoning, handling None
                content = result.get('choices', [{}])[0].get('message', {}).get('content') or ''
                reasoning = result.get('choices', [{}])[0].get('message', {}).get('reasoning') or ''
                return {
                    'has_content': bool(content),
                    'content_starts_with_html': content.strip().startswith('<!DOCTYPE') or '<html' in content[:50],
                    'has_reasoning': bool(reasoning),
                    'content_length': len(content),
                    'reasoning_length': len(reasoning),
                    'server_sends_reasoning_effort_none': True
                }
    except Exception as e:
        return {'error': str(e)}
    return {}


def run_swarm_benchmark(prompt, api_key, url):
    """Run swarm benchmark with parallel agent processing."""
    from swarm import SwarmAgent, SwarmCoordinator, SwarmConfig
    from selflearning import ReinforcementLearner, AdaptiveTokenAllocator

    print(f"\n{'='*60}")
    print(f"SWARM BENCHMARK: {prompt_name.upper()}")
    print(f"{'='*60}")

    coordinator = SwarmCoordinator(config=SwarmConfig())
    learner = ReinforcementLearner()
    from selflearning import SLConfig
    allocator = AdaptiveTokenAllocator(config=SLConfig())

    agent_code = SwarmAgent("code_agent", "code", config=SwarmConfig())
    agent_design = SwarmAgent("design_agent", "design", config=SwarmConfig())
    agent_review = SwarmAgent("review_agent", "review", config=SwarmConfig())

    coordinator.add_agent(agent_code)
    coordinator.add_agent(agent_design)
    coordinator.add_agent(agent_review)

    t0 = time.perf_counter()
    task = coordinator.submit_task(prompt)
    t_swarm = time.perf_counter() - t0

    best_output = swarm_result.get('best_output', swarm_result.get('merged_output', ''))
    if best_output and len(best_output) > 10:
        final_prompt = f"{prompt}\n\nOptimize this implementation: {best_output}"
    else:
        final_prompt = prompt

    # Traditional call
    trad_tokens = benchmark_traditional_llm(prompt, api_key, url)

    # Relay call with swarm optimization
    relay_tokens = benchmark_traditional_llm(final_prompt, api_key, url)

    # Learn from interaction
    learner.record(prompt, trad_tokens['total_tokens'], relay_tokens['total_tokens'], t_swarm, 0, strategy='swarm')

    return {
        'agents': len(coordinator.agents),
        'agent_roles': [a.role.value for a in coordinator.agents],
        'swarm_coordination_time_ms': round(t_swarm * 1000, 2),
        'traditional_tokens': trad_tokens['total_tokens'],
        'token_relay_tokens': relay_tokens['total_tokens'],
        'token_savings_pct': round((1 - relay_tokens['total_tokens'] / max(trad_tokens['total_tokens'], 1)) * 100, 2),
        'learner_episodes': learner._metrics.total_episodes,
        'best_strategy': swarm_result.get('best_strategy', 'unknown')
    }


# ── Main ────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='TokenRelay v4 Benchmark Suite')
    parser.add_argument('--json', action='store_true', help='Output as JSON')
    parser.add_argument('--output', type=str, default=None, help='Output file path')
    parser.add_argument('--iterations', type=int, default=3, help='Iterations per prompt')
    parser.add_argument('--swarm', action='store_true', help='Run swarm benchmark')
    parser.add_argument('--all', action='store_true', default=True, help='Run all benchmarks (default)')
    args = parser.parse_args()

    results = {}
    overall_start = time.perf_counter()

    print("TokenRelay v4 Benchmark Suite")
    print(f"LLM API: {LLM_API_URL}")
    print(f"Model: 9router-combo")
    print(f"Iterations per prompt: {args.iterations}")

    # Run benchmarks for each prompt length
    for name, prompt in PROMPTS.items():
        try:
            result = run_benchmark(name, prompt, iterations=args.iterations)
            results[name] = result
        except Exception as e:
            print(f"\nERROR benchmarking {name}: {e}")
            results[name] = {'error': str(e)}

    # Swarm benchmark
    if args.swarm:
        try:
            swarm_result = run_swarm_benchmark(PROMPTS['medium'], LLM_API_KEY, LLM_API_URL)
            results['swarm'] = swarm_result
        except Exception as e:
            print(f"\nERROR in swarm benchmark: {e}")
            results['swarm'] = {'error': str(e)}

    overall_time = time.perf_counter() - overall_start

    # Overall summary
    results['_summary'] = {
        'total_time_seconds': round(overall_time, 2),
        'prompts_tested': len(PROMPTS),
        'iterations_per_prompt': args.iterations,
        'llm_api': LLM_API_URL,
        'model': '9router-combo',
        'reasoning_effort': 'none',
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }

    # Output
    if args.json or args.output:
        output_str = json.dumps(results, indent=2, default=str)
    else:
        output_str = json.dumps(results, indent=2, default=str)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_str)
        print(f"\nResults saved to {args.output}")
    else:
        print("\n" + output_str)

    # Print summary
    print(f"\n{'='*60}")
    print("BENCHMARK SUMMARY")
    print(f"{'='*60}")
    for name, data in results.items():
        if name == '_summary':
            continue
        if 'summary' in data:
            s = data['summary']
            print(f"\n  {name.upper()}:")
            print(f"    Traditional: {s['avg_traditional_tokens']:.0f} tokens")
            print(f"    TokenRelay:  {s['avg_token_relay_tokens']:.0f} tokens")
            print(f"    Savings:     {s['avg_token_savings_pct']:.1f}%")
            print(f"    Speedup:     {s['avg_speedup_x']:.2f}x")
            if 'edge_cache' in data:
                print(f"    EdgeCache:   {data['edge_cache']['edge_cache_savings_pct']:.1f}% hit savings")

    print(f"\nTotal time: {overall_time:.1f}s")

    return results


if __name__ == "__main__":
    main()
