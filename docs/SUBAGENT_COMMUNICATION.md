
╔══════════════════════════════════════════════════════════════════════════╗
║           TOKENS RELAY — SUBAGENT DELEGATION COMMUNICATION             ║
║                    Server Status: RUNNING on port 8081                  ║
╚══════════════════════════════════════════════════════════════════════════╝

SERVER: TokenRelay v4.0.0 — http://localhost:8081/benchmark.html
STATUS: ✅ Healthy, all modules loaded (V3Orchestrator, EdgeCache, SwarmAgent)

══════════════════════════════════════════════════════════════════════════
HOW SUBAGENT DELEGATION WORKS
══════════════════════════════════════════════════════════════════════════

1. INTENT CLASSIFICATION
   The V3Orchestrator classifies the user's prompt into an intent:
   • query    → 2 subagents (research_agent, summarizer_agent)
   • command  → 3 subagents (coder_agent, reviewer_agent, optimizer_agent)
   • request  → 3 subagents (analyst_agent, planner_agent, executor_agent)
   • feedback → 2 subagents (evaluator_agent, improver_agent)
   • system   → 1 subagent  (coordinator_agent)

2. SWARM COORDINATOR CREATION
   A SwarmCoordinator is instantiated and specialist subagents are
   registered based on the intent classification. Each agent has a
   specific role and capability vector.

3. TASK DELEGATION
   The prompt is assigned as a task to each subagent via:
     coordinator.register_agent(agent)  → agent.assign_task(prompt)
   
   Each agent independently processes the prompt and stores its
   output in its task_queue.

4. COORDINATOR MERGE
   The coordinator collects all agent outputs and merges them.
   The merged output is prefixed with "OPT:" or "v3:" to create
   a compressed prompt that is different from the Traditional LLM's
   cache key.

5. LLM CALL
   The compressed prompt is sent to the LLM (localhost:20128) with
   reasoning_effort: "none", max_tokens: 2000.
   The LLM returns the final response.

6. REINFORCEMENT LEARNING
   ReinforcementLearner records the interaction (prompt, token counts,
   execution times) for future optimization of agent count and types.

══════════════════════════════════════════════════════════════════════════
BENCHMARK RESULTS
══════════════════════════════════════════════════════════════════════════

QUERY PROMPT: "What is the capital of France and explain..."
  Intent: query (confidence: 1.0)
  Subagents: 2 → research_agent, summarizer_agent
  Traditional: 2242 tokens | 1497ms
  TokenRelay:  2572 tokens | 2081ms
  V3 Pipeline: ATC→CAR→BRP→POS→TEQ→EPC (8 steps)
  Cache: MISS

COMMAND PROMPT: "Write a Python script to sort a list..."
  Intent: command (confidence: 0.67)
  Subagents: 3 → coder_agent, reviewer_agent, optimizer_agent
  Traditional: 3108 tokens | 3467ms
  TokenRelay:  3013 tokens | 2998ms
  Token Savings: ~3% (3 subagents reduced tokens vs Traditional)
  V3 Pipeline: ATC→CAR→BRP→POS→TEQ→EPC (8 steps)
  Cache: MISS

══════════════════════════════════════════════════════════════════════════
COMMUNICATION FLOW DIAGRAM
══════════════════════════════════════════════════════════════════════════

USER PROMPT
    │
    ▼
┌─────────────────┐
│  V3Orchestrator │  ← Intent Classification (query/command/request)
│  Intent Classifier│
└────────┬────────┘
         │ intent
         ▼
┌─────────────────┐
│ SwarmCoordinator│  ← Creates agent pool based on intent
└────────┬────────┘
         │
    ┌────┼────┐
    │         │
    ▼         ▼
┌────────┐ ┌────────┐
│research│ │summariz│  ← 2 agents for query
│_agent  │ │_agent  │
└───┬────┘ └───┬────┘
    │          │
    │  assign_task(prompt)
    │          │
    ▼          ▼
┌─────────────────┐
│  task_queue     │  ← Each agent processes the prompt
│  (independent)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Coordinator    │  ← Merges all agent outputs
│  Merge Layer    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Compressed     │  ← "v3:" prefix to avoid cache collision
│  Prompt         │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  LLM API        │  ← reasoning_effort: "none"
│  localhost:20128│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Final Response │  ← HTML returned
└─────────────────┘

══════════════════════════════════════════════════════════════════════════
KEY ARCHITECTURAL COMPONENTS
══════════════════════════════════════════════════════════════════════════

• _relay_pipeline()  — Main subagent delegation method in server.py
• SwarmCoordinator   — Manages agent pool, task distribution, merging
• SwarmAgent         — Individual specialist agents with roles
• ReinforcementLearner — Tracks interactions for agent optimization
• V3Orchestrator     — Handles ATC→CAR→BRP→POS→TEQ→EPC pipeline
• EdgeCache          — Caches responses (50 max entries, 1hr TTL)
• _get_swarm()       — Lazy import helper for swarm modules
• _get_selflearning() — Lazy import helper for learning module

══════════════════════════════════════════════════════════════════════════
SUBAGENT CONFIGURATION BY INTENT
══════════════════════════════════════════════════════════════════════════

Intent    | Agents              | Count | Purpose
──────────┼─────────────────────┼───────┼─────────────────────────────
query     │ research_agent      │  2    │ Research + Summarize
          │ summarizer_agent    │       │
──────────┼─────────────────────┼───────┼─────────────────────────────
command   │ coder_agent         │  3    │ Code + Review + Optimize
          │ reviewer_agent      │       │
          │ optimizer_agent     │       │
──────────┼─────────────────────┼───────┼─────────────────────────────
request   │ analyst_agent       │  3    │ Analyze + Plan + Execute
          │ planner_agent       │       │
          │ executor_agent      │       │
──────────┼─────────────────────┼───────┼─────────────────────────────
feedback  │ evaluator_agent     │  2    │ Evaluate + Improve
          │ improver_agent      │       │
──────────┼─────────────────────┼───────┼─────────────────────────────
system    │ coordinator_agent   │  1    │ Coordinates everything

══════════════════════════════════════════════════════════════════════════
NOTES
══════════════════════════════════════════════════════════════════════════

• Subagents are lazily loaded via _get_swarm() — zero module import
  overhead at server startup
• The "v3:" prefix on compressed_text ensures different cache keys
  between Traditional LLM and TokenRelay paths
• ReinforcementLearner records interactions for future agent count
  optimization (finding the "sweet spot")
• SwarmAgent instances are created fresh per request and cleaned up
  via gc.collect() to prevent memory leaks
• The server uses 400MB memory limit with EdgeCache max_entries=50
• All 8 V3 pipeline stages execute: ATC→CAR→BRP→POS→TEQ→EPC→cache→output
