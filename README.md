# ⚙️ AetherFlow

[![CI](https://github.com/blenfesehaye4-afk/aether-flow/actions/workflows/ci.yml/badge.svg)](https://github.com/blenfesehaye4-afk/aether-flow/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **Distributed DAG workflow orchestration engine, 5-part cron scheduler, resilient task retries & real-time web visualizer in Python.**

AetherFlow is an enterprise-grade pipeline orchestrator and asynchronous task execution engine for Directed Acyclic Graphs (DAGs). It combines cycle detection, Kahn's topological sort, prioritized task scheduling, full jitter exponential retries, dead-letter fallbacks, resource concurrency throttling, durable SQLite state persistence, workflow checkpoint resumption, standard 5-part cron scheduling, and an interactive real-time Web DAG visualization dashboard.

---

## 🌟 What's New in v0.3.0

- 🌐 **Interactive Web DAG Visualizer & Live Dashboard** ([`aether_flow/dashboard.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/aether-flow/aether_flow/dashboard.py)):
  - Built-in FastAPI/HTML5 real-time monitoring dashboard with SVG node layouts.
  - Live execution tracking (Pending, Running, Completed, Failed, Retrying, Skipped).
  - REST API endpoints for DAG inspection, manual run triggering, and one-click checkpoint resumption.
- ⏱️ **Standard 5-Field Cron Scheduler** ([`aether_flow/cron.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/aether-flow/aether_flow/cron.py)):
  - Zero-dependency cron syntax parser (`min hour dom month dow`) supporting `*`, `*/n`, `n-m`, and lists.
  - Next fire-time calculation and automated background scheduling (`scheduler.schedule_cron(dag, "*/15 * * * *")`).
- 🛡️ **Advanced Task Retries & Jitter Backoffs** ([`aether_flow/retries.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/aether-flow/aether_flow/retries.py)):
  - Backoff strategies: `FIXED`, `LINEAR`, `EXPONENTIAL`, `FULL_JITTER`, and `EQUAL_JITTER`.
  - Granular exception filtering (`retry_on=[ConnectionError]`, `dont_retry_on=[ValueError]`).
  - Graceful dead-letter fallback callbacks (`fallback=lambda exc, ctx: ...`).
- ⚡ **Priority Task Queue & Resource Concurrency Throttling** ([`aether_flow/workers.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/aether-flow/aether_flow/workers.py)):
  - Strict priority-based task dispatch (`CRITICAL`, `HIGH`, `NORMAL`, `LOW`).
  - Tag-based concurrency limiters (e.g. rate-limit external LLM/API calls to 2 concurrent tasks while running compute tasks up to global limits).

---

## 🏗️ Architecture

```
[ Trigger Event / Cron Scheduler (*/5 * * * *) ]
                        │
                        ▼
           ┌────────────────────────┐
           │ Workflow Engine & DAG  │
           └───────────┬────────────┘
                       │
       ┌───────────────┼───────────────┐
       ▼               ▼               ▼
┌──────────────┐┌──────────────┐┌──────────────┐
│ Priority Q   ││ Concurrency  ││ Web DAG UI   │
│ (CRITICAL ➔) ││ Limiter      ││ (Live Visual)│
└──────┬───────┘└──────┬───────┘└──────┬───────┘
       │               │               │
       └───────────────┼───────────────┘
                       │
             [ Worker Execution Pool ]
          ┌─────────────────────────────┐
          │ Step 1: Ingest (Root)       │
          └──────────────┬──────────────┘
                         │
                ┌────────┴────────┐
                ▼                 ▼
         ┌──────────────┐  ┌──────────────┐
         │ Embeddings   │  │ Anomaly Check│ (Concurrent)
         │ (Jitter Back)│  │ [CRITICAL]   │
         └──────┬───────┘  └──────┬───────┘
                └────────┬────────┘
                         ▼
          ┌─────────────────────────────┐
          │ Step 3: Consolidate & Load  │
          └──────────────┬──────────────┘
                         ▼
          ┌─────────────────────────────┐
          │ SQLite State & Checkpoints  │ ➔ Resumption & Event Auditing
          └─────────────────────────────┘
```

---

## 💻 Python Quickstart

### 1. Advanced Retries, Jitter & Fallbacks

```python
import asyncio
from aether_flow import DAG, Task, WorkflowEngine, AdvancedRetryPolicy, BackoffStrategy

dag = DAG("resilient_ai_pipeline")

attempt = 0
def flaky_api_call():
    global attempt
    attempt += 1
    if attempt < 3:
        raise ConnectionError("Upstream service timeout")
    return "API response data"

# Configure exponential backoff with full jitter and fallback
policy = AdvancedRetryPolicy(
    max_retries=3,
    initial_delay=0.1,
    backoff_factor=2.0,
    strategy=BackoffStrategy.FULL_JITTER,
    fallback=lambda exc, ctx: "Fallback Cached Result",
)

dag.add_task(Task("api_step", flaky_api_call, retries=policy))

async def main():
    engine = WorkflowEngine()
    run = await engine.run(dag)
    print("Workflow Status:", run.status)
    print("Output:", run.results["api_step"].output)

asyncio.run(main())
```

### 2. Cron Scheduling & Priority Queues

```python
import asyncio
from aether_flow import DAG, Task, TaskPriority, WorkflowEngine, WorkflowScheduler

engine = WorkflowEngine(max_concurrency=2, group_limits={"database": 1})
scheduler = WorkflowScheduler(engine)

cron_dag = DAG("nightly_sync", cron="0 2 * * *")

t_high = cron_dag.add_task(Task("critical_sync", lambda: "sync completed", priority=TaskPriority.CRITICAL))
t_norm = cron_dag.add_task(Task("audit_log", lambda: "logged", priority=TaskPriority.NORMAL))

# Schedule recurring execution every 15 minutes
scheduler.schedule_cron(cron_dag, "*/15 * * * *", on_complete=lambda r: print(f"Run {r.run_id} finished."))
```

### 3. Launching the Web DAG Dashboard

```python
from aether_flow import DAG, Task, WorkflowEngine, register_dag, set_dashboard_engine
from aether_flow.dashboard import app
import uvicorn

dag = DAG("etl_demo", name="ETL Demo Pipeline")
t1 = dag.add_task(Task("extract", lambda: [1, 2, 3]))
t2 = dag.add_task(Task("transform", lambda extract: [x * 10 for x in extract]))
t1 >> t2

engine = WorkflowEngine()
set_dashboard_engine(engine, [dag])

# Run with: uvicorn aether_flow.dashboard:app --host 127.0.0.1 --port 8500
```

---

## 🖥️ Command-Line Interface (CLI)

```bash
# Start the interactive web DAG visualizer and dashboard
aether-flow ui --port 8500

# Run rich terminal demonstration showcasing priorities, jitter, and fallbacks
aether-flow demo

# Display ASCII dependency tree of a workflow
aether-flow graph pipeline.py

# Execute workflow defined in a Python script
aether-flow run pipeline.py --concurrency 4
```

---

## 🌐 Dashboard REST API Endpoints

| Method | Endpoint | Description |
|:---|:---|:---|
| `GET` | `/health` | Service health and active version check |
| `GET` | `/` | Real-time interactive HTML5/SVG DAG visualizer |
| `GET` | `/api/dags` | List all registered DAGs, node topologies, and priorities |
| `POST` | `/api/dags/{id}/run` | Asynchronously trigger execution of a workflow DAG |
| `GET` | `/api/runs` | List historical execution records and summaries |
| `GET` | `/api/runs/{run_id}` | Detailed task results, durations, and event audit trails |
| `POST` | `/api/runs/{run_id}/resume` | Resume failed/interrupted run from checkpoint |

---

## 🧪 Running Tests

```bash
pytest -v
```

All 20 unit and integration tests validate DAG topology, cycle detection, Kahn's topological sort, SQLite checkpoints, jitter retry backoffs, cron expression triggers, priority queues, and dashboard REST endpoints.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
