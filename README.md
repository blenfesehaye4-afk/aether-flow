# ⚙️ AetherFlow

[![CI](https://github.com/blenfesehaye4-afk/aether-flow/actions/workflows/ci.yml/badge.svg)](https://github.com/blenfesehaye4-afk/aether-flow/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **Lightweight, event-driven DAG workflow orchestration engine, durable SQLite checkpoints & async task runner in Python.**

AetherFlow is an embeddable pipeline orchestrator for Directed Acyclic Graphs (DAGs). It combines cycle detection, Kahn's topological sort, non-blocking asynchronous execution pools, parameter forwarding, ACID SQLite state persistence, workflow checkpoint resumption, and recurring cron-style interval scheduling.

---

## 🌟 What's New in v0.2.0

- 💾 **Durable SQLite State Store** ([`aether_flow/persistence.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/aether-flow/aether_flow/persistence.py)): Persistent storage for workflow runs, task checkpoints, outputs, and event streams. Survives process crashes.
- ⏯️ **Workflow Checkpoint Resumption**: Resume interrupted or failed pipelines (`engine.resume(dag, run_id)`). Completed tasks are loaded from disk and skipped; only failed or pending tasks re-run.
- ⏱️ **Interval Scheduler** ([`aether_flow/scheduler.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/aether-flow/aether_flow/scheduler.py)): Background interval scheduler for automated recurring pipeline executions.

---

## 🏗️ Architecture

```
          ┌─────────────┐
          │  Extract    │
          └──────┬──────┘
                 │
        ┌────────┴────────┐
        ▼                 ▼
 ┌──────────────┐  ┌──────────────┐
 │ Clean Users  │  │ Clean Orders │   (Fan-out: runs concurrently)
 └──────┬───────┘  └──────┬───────┘
        └────────┬────────┘
                 ▼
          ┌─────────────┐
          │  Join & Load│             (Fan-in: awaits all upstreams)
          └─────────────┘
                 │
                 ▼
 ┌───────────────────────────────┐
 │ SQLite Checkpoint Database    │ ➔ Stores run context, status, and task outputs
 └───────────────────────────────┘
```

---

## 💻 Python Quickstart (Checkpoint & Resumption)

```python
import asyncio
from aether_flow import DAG, Task, WorkflowEngine, SQLiteStateStore

# Initialize persistent SQLite store
store = SQLiteStateStore("pipeline_state.db")
engine = WorkflowEngine(state_store=store)

workflow = DAG("resilient_etl")

t1 = workflow.add_task(Task("step_1", lambda: "raw_data"))
t2 = workflow.add_task(Task("step_2", lambda step_1: step_1.upper()))
t1 >> t2

async def main():
    # 1. Execute workflow (auto-checkpoints each task)
    run = await engine.run(workflow)
    print(f"Run completed with status: {run.status}")

    # 2. If a pipeline fails midway, resume from the exact checkpoint:
    # resumed = await engine.resume(workflow, run.run_id)

asyncio.run(main())
```

---

## 🧪 Running Tests

```bash
pytest -v
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
