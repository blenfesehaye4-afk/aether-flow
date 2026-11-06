# ⚙️ AetherFlow

[![CI](https://github.com/blenfesehaye4-afk/aether-flow/actions/workflows/ci.yml/badge.svg)](https://github.com/blenfesehaye4-afk/aether-flow/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **Lightweight, event-driven DAG workflow orchestration engine & async task runner in Python.**

AetherFlow is an embeddable pipeline orchestrator for Directed Acyclic Graphs (DAGs). It combines cycle detection, Kahn's topological sort, non-blocking asynchronous execution pools, parameter forwarding, event-sourced audit states, and exponential retry policies.

---

## 🌟 Key Architecture & Features

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
```

- 🔄 **Intuitive Bitshift DSL**: Compose complex DAG dependencies naturally with `task_a >> [task_b, task_c] >> task_d`.
- 🛡️ **Graph Verification & Cycle Detection**: Pre-flight validation detects missing dependencies and loops using 3-color DFS.
- ⚡ **Asyncio & Thread Pooling**: First-class support for both async `async def` coroutines and CPU-bound synchronous functions.
- 🔁 **Resilient Retries & Timeouts**: Configurable retry policies with exponential backoff and execution timeout guards.
- 📦 **Automated Context & Data Pipelining**: Upstream task return values are automatically forwarded to matching downstream function parameters.
- 📊 **Event-Sourced Audit Logs**: Granular lifecycle events (`TASK_SCHEDULED`, `TASK_STARTED`, `TASK_COMPLETED`, `TASK_FAILED`, `TASK_RETRYING`).
- 🖥️ **Rich CLI**: Visualize workflow dependency trees and inspect execution logs directly in your terminal.

---

## 📦 Installation

```bash
pip install aether-flow
```

Or install in development mode from source:

```bash
git clone https://github.com/blenfesehaye4-afk/aether-flow.git
cd aether-flow
pip install -e .[dev]
```

---

## 💻 Python Quickstart

Create a workflow file (e.g., `pipeline.py`):

```python
import asyncio
from aether_flow import DAG, Task, RetryPolicy

# Define workflow
workflow = DAG("analytics_pipeline", name="Daily Analytics")

# Define tasks
async def fetch_metrics():
    await asyncio.sleep(0.05)
    return {"active_users": 1420, "revenue": 52300}

def score_metrics(fetch_metrics: dict):
    return fetch_metrics["revenue"] / fetch_metrics["active_users"]

def notify_slack(score_metrics: float):
    print(f"Daily ARPU: ${score_metrics:.2f}")
    return True

t_fetch = workflow.add_task(Task("fetch_metrics", fetch_metrics))
t_score = workflow.add_task(Task("score_metrics", score_metrics))
t_notify = workflow.add_task(Task("notify_slack", notify_slack))

# Chain dependencies
t_fetch >> t_score >> t_notify

# Execute
if __name__ == "__main__":
    from aether_flow import WorkflowEngine
    engine = WorkflowEngine(max_concurrency=4)
    run = asyncio.run(engine.run(workflow))
    print(f"Status: {run.status}")
```

---

## 🖥️ Command-Line Interface (CLI)

### 1. Visualize DAG Graph Tree
```bash
aether-flow graph pipeline.py
```
```text
╭──────────────── Workflow Dependency Graph ────────────────╮
│ DAG: Daily Analytics (3 tasks)                            │
│ └── fetch_metrics (fetch_metrics) (root)                  │
│     └── score_metrics (score_metrics)                     │
│         └── notify_slack (notify_slack)                   │
╰───────────────────────────────────────────────────────────╯
```

### 2. Execute Workflow
```bash
aether-flow run pipeline.py --concurrency 4
```

---

## 🧪 Running Tests

```bash
pytest -v
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
