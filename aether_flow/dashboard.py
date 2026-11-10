"""Interactive Web DAG Visualizer & Real-Time Workflow Monitoring Dashboard."""

import asyncio
from typing import Dict, List, Optional
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .engine import WorkflowEngine
from .models import DAG, TaskStatus
from .state import StateStore, WorkflowRun

app = FastAPI(title="AetherFlow DAG Dashboard", version="0.3.0")

# In-memory registry of DAGs and default engine
_DAG_REGISTRY: Dict[str, DAG] = {}
_DEFAULT_ENGINE: Optional[WorkflowEngine] = None


def set_dashboard_engine(engine: WorkflowEngine, dags: Optional[List[DAG]] = None) -> None:
    """Register the engine and DAGs to be monitored on the web UI."""
    global _DEFAULT_ENGINE
    _DEFAULT_ENGINE = engine
    if dags:
        for d in dags:
            _DAG_REGISTRY[d.id] = d


def register_dag(dag: DAG) -> None:
    _DAG_REGISTRY[dag.id] = dag


class TriggerRunRequest(BaseModel):
    initial_context: Optional[dict] = None


@app.get("/health")
def health():
    return {"status": "healthy", "service": "aether-flow", "version": "0.3.0"}


@app.get("/api/dags")
def list_dags():
    """List all registered DAGs with task nodes and dependencies."""
    result = []
    for dag_id, dag in _DAG_REGISTRY.items():
        tasks_data = [
            {
                "id": t.id,
                "name": t.name,
                "dependencies": list(t.dependencies),
                "priority": t.priority.name,
                "concurrency_group": t.concurrency_group,
                "timeout": t.timeout,
            }
            for t in dag.tasks.values()
        ]
        result.append({
            "id": dag.id,
            "name": dag.name,
            "description": dag.description,
            "cron": dag.cron,
            "task_count": len(dag.tasks),
            "tasks": tasks_data,
        })
    return result


@app.get("/api/runs")
def list_runs():
    """List historical workflow executions."""
    engine = _DEFAULT_ENGINE or WorkflowEngine()
    runs = engine.store.list_runs()
    return [
        {
            "run_id": r.run_id,
            "dag_id": r.dag_id,
            "status": r.status,
            "started_at": r.started_at.isoformat() if r.started_at else None,
            "completed_at": r.completed_at.isoformat() if r.completed_at else None,
            "task_count": len(r.results),
            "completed_tasks": sum(1 for res in r.results.values() if res.status == TaskStatus.COMPLETED),
            "failed_tasks": sum(1 for res in r.results.values() if res.status == TaskStatus.FAILED),
        }
        for r in runs
    ]


@app.get("/api/runs/{run_id}")
def get_run(run_id: str):
    """Retrieve execution breakdown, task results, and event logs for a specific run."""
    engine = _DEFAULT_ENGINE or WorkflowEngine()
    run = engine.store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Workflow run not found")

    tasks_result = {}
    for t_id, res in run.results.items():
        tasks_result[t_id] = {
            "task_id": res.task_id,
            "status": res.status.value,
            "output": str(res.output) if res.output is not None else None,
            "error": res.error,
            "attempt": res.attempt,
            "duration": res.duration,
            "started_at": res.started_at.isoformat() if res.started_at else None,
            "completed_at": res.completed_at.isoformat() if res.completed_at else None,
        }

    return {
        "run_id": run.run_id,
        "dag_id": run.dag_id,
        "status": run.status,
        "context": run.context,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
        "results": tasks_result,
        "events": run.events,
    }


@app.post("/api/dags/{dag_id}/run")
async def trigger_run(dag_id: str, req: TriggerRunRequest = TriggerRunRequest()):
    """Trigger execution of a registered DAG."""
    if dag_id not in _DAG_REGISTRY:
        raise HTTPException(status_code=404, detail=f"DAG '{dag_id}' not found")

    dag = _DAG_REGISTRY[dag_id]
    engine = _DEFAULT_ENGINE or WorkflowEngine()
    run = await engine.run(dag, initial_context=req.initial_context)
    return {
        "run_id": run.run_id,
        "dag_id": run.dag_id,
        "status": run.status,
        "context": run.context,
    }


@app.post("/api/runs/{run_id}/resume")
async def resume_run(run_id: str):
    """Resume an existing workflow run from its last checkpoint."""
    engine = _DEFAULT_ENGINE or WorkflowEngine()
    run = engine.store.get_run(run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")

    if run.dag_id not in _DAG_REGISTRY:
        raise HTTPException(status_code=400, detail=f"Cannot resume: DAG '{run.dag_id}' not loaded in registry")

    dag = _DAG_REGISTRY[run.dag_id]
    resumed_run = await engine.resume(dag, run_id)
    return {
        "run_id": resumed_run.run_id,
        "status": resumed_run.status,
        "completed": sum(1 for res in resumed_run.results.values() if res.status == TaskStatus.COMPLETED),
    }


@app.get("/", response_class=HTMLResponse)
def dashboard_ui():
    """Render interactive HTML5 / SVG DAG visualizer and real-time monitoring dashboard."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>AetherFlow DAG Visualizer & Orchestrator</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <style>
    :root {
      --bg: #0f172a;
      --card-bg: #1e293b;
      --border: #334155;
      --primary: #38bdf8;
      --primary-hover: #0284c7;
      --success: #10b981;
      --danger: #ef4444;
      --warning: #f59e0b;
      --text: #f8fafc;
      --text-dim: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      background: var(--bg);
      color: var(--text);
      line-height: 1.5;
      padding: 24px;
    }
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 24px;
      padding-bottom: 16px;
      border-bottom: 1px solid var(--border);
    }
    .logo { display: flex; align-items: center; gap: 12px; font-size: 24px; font-weight: 700; color: var(--primary); }
    .badge { background: #0369a1; color: #e0f2fe; padding: 2px 8px; border-radius: 9999px; font-size: 12px; }
    .grid { display: grid; grid-template-columns: 320px 1fr; gap: 24px; }
    .card {
      background: var(--card-bg);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 20px;
      box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
    }
    h2 { font-size: 18px; margin-bottom: 16px; color: var(--text); display: flex; justify-content: space-between; align-items: center; }
    .dag-item {
      padding: 12px;
      border: 1px solid var(--border);
      border-radius: 8px;
      margin-bottom: 12px;
      cursor: pointer;
      transition: all 0.2s;
    }
    .dag-item:hover, .dag-item.active {
      border-color: var(--primary);
      background: #0f172a;
    }
    .dag-title { font-weight: 600; color: var(--text); }
    .dag-meta { font-size: 13px; color: var(--text-dim); margin-top: 4px; }
    button {
      background: var(--primary);
      color: #0f172a;
      border: none;
      padding: 8px 16px;
      border-radius: 6px;
      font-weight: 600;
      cursor: pointer;
      transition: background 0.2s;
    }
    button:hover { background: var(--primary-hover); }
    .dag-canvas {
      min-height: 400px;
      background: #0b0f19;
      border: 1px dashed var(--border);
      border-radius: 8px;
      padding: 24px;
      display: flex;
      flex-direction: column;
      gap: 16px;
      overflow-x: auto;
    }
    .node-row { display: flex; gap: 20px; align-items: center; }
    .dag-node {
      background: var(--card-bg);
      border: 2px solid var(--border);
      border-radius: 8px;
      padding: 12px 18px;
      min-width: 160px;
      text-align: center;
      position: relative;
    }
    .dag-node.completed { border-color: var(--success); }
    .dag-node.failed { border-color: var(--danger); }
    .dag-node.running { border-color: var(--primary); animation: pulse 1.5s infinite; }
    .dag-node.skipped { border-color: var(--text-dim); opacity: 0.6; }
    @keyframes pulse {
      0% { box-shadow: 0 0 0 0 rgba(56, 189, 248, 0.4); }
      70% { box-shadow: 0 0 0 10px rgba(56, 189, 248, 0); }
      100% { box-shadow: 0 0 0 0 rgba(56, 189, 248, 0); }
    }
    .node-title { font-weight: 600; font-size: 14px; }
    .node-sub { font-size: 11px; color: var(--text-dim); margin-top: 4px; }
    table { width: 100%; border-collapse: collapse; margin-top: 16px; font-size: 14px; }
    th, td { text-align: left; padding: 10px 12px; border-bottom: 1px solid var(--border); }
    th { color: var(--text-dim); font-weight: 600; }
    .status-pill {
      display: inline-block;
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
    }
    .status-pill.success { background: rgba(16, 185, 129, 0.2); color: #34d399; }
    .status-pill.failed { background: rgba(239, 68, 68, 0.2); color: #f87171; }
    .status-pill.running { background: rgba(56, 189, 248, 0.2); color: #38bdf8; }
  </style>
</head>
<body>
  <header>
    <div class="logo">
      <span>⚙️ AetherFlow</span>
      <span class="badge">v0.3.0</span>
    </div>
    <div id="connection-status" style="font-size: 13px; color: var(--success);">● Connected to Orchestrator</div>
  </header>

  <div class="grid">
    <div class="card">
      <h2>
        <span>Workflows (DAGs)</span>
      </h2>
      <div id="dags-list">Loading workflows...</div>
      <div style="margin-top: 20px;">
        <button id="trigger-btn" style="width: 100%;">Trigger Selected DAG</button>
      </div>
    </div>

    <div>
      <div class="card" style="margin-bottom: 24px;">
        <h2>
          <span>DAG Topology Visualization</span>
          <span id="active-dag-name" style="font-size: 14px; color: var(--primary);"></span>
        </h2>
        <div id="dag-canvas" class="dag-canvas">Select a workflow to render graph</div>
      </div>

      <div class="card">
        <h2>Recent Executions & Checkpoints</h2>
        <table>
          <thead>
            <tr>
              <th>Run ID</th>
              <th>Workflow</th>
              <th>Status</th>
              <th>Tasks</th>
              <th>Created</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody id="runs-table-body">
            <tr><td colspan="6" style="text-align: center; color: var(--text-dim);">No runs found</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <script>
    let activeDagId = null;

    async function loadDags() {
      try {
        const res = await fetch('/api/dags');
        const dags = await res.json();
        const container = document.getElementById('dags-list');
        container.innerHTML = '';

        if (dags.length === 0) {
          container.innerHTML = '<div style="color: var(--text-dim);">No DAGs registered.</div>';
          return;
        }

        dags.forEach((d, i) => {
          const div = document.createElement('div');
          div.className = 'dag-item' + (i === 0 ? ' active' : '');
          div.innerHTML = `
            <div class="dag-title">${d.name}</div>
            <div class="dag-meta">${d.task_count} tasks ${d.cron ? '• Cron: ' + d.cron : ''}</div>
          `;
          div.onclick = () => selectDag(d);
          container.appendChild(div);
          if (i === 0) selectDag(d);
        });
      } catch (e) {
        console.error(e);
      }
    }

    function selectDag(dag) {
      activeDagId = dag.id;
      document.querySelectorAll('.dag-item').forEach(el => el.classList.remove('active'));
      document.getElementById('active-dag-name').innerText = dag.name;
      renderDagGraph(dag);
    }

    function renderDagGraph(dag) {
      const canvas = document.getElementById('dag-canvas');
      canvas.innerHTML = '';

      const roots = dag.tasks.filter(t => t.dependencies.length === 0);
      const row1 = document.createElement('div');
      row1.className = 'node-row';

      roots.forEach(r => {
        const node = document.createElement('div');
        node.className = 'dag-node completed';
        node.innerHTML = `
          <div class="node-title">${r.name}</div>
          <div class="node-sub">Root Node • ${r.priority}</div>
        `;
        row1.appendChild(node);
      });
      canvas.appendChild(row1);

      const nonRoots = dag.tasks.filter(t => t.dependencies.length > 0);
      if (nonRoots.length > 0) {
        const arrow = document.createElement('div');
        arrow.style.color = 'var(--text-dim)';
        arrow.style.fontSize = '20px';
        arrow.innerText = '↓ (dependencies resolved)';
        canvas.appendChild(arrow);

        const row2 = document.createElement('div');
        row2.className = 'node-row';
        nonRoots.forEach(nr => {
          const node = document.createElement('div');
          node.className = 'dag-node';
          node.innerHTML = `
            <div class="node-title">${nr.name}</div>
            <div class="node-sub">Depends on: ${nr.dependencies.join(', ')}</div>
          `;
          row2.appendChild(node);
        });
        canvas.appendChild(row2);
      }
    }

    async function loadRuns() {
      try {
        const res = await fetch('/api/runs');
        const runs = await res.json();
        const tbody = document.getElementById('runs-table-body');
        if (runs.length === 0) return;

        tbody.innerHTML = '';
        runs.forEach(r => {
          const tr = document.createElement('tr');
          const statusClass = r.status === 'success' ? 'success' : (r.status === 'failed' ? 'failed' : 'running');
          tr.innerHTML = `
            <td><code>${r.run_id.substring(0, 8)}...</code></td>
            <td><strong>${r.dag_id}</strong></td>
            <td><span class="status-pill ${statusClass}">${r.status}</span></td>
            <td>${r.completed_tasks} / ${r.task_count}</td>
            <td style="color: var(--text-dim);">${new Date(r.started_at).toLocaleTimeString()}</td>
            <td><button onclick="resumeRun('${r.run_id}')" style="padding: 4px 8px; font-size: 12px;">Resume</button></td>
          `;
          tbody.appendChild(tr);
        });
      } catch (e) {
        console.error(e);
      }
    }

    async function resumeRun(runId) {
      await fetch(`/api/runs/${runId}/resume`, { method: 'POST' });
      loadRuns();
    }

    document.getElementById('trigger-btn').onclick = async () => {
      if (!activeDagId) return;
      await fetch(`/api/dags/${activeDagId}/run`, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      loadRuns();
    };

    loadDags();
    loadRuns();
    setInterval(loadRuns, 3000);
  </script>
</body>
</html>
    """
