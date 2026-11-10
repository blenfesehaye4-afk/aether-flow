"""Rich CLI interface for visualizing, executing, auditing, and serving AetherFlow workflows."""

import argparse
import asyncio
import importlib.util
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

from .dashboard import app as dashboard_app, register_dag, set_dashboard_engine
from .engine import WorkflowEngine
from .graph import GraphValidator
from .models import DAG, Task, TaskStatus
from .retries import AdvancedRetryPolicy, BackoffStrategy
from .workers import TaskPriority

console = Console()


def load_dag_from_file(filepath: Path) -> DAG:
    """Dynamically import a Python file and search for DAG instances."""
    if not filepath.exists():
        raise FileNotFoundError(f"Workflow file '{filepath}' does not exist.")

    spec = importlib.util.spec_from_file_location("workflow_module", filepath)
    if not spec or not spec.loader:
        raise ImportError(f"Could not load module from {filepath}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    for attr_name in dir(module):
        val = getattr(module, attr_name)
        if isinstance(val, DAG):
            return val

    raise ValueError(f"No DAG object found in '{filepath}'. Define a DAG instance.")


def print_dag_graph(dag: DAG) -> None:
    """Print ASCII tree view of the workflow graph."""
    GraphValidator.validate(dag)
    tree = Tree(f"[bold cyan]DAG: {dag.name}[/bold cyan] ({len(dag.tasks)} tasks)")

    # Find root tasks (no dependencies)
    roots = [t for t in dag.tasks.values() if not t.dependencies]

    def add_children(parent_node, parent_task_id: str):
        children = [t for t in dag.tasks.values() if parent_task_id in t.dependencies]
        for child in children:
            prio_badge = f"[magenta][{child.priority.name}][/magenta]" if child.priority != TaskPriority.NORMAL else ""
            child_node = parent_node.add(f"[green]{child.name}[/green] {prio_badge} [dim]({child.id})[/dim]")
            add_children(child_node, child.id)

    for root in roots:
        prio_badge = f"[magenta][{root.priority.name}][/magenta]" if root.priority != TaskPriority.NORMAL else ""
        root_node = tree.add(f"[bold yellow]{root.name}[/bold yellow] {prio_badge} [dim]({root.id})[/dim] (root)")
        add_children(root_node, root.id)

    console.print(Panel(tree, title="Workflow Dependency Graph", expand=False))


async def execute_dag(dag: DAG, concurrency: int = 4) -> None:
    """Execute DAG and print execution table."""
    engine = WorkflowEngine(max_concurrency=concurrency)
    console.print(f"[bold green]Starting workflow execution:[/bold green] [cyan]{dag.name}[/cyan]")

    run = await engine.run(dag)

    table = Table(title=f"Workflow Execution Summary: {run.status.upper()}")
    table.add_column("Task ID", style="cyan")
    table.add_column("Status", justify="center")
    table.add_column("Duration", justify="right")
    table.add_column("Output / Error")

    for task_id, res in run.results.items():
        status_color = "green" if res.status == TaskStatus.COMPLETED else "red" if res.status == TaskStatus.FAILED else "yellow"
        out_str = str(res.output) if res.output is not None else (f"[red]{res.error}[/red]" if res.error else "-")
        table.add_row(
            task_id,
            f"[{status_color}]{res.status.value.upper()}[/{status_color}]",
            f"{res.duration:.3f} s",
            out_str[:80],
        )

    console.print(table)


def run_demo():
    """Run an interactive demonstration of AetherFlow v0.3.0 features."""
    console.print(Panel(
        "[bold cyan]AetherFlow v0.3.0 Architecture Showcase[/bold cyan]\n"
        "Demonstrating Priority Queues, Jitter Retries, Fallbacks, and Distributed Graph Execution.",
        expand=False,
    ))

    dag = DAG("enterprise_ai_pipeline", name="Enterprise AI Ingestion & Analysis")

    # 1. Fetch Raw Data (Root)
    async def fetch_data():
        await asyncio.sleep(0.05)
        return {"records": [101, 102, 103], "batch_id": "b-99"}

    t_fetch = dag.add_task(Task("fetch_data", fetch_data, name="Fetch Batch Data", priority=TaskPriority.HIGH))

    # 2. Flaky Embedding task with Exponential Backoff + Full Jitter & Fallback
    attempt_counter = {"count": 0}

    async def compute_embeddings(fetch_data):
        attempt_counter["count"] += 1
        if attempt_counter["count"] < 3:
            raise ConnectionError("Upstream vector service unavailable (simulated timeout)")
        return f"Embeddings computed for {len(fetch_data['records'])} records"

    policy = AdvancedRetryPolicy(
        max_retries=3,
        initial_delay=0.05,
        backoff_factor=1.5,
        strategy=BackoffStrategy.FULL_JITTER,
        fallback=lambda exc, ctx: "Cached Embeddings (Fallback)",
    )

    t_embed = dag.add_task(Task("embeddings", compute_embeddings, name="Compute Embeddings (with Jitter Retries)", retries=policy))

    # 3. High-Priority Anomaly Detection Task
    async def detect_anomalies(fetch_data):
        await asyncio.sleep(0.02)
        return {"anomalies_detected": 0, "status": "clean"}

    t_anomaly = dag.add_task(Task("anomaly_check", detect_anomalies, name="Real-Time Anomaly Check", priority=TaskPriority.CRITICAL))

    # 4. Fan-In Summarization
    async def generate_report(embeddings, anomaly_check):
        return f"Summary: {embeddings} | Anomalies: {anomaly_check['status']}"

    t_report = dag.add_task(Task("generate_report", generate_report, name="Consolidate Report"))

    # Graph wiring: fetch_data >> [embeddings, anomaly_check] >> generate_report
    t_fetch >> [t_embed, t_anomaly]
    t_embed >> t_report
    t_anomaly >> t_report

    print_dag_graph(dag)
    asyncio.run(execute_dag(dag, concurrency=4))


def run_ui(host: str = "127.0.0.1", port: int = 8500):
    """Launch the Web DAG Dashboard."""
    import uvicorn
    console.print(Panel(
        f"[bold green]Starting AetherFlow Web Visualizer[/bold green]\n"
        f"URL: [cyan]http://{host}:{port}[/cyan]",
        expand=False,
    ))

    # Register sample workflow for visualization
    sample_dag = DAG("sample_pipeline", name="Data Ingestion & Training Pipeline", cron="*/15 * * * *")
    t1 = sample_dag.add_task(Task("extract_db", lambda: "raw_data", name="Extract from SQL", priority=TaskPriority.HIGH))
    t2 = sample_dag.add_task(Task("transform_data", lambda extract_db: extract_db.upper(), name="Feature Engineering"))
    t3 = sample_dag.add_task(Task("model_train", lambda transform_data: f"Model on {transform_data}", name="Train XGBoost", priority=TaskPriority.CRITICAL))
    t1 >> t2 >> t3
    register_dag(sample_dag)

    uvicorn.run(dashboard_app, host=host, port=port)


def main():
    parser = argparse.ArgumentParser(
        prog="aether-flow",
        description="⚙️ AetherFlow: Lightweight, event-driven DAG workflow orchestration engine.",
    )
    subparsers = parser.add_subparsers(dest="command")

    # Run
    run_cmd = subparsers.add_parser("run", help="Run a workflow defined in a Python script")
    run_cmd.add_argument("file", help="Path to Python workflow file")
    run_cmd.add_argument("--concurrency", type=int, default=4, help="Max concurrent workers")

    # Graph
    graph_cmd = subparsers.add_parser("graph", help="Display visual dependency graph of a workflow")
    graph_cmd.add_argument("file", help="Path to Python workflow file")

    # Demo
    subparsers.add_parser("demo", help="Run showcase of v0.3.0 features (Priorities, Jitter, Fallbacks)")

    # UI
    ui_cmd = subparsers.add_parser("ui", help="Start the interactive web DAG visualizer and dashboard")
    ui_cmd.add_argument("--host", default="127.0.0.1", help="Host address")
    ui_cmd.add_argument("--port", type=int, default=8500, help="Port number")

    args = parser.parse_args()

    if args.command == "run":
        dag = load_dag_from_file(Path(args.file))
        asyncio.run(execute_dag(dag, concurrency=args.concurrency))
    elif args.command == "graph":
        dag = load_dag_from_file(Path(args.file))
        print_dag_graph(dag)
    elif args.command == "demo":
        run_demo()
    elif args.command == "ui":
        run_ui(host=args.host, port=args.port)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
