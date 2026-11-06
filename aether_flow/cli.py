"""Rich CLI interface for visualizing, executing, and auditing AetherFlow workflows."""

import argparse
import asyncio
import importlib.util
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree

from .engine import WorkflowEngine
from .graph import GraphValidator
from .models import DAG, TaskStatus

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
        # Find tasks that depend on parent_task_id
        children = [t for t in dag.tasks.values() if parent_task_id in t.dependencies]
        for child in children:
            child_node = parent_node.add(f"[green]{child.name}[/green] [dim]({child.id})[/dim]")
            add_children(child_node, child.id)

    for root in roots:
        root_node = tree.add(f"[bold yellow]{root.name}[/bold yellow] [dim]({root.id})[/dim] (root)")
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

    args = parser.parse_args()

    if args.command == "run":
        dag = load_dag_from_file(Path(args.file))
        asyncio.run(execute_dag(dag, concurrency=args.concurrency))
    elif args.command == "graph":
        dag = load_dag_from_file(Path(args.file))
        print_dag_graph(dag)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
