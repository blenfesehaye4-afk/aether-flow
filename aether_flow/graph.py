"""Graph validation, cycle detection, and topological sorting algorithms."""

from typing import Dict, List, Set
from .models import DAG, Task


class CyclicDependencyError(Exception):
    """Raised when a circular dependency loop is discovered in a workflow graph."""
    pass


class GraphValidator:
    """Validates structural integrity of Directed Acyclic Graphs."""

    @staticmethod
    def validate(dag: DAG) -> None:
        """Verify that all referenced dependencies exist and graph contains no cycles."""
        # 1. Check all dependencies exist
        for task_id, task in dag.tasks.items():
            for dep in task.dependencies:
                if dep not in dag.tasks:
                    raise ValueError(f"Task '{task_id}' depends on non-existent task '{dep}'")

        # 2. Cycle detection via Depth-First Search 3-Coloring (0=White, 1=Gray, 2=Black)
        state: Dict[str, int] = {k: 0 for k in dag.tasks}
        cycle_path: List[str] = []

        def dfs(node: str) -> bool:
            state[node] = 1  # Visiting
            cycle_path.append(node)

            for dep in dag.tasks[node].dependencies:
                if state[dep] == 1:
                    cycle_path.append(dep)
                    return True
                if state[dep] == 0:
                    if dfs(dep):
                        return True

            state[node] = 2  # Visited
            cycle_path.pop()
            return False

        for node in dag.tasks:
            if state[node] == 0:
                if dfs(node):
                    path_str = " -> ".join(cycle_path)
                    raise CyclicDependencyError(f"Cycle detected in DAG '{dag.id}': {path_str}")

    @classmethod
    def topological_sort(cls, dag: DAG) -> List[str]:
        """Compute a valid sequential execution order using Kahn's algorithm."""
        cls.validate(dag)

        in_degree: Dict[str, int] = {t_id: 0 for t_id in dag.tasks}
        for task in dag.tasks.values():
            for dep in task.dependencies:
                # dep must run before task, so task has in-degree from dep
                in_degree[task.id] += 1

        queue = [t_id for t_id, deg in in_degree.items() if deg == 0]
        sorted_order: List[str] = []

        while queue:
            curr = queue.pop(0)
            sorted_order.append(curr)

            # For each task that depends on curr, decrement its in-degree
            for task in dag.tasks.values():
                if curr in task.dependencies:
                    in_degree[task.id] -= 1
                    if in_degree[task.id] == 0:
                        queue.append(task.id)

        return sorted_order

    @staticmethod
    def get_ready_tasks(
        dag: DAG,
        completed: Set[str],
        running: Set[str],
        failed: Set[str],
        skipped: Set[str],
    ) -> List[str]:
        """Identify tasks whose upstream dependencies have all completed successfully."""
        ready: List[str] = []
        for task_id, task in dag.tasks.items():
            if task_id in completed or task_id in running or task_id in failed or task_id in skipped:
                continue

            # Check if all upstream tasks completed
            deps = task.dependencies
            if deps.issubset(completed):
                ready.append(task_id)

        return ready
