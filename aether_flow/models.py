"""Data models for AetherFlow DAG workflow orchestration."""

from __future__ import annotations
import inspect
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Union


class TaskStatus(str, Enum):
    PENDING = "pending"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


@dataclass
class RetryPolicy:
    max_retries: int = 3
    initial_delay: float = 0.5
    backoff_factor: float = 2.0
    max_delay: float = 10.0


@dataclass
class TaskResult:
    task_id: str
    status: TaskStatus
    output: Any = None
    error: Optional[str] = None
    attempt: int = 1
    duration: float = 0.0
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class Task:
    """Executable node in a Directed Acyclic Graph (DAG)."""

    def __init__(
        self,
        id: str,
        fn: Callable[..., Any],
        name: Optional[str] = None,
        retries: Optional[Union[int, RetryPolicy]] = None,
        timeout: Optional[float] = None,
        dependencies: Optional[Set[str]] = None,
    ):
        self.id = id
        self.fn = fn
        self.name = name or id
        self.timeout = timeout
        self.dependencies: Set[str] = set(dependencies) if dependencies else set()

        if isinstance(retries, int):
            self.retry_policy = RetryPolicy(max_retries=retries)
        elif isinstance(retries, RetryPolicy):
            self.retry_policy = retries
        else:
            self.retry_policy = RetryPolicy(max_retries=0)

    @property
    def is_coroutine(self) -> bool:
        return inspect.iscoroutinefunction(self.fn)

    def depends_on(self, *tasks: "Task") -> "Task":
        """Explicitly declare dependencies."""
        for t in tasks:
            self.dependencies.add(t.id)
        return self

    def __rshift__(self, other: Union["Task", List["Task"]]) -> Union["Task", List["Task"]]:
        """Bitwise shift operator for chaining: task_a >> task_b or task_a >> [t1, t2]."""
        if isinstance(other, list):
            for t in other:
                t.dependencies.add(self.id)
            return other
        other.dependencies.add(self.id)
        return other

    def __repr__(self) -> str:
        return f"<Task id='{self.id}' deps={list(self.dependencies)}>"


class DAG:
    """Directed Acyclic Graph container organizing tasks and dependencies."""

    def __init__(self, id: str, name: Optional[str] = None, description: Optional[str] = None):
        self.id = id
        self.name = name or id
        self.description = description or ""
        self.tasks: Dict[str, Task] = {}

    def add_task(self, task: Task) -> Task:
        """Register a task node in this DAG."""
        self.tasks[task.id] = task
        return task

    def task(
        self,
        id: Optional[str] = None,
        name: Optional[str] = None,
        retries: Optional[Union[int, RetryPolicy]] = None,
        timeout: Optional[float] = None,
    ) -> Callable[[Callable[..., Any]], Task]:
        """Decorator for registering tasks."""
        def decorator(fn: Callable[..., Any]) -> Task:
            task_id = id or fn.__name__
            t = Task(id=task_id, fn=fn, name=name, retries=retries, timeout=timeout)
            self.add_task(t)
            return t
        return decorator

    def __len__(self) -> int:
        return len(self.tasks)

    def __iter__(self):
        return iter(self.tasks.values())
