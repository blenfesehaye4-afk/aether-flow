"""Fluent builder DSL and decorators for creating AetherFlow workflows."""

from typing import Callable, Optional
from .models import DAG, RetryPolicy, Task


def dag(id: str, name: Optional[str] = None, description: Optional[str] = None) -> DAG:
    """Create a new DAG workflow."""
    return DAG(id=id, name=name, description=description)


def task(
    id: Optional[str] = None,
    name: Optional[str] = None,
    retries: Optional[int] = None,
    timeout: Optional[float] = None,
) -> Callable:
    """Decorator to define a standalone Task node."""
    def decorator(fn: Callable) -> Task:
        task_id = id or fn.__name__
        return Task(id=task_id, fn=fn, name=name, retries=retries, timeout=timeout)
    return decorator
