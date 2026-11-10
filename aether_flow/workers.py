"""Task priority definitions, resource concurrency throttling, and worker dispatchers."""

import asyncio
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Dict, List, Optional, Set


class TaskPriority(IntEnum):
    """Integer priority hierarchy for DAG task execution."""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


class ConcurrencyLimiter:
    """Manages group-level concurrency throttling (e.g. max 2 DB queries or API calls)."""

    def __init__(self, limits: Optional[Dict[str, int]] = None):
        self._limits = limits or {}
        self._semaphores: Dict[str, asyncio.Semaphore] = {}

    def set_limit(self, group: str, max_concurrent: int) -> None:
        """Define concurrency ceiling for a named group."""
        self._limits[group] = max_concurrent
        self._semaphores[group] = asyncio.Semaphore(max_concurrent)

    def get_semaphore(self, group: Optional[str]) -> Optional[asyncio.Semaphore]:
        """Get or initialize semaphore for a group."""
        if not group or group not in self._limits:
            return None
        if group not in self._semaphores:
            self._semaphores[group] = asyncio.Semaphore(self._limits[group])
        return self._semaphores[group]


class PriorityTaskQueue:
    """Orders ready tasks according to TaskPriority descending and ready timestamp."""

    def __init__(self):
        self._items: List[dict] = []

    def push(self, task_id: str, priority: TaskPriority = TaskPriority.NORMAL) -> None:
        self._items.append({
            "task_id": task_id,
            "priority": int(priority),
        })
        # Sort by priority descending
        self._items.sort(key=lambda x: x["priority"], reverse=True)

    def pop_next(self, eligible_task_ids: Set[str]) -> Optional[str]:
        """Pop the highest-priority task that is currently eligible to run."""
        for i, item in enumerate(self._items):
            if item["task_id"] in eligible_task_ids:
                return self._items.pop(i)["task_id"]
        return None

    def remove(self, task_id: str) -> None:
        self._items = [it for it in self._items if it["task_id"] != task_id]

    def __len__(self) -> int:
        return len(self._items)
