"""Interval-based scheduler and trigger system for periodic workflow execution."""

import asyncio
from typing import Callable, Optional
from .engine import WorkflowEngine
from .models import DAG
from .state import WorkflowRun


class WorkflowScheduler:
    """Schedules recurring workflow runs at fixed intervals."""

    def __init__(self, engine: WorkflowEngine):
        self.engine = engine
        self._running = False
        self._tasks: list[asyncio.Task] = []

    def schedule_interval(
        self,
        dag: DAG,
        interval_seconds: float,
        on_complete: Optional[Callable[[WorkflowRun], None]] = None,
        max_runs: Optional[int] = None,
    ) -> asyncio.Task:
        """Schedule a DAG to execute every `interval_seconds` asynchronously."""
        async def loop():
            runs_executed = 0
            while self._running:
                run = await self.engine.run(dag)
                runs_executed += 1
                if on_complete:
                    on_complete(run)

                if max_runs and runs_executed >= max_runs:
                    break

                await asyncio.sleep(interval_seconds)

        self._running = True
        t = asyncio.create_task(loop())
        self._tasks.append(t)
        return t

    def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
