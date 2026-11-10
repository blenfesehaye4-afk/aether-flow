"""Interval and cron-syntax schedulers for periodic DAG workflow execution."""

import asyncio
from typing import Callable, Dict, List, Optional
from .cron import CronTrigger
from .engine import WorkflowEngine
from .models import DAG
from .state import WorkflowRun


class WorkflowScheduler:
    """Schedules recurring workflow runs at fixed intervals or cron expressions."""

    def __init__(self, engine: WorkflowEngine):
        self.engine = engine
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._scheduled_dags: Dict[str, dict] = {}

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
        self._scheduled_dags[dag.id] = {
            "type": "interval",
            "interval_seconds": interval_seconds,
            "dag": dag,
        }
        return t

    def schedule_cron(
        self,
        dag: DAG,
        cron_expression: str,
        on_complete: Optional[Callable[[WorkflowRun], None]] = None,
        max_runs: Optional[int] = None,
    ) -> asyncio.Task:
        """Schedule a DAG to execute based on a standard 5-part cron expression."""
        trigger = CronTrigger(cron_expression)

        async def cron_loop():
            runs_executed = 0
            while self._running:
                wait_sec = trigger.seconds_until_next()
                if wait_sec > 0:
                    await asyncio.sleep(wait_sec)

                if not self._running:
                    break

                run = await self.engine.run(dag)
                runs_executed += 1
                if on_complete:
                    on_complete(run)

                if max_runs and runs_executed >= max_runs:
                    break

                # Sleep small offset to avoid double-triggering in the same second
                await asyncio.sleep(1.0)

        self._running = True
        t = asyncio.create_task(cron_loop())
        self._tasks.append(t)
        self._scheduled_dags[dag.id] = {
            "type": "cron",
            "expression": cron_expression,
            "dag": dag,
        }
        return t

    def stop(self) -> None:
        self._running = False
        for t in self._tasks:
            t.cancel()
        self._tasks.clear()

    def list_schedules(self) -> List[dict]:
        """Return metadata for all active schedules."""
        return [
            {"dag_id": k, **v, "running": self._running}
            for k, v in self._scheduled_dags.items()
        ]
