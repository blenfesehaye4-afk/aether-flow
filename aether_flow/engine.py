"""Workflow execution engine with concurrent async worker pool and resilient retries."""

import asyncio
import inspect
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from .graph import GraphValidator
from .models import DAG, Task, TaskResult, TaskStatus
from .state import StateStore, WorkflowRun


class WorkflowEngine:
    """Async execution runtime for DAG workflows."""

    def __init__(self, state_store: Optional[StateStore] = None, max_concurrency: int = 4):
        self.store = state_store or StateStore()
        self.max_concurrency = max_concurrency

    async def _execute_task_attempt(self, task: Task, run: WorkflowRun, attempt: int) -> TaskResult:
        start_time = time.perf_counter()
        started_dt = datetime.now(timezone.utc)

        # Inspect signature to intelligently inject context or dependencies
        sig = inspect.signature(task.fn)
        kwargs = {}
        if "context" in sig.parameters:
            kwargs["context"] = run.context

        # Also supply upstream outputs if parameter names match
        for param in sig.parameters:
            if param in run.context and param != "context":
                kwargs[param] = run.context[param]

        try:
            if task.is_coroutine:
                if task.timeout:
                    output = await asyncio.wait_for(task.fn(**kwargs), timeout=task.timeout)
                else:
                    output = await task.fn(**kwargs)
            else:
                if task.timeout:
                    output = await asyncio.wait_for(
                        asyncio.to_thread(task.fn, **kwargs),
                        timeout=task.timeout,
                    )
                else:
                    output = await asyncio.to_thread(task.fn, **kwargs)

            dur = time.perf_counter() - start_time
            return TaskResult(
                task_id=task.id,
                status=TaskStatus.COMPLETED,
                output=output,
                attempt=attempt,
                duration=round(dur, 4),
                started_at=started_dt,
                completed_at=datetime.now(timezone.utc),
            )

        except Exception as e:
            dur = time.perf_counter() - start_time
            return TaskResult(
                task_id=task.id,
                status=TaskStatus.FAILED,
                error=f"{type(e).__name__}: {str(e)}",
                attempt=attempt,
                duration=round(dur, 4),
                started_at=started_dt,
                completed_at=datetime.now(timezone.utc),
            )

    async def _run_task_with_retries(self, task: Task, run: WorkflowRun) -> TaskResult:
        policy = task.retry_policy
        attempt = 1
        current_delay = policy.initial_delay

        while True:
            run.record_event("TASK_STARTED", {"task_id": task.id, "attempt": attempt})
            result = await self._execute_task_attempt(task, run, attempt)

            if result.status == TaskStatus.COMPLETED:
                run.record_event("TASK_COMPLETED", {"task_id": task.id, "attempt": attempt})
                return result

            # Failure branch
            if attempt > policy.max_retries:
                run.record_event("TASK_FAILED", {"task_id": task.id, "error": result.error, "attempt": attempt})
                return result

            # Retry with exponential backoff
            run.record_event("TASK_RETRYING", {"task_id": task.id, "attempt": attempt, "backoff": current_delay})
            await asyncio.sleep(current_delay)
            current_delay = min(policy.max_delay, current_delay * policy.backoff_factor)
            attempt += 1

    async def run(self, dag: DAG, initial_context: Optional[dict] = None) -> WorkflowRun:
        """Execute all tasks in the DAG adhering to dependency order and concurrency limits."""
        GraphValidator.validate(dag)
        run = self.store.create_run(dag.id, initial_context)

        completed: Set[str] = set()
        running: Dict[str, asyncio.Task] = {}
        failed: Set[str] = set()
        skipped: Set[str] = set()

        total_tasks = len(dag.tasks)

        while (len(completed) + len(failed) + len(skipped)) < total_tasks:
            # 1. Discover tasks ready to run
            ready = GraphValidator.get_ready_tasks(dag, completed, set(running.keys()), failed, skipped)

            # 2. Check if remaining tasks must be skipped due to upstream failure
            if not ready and not running:
                for t_id, task in dag.tasks.items():
                    if t_id not in completed and t_id not in failed and t_id not in skipped:
                        if any(dep in failed or dep in skipped for dep in task.dependencies):
                            skipped.add(t_id)
                            res = TaskResult(task_id=t_id, status=TaskStatus.SKIPPED, error="Upstream dependency failed")
                            run.results[t_id] = res
                            run.record_event("TASK_SKIPPED", {"task_id": t_id})
                if (len(completed) + len(failed) + len(skipped)) == total_tasks:
                    break

            # 3. Launch ready tasks up to concurrency capacity
            available_slots = max(0, self.max_concurrency - len(running))
            for task_id in ready[:available_slots]:
                task_obj = dag.tasks[task_id]
                run.record_event("TASK_SCHEDULED", {"task_id": task_id})
                coro = self._run_task_with_retries(task_obj, run)
                async_task = asyncio.create_task(coro)
                running[task_id] = async_task

            if not running:
                await asyncio.sleep(0.01)
                continue

            # 4. Wait for at least one running task to complete
            done, _ = await asyncio.wait(running.values(), return_when=asyncio.FIRST_COMPLETED)

            for async_task in done:
                # Find task_id
                finished_id = None
                for t_id, t_coro in running.items():
                    if t_coro == async_task:
                        finished_id = t_id
                        break

                if finished_id:
                    del running[finished_id]
                    res = async_task.result()
                    run.results[finished_id] = res

                    if res.status == TaskStatus.COMPLETED:
                        completed.add(finished_id)
                        run.context[finished_id] = res.output
                    else:
                        failed.add(finished_id)

        run.completed_at = datetime.now(timezone.utc)
        run.status = "failed" if failed else "success"
        run.record_event("WORKFLOW_COMPLETED", {"status": run.status, "completed": len(completed), "failed": len(failed)})

        return run
