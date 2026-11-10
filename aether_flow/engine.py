"""Workflow execution engine with concurrent async worker pool, priority scheduling, resilient retries, and checkpoint resumption."""

import asyncio
import inspect
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from .graph import GraphValidator
from .models import DAG, Task, TaskResult, TaskStatus
from .retries import AdvancedRetryPolicy
from .state import StateStore, WorkflowRun
from .workers import ConcurrencyLimiter, PriorityTaskQueue, TaskPriority


class WorkflowEngine:
    """Async execution runtime for DAG workflows."""

    def __init__(
        self,
        state_store: Optional[StateStore] = None,
        max_concurrency: int = 4,
        group_limits: Optional[Dict[str, int]] = None,
    ):
        self.store = state_store or StateStore()
        self.max_concurrency = max_concurrency
        self.concurrency_limiter = ConcurrencyLimiter(group_limits)

    def _save_checkpoint(self, run: WorkflowRun) -> None:
        if hasattr(self.store, "save_run"):
            try:
                self.store.save_run(run)
            except Exception:
                pass

    async def _execute_task_attempt(self, task: Task, run: WorkflowRun, attempt: int) -> TaskResult:
        start_time = time.perf_counter()
        started_dt = datetime.now(timezone.utc)

        sig = inspect.signature(task.fn)
        kwargs = {}
        if "context" in sig.parameters:
            kwargs["context"] = run.context

        for param in sig.parameters:
            if param in run.context and param != "context":
                kwargs[param] = run.context[param]

        sem = self.concurrency_limiter.get_semaphore(task.concurrency_group)

        try:
            async def _invoke():
                if task.is_coroutine:
                    if task.timeout:
                        return await asyncio.wait_for(task.fn(**kwargs), timeout=task.timeout)
                    return await task.fn(**kwargs)
                else:
                    if task.timeout:
                        return await asyncio.wait_for(
                            asyncio.to_thread(task.fn, **kwargs),
                            timeout=task.timeout,
                        )
                    return await asyncio.to_thread(task.fn, **kwargs)

            if sem:
                async with sem:
                    output = await _invoke()
            else:
                output = await _invoke()

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

        # Determine max retries & delays
        max_retries = getattr(policy, "max_retries", 0)

        while True:
            run.record_event("TASK_STARTED", {"task_id": task.id, "attempt": attempt})
            result = await self._execute_task_attempt(task, run, attempt)

            if result.status == TaskStatus.COMPLETED:
                run.record_event("TASK_COMPLETED", {"task_id": task.id, "attempt": attempt})
                return result

            # Check if retry is allowed
            should_retry = False
            delay = 0.5
            if attempt <= max_retries:
                if isinstance(policy, AdvancedRetryPolicy):
                    # Extract exception type from error string if needed or pass Generic Exception
                    should_retry = True
                    delay = policy.calculate_delay(attempt)
                else:
                    should_retry = True
                    delay = min(getattr(policy, "max_delay", 10.0), getattr(policy, "initial_delay", 0.5) * (getattr(policy, "backoff_factor", 2.0) ** (attempt - 1)))

            if not should_retry:
                # Check for fallback callback hook
                if isinstance(policy, AdvancedRetryPolicy) and policy.fallback:
                    try:
                        fallback_out = policy.fallback(Exception(result.error), run.context)
                        run.record_event("TASK_FALLBACK_EXECUTED", {"task_id": task.id})
                        return TaskResult(
                            task_id=task.id,
                            status=TaskStatus.COMPLETED,
                            output=fallback_out,
                            attempt=attempt,
                            duration=result.duration,
                            started_at=result.started_at,
                            completed_at=datetime.now(timezone.utc),
                        )
                    except Exception as fb_err:
                        result.error += f" | Fallback failed: {fb_err}"

                run.record_event("TASK_FAILED", {"task_id": task.id, "error": result.error, "attempt": attempt})
                return result

            run.record_event("TASK_RETRYING", {"task_id": task.id, "attempt": attempt, "backoff": delay})
            await asyncio.sleep(delay)
            attempt += 1

    async def run(
        self,
        dag: DAG,
        initial_context: Optional[dict] = None,
        existing_run: Optional[WorkflowRun] = None,
    ) -> WorkflowRun:
        """Execute all tasks in the DAG adhering to dependency order, priority, and concurrency limits."""
        GraphValidator.validate(dag)
        run = existing_run or self.store.create_run(dag.id, initial_context)

        completed: Set[str] = set()
        running: Dict[str, asyncio.Task] = {}
        failed: Set[str] = set()
        skipped: Set[str] = set()

        priority_queue = PriorityTaskQueue()

        # If resuming from checkpoint, seed completed tasks
        for t_id, res in run.results.items():
            if res.status == TaskStatus.COMPLETED:
                completed.add(t_id)
                run.context[t_id] = res.output
            elif res.status == TaskStatus.SKIPPED:
                skipped.add(t_id)

        self._save_checkpoint(run)
        total_tasks = len(dag.tasks)

        while (len(completed) + len(failed) + len(skipped)) < total_tasks:
            ready = GraphValidator.get_ready_tasks(dag, completed, set(running.keys()), failed, skipped)

            if not ready and not running:
                for t_id, task in dag.tasks.items():
                    if t_id not in completed and t_id not in failed and t_id not in skipped:
                        if any(dep in failed or dep in skipped for dep in task.dependencies):
                            skipped.add(t_id)
                            res = TaskResult(task_id=t_id, status=TaskStatus.SKIPPED, error="Upstream dependency failed")
                            run.results[t_id] = res
                            run.record_event("TASK_SKIPPED", {"task_id": t_id})
                            self._save_checkpoint(run)

                if (len(completed) + len(failed) + len(skipped)) == total_tasks:
                    break

            # Enqueue newly ready tasks with their priorities
            for task_id in ready:
                priority_queue.push(task_id, dag.tasks[task_id].priority)

            # Dispatch tasks according to priority up to available concurrency slots
            available_slots = max(0, self.max_concurrency - len(running))
            ready_set = set(ready)

            while available_slots > 0 and len(priority_queue) > 0:
                next_id = priority_queue.pop_next(ready_set)
                if not next_id:
                    break
                ready_set.remove(next_id)
                available_slots -= 1

                task_obj = dag.tasks[next_id]
                run.record_event("TASK_SCHEDULED", {"task_id": next_id, "priority": task_obj.priority.name})
                coro = self._run_task_with_retries(task_obj, run)
                running[next_id] = asyncio.create_task(coro)

            if not running:
                await asyncio.sleep(0.01)
                continue

            done, _ = await asyncio.wait(running.values(), return_when=asyncio.FIRST_COMPLETED)

            for async_task in done:
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

                    self._save_checkpoint(run)

        run.completed_at = datetime.now(timezone.utc)
        run.status = "failed" if failed else "success"
        run.record_event("WORKFLOW_COMPLETED", {"status": run.status, "completed": len(completed), "failed": len(failed)})
        self._save_checkpoint(run)

        return run

    async def resume(self, dag: DAG, run_id: str) -> WorkflowRun:
        """Resume an existing workflow run from its last checkpoint, skipping completed tasks."""
        if hasattr(self.store, "load_run"):
            run = self.store.load_run(run_id)
        else:
            run = self.store.get_run(run_id)

        if not run:
            raise ValueError(f"Run ID '{run_id}' not found in state store.")

        # Clear failed tasks so they are re-executed
        for t_id in list(run.results.keys()):
            if run.results[t_id].status in (TaskStatus.FAILED, TaskStatus.SKIPPED):
                del run.results[t_id]

        run.status = "running"
        run.record_event("WORKFLOW_RESUMED", {"run_id": run_id})
        return await self.run(dag, existing_run=run)
