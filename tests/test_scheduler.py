import asyncio
import pytest
from aether_flow.engine import WorkflowEngine
from aether_flow.models import DAG, Task
from aether_flow.scheduler import WorkflowScheduler


@pytest.mark.asyncio
async def test_workflow_scheduler_interval():
    counter = 0

    def tick():
        nonlocal counter
        counter += 1
        return counter

    d = DAG("ticker")
    d.add_task(Task("tick", tick))

    engine = WorkflowEngine()
    scheduler = WorkflowScheduler(engine)

    completed_runs = []
    scheduler.schedule_interval(
        dag=d,
        interval_seconds=0.01,
        on_complete=lambda r: completed_runs.append(r),
        max_runs=3,
    )

    await asyncio.sleep(0.06)
    scheduler.stop()

    assert len(completed_runs) == 3
    assert counter == 3
