import asyncio
import pytest
from aether_flow.engine import WorkflowEngine
from aether_flow.models import DAG, RetryPolicy, Task, TaskStatus


@pytest.mark.asyncio
async def test_workflow_execution_and_context_propagation():
    d = DAG("math_pipeline")

    async def step1():
        return 10

    def step2(step1: int):
        return step1 * 2

    def step3(step2: int):
        return step2 + 5

    t1 = d.add_task(Task("step1", step1))
    t2 = d.add_task(Task("step2", step2))
    t3 = d.add_task(Task("step3", step3))

    t1 >> t2 >> t3

    engine = WorkflowEngine()
    run = await engine.run(d)

    assert run.status == "success"
    assert run.results["step1"].output == 10
    assert run.results["step2"].output == 20
    assert run.results["step3"].output == 25


@pytest.mark.asyncio
async def test_workflow_retry_and_recovery():
    d = DAG("retry_pipeline")
    attempts = 0

    def flaky_task():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionResetError("Temporary network blip")
        return "recovered"

    t = d.add_task(Task("flaky", flaky_task, retries=RetryPolicy(max_retries=3, initial_delay=0.01)))

    engine = WorkflowEngine()
    run = await engine.run(d)

    assert run.status == "success"
    assert run.results["flaky"].status == TaskStatus.COMPLETED
    assert run.results["flaky"].output == "recovered"
    assert run.results["flaky"].attempt == 3


@pytest.mark.asyncio
async def test_workflow_upstream_failure_skips_downstream():
    d = DAG("failure_pipeline")

    def fail_step():
        raise RuntimeError("Fatal crash")

    def downstream_step():
        return "never executed"

    t1 = d.add_task(Task("fail_step", fail_step))
    t2 = d.add_task(Task("downstream_step", downstream_step))

    t1 >> t2

    engine = WorkflowEngine()
    run = await engine.run(d)

    assert run.status == "failed"
    assert run.results["fail_step"].status == TaskStatus.FAILED
    assert run.results["downstream_step"].status == TaskStatus.SKIPPED
