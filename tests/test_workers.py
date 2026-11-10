import asyncio
import pytest
from aether_flow import (
    AdvancedRetryPolicy,
    BackoffStrategy,
    DAG,
    Task,
    TaskPriority,
    TaskStatus,
    WorkflowEngine,
)


@pytest.mark.asyncio
async def test_task_priority_dispatch():
    engine = WorkflowEngine(max_concurrency=1)  # Force sequential dispatch to verify priority order
    dag = DAG("priority_pipeline")

    execution_order = []

    def make_task(t_id, priority):
        async def fn():
            execution_order.append(t_id)
            return t_id
        return Task(t_id, fn, priority=priority)

    # 3 parallel ready tasks with different priorities
    t_low = dag.add_task(make_task("t_low", TaskPriority.LOW))
    t_high = dag.add_task(make_task("t_high", TaskPriority.HIGH))
    t_crit = dag.add_task(make_task("t_crit", TaskPriority.CRITICAL))

    run = await engine.run(dag)
    assert run.status == "success"

    # Critical should run first, then High, then Low
    assert execution_order == ["t_crit", "t_high", "t_low"]


@pytest.mark.asyncio
async def test_concurrency_group_limiting():
    # Set group "api" max concurrency to 1
    engine = WorkflowEngine(max_concurrency=4, group_limits={"api": 1})
    dag = DAG("throttled_pipeline")

    active_api_tasks = 0
    max_observed_api = 0

    async def api_task():
        nonlocal active_api_tasks, max_observed_api
        active_api_tasks += 1
        max_observed_api = max(max_observed_api, active_api_tasks)
        await asyncio.sleep(0.05)
        active_api_tasks -= 1
        return "done"

    t1 = dag.add_task(Task("api_1", api_task, concurrency_group="api"))
    t2 = dag.add_task(Task("api_2", api_task, concurrency_group="api"))
    t3 = dag.add_task(Task("api_3", api_task, concurrency_group="api"))

    run = await engine.run(dag)
    assert run.status == "success"
    # Even though engine max_concurrency is 4, api group limit is 1
    assert max_observed_api == 1


@pytest.mark.asyncio
async def test_task_fallback_execution():
    engine = WorkflowEngine()
    dag = DAG("fallback_dag")

    def failing_fn():
        raise RuntimeError("Fatal database connection crash")

    policy = AdvancedRetryPolicy(
        max_retries=1,
        initial_delay=0.01,
        fallback=lambda exc, ctx: "fallback_recovery_value",
    )

    t = dag.add_task(Task("flaky_node", failing_fn, retries=policy))
    run = await engine.run(dag)

    assert run.status == "success"
    assert run.results["flaky_node"].status == TaskStatus.COMPLETED
    assert run.results["flaky_node"].output == "fallback_recovery_value"
