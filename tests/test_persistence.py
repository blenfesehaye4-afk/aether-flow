from pathlib import Path
import pytest
from aether_flow.engine import WorkflowEngine
from aether_flow.models import DAG, Task, TaskStatus
from aether_flow.persistence import SQLiteStateStore


@pytest.mark.asyncio
async def test_sqlite_persistence_and_checkpoint_resume(tmp_path: Path):
    db_file = tmp_path / "test_aether.db"
    store = SQLiteStateStore(db_file)

    d = DAG("etl_pipeline")
    fail_first = True

    def extract():
        return {"records": [1, 2, 3]}

    def flaky_transform(extract: dict):
        nonlocal fail_first
        if fail_first:
            raise RuntimeError("Database connection timed out")
        return len(extract["records"])

    def load(flaky_transform: int):
        return f"Loaded {flaky_transform} items"

    t1 = d.add_task(Task("extract", extract))
    t2 = d.add_task(Task("flaky_transform", flaky_transform))
    t3 = d.add_task(Task("load", load))

    t1 >> t2 >> t3

    # Run 1: Should fail on transform
    engine = WorkflowEngine(state_store=store)
    run1 = await engine.run(d)

    assert run1.status == "failed"
    assert run1.results["extract"].status == TaskStatus.COMPLETED
    assert run1.results["flaky_transform"].status == TaskStatus.FAILED
    assert run1.results["load"].status == TaskStatus.SKIPPED

    # Verify run saved in SQLite
    reloaded = store.load_run(run1.run_id)
    assert reloaded is not None
    assert reloaded.results["extract"].output == {"records": [1, 2, 3]}

    # Fix error condition and resume from exact checkpoint
    fail_first = False
    resumed_run = await engine.resume(d, run1.run_id)

    assert resumed_run.status == "success"
    # Extract was NOT re-run, loaded from checkpoint
    assert resumed_run.results["extract"].status == TaskStatus.COMPLETED
    assert resumed_run.results["flaky_transform"].status == TaskStatus.COMPLETED
    assert resumed_run.results["load"].output == "Loaded 3 items"
