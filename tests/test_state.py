import pytest
from aether_flow.state import StateStore


def test_state_store_and_event_auditing():
    store = StateStore()
    run = store.create_run("test_dag", initial_context={"env": "prod"})

    assert run.dag_id == "test_dag"
    assert run.status == "running"
    assert run.context["env"] == "prod"
    assert len(run.events) >= 1

    run.record_event("CUSTOM_METRIC", {"cpu_percent": 14.2})
    assert len(run.events) == 2

    retrieved = store.get_run(run.run_id)
    assert retrieved is not None
    assert retrieved.run_id == run.run_id
