import pytest
from fastapi.testclient import TestClient
from aether_flow import DAG, Task, WorkflowEngine
from aether_flow.dashboard import app, register_dag, set_dashboard_engine

client = TestClient(app)


def test_dashboard_health_and_ui():
    # Health
    res = client.get("/health")
    assert res.status_code == 200
    assert res.json() == {"status": "healthy", "service": "aether-flow", "version": "0.3.0"}

    # UI HTML response
    ui_res = client.get("/")
    assert ui_res.status_code == 200
    assert "AetherFlow" in ui_res.text
    assert "DAG Topology Visualization" in ui_res.text


def test_dashboard_dag_registration_and_execution():
    dag = DAG("test_ui_pipeline", name="Test Pipeline")
    t1 = dag.add_task(Task("step_1", lambda: "step 1 output"))
    t2 = dag.add_task(Task("step_2", lambda step_1: f"processed {step_1}"))
    t1 >> t2

    engine = WorkflowEngine()
    set_dashboard_engine(engine, [dag])

    # 1. List DAGs
    dags_res = client.get("/api/dags")
    assert dags_res.status_code == 200
    data = dags_res.json()
    assert any(d["id"] == "test_ui_pipeline" for d in data)

    # 2. Trigger Run
    run_res = client.post("/api/dags/test_ui_pipeline/run", json={})
    assert run_res.status_code == 200
    run_data = run_res.json()
    run_id = run_data["run_id"]
    assert run_data["status"] == "success"

    # 3. List Runs
    list_runs_res = client.get("/api/runs")
    assert list_runs_res.status_code == 200
    assert any(r["run_id"] == run_id for r in list_runs_res.json())

    # 4. Get Run Details
    detail_res = client.get(f"/api/runs/{run_id}")
    assert detail_res.status_code == 200
    details = detail_res.json()
    assert details["status"] == "success"
    assert "step_1" in details["results"]
    assert "step_2" in details["results"]
