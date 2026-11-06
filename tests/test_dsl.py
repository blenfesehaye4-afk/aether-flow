from aether_flow.dsl import dag, task
from aether_flow.models import Task


def test_fan_out_and_fan_in_operators():
    d = dag("etl_pipeline")

    t_start = d.add_task(Task("start", lambda: "start"))
    t_branch_a = d.add_task(Task("branch_a", lambda: "A"))
    t_branch_b = d.add_task(Task("branch_b", lambda: "B"))
    t_join = d.add_task(Task("join", lambda: "done"))

    # Fan out: start >> [branch_a, branch_b]
    t_start >> [t_branch_a, t_branch_b]
    assert "start" in t_branch_a.dependencies
    assert "start" in t_branch_b.dependencies

    # Fan in: [branch_a, branch_b] >> join
    t_branch_a >> t_join
    t_branch_b >> t_join
    assert "branch_a" in t_join.dependencies
    assert "branch_b" in t_join.dependencies
