import pytest
from aether_flow.graph import CyclicDependencyError, GraphValidator
from aether_flow.models import DAG, Task


def test_valid_topological_sort():
    d = DAG("test_dag")
    t1 = d.add_task(Task("extract", lambda: 1))
    t2 = d.add_task(Task("transform", lambda: 2))
    t3 = d.add_task(Task("load", lambda: 3))

    t1 >> t2 >> t3

    order = GraphValidator.topological_sort(d)
    assert order == ["extract", "transform", "load"]


def test_cycle_detection():
    d = DAG("cycle_dag")
    t1 = d.add_task(Task("a", lambda: 1))
    t2 = d.add_task(Task("b", lambda: 2))
    t3 = d.add_task(Task("c", lambda: 3))

    t1 >> t2 >> t3 >> t1  # Creates circular loop

    with pytest.raises(CyclicDependencyError):
        GraphValidator.validate(d)


def test_missing_dependency():
    d = DAG("missing_dep")
    t1 = d.add_task(Task("a", lambda: 1, dependencies={"nonexistent"}))

    with pytest.raises(ValueError, match="non-existent task"):
        GraphValidator.validate(d)
