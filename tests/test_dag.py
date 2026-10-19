"""Unit tests for DAG structure, topological sort, cycle detection, and export."""

import pytest
from taskmesh.dag import DAG
from taskmesh.task import Task
from taskmesh.types import (
    CycleError,
    DependencyNotFoundError,
    PipelineValidationError,
)


def dummy_fn():
    return 42


def test_dag_creation_and_add_task():
    dag = DAG("test_dag", description="test description")
    assert dag.name == "test_dag"
    assert dag.description == "test description"
    assert len(dag) == 0

    t1 = Task("task_a", dummy_fn)
    dag.add_task(t1)
    assert len(dag) == 1
    assert dag.get_task("task_a") is t1


def test_dag_duplicate_task_error():
    dag = DAG()
    dag.add_task(Task("task_a", dummy_fn))
    with pytest.raises(PipelineValidationError, match="already defined"):
        dag.add_task(Task("task_a", dummy_fn))


def test_dag_task_decorator():
    dag = DAG("decorated")

    @dag.task()
    def step_one():
        return 1

    @dag.task(dependencies=["step_one"])
    def step_two(ctx):
        return ctx.get("step_one") + 1

    assert "step_one" in dag.tasks
    assert "step_two" in dag.tasks
    assert dag.get_task("step_two").dependencies == {"step_one"}


def test_dag_missing_dependency():
    dag = DAG()
    dag.add_task(Task("step_b", dummy_fn, dependencies=["non_existent_a"]))
    with pytest.raises(DependencyNotFoundError) as exc_info:
        dag.validate()
    assert exc_info.value.task_name == "step_b"
    assert exc_info.value.missing_dependency == "non_existent_a"


def test_dag_direct_cycle_detection():
    dag = DAG()
    dag.add_task(Task("A", dummy_fn, dependencies=["B"]))
    dag.add_task(Task("B", dummy_fn, dependencies=["A"]))

    with pytest.raises(CycleError) as exc_info:
        dag.validate()
    assert len(exc_info.value.cycle_path) >= 2


def test_dag_indirect_cycle_detection():
    dag = DAG()
    dag.add_task(Task("A", dummy_fn, dependencies=["C"]))
    dag.add_task(Task("B", dummy_fn, dependencies=["A"]))
    dag.add_task(Task("C", dummy_fn, dependencies=["B"]))

    with pytest.raises(CycleError):
        dag.get_topological_order()


def test_dag_diamond_topological_sort():
    # A -> B, A -> C, B -> D, C -> D
    dag = DAG("diamond")
    dag.add_task(Task("A", dummy_fn))
    dag.add_task(Task("B", dummy_fn, dependencies=["A"]))
    dag.add_task(Task("C", dummy_fn, dependencies=["A"]))
    dag.add_task(Task("D", dummy_fn, dependencies=["B", "C"]))

    order = dag.get_topological_order()
    assert order[0] == "A"
    assert order[-1] == "D"
    assert set(order[1:3]) == {"B", "C"}


def test_dag_dependents_and_downstream():
    dag = DAG()
    dag.add_task(Task("A", dummy_fn))
    dag.add_task(Task("B", dummy_fn, dependencies=["A"]))
    dag.add_task(Task("C", dummy_fn, dependencies=["B"]))
    dag.add_task(Task("D", dummy_fn, dependencies=["A"]))

    assert dag.get_dependents("A") == {"B", "D"}
    assert dag.get_all_downstream("A") == {"B", "C", "D"}
    assert dag.get_all_downstream("B") == {"C"}
    assert dag.get_all_downstream("C") == set()


def test_dag_dot_export():
    dag = DAG("sample_dot")
    dag.add_task(Task("first", dummy_fn))
    dag.add_task(Task("second", dummy_fn, dependencies=["first"]))

    dot = dag.to_dot()
    assert 'digraph "sample_dot"' in dot
    assert '"first" -> "second";' in dot


def test_dag_ascii_export():
    dag = DAG("sample_ascii", description="Ascii preview")
    dag.add_task(Task("start", dummy_fn))
    dag.add_task(Task("end", dummy_fn, dependencies=["start"], retries=2))

    ascii_out = dag.to_ascii()
    assert "Pipeline DAG: sample_ascii" in ascii_out
    assert "[start]" in ascii_out
    assert "[end]" in ascii_out
    assert "depends on: start" in ascii_out
    assert "retries: 2" in ascii_out
