"""Additional unit tests covering TaskContext, PipelineResult, and edge cases."""

import pytest
from taskmesh.types import (
    ErrorPolicy,
    PipelineResult,
    PipelineValidationError,
    TaskContext,
    TaskResult,
    TaskStatus,
)


def test_task_context_dict_operations():
    ctx = TaskContext({"a": 1, "b": 2})
    assert len(ctx) == 2
    assert "a" in ctx
    assert "c" not in ctx
    assert ctx["a"] == 1
    assert ctx.get("c", 99) == 99

    ctx.set("c", 3)
    assert len(ctx) == 3
    assert ctx["c"] == 3

    ctx.update({"d": 4, "e": 5})
    assert len(ctx) == 5
    assert ctx["d"] == 4

    keys = list(iter(ctx))
    assert set(keys) == {"a", "b", "c", "d", "e"}

    as_dict = ctx.to_dict()
    assert isinstance(as_dict, dict)
    assert as_dict["e"] == 5
    assert "TaskContext" in repr(ctx)


def test_task_result_properties():
    res = TaskResult(
        task_name="t1",
        status=TaskStatus.SUCCESS,
        output={"key": "val"},
        start_time=100.0,
        end_time=102.5,
        attempts=2,
    )
    assert res.duration == 2.5
    assert res.is_success is True

    d = res.to_dict()
    assert d["task_name"] == "t1"
    assert d["status"] == "SUCCESS"
    assert d["duration"] == 2.5
    assert d["attempts"] == 2


def test_pipeline_result_to_dict_and_get():
    r1 = TaskResult("step1", TaskStatus.SUCCESS, output="data1")
    r2 = TaskResult("step2", TaskStatus.FAILED, error="Some error")
    pr = PipelineResult(
        success=False,
        results={"step1": r1, "step2": r2},
        total_duration=1.23,
    )

    assert pr.get("step1") == "data1"
    assert pr.get("step2") is None
    assert pr.get("step2", "default_val") == "default_val"
    assert pr.get("missing", 42) == 42

    d = pr.to_dict()
    assert d["success"] is False
    assert d["total_duration"] == 1.23
    assert "step1" in d["results"]
    assert d["outputs"] == {"step1": "data1"}
