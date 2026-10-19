"""Unit tests for Task execution, retries, timeouts, and conditions."""

import time
import pytest
from taskmesh.task import Task
from taskmesh.types import TaskContext, TaskStatus, TaskTimeoutError


def test_task_init_validation():
    with pytest.raises(ValueError, match="non-empty"):
        Task("", lambda: None)
    with pytest.raises(TypeError, match="callable"):
        Task("invalid_task", "not_a_function")  # type: ignore


def test_task_execute_success():
    task = Task("calc", lambda ctx: ctx.get("base", 10) * 2)
    ctx = TaskContext({"base": 5})
    result = task.execute(ctx)

    assert result.status == TaskStatus.SUCCESS
    assert result.output == 10
    assert result.attempts == 1
    assert result.is_success is True
    assert result.duration >= 0


def test_task_execute_no_args():
    task = Task("simple", lambda: "hello world")
    result = task.execute(TaskContext())
    assert result.status == TaskStatus.SUCCESS
    assert result.output == "hello world"


def test_task_condition_skip():
    task = Task(
        "conditional",
        lambda: 100,
        condition=lambda ctx: ctx.get("enable_feature", False),
    )
    # Feature disabled: should skip
    res1 = task.execute(TaskContext({"enable_feature": False}))
    assert res1.status == TaskStatus.SKIPPED
    assert res1.attempts == 0

    # Feature enabled: should run
    res2 = task.execute(TaskContext({"enable_feature": True}))
    assert res2.status == TaskStatus.SUCCESS
    assert res2.output == 100


def test_task_condition_error():
    def broken_condition(ctx):
        raise ValueError("Bad condition")

    task = Task("bad_cond", lambda: 100, condition=broken_condition)
    res = task.execute(TaskContext())
    assert res.status == TaskStatus.FAILED
    assert "Task condition check failed" in res.error


def test_task_retries_eventual_success():
    counter = 0

    def flaky_func():
        nonlocal counter
        counter += 1
        if counter < 3:
            raise ConnectionError(f"Attempt {counter} failed")
        return "finally succeeded"

    task = Task("flaky", flaky_func, retries=3, retry_delay=0.01)
    res = task.execute(TaskContext())

    assert res.status == TaskStatus.SUCCESS
    assert res.output == "finally succeeded"
    assert res.attempts == 3


def test_task_retries_exhausted():
    def always_fails():
        raise RuntimeError("Permanent failure")

    task = Task("doomed", always_fails, retries=2, retry_delay=0.01)
    res = task.execute(TaskContext())

    assert res.status == TaskStatus.FAILED
    assert "Permanent failure" in res.error
    assert res.attempts == 3


def test_task_timeout():
    def slow_func():
        time.sleep(0.5)
        return "done"

    task = Task("slow", slow_func, timeout=0.05)
    res = task.execute(TaskContext())

    assert res.status == TaskStatus.FAILED
    assert "timed out after 0.05 seconds" in res.error
