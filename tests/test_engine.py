"""Unit tests for the multi-threaded DAG execution Engine."""

import time
import pytest
from taskmesh.dag import DAG
from taskmesh.engine import Engine
from taskmesh.task import Task
from taskmesh.types import ErrorPolicy, TaskStatus


def test_engine_linear_execution():
    dag = DAG("linear")

    @dag.task()
    def step1():
        return 10

    @dag.task(dependencies=["step1"])
    def step2(ctx):
        return ctx.get("step1") * 2

    @dag.task(dependencies=["step2"])
    def step3(ctx):
        return ctx.get("step2") + 5

    engine = Engine(max_workers=2)
    res = engine.run(dag)

    assert res.success is True
    assert res.outputs == {"step1": 10, "step2": 20, "step3": 25}
    assert res.get("step3") == 25
    assert res.total_duration > 0


def test_engine_diamond_parallel_execution():
    dag = DAG("diamond_parallel")

    @dag.task()
    def start():
        return 1

    @dag.task(dependencies=["start"])
    def branch_a(ctx):
        time.sleep(0.05)
        return ctx.get("start") + 10

    @dag.task(dependencies=["start"])
    def branch_b(ctx):
        time.sleep(0.05)
        return ctx.get("start") + 20

    @dag.task(dependencies=["branch_a", "branch_b"])
    def join(ctx):
        return ctx.get("branch_a") + ctx.get("branch_b")

    engine = Engine(max_workers=4)
    start_time = time.time()
    res = engine.run(dag)
    elapsed = time.time() - start_time

    assert res.success is True
    assert res.outputs["join"] == 32
    # Because branch_a and branch_b run in parallel, elapsed should be noticeably less
    # than sequential 0.05 + 0.05 = 0.10s (allowing small overhead buffer)
    assert elapsed < 0.18


def test_engine_abort_policy():
    dag = DAG("test_abort")

    @dag.task()
    def good_root():
        return "ok"

    @dag.task(dependencies=["good_root"])
    def bad_task():
        raise RuntimeError("Fatal branch error")

    @dag.task(dependencies=["bad_task"])
    def child_of_bad():
        return "should not run"

    @dag.task(dependencies=["good_root"])
    def independent_task():
        time.sleep(0.1)
        return "independent"

    engine = Engine(max_workers=2)
    res = engine.run(dag)

    assert res.success is False
    assert res.results["bad_task"].status == TaskStatus.FAILED
    assert res.results["child_of_bad"].status in (TaskStatus.CANCELLED, TaskStatus.SKIPPED)


def test_engine_continue_policy():
    dag = DAG("test_continue")

    @dag.task()
    def root():
        return "root"

    @dag.task(dependencies=["root"], error_policy=ErrorPolicy.CONTINUE)
    def failing_branch():
        raise ValueError("Non-fatal branch failure")

    @dag.task(dependencies=["failing_branch"])
    def dependent_of_failing():
        return "must be skipped"

    @dag.task(dependencies=["root"])
    def healthy_branch():
        return "healthy"

    engine = Engine(max_workers=2)
    res = engine.run(dag)

    # Pipeline is not marked completely successful because failing_branch failed
    assert res.success is False
    assert res.results["failing_branch"].status == TaskStatus.FAILED
    assert res.results["dependent_of_failing"].status == TaskStatus.SKIPPED
    assert res.results["healthy_branch"].status == TaskStatus.SUCCESS
    assert res.outputs["healthy_branch"] == "healthy"


def test_engine_ignore_policy():
    dag = DAG("test_ignore")

    @dag.task(error_policy=ErrorPolicy.IGNORE)
    def non_critical():
        raise KeyError("Ignore me")

    @dag.task(dependencies=["non_critical"])
    def downstream_step():
        return "ran despite upstream error"

    engine = Engine(max_workers=2)
    res = engine.run(dag)

    assert res.results["non_critical"].status == TaskStatus.FAILED
    assert res.results["downstream_step"].status == TaskStatus.SUCCESS
    assert res.outputs["downstream_step"] == "ran despite upstream error"


def test_engine_callbacks():
    dag = DAG("callbacks")
    started = []
    completed = []
    pipe_events = []

    @dag.task()
    def t1():
        return "t1"

    engine = Engine(
        max_workers=1,
        on_pipeline_start=lambda d: pipe_events.append("start"),
        on_task_start=lambda t: started.append(t.name),
        on_task_complete=lambda t, r: completed.append((t.name, r.status)),
        on_pipeline_complete=lambda d, r: pipe_events.append("complete"),
    )
    res = engine.run(dag)

    assert res.success is True
    assert started == ["t1"]
    assert completed == [("t1", TaskStatus.SUCCESS)]
    assert pipe_events == ["start", "complete"]


def test_engine_initial_context():
    dag = DAG("initial_ctx")

    @dag.task()
    def read_config(ctx):
        env = ctx.get("environment")
        return f"running in {env}"

    engine = Engine()
    res = engine.run(dag, initial_context={"environment": "production"})

    assert res.success is True
    assert res.outputs["read_config"] == "running in production"
