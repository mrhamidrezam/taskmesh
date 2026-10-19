"""Multi-threaded pipeline execution engine with dependency-aware scheduling."""

from __future__ import annotations

import logging
import time
from concurrent import futures
from typing import Any, Callable, Dict, Optional, Set

from taskmesh.dag import DAG
from taskmesh.task import Task
from taskmesh.types import (
    ErrorPolicy,
    PipelineResult,
    TaskContext,
    TaskResult,
    TaskStatus,
)

logger = logging.getLogger("taskmesh")


class Engine:
    """Executes DAGs concurrently using a pool of worker threads.

    Respects task dependency constraints, dynamically dispatches ready tasks,
    enforces error handling policies (ABORT, CONTINUE, IGNORE), and populates
    a shared execution context.
    """

    def __init__(
        self,
        max_workers: int = 4,
        on_task_start: Optional[Callable[[Task], None]] = None,
        on_task_complete: Optional[Callable[[Task, TaskResult], None]] = None,
        on_pipeline_start: Optional[Callable[[DAG], None]] = None,
        on_pipeline_complete: Optional[Callable[[DAG, PipelineResult], None]] = None,
    ):
        if max_workers < 1:
            raise ValueError("max_workers must be at least 1.")
        self.max_workers = max_workers
        self.on_task_start = on_task_start
        self.on_task_complete = on_task_complete
        self.on_pipeline_start = on_pipeline_start
        self.on_pipeline_complete = on_pipeline_complete

    def run(
        self,
        dag: DAG,
        initial_context: Optional[Dict[str, Any]] = None,
    ) -> PipelineResult:
        """Run the specified DAG to completion or until an abort occurs.

        Args:
            dag: The directed acyclic graph of tasks to execute.
            initial_context: Optional baseline variables for TaskContext.

        Returns:
            PipelineResult containing individual task outputs, errors, and timing.
        """
        dag.validate()

        start_time = time.time()
        if self.on_pipeline_start:
            try:
                self.on_pipeline_start(dag)
            except Exception as e:
                logger.warning("Pipeline start callback error: %s", e)

        context = TaskContext(initial_context)
        results: Dict[str, TaskResult] = {}
        finished: Set[str] = set()
        satisfied: Set[str] = set()
        in_progress: Set[str] = set()

        all_task_names = set(dag.tasks.keys())
        aborted = False

        with futures.ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            active_futures: Dict[futures.Future[TaskResult], Task] = {}

            while len(finished) < len(all_task_names):
                if aborted:
                    break

                # 1. Identify tasks ready for execution
                ready_tasks: list[Task] = []
                for name, task in dag.tasks.items():
                    if name not in finished and name not in in_progress:
                        # Task is ready if all its dependencies are satisfied
                        if task.dependencies.issubset(satisfied):
                            ready_tasks.append(task)

                # 2. Submit ready tasks to the thread pool
                for task in ready_tasks:
                    in_progress.add(task.name)
                    if self.on_task_start:
                        try:
                            self.on_task_start(task)
                        except Exception as e:
                            logger.warning("Task start callback error for %s: %s", task.name, e)

                    fut = pool.submit(task.execute, context)
                    active_futures[fut] = task

                # 3. Check for completion or stalled dependencies
                if not active_futures:
                    # No active tasks and no ready tasks - remaining cannot be fulfilled
                    remaining = all_task_names - finished - in_progress
                    for r_name in remaining:
                        finished.add(r_name)
                        results[r_name] = TaskResult(
                            task_name=r_name,
                            status=TaskStatus.SKIPPED,
                            error="Skipped due to unfulfilled upstream dependencies.",
                        )
                    break

                # 4. Wait for at least one active task to complete
                done_futures, _ = futures.wait(
                    active_futures.keys(),
                    return_when=futures.FIRST_COMPLETED,
                )

                for fut in done_futures:
                    task = active_futures.pop(fut)
                    in_progress.remove(task.name)
                    finished.add(task.name)

                    try:
                        result = fut.result()
                    except Exception as exc:
                        result = TaskResult(
                            task_name=task.name,
                            status=TaskStatus.FAILED,
                            error=f"Uncaught task exception: {exc}",
                        )

                    results[task.name] = result

                    if self.on_task_complete:
                        try:
                            self.on_task_complete(task, result)
                        except Exception as e:
                            logger.warning("Task complete callback error for %s: %s", task.name, e)

                    # 5. Handle task completion status
                    if result.status == TaskStatus.SUCCESS:
                        satisfied.add(task.name)
                        context.set(task.name, result.output)

                    elif result.status == TaskStatus.SKIPPED:
                        # Downstream dependent tasks are also skipped
                        for downstream_name in dag.get_all_downstream(task.name):
                            if downstream_name not in finished:
                                finished.add(downstream_name)
                                results[downstream_name] = TaskResult(
                                    task_name=downstream_name,
                                    status=TaskStatus.SKIPPED,
                                    error=f"Upstream task '{task.name}' was skipped.",
                                )

                    elif result.status == TaskStatus.FAILED:
                        if task.error_policy == ErrorPolicy.IGNORE:
                            # Treat as non-fatal: mark dependency satisfied so downstream can proceed
                            satisfied.add(task.name)
                        elif task.error_policy == ErrorPolicy.CONTINUE:
                            # Skip all downstream dependents, but let other independent branches run
                            for downstream_name in dag.get_all_downstream(task.name):
                                if downstream_name not in finished:
                                    finished.add(downstream_name)
                                    results[downstream_name] = TaskResult(
                                        task_name=downstream_name,
                                        status=TaskStatus.SKIPPED,
                                        error=f"Upstream dependency '{task.name}' failed with CONTINUE policy.",
                                    )
                        else:  # ErrorPolicy.ABORT
                            aborted = True
                            # Mark all remaining unstarted tasks as CANCELLED
                            remaining = all_task_names - finished - set(results.keys())
                            for r_name in remaining:
                                finished.add(r_name)
                                results[r_name] = TaskResult(
                                    task_name=r_name,
                                    status=TaskStatus.CANCELLED,
                                    error=f"Pipeline aborted due to failure in task '{task.name}'.",
                                )
                            # Cancel pending futures where possible
                            for pending_fut in active_futures:
                                pending_fut.cancel()
                            break

        total_duration = max(0.0, time.time() - start_time)

        # Check overall pipeline success: no fatal failures and not aborted
        fatal_failures = [
            name for name, res in results.items()
            if res.status == TaskStatus.FAILED and dag.get_task(name).error_policy != ErrorPolicy.IGNORE
        ]
        pipeline_success = len(fatal_failures) == 0 and not aborted

        pipeline_result = PipelineResult(
            success=pipeline_success,
            results=results,
            total_duration=total_duration,
        )

        if self.on_pipeline_complete:
            try:
                self.on_pipeline_complete(dag, pipeline_result)
            except Exception as e:
                logger.warning("Pipeline complete callback error: %s", e)

        return pipeline_result
