"""Task definition, execution wrapper, retry logic, and timeout enforcement."""

from __future__ import annotations

import inspect
import time
import traceback
from concurrent import futures
from typing import Any, Callable, Optional, Set

from taskmesh.types import (
    ErrorPolicy,
    TaskContext,
    TaskResult,
    TaskStatus,
    TaskTimeoutError,
)


class Task:
    """Represents an atomic, executable unit of work in a DAG."""

    def __init__(
        self,
        name: str,
        fn: Callable[..., Any],
        dependencies: Optional[Set[str] | list[str]] = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        retry_backoff: float = 1.0,
        timeout: Optional[float] = None,
        condition: Optional[Callable[[TaskContext], bool]] = None,
        error_policy: ErrorPolicy = ErrorPolicy.ABORT,
        description: str = "",
    ):
        if not name or not isinstance(name, str):
            raise ValueError("Task name must be a non-empty string.")
        if not callable(fn):
            raise TypeError(f"Task '{name}' requires a callable function.")

        self.name = name.strip()
        self.fn = fn
        self.dependencies: Set[str] = set(dependencies or [])
        self.retries = max(0, int(retries))
        self.retry_delay = max(0.0, float(retry_delay))
        self.retry_backoff = max(1.0, float(retry_backoff))
        self.timeout = float(timeout) if timeout is not None else None
        self.condition = condition
        self.error_policy = error_policy
        self.description = description

    def _invoke_function(self, context: TaskContext) -> Any:
        """Call target function with context if signature accepts arguments."""
        sig = inspect.signature(self.fn)
        num_params = len(sig.parameters)
        if num_params == 0:
            return self.fn()
        return self.fn(context)

    def execute(self, context: TaskContext) -> TaskResult:
        """Execute the task with condition check, retries, and optional timeout."""
        # 1. Condition check
        if self.condition is not None:
            try:
                should_run = bool(self.condition(context))
            except Exception as e:
                return TaskResult(
                    task_name=self.name,
                    status=TaskStatus.FAILED,
                    error=f"Task condition check failed: {e}\n{traceback.format_exc()}",
                    attempts=1,
                )
            if not should_run:
                return TaskResult(
                    task_name=self.name,
                    status=TaskStatus.SKIPPED,
                    attempts=0,
                )

        # 2. Execution attempt loop
        start_time = time.time()
        attempts = 0
        max_attempts = 1 + self.retries
        current_delay = self.retry_delay
        last_error: Optional[str] = None

        while attempts < max_attempts:
            attempts += 1
            attempt_start = time.time()
            try:
                if self.timeout is not None and self.timeout > 0:
                    with futures.ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(self._invoke_function, context)
                        try:
                            output = future.result(timeout=self.timeout)
                        except futures.TimeoutError:
                            raise TaskTimeoutError(
                                f"Task '{self.name}' timed out after {self.timeout} seconds."
                            )
                else:
                    output = self._invoke_function(context)

                # Success
                end_time = time.time()
                return TaskResult(
                    task_name=self.name,
                    status=TaskStatus.SUCCESS,
                    output=output,
                    start_time=start_time,
                    end_time=end_time,
                    attempts=attempts,
                )

            except Exception as exc:
                last_error = f"{type(exc).__name__}: {str(exc)}\n{traceback.format_exc()}"
                if attempts < max_attempts:
                    if current_delay > 0:
                        time.sleep(current_delay)
                    current_delay *= self.retry_backoff

        # All attempts exhausted
        end_time = time.time()
        return TaskResult(
            task_name=self.name,
            status=TaskStatus.FAILED,
            error=last_error,
            start_time=start_time,
            end_time=end_time,
            attempts=attempts,
        )

    def __repr__(self) -> str:
        deps = sorted(list(self.dependencies))
        return f"Task(name='{self.name}', dependencies={deps}, retries={self.retries})"
