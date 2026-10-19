"""Core types, enums, data containers, and exception classes for taskmesh."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterator, Mapping, Optional


class TaskStatus(str, Enum):
    """Execution states for a task."""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"
    CANCELLED = "CANCELLED"


class ErrorPolicy(str, Enum):
    """Determines pipeline behavior when a task fails."""
    ABORT = "ABORT"        # Immediately abort and cancel remaining pending tasks
    CONTINUE = "CONTINUE"  # Skip downstream dependents of failed task, but proceed with independent tasks
    IGNORE = "IGNORE"      # Treat task failure as non-fatal and proceed as if success


class TaskMeshError(Exception):
    """Base exception for taskmesh operations."""


class CycleError(TaskMeshError):
    """Raised when circular dependencies are detected in a DAG."""

    def __init__(self, cycle_path: list[str]):
        self.cycle_path = cycle_path
        chain = " -> ".join(cycle_path)
        super().__init__(f"Circular dependency detected in DAG: {chain}")


class DependencyNotFoundError(TaskMeshError):
    """Raised when a task depends on an undeclared task name."""

    def __init__(self, task_name: str, missing_dependency: str):
        self.task_name = task_name
        self.missing_dependency = missing_dependency
        super().__init__(
            f"Task '{task_name}' depends on undeclared task '{missing_dependency}'"
        )


class TaskTimeoutError(TaskMeshError):
    """Raised when a task execution exceeds its configured timeout."""


class PipelineValidationError(TaskMeshError):
    """Raised when pipeline structure or configuration is invalid."""


@dataclass
class TaskResult:
    """Stores the execution output, timing, and error details of a task."""
    task_name: str
    status: TaskStatus = TaskStatus.PENDING
    output: Any = None
    error: Optional[str] = None
    start_time: float = 0.0
    end_time: float = 0.0
    attempts: int = 0

    @property
    def duration(self) -> float:
        """Returns elapsed execution time in seconds."""
        if self.end_time > 0 and self.start_time > 0:
            return max(0.0, self.end_time - self.start_time)
        return 0.0

    @property
    def is_success(self) -> bool:
        """Returns True if task ended with SUCCESS."""
        return self.status == TaskStatus.SUCCESS

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to serializable dictionary."""
        return {
            "task_name": self.task_name,
            "status": self.status.value,
            "output": self.output,
            "error": self.error,
            "duration": round(self.duration, 4),
            "attempts": self.attempts,
        }


@dataclass
class PipelineResult:
    """Summary of the full pipeline execution."""
    success: bool
    results: Dict[str, TaskResult] = field(default_factory=dict)
    total_duration: float = 0.0

    @property
    def outputs(self) -> Dict[str, Any]:
        """Convenience map of task names to their return values for successful tasks."""
        return {
            name: result.output
            for name, result in self.results.items()
            if result.status == TaskStatus.SUCCESS
        }

    def get(self, task_name: str, default: Any = None) -> Any:
        """Retrieve output of a given task."""
        res = self.results.get(task_name)
        if res and res.status == TaskStatus.SUCCESS:
            return res.output
        return default

    def to_dict(self) -> Dict[str, Any]:
        """Serialize pipeline outcome to dictionary."""
        return {
            "success": self.success,
            "total_duration": round(self.total_duration, 4),
            "results": {
                name: res.to_dict() for name, res in self.results.items()
            },
            "outputs": self.outputs,
        }


class TaskContext(Mapping[str, Any]):
    """Thread-safe context passed to tasks during pipeline execution.

    Allows tasks to read upstream results and set custom variables.
    Implements standard mapping methods for easy dict-like access.
    """

    def __init__(self, initial_data: Optional[Dict[str, Any]] = None):
        self._lock = threading.RLock()
        self._data: Dict[str, Any] = dict(initial_data) if initial_data else {}

    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._data[key] = value

    def update(self, other: Mapping[str, Any]) -> None:
        with self._lock:
            self._data.update(other)

    def __getitem__(self, key: str) -> Any:
        with self._lock:
            return self._data[key]

    def __contains__(self, key: object) -> bool:
        with self._lock:
            return key in self._data

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(list(self._data.keys()))

    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._data)

    def __repr__(self) -> str:
        with self._lock:
            return f"TaskContext({self._data})"
