"""Directed Acyclic Graph (DAG) structure, validation, topological sort, and visualization."""

from __future__ import annotations

from collections import defaultdict, deque
from typing import Any, Callable, Dict, Iterator, List, Optional, Set

from taskmesh.task import Task
from taskmesh.types import (
    CycleError,
    DependencyNotFoundError,
    ErrorPolicy,
    PipelineValidationError,
    TaskContext,
)


class DAG:
    """Manages a collection of tasks connected via directed dependency relationships."""

    def __init__(self, name: str = "pipeline", description: str = ""):
        self.name = name
        self.description = description
        self._tasks: Dict[str, Task] = {}

    @property
    def tasks(self) -> Dict[str, Task]:
        """Dictionary of registered tasks indexed by name."""
        return dict(self._tasks)

    def add_task(self, task: Task) -> DAG:
        """Register a new task in the DAG.

        Raises:
            PipelineValidationError: If a task with the same name already exists.
        """
        if task.name in self._tasks:
            raise PipelineValidationError(
                f"Task '{task.name}' is already defined in DAG '{self.name}'."
            )
        self._tasks[task.name] = task
        return self

    def task(
        self,
        name: Optional[str] = None,
        dependencies: Optional[List[str] | Set[str]] = None,
        retries: int = 0,
        retry_delay: float = 0.0,
        retry_backoff: float = 1.0,
        timeout: Optional[float] = None,
        condition: Optional[Callable[[TaskContext], bool]] = None,
        error_policy: ErrorPolicy = ErrorPolicy.ABORT,
        description: str = "",
    ) -> Callable[[Callable[..., Any]], Task]:
        """Decorator for declaring and registering a task function into the DAG."""

        def decorator(fn: Callable[..., Any]) -> Task:
            task_name = name or fn.__name__
            t = Task(
                name=task_name,
                fn=fn,
                dependencies=dependencies,
                retries=retries,
                retry_delay=retry_delay,
                retry_backoff=retry_backoff,
                timeout=timeout,
                condition=condition,
                error_policy=error_policy,
                description=description or (fn.__doc__ or "").strip(),
            )
            self.add_task(t)
            return t

        return decorator

    def get_task(self, name: str) -> Task:
        """Fetch a task by name."""
        if name not in self._tasks:
            raise KeyError(f"Task '{name}' not found in DAG '{self.name}'.")
        return self._tasks[name]

    def get_dependencies(self, task_name: str) -> Set[str]:
        """Return the immediate upstream dependencies of a task."""
        return set(self.get_task(task_name).dependencies)

    def get_dependents(self, task_name: str) -> Set[str]:
        """Return immediate downstream tasks that depend directly on this task."""
        self.get_task(task_name)  # ensure task exists
        return {
            name
            for name, task in self._tasks.items()
            if task_name in task.dependencies
        }

    def get_all_downstream(self, task_name: str) -> Set[str]:
        """Return all tasks transitively dependent on the given task."""
        downstream: Set[str] = set()
        queue = deque([task_name])
        while queue:
            curr = queue.popleft()
            for child in self.get_dependents(curr):
                if child not in downstream:
                    downstream.add(child)
                    queue.append(child)
        return downstream

    def validate(self) -> None:
        """Validate DAG integrity.

        Checks for missing dependencies and cyclic references.
        Raises:
            DependencyNotFoundError: If a dependency does not exist.
            CycleError: If a cycle is present in the graph.
        """
        # 1. Check for missing dependencies
        for name, task in self._tasks.items():
            for dep in task.dependencies:
                if dep not in self._tasks:
                    raise DependencyNotFoundError(task_name=name, missing_dependency=dep)

        # 2. Cycle detection using DFS with path tracking
        visited: Dict[str, int] = {}  # 0: unvisited, 1: visiting, 2: visited
        for name in self._tasks:
            visited[name] = 0

        path: List[str] = []

        def dfs(curr: str) -> None:
            visited[curr] = 1
            path.append(curr)

            for dep in self._tasks[curr].dependencies:
                if visited[dep] == 1:
                    # Cycle detected: trace path from first occurrence of dep
                    cycle_start_idx = path.index(dep)
                    cycle_subpath = path[cycle_start_idx:] + [dep]
                    # Upstream dependencies flow from dep to curr, cycle display:
                    raise CycleError(cycle_subpath)
                elif visited[dep] == 0:
                    dfs(dep)

            path.pop()
            visited[curr] = 2

        for name in self._tasks:
            if visited[name] == 0:
                dfs(name)

    def get_topological_order(self) -> List[str]:
        """Compute and return a valid topological execution order (Kahn's Algorithm)."""
        self.validate()

        in_degree: Dict[str, int] = {name: 0 for name in self._tasks}
        graph: Dict[str, List[str]] = defaultdict(list)

        for name, task in self._tasks.items():
            for dep in task.dependencies:
                graph[dep].append(name)
                in_degree[name] += 1

        queue = deque([name for name, deg in in_degree.items() if deg == 0])
        ordered: List[str] = []

        while queue:
            node = queue.popleft()
            ordered.append(node)
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(ordered) != len(self._tasks):
            # This shouldn't normally happen after validate(), but safeguard:
            raise CycleError(["(unresolved cyclic dependency)"])

        return ordered

    def to_dot(self) -> str:
        """Export graph to Graphviz DOT syntax."""
        lines = [f'digraph "{self.name}" {{', "  rankdir=LR;", "  node [shape=box, style=rounded];"]
        for name, task in self._tasks.items():
            lines.append(f'  "{name}" [label="{name}"];')

        for name, task in self._tasks.items():
            for dep in task.dependencies:
                lines.append(f'  "{dep}" -> "{name}";')

        lines.append("}")
        return "\n".join(lines)

    def to_ascii(self) -> str:
        """Generate a clean ASCII overview of the DAG structure and dependencies."""
        self.validate()
        order = self.get_topological_order()
        lines = [f"Pipeline DAG: {self.name}", "=" * (len(self.name) + 14)]
        if self.description:
            lines.append(f"Description: {self.description}")
            lines.append("-" * (len(self.name) + 14))

        for task_name in order:
            task = self._tasks[task_name]
            deps = sorted(list(task.dependencies))
            dep_str = ", ".join(deps) if deps else "(root - no dependencies)"
            lines.append(f"  * [{task.name}]")
            lines.append(f"      depends on: {dep_str}")
            if task.retries:
                lines.append(f"      retries: {task.retries} (delay: {task.retry_delay}s)")
            if task.timeout:
                lines.append(f"      timeout: {task.timeout}s")
            if task.error_policy != ErrorPolicy.ABORT:
                lines.append(f"      error policy: {task.error_policy.value}")

        return "\n".join(lines)

    def __len__(self) -> int:
        return len(self._tasks)

    def __iter__(self) -> Iterator[str]:
        return iter(self._tasks)
