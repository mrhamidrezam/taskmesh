"""Workflow loader for JSON and dictionary defined pipelines."""

from __future__ import annotations

import importlib
import json
import os
import re
import subprocess
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional

from taskmesh.dag import DAG
from taskmesh.task import Task
from taskmesh.types import ErrorPolicy, PipelineValidationError, TaskContext


def _interpolate_template(template: str, context: TaskContext) -> str:
    """Replace {{ var }} and {var} patterns with values from TaskContext."""
    def replacer(match: re.Match) -> str:
        var_name = match.group(1).strip()
        val = context.get(var_name)
        return "" if val is None else str(val)

    # Match {{ key }} or {key}
    res = re.sub(r"\{\{\s*([\w\.\-]+)\s*\}\}", replacer, template)
    return res


def _create_shell_handler(command: str) -> Callable[[TaskContext], Dict[str, Any]]:
    """Build a callable that executes a system shell command."""
    def handler(context: TaskContext) -> Dict[str, Any]:
        rendered_cmd = _interpolate_template(command, context)
        proc = subprocess.run(
            rendered_cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimeError(
                f"Command '{rendered_cmd}' failed with returncode {proc.returncode}:\n{proc.stderr}"
            )
        return {
            "stdout": proc.stdout.strip(),
            "stderr": proc.stderr.strip(),
            "returncode": proc.returncode,
        }

    return handler


def _create_python_handler(
    code: Optional[str] = None,
    expr: Optional[str] = None,
) -> Callable[[TaskContext], Any]:
    """Build a callable that executes a Python expression or code block."""
    if expr:
        def expr_handler(context: TaskContext) -> Any:
            safe_globals = {
                "context": context,
                "os": os,
                "json": json,
            }
            return eval(expr, safe_globals, {"ctx": context})

        return expr_handler

    if code:
        def code_handler(context: TaskContext) -> Any:
            local_scope: Dict[str, Any] = {"ctx": context, "result": None}
            global_scope = {"context": context, "os": os, "json": json}
            exec(code, global_scope, local_scope)
            return local_scope.get("result")

        return code_handler

    raise PipelineValidationError("Python task must specify 'expr' or 'code'.")


def _create_http_handler(
    url: str,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    body: Optional[str | Dict[str, Any]] = None,
) -> Callable[[TaskContext], Dict[str, Any]]:
    """Build a callable that executes an HTTP request via standard library urllib."""
    def http_handler(context: TaskContext) -> Dict[str, Any]:
        rendered_url = _interpolate_template(url, context)
        req_headers = dict(headers or {})
        req_data = None

        if body:
            if isinstance(body, dict):
                req_data = json.dumps(body).encode("utf-8")
                req_headers.setdefault("Content-Type", "application/json")
            elif isinstance(body, str):
                rendered_body = _interpolate_template(body, context)
                req_data = rendered_body.encode("utf-8")

        req = urllib.request.Request(
            rendered_url,
            data=req_data,
            headers=req_headers,
            method=method.upper(),
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                content_type = response.headers.get("Content-Type", "")
                raw_data = response.read().decode("utf-8", errors="replace")
                parsed_data = raw_data
                if "application/json" in content_type:
                    try:
                        parsed_data = json.loads(raw_data)
                    except json.JSONDecodeError:
                        pass
                return {
                    "status": response.status,
                    "data": parsed_data,
                    "headers": dict(response.headers),
                }
        except urllib.error.HTTPError as err:
            raise RuntimeError(f"HTTP {err.code}: {err.reason}")

    return http_handler


def _create_custom_handler(func_path: str) -> Callable[..., Any]:
    """Import and return a custom Python function by 'module.path:func_name'."""
    if ":" not in func_path:
        raise PipelineValidationError(
            f"Invalid function path '{func_path}'. Expected format 'module.submodule:function_name'."
        )
    mod_name, func_name = func_path.split(":", 1)
    try:
        mod = importlib.import_module(mod_name)
    except ImportError as e:
        raise PipelineValidationError(f"Could not import module '{mod_name}': {e}")
    if not hasattr(mod, func_name):
        raise PipelineValidationError(f"Module '{mod_name}' has no attribute '{func_name}'.")
    target = getattr(mod, func_name)
    if not callable(target):
        raise PipelineValidationError(f"'{func_path}' is not callable.")
    return target


def load_pipeline_dict(data: Dict[str, Any]) -> DAG:
    """Parse a dictionary into an executable DAG pipeline."""
    if not isinstance(data, dict):
        raise PipelineValidationError("Pipeline definition must be a dictionary.")

    name = data.get("name", "pipeline")
    description = data.get("description", "")
    tasks_data = data.get("tasks", [])

    if not isinstance(tasks_data, list):
        raise PipelineValidationError("'tasks' field must be a list.")

    dag = DAG(name=name, description=description)

    for idx, tdata in enumerate(tasks_data):
        if not isinstance(tdata, dict):
            raise PipelineValidationError(f"Task at index {idx} must be a dictionary.")

        t_name = tdata.get("name")
        if not t_name:
            raise PipelineValidationError(f"Task at index {idx} missing required 'name' field.")

        t_type = tdata.get("type", "python")
        dependencies = tdata.get("dependencies", [])
        retries = tdata.get("retries", 0)
        retry_delay = tdata.get("retry_delay", 0.0)
        retry_backoff = tdata.get("retry_backoff", 1.0)
        timeout = tdata.get("timeout")
        desc = tdata.get("description", "")

        err_pol_str = tdata.get("error_policy", "ABORT").upper()
        try:
            error_policy = ErrorPolicy[err_pol_str]
        except KeyError:
            error_policy = ErrorPolicy.ABORT

        # Build handler based on type
        if t_type == "shell":
            cmd = tdata.get("command")
            if not cmd:
                raise PipelineValidationError(f"Shell task '{t_name}' missing 'command'.")
            fn = _create_shell_handler(cmd)

        elif t_type == "python":
            code = tdata.get("code")
            expr = tdata.get("expr")
            if not code and not expr:
                raise PipelineValidationError(f"Python task '{t_name}' missing 'code' or 'expr'.")
            fn = _create_python_handler(code=code, expr=expr)

        elif t_type == "http":
            url = tdata.get("url")
            if not url:
                raise PipelineValidationError(f"HTTP task '{t_name}' missing 'url'.")
            fn = _create_http_handler(
                url=url,
                method=tdata.get("method", "GET"),
                headers=tdata.get("headers"),
                body=tdata.get("body"),
            )

        elif t_type == "custom":
            func_target = tdata.get("function")
            if not func_target:
                raise PipelineValidationError(f"Custom task '{t_name}' missing 'function'.")
            fn = _create_custom_handler(func_target)

        else:
            raise PipelineValidationError(
                f"Unknown task type '{t_type}' for task '{t_name}'. Supported: shell, python, http, custom."
            )

        task = Task(
            name=t_name,
            fn=fn,
            dependencies=dependencies,
            retries=retries,
            retry_delay=retry_delay,
            retry_backoff=retry_backoff,
            timeout=timeout,
            error_policy=error_policy,
            description=desc,
        )
        dag.add_task(task)

    dag.validate()
    return dag


def load_pipeline_file(file_path: str) -> DAG:
    """Load a DAG from a JSON pipeline definition file."""
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"Pipeline file not found: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as err:
            raise PipelineValidationError(f"Invalid JSON in '{file_path}': {err}")

    return load_pipeline_dict(data)
