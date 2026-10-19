"""Unit tests for the taskmesh Command Line Interface."""

import json
from io import StringIO
import pytest
from taskmesh.cli import main


def test_cli_version(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["--version"])
    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "taskmesh" in captured.out or "taskmesh" in captured.err


def test_cli_no_args(capsys):
    ret = main([])
    assert ret == 0
    captured = capsys.readouterr()
    assert "usage:" in captured.out


def test_cli_validate_success(tmp_path, capsys):
    pipe_file = tmp_path / "valid.json"
    pipe_file.write_text(
        json.dumps(
            {
                "name": "cli_valid",
                "tasks": [
                    {"name": "a", "type": "python", "expr": "1"},
                    {"name": "b", "type": "python", "expr": "2", "dependencies": ["a"]},
                ],
            }
        ),
        encoding="utf-8",
    )
    ret = main(["validate", str(pipe_file)])
    assert ret == 0
    captured = capsys.readouterr()
    assert "Pipeline 'cli_valid' is VALID." in captured.out
    assert "a -> b" in captured.out


def test_cli_validate_failure_cycle(tmp_path, capsys):
    pipe_file = tmp_path / "cyclic.json"
    pipe_file.write_text(
        json.dumps(
            {
                "name": "cyclic_pipe",
                "tasks": [
                    {"name": "a", "type": "python", "expr": "1", "dependencies": ["b"]},
                    {"name": "b", "type": "python", "expr": "2", "dependencies": ["a"]},
                ],
            }
        ),
        encoding="utf-8",
    )
    ret = main(["validate", str(pipe_file)])
    assert ret == 1
    captured = capsys.readouterr()
    assert "Validation Failed: Circular dependency detected" in captured.err


def test_cli_run_success(tmp_path, capsys):
    pipe_file = tmp_path / "runnable.json"
    pipe_file.write_text(
        json.dumps(
            {
                "name": "cli_runner",
                "tasks": [
                    {"name": "step1", "type": "python", "expr": "10 + 20"},
                ],
            }
        ),
        encoding="utf-8",
    )
    ret = main(["run", str(pipe_file)])
    assert ret == 0
    captured = capsys.readouterr()
    assert "Executing pipeline 'cli_runner'" in captured.out
    assert "Pipeline Result: SUCCESS" in captured.out


def test_cli_run_json_output(tmp_path, capsys):
    pipe_file = tmp_path / "json_pipe.json"
    pipe_file.write_text(
        json.dumps(
            {
                "name": "json_test",
                "tasks": [
                    {"name": "fetch", "type": "python", "expr": "{'status': 'ok'}"},
                ],
            }
        ),
        encoding="utf-8",
    )
    ret = main(["run", str(pipe_file), "--json-output"])
    assert ret == 0
    captured = capsys.readouterr()
    output_obj = json.loads(captured.out)
    assert output_obj["success"] is True
    assert output_obj["results"]["fetch"]["status"] == "SUCCESS"
    assert output_obj["outputs"]["fetch"] == {"status": "ok"}


def test_cli_graph_ascii_and_dot(tmp_path, capsys):
    pipe_file = tmp_path / "graph_pipe.json"
    pipe_file.write_text(
        json.dumps(
            {
                "name": "graph_pipe",
                "tasks": [
                    {"name": "n1", "type": "python", "expr": "1"},
                    {"name": "n2", "type": "python", "expr": "2", "dependencies": ["n1"]},
                ],
            }
        ),
        encoding="utf-8",
    )

    # ASCII graph
    ret_ascii = main(["graph", str(pipe_file), "--format", "ascii"])
    assert ret_ascii == 0
    cap_ascii = capsys.readouterr()
    assert "Pipeline DAG: graph_pipe" in cap_ascii.out
    assert "[n1]" in cap_ascii.out
    assert "[n2]" in cap_ascii.out

    # DOT graph
    ret_dot = main(["graph", str(pipe_file), "--format", "dot"])
    assert ret_dot == 0
    cap_dot = capsys.readouterr()
    assert 'digraph "graph_pipe"' in cap_dot.out
    assert '"n1" -> "n2";' in cap_dot.out
