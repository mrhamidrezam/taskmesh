"""Unit tests for loading pipelines from dictionaries and JSON files."""

import json
import pytest
from taskmesh.engine import Engine
from taskmesh.loader import load_pipeline_dict, load_pipeline_file
from taskmesh.types import PipelineValidationError


def test_load_pipeline_dict_python():
    data = {
        "name": "python_eval_pipe",
        "description": "Calculates math expressions",
        "tasks": [
            {
                "name": "calc_a",
                "type": "python",
                "expr": "10 * 5",
            },
            {
                "name": "calc_b",
                "type": "python",
                "expr": "ctx.get('calc_a') + 25",
                "dependencies": ["calc_a"],
            },
        ],
    }

    dag = load_pipeline_dict(data)
    assert dag.name == "python_eval_pipe"
    assert len(dag) == 2

    engine = Engine()
    res = engine.run(dag)
    assert res.success is True
    assert res.outputs["calc_a"] == 50
    assert res.outputs["calc_b"] == 75


def test_load_pipeline_python_code_block():
    data = {
        "name": "code_block_pipe",
        "tasks": [
            {
                "name": "generate_list",
                "type": "python",
                "code": "result = [x ** 2 for x in range(5)]",
            }
        ],
    }
    dag = load_pipeline_dict(data)
    engine = Engine()
    res = engine.run(dag)
    assert res.success is True
    assert res.outputs["generate_list"] == [0, 1, 4, 9, 16]


def test_load_pipeline_shell(tmp_path):
    out_file = tmp_path / "test_out.txt"
    # Using python executable via shell for robust cross-platform echo
    cmd = f'python -c "print(\'taskmesh_test_token\')"'
    data = {
        "name": "shell_pipe",
        "tasks": [
            {
                "name": "run_cmd",
                "type": "shell",
                "command": cmd,
            }
        ],
    }
    dag = load_pipeline_dict(data)
    engine = Engine()
    res = engine.run(dag)

    assert res.success is True
    assert res.outputs["run_cmd"]["returncode"] == 0
    assert "taskmesh_test_token" in res.outputs["run_cmd"]["stdout"]


def test_load_pipeline_file(tmp_path):
    pipeline_json = tmp_path / "pipeline.json"
    content = {
        "name": "from_file",
        "tasks": [
            {"name": "t1", "type": "python", "expr": "'loaded_from_disk'"}
        ],
    }
    pipeline_json.write_text(json.dumps(content), encoding="utf-8")

    dag = load_pipeline_file(str(pipeline_json))
    assert dag.name == "from_file"
    assert "t1" in dag.tasks


def test_load_pipeline_missing_name():
    with pytest.raises(PipelineValidationError, match="missing required 'name'"):
        load_pipeline_dict({"tasks": [{"type": "python", "expr": "1"}]})


def test_load_pipeline_unknown_type():
    with pytest.raises(PipelineValidationError, match="Unknown task type"):
        load_pipeline_dict(
            {"tasks": [{"name": "bad", "type": "nonexistent_type"}]}
        )


def sample_external_task(ctx):
    return "custom_val_" + str(ctx.get("num", 0))


def test_load_pipeline_custom_function():
    data = {
        "name": "custom_func_pipe",
        "tasks": [
            {
                "name": "custom_step",
                "type": "custom",
                "function": "tests.test_loader:sample_external_task",
            }
        ],
    }
    dag = load_pipeline_dict(data)
    engine = Engine()
    res = engine.run(dag, initial_context={"num": 42})
    assert res.success is True
    assert res.outputs["custom_step"] == "custom_val_42"


def test_load_pipeline_custom_invalid_function():
    data = {
        "name": "bad_custom",
        "tasks": [
            {
                "name": "custom_step",
                "type": "custom",
                "function": "invalid_no_colon",
            }
        ],
    }
    with pytest.raises(PipelineValidationError, match="Expected format"):
        load_pipeline_dict(data)


def test_load_pipeline_missing_file():
    with pytest.raises(FileNotFoundError):
        load_pipeline_file("this_file_does_not_exist_12345.json")

