# taskmesh Setup & Installation Guide

This guide details installation options, development setup, test execution, and deployment instructions for `taskmesh`.

---

## 1. System Requirements

- **Python**: Version 3.9 or higher (tested up to Python 3.14+).
- **Operating System**: Linux, macOS, or Windows.
- **Dependencies**: None. `taskmesh` operates completely within the Python Standard Library.

---

## 2. Installation

### From Source (Editable / Development Mode)

To install `taskmesh` locally for active development:

```bash
git clone https://github.com/mrhamidrezam/taskmesh.git
cd taskmesh

# Install in editable mode
pip install -e .

# Or install with dev dependencies (pytest)
pip install -e ".[dev]"
```

### Standard User Installation

```bash
pip install .
```

Verify your installation:

```bash
taskmesh --version
```

---

## 3. Running Unit Tests

The test suite utilizes `pytest` to exercise all DAG logic, concurrency models, retry policies, and CLI workflows.

### Run All Tests

```bash
pytest
```

### Run with Verbose Output

```bash
pytest -v
```

### Run Specific Test Modules

```bash
# Test engine & concurrency
pytest tests/test_engine.py -v

# Test graph topological sorting & cycle detection
pytest tests/test_dag.py -v

# Test CLI integration
pytest tests/test_cli.py -v
```

---

## 4. Quick Verification Run

Run the included example script to verify that thread pooling and dependency resolution are operating properly:

```bash
python examples/programmatic_dag.py
```

Or run the JSON workflow example using the CLI:

```bash
taskmesh run examples/data_pipeline.json
```
