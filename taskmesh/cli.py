"""Command Line Interface for taskmesh."""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

from taskmesh import __version__
from taskmesh.engine import Engine
from taskmesh.loader import load_pipeline_file
from taskmesh.types import TaskMeshError, TaskStatus


def _format_status_colored(status: TaskStatus) -> str:
    """Format status for terminal presentation."""
    return f"[{status.value}]"


def run_pipeline_command(args: argparse.Namespace) -> int:
    """Execute pipeline file and print results."""
    try:
        dag = load_pipeline_file(args.pipeline_file)
    except TaskMeshError as e:
        print(f"Error loading pipeline: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        return 1

    if not args.json_output:
        print(f"Executing pipeline '{dag.name}' ({len(dag)} tasks, max_workers={args.workers})...\n")

    def on_task_start(task):
        if not args.json_output:
            print(f"  -> Starting task: {task.name}")

    def on_task_complete(task, result):
        if not args.json_output:
            status_tag = _format_status_colored(result.status)
            duration_str = f"({result.duration:.3f}s)"
            if result.status == TaskStatus.SUCCESS:
                print(f"  {status_tag} {task.name} {duration_str}")
            elif result.status == TaskStatus.SKIPPED:
                print(f"  {status_tag} {task.name}")
            else:
                print(f"  {status_tag} {task.name} {duration_str}: {result.error}")

    engine = Engine(
        max_workers=args.workers,
        on_task_start=on_task_start,
        on_task_complete=on_task_complete,
    )

    result = engine.run(dag)

    if args.json_output:
        print(json.dumps(result.to_dict(), indent=2))
    else:
        print("\n" + "=" * 40)
        overall_status = "SUCCESS" if result.success else "FAILED"
        print(f"Pipeline Result: {overall_status} (Total Duration: {result.total_duration:.3f}s)")
        print("=" * 40)

    if args.strict and not result.success:
        return 1
    return 0 if result.success else 1


def validate_pipeline_command(args: argparse.Namespace) -> int:
    """Validate pipeline file structure without execution."""
    try:
        dag = load_pipeline_file(args.pipeline_file)
        dag.validate()
        order = dag.get_topological_order()
        print(f"Pipeline '{dag.name}' is VALID.")
        print(f"Tasks: {len(dag)}")
        print(f"Topological Execution Order: {' -> '.join(order)}")
        return 0
    except TaskMeshError as e:
        print(f"Validation Failed: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def graph_pipeline_command(args: argparse.Namespace) -> int:
    """Render DAG visualization in ASCII or DOT format."""
    try:
        dag = load_pipeline_file(args.pipeline_file)
        if args.format == "dot":
            print(dag.to_dot())
        else:
            print(dag.to_ascii())
        return 0
    except TaskMeshError as e:
        print(f"Error rendering graph: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


def build_parser() -> argparse.ArgumentParser:
    """Build argument parser for taskmesh CLI."""
    parser = argparse.ArgumentParser(
        prog="taskmesh",
        description="Lightweight DAG task orchestration and pipeline execution engine.",
    )
    parser.add_argument(
        "--version",
        "-v",
        action="version",
        version=f"%(prog)s {__version__}",
    )

    subparsers = parser.add_subparsers(dest="command", help="Subcommand to execute")

    # Subcommand: run
    run_parser = subparsers.add_parser("run", help="Execute a pipeline from a JSON definition file.")
    run_parser.add_argument("pipeline_file", help="Path to pipeline definition JSON file.")
    run_parser.add_argument(
        "--workers",
        "-w",
        type=int,
        default=4,
        help="Maximum concurrent worker threads (default: 4).",
    )
    run_parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with non-zero code on any failure.",
    )
    run_parser.add_argument(
        "--json-output",
        "-j",
        action="store_true",
        help="Output full execution results as JSON.",
    )

    # Subcommand: validate
    val_parser = subparsers.add_parser("validate", help="Validate a pipeline file structure and dependencies.")
    val_parser.add_argument("pipeline_file", help="Path to pipeline definition JSON file.")

    # Subcommand: graph
    graph_parser = subparsers.add_parser("graph", help="Render DAG dependency graph.")
    graph_parser.add_argument("pipeline_file", help="Path to pipeline definition JSON file.")
    graph_parser.add_argument(
        "--format",
        "-f",
        choices=["ascii", "dot"],
        default="ascii",
        help="Output format (ascii or dot, default: ascii).",
    )

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    """CLI entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 0

    if args.command == "run":
        return run_pipeline_command(args)
    elif args.command == "validate":
        return validate_pipeline_command(args)
    elif args.command == "graph":
        return graph_pipeline_command(args)

    return 0


if __name__ == "__main__":
    sys.exit(main())
