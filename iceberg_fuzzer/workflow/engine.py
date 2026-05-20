"""
Workflow engine for iceberg-fuzzer.

Runs multi-step workflows against existing Iceberg tables.
Validates with all available query engines and asserts cross-engine agreement on content hashes.

Step operations:
  - replace: Runs an external command that should not change logical content.
    Captures a checkpoint before, validates row count + content hash after.
  - append: Adds rows to the table via a backend. Validates row count increased by num_rows.
"""

import os
import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from string import Template

import yaml

from iceberg_fuzzer.backend import AssertionResult, Checkpoint, ValidationEngine
from iceberg_fuzzer.backend.duckdb import DuckDBValidation
from iceberg_fuzzer.backend.pyspark.mutation import PySparkMutation
from iceberg_fuzzer.backend.pyspark.validation import PySparkValidation

StepId = str

OperationHandlerReturnType = tuple[list[AssertionResult], str | None]


@dataclass
class TableResult:
    """Result of running a workflow against a single table.

    Collects per-step assertion results and any fatal error that
    halted execution. A table passes if all assertions pass and
    no unrecoverable error occurred.
    """
    table_name: str
    namespace: str
    step_assertions: dict[StepId, list[AssertionResult]] = field(default_factory=dict)
    error: str = ""

    @property
    def passed(self):
        if self.error:
            return False
        return all(
            a.passed
            for assertions in self.step_assertions.values()
            for a in assertions
        )


def _cross_engine_assertions(post_hashes: dict[str, int]) -> list[AssertionResult]:
    """Assert that all engines agree on the content hash."""
    if len(post_hashes) < 2:
        return []

    values = list(post_hashes.values())
    all_agree = all(v == values[0] for v in values)
    return [AssertionResult(
        name="cross_engine.content_hash",
        passed=all_agree,
        expected=f"all engines agree",
        actual=f"{post_hashes}" if not all_agree else f"agreed ({values[0]})",
    )]


def _get_or_init_mutation_engine(name: str, warehouse_path: str, mutation_engines: dict):
    """Return a cached mutation engine, creating it on first use."""
    if name not in mutation_engines:
        if name == 'pyspark':
            mutation_engines[name] = PySparkMutation(warehouse_path)
        else:
            raise ValueError(f"Unknown backend: {name}")
    return mutation_engines[name]


@dataclass
class Workflow:
    """A parsed and validated workflow definition."""
    name: str
    steps: list[dict]


def load_workflow(path: str) -> Workflow:
    """Load and validate a workflow definition from a YAML file."""
    with open(path, 'r') as f:
        workflow_def = yaml.safe_load(f)

    if 'name' not in workflow_def:
        raise ValueError(f"Workflow YAML missing required 'name' field: {path}")
    if 'steps' not in workflow_def or not workflow_def['steps']:
        raise ValueError(f"Workflow YAML missing or empty 'steps' field: {path}")

    for i, step in enumerate(workflow_def['steps']):
        if 'id' not in step:
            raise ValueError(f"Step {i} missing required 'id' field")
        if 'operation' not in step:
            raise ValueError(f"Step '{step.get('id', i)}' missing required 'operation' field")
        op = step['operation']
        if op == 'replace' and 'command' not in step:
            raise ValueError(f"Step '{step['id']}' (replace) missing required 'command' field")
        if op == 'append':
            if 'backend' not in step:
                raise ValueError(f"Step '{step['id']}' (append) missing required 'backend' field")
            if 'num_rows' not in step:
                raise ValueError(f"Step '{step['id']}' (append) missing required 'num_rows' field")

    return Workflow(name=workflow_def['name'], steps=workflow_def['steps'])



def _handle_replace(step, engines, warehouse_path, namespace, name, placeholders, output_dir, workflow_name, dry_run=False) -> OperationHandlerReturnType:
    """Handle a 'replace' step: checkpoint with all engines, run command, validate all."""
    step_id = step['id']
    resolved = Template(step['command']).safe_substitute(placeholders)

    if dry_run:
        print(f"    {step_id} [replace]: {resolved.strip()}")
        return [], None

    checkpoints: dict[str, Checkpoint] = {}
    for engine_name, engine in engines:
        checkpoints[engine_name] = engine.capture_checkpoint(warehouse_path, namespace, name)

    out_dir = Path(output_dir) / workflow_name
    out_dir.mkdir(parents=True, exist_ok=True)

    stdout_path = out_dir / f"{namespace}.{name}.{step_id}.stdout"
    stderr_path = out_dir / f"{namespace}.{name}.{step_id}.stderr"

    with open(stdout_path, 'w') as out_f, open(stderr_path, 'w') as err_f:
        try:
            result = subprocess.run(
                shlex.split(resolved),
                shell=False,
                stdout=out_f,
                stderr=err_f,
                timeout=step.get('timeout', 300),
            )
            returncode = result.returncode
        except subprocess.TimeoutExpired:
            err_f.write(f"\nOperation timed out after {step.get('timeout', 300)}s\n")
            returncode = -1

    if returncode != 0:
        return [], f"Step '{step_id}' exited with code {returncode}"

    assertions: list[AssertionResult] = []
    post_hashes: dict[str, int] = {}

    for engine_name, engine in engines:
        engine_assertions = engine.validate(checkpoints[engine_name], warehouse_path, namespace, name)
        for a in engine_assertions:
            assertions.append(AssertionResult(
                name=f"{engine_name}.{a.name}",
                passed=a.passed,
                expected=a.expected,
                actual=a.actual,
            ))
            if a.name == 'content_hash':
                post_hashes[engine_name] = a.actual

    assertions.extend(_cross_engine_assertions(post_hashes))
    return assertions, None


def _handle_append(step, engines, warehouse_path, namespace, name, mutation_engines, dry_run=False) -> OperationHandlerReturnType:
    """Handle an 'append' step: append rows, validate row count with all engines."""
    step_id = step['id']
    num_rows = step['num_rows']

    if dry_run:
        print(f"    {step_id} [append]: {num_rows} rows via {step['backend']}")
        return [], None

    backend = _get_or_init_mutation_engine(step['backend'], warehouse_path, mutation_engines)

    # Use first engine for pre-count (all should agree)
    _, first_engine = engines[0]
    pre_count = first_engine.capture_checkpoint(warehouse_path, namespace, name).row_count
    backend.append_rows(namespace, name, num_rows)

    assertions: list[AssertionResult] = []
    post_hashes: dict[str, int] = {}

    for engine_name, engine in engines:
        post = engine.capture_checkpoint(warehouse_path, namespace, name)
        assertions.append(AssertionResult(
            name=f"{engine_name}.row_count",
            passed=(pre_count + num_rows) == post.row_count,
            expected=pre_count + num_rows,
            actual=post.row_count,
        ))
        post_hashes[engine_name] = post.content_hash

    assertions.extend(_cross_engine_assertions(post_hashes))
    return assertions, None


def _process_table(
    workflow_def: Workflow,
    warehouse_path: str,
    namespace: str,
    table_name: str,
    output_dir: str,
    engines: list[tuple[str, ValidationEngine]],
    dry_run: bool = False,
) -> TableResult:
    """Process a single table through all workflow steps."""
    workflow_name = workflow_def.name
    table_path = os.path.join(warehouse_path, namespace, table_name)

    result = TableResult(table_name=table_name, namespace=namespace)

    if not dry_run and not Path(table_path).exists():
        result.error = f"Table not found at {table_path}"
        return result

    placeholders = {
        'table_name': table_name,
        'namespace': namespace,
        'warehouse': warehouse_path,
    }

    mutation_engines = {}

    try:
        for step in workflow_def.steps:
            step_id = step['id']
            operation = step['operation']
            assertions: list[AssertionResult] = []
            error: str | None = None

            try:
                if operation == 'replace':
                    assertions, error = _handle_replace(
                        step, engines, warehouse_path, namespace, table_name, placeholders, output_dir, workflow_name, dry_run
                    )
                elif operation == 'append':
                    assertions, error = _handle_append(
                        step, engines, warehouse_path, namespace, table_name, mutation_engines, dry_run
                    )
                else:
                    error = f"Unknown operation '{operation}' at step '{step_id}'"
            except Exception as e:
                error = f"Step '{step_id}' failed: {e}"

            result.step_assertions[step_id] = assertions

            if error:
                result.error = error
                return result
    finally:
        for me in mutation_engines.values():
            me.close()

    return result


def _print_table_result(result: TableResult):
    """Print per-table result."""
    prefix = "✅" if result.passed else "❌"
    qualified = f"{result.namespace}.{result.table_name}"

    if result.error:
        # Show any steps that completed before the error
        for step_id, assertions in result.step_assertions.items():
            summary = ", ".join(f"{a.name}={'PASS' if a.passed else 'FAIL'}" for a in assertions)
            print(f"  ✅ {qualified} [{step_id}]: {summary}")
        print(f"  {prefix} {qualified}: FAILED - {result.error}")
        return

    for step_id, assertions in result.step_assertions.items():
        summary = ", ".join(f"{a.name}={'PASS' if a.passed else 'FAIL'}" for a in assertions)
        step_passed = all(a.passed for a in assertions)
        step_prefix = "✅" if step_passed else "❌"
        print(f"  {step_prefix} {qualified} [{step_id}]: {summary}")
        for a in assertions:
            if not a.passed:
                print(f"       {a.name}: expected={a.expected}, actual={a.actual}")


def run_workflow(
    workflow_file: str,
    warehouse_path: str,
    table_name: str,
    namespace: str,
    dry_run: bool = False,
    output_dir: str = 'output/workflow-logs',
) -> TableResult | None:
    """Run a workflow against a single table.

    Validates each step with all available engines (DuckDB, PySpark) and
    asserts cross-engine content hash agreement.

    Returns a TableResult, or None if dry_run is True.
    """
    workflow_def = load_workflow(workflow_file)

    print(f"\n{'='*60}")
    print(f"WORKFLOW: {workflow_def.name}")
    print(f"{'='*60}")

    if dry_run:
        print(f"  {namespace}.{table_name}:")
        engines = []
    else:
        engines = [
            ("duckdb", DuckDBValidation()),
            ("pyspark", PySparkValidation(warehouse_path)),
        ]

    try:
        result = _process_table(workflow_def, warehouse_path, namespace, table_name, output_dir, engines, dry_run)
    finally:
        for _, engine in engines:
            engine.close()

    if not dry_run:
        _print_table_result(result)

    return result if not dry_run else None
