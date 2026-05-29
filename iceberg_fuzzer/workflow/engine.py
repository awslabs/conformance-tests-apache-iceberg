"""
Workflow engine for iceberg-fuzzer.

Runs multi-step workflows against existing Iceberg tables.
"""

import shlex
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from string import Template

from iceberg_fuzzer.backend import AssertionResult, Checkpoint, ValidationEngine
from iceberg_fuzzer.backend.duckdb import DuckDBValidation
from iceberg_fuzzer.backend.pyspark.mutation import PySparkMutation
from iceberg_fuzzer.backend.pyspark.validation import PySparkValidation
from iceberg_fuzzer.workflow.loader import Workflow, load_workflow

StepId = str
EngineName = str
SnapshotId = int


@dataclass
class OperationResult:
    """Result of a single operation handler."""
    assertions: list[AssertionResult]
    error: str | None = None
    snapshot_id: SnapshotId | None = None

    @classmethod
    def success(cls, assertions: list[AssertionResult], snapshot_id: SnapshotId | None = None):
        return cls(assertions=assertions, snapshot_id=snapshot_id)

    @classmethod
    def failed(cls, error: str):
        return cls(assertions=[], error=error)

    @classmethod
    def dry_run(cls):
        return cls(assertions=[])


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
            a.passed for assertions in self.step_assertions.values() for a in assertions
        )


def _init_mutation_engines(
    warehouse_path: str, backends: set[str], seed: int | None = None
) -> dict:
    """Create mutation engine instances for the requested backends."""
    engines = {}
    for name in backends:
        if name == "pyspark":
            engines[name] = PySparkMutation(warehouse_path, seed=seed)
        else:
            raise ValueError(f"Unknown backend: {name}")
    return engines


def _get_mutation_engine(step: dict, mutation_engines: dict):
    """Return the mutation engine for a step's declared backend."""
    return mutation_engines[step['config']['backend']]


def _cross_engine_assertions(post_hashes: dict[str, int]) -> list[AssertionResult]:
    """Assert that all engines agree on the content hash."""
    if len(post_hashes) < 2:
        return []

    values = list(post_hashes.values())
    all_agree = all(v == values[0] for v in values)
    return [AssertionResult(
        name="cross_engine.content_hash",
        passed=all_agree,
        expected="all engines agree",
        actual=f"{post_hashes}" if not all_agree else f"agreed ({values[0]})",
    )]


def _handle_replace(step, engines, warehouse_path, namespace, name, placeholders, output_dir, workflow_name, dry_run=False) -> OperationResult:
    """Handle a 'replace' step: checkpoint with all engines, run command, validate all."""
    step_id = step['id']
    step_config = step['config']
    resolved = Template(step_config['command']).safe_substitute(placeholders)

    if dry_run:
        print(f"    {step_id} [replace]: {resolved.strip()}")
        return OperationResult.dry_run()

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
                timeout=step_config.get('timeout', 300),
            )
            returncode = result.returncode
        except subprocess.TimeoutExpired:
            err_f.write(f"\nOperation timed out after {step_config.get('timeout', 300)}s\n")
            returncode = -1

    if returncode != 0:
        return OperationResult.failed(f"Step '{step_id}' exited with code {returncode}")

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
    return OperationResult.success(assertions)


def _handle_append(step, engines, warehouse_path, namespace, name, mutation_engines, dry_run=False) -> OperationResult:
    """Handle an 'append' step: append rows, validate row count with all engines."""
    step_id = step['id']
    step_config = step['config']
    num_rows = step_config['num_rows']

    if dry_run:
        print(f"    {step_id} [append]: {num_rows} rows via {step_config['backend']}")
        return OperationResult.dry_run()

    backend = _get_mutation_engine(step, mutation_engines)

    # Use first engine for pre-count (all should agree)
    _, first_engine = engines[0]
    pre_count = first_engine.capture_checkpoint(warehouse_path, namespace, name).row_count
    snapshot_id = backend.append_rows(namespace, name, num_rows)

    assertions: list[AssertionResult] = []
    post_hashes: dict[str, int] = {}

    for engine_name, engine in engines:
        post = engine.capture_checkpoint(warehouse_path, namespace, name)
        assertions.append(AssertionResult.row_count(pre_count + num_rows, post.row_count, prefix=engine_name))
        post_hashes[engine_name] = post.content_hash

    assertions.extend(_cross_engine_assertions(post_hashes))
    return OperationResult.success(assertions, snapshot_id=snapshot_id)


def _handle_delete(step, engines, warehouse_path, namespace, name, mutation_engines, dry_run=False) -> OperationResult:
    """Handle a 'delete' step: delete random rows, validate row count."""
    step_id = step['id']
    step_config = step['config']
    num_rows = step_config['num_rows']

    if dry_run:
        print(f"    {step_id} [delete]: {num_rows} rows via {step_config['backend']}")
        return OperationResult.dry_run()

    backend = _get_mutation_engine(step, mutation_engines)

    _, first_engine = engines[0]
    pre_count = first_engine.capture_checkpoint(warehouse_path, namespace, name).row_count
    snapshot_id = backend.delete_rows(namespace, name, num_rows)

    assertions: list[AssertionResult] = []
    post_hashes: dict[str, int] = {}

    for engine_name, engine in engines:
        post = engine.capture_checkpoint(warehouse_path, namespace, name)
        assertions.append(AssertionResult.row_count(pre_count - num_rows, post.row_count, prefix=engine_name))
        post_hashes[engine_name] = post.content_hash

    assertions.extend(_cross_engine_assertions(post_hashes))
    return OperationResult.success(assertions, snapshot_id=snapshot_id)


def _handle_rollback(step, engines, warehouse_path, namespace, name, mutation_engines,
                     snapshot_id_by_step, accumulated_checkpoints, dry_run=False) -> OperationResult:
    """Handle a 'rollback' step: roll back to a prior step's snapshot, then validate."""
    step_id = step['id']
    step_config = step['config']
    to_step = step_config['to_step']

    if dry_run:
        print(f"    {step_id} [rollback]: to step '{to_step}' via {step_config['backend']}")
        return OperationResult.dry_run()

    if to_step not in snapshot_id_by_step:
        return OperationResult.failed(f"Step '{step_id}': no snapshot_id recorded for step '{to_step}'")

    if to_step not in accumulated_checkpoints:
        return OperationResult.failed(f"Step '{step_id}': no checkpoint stored for step '{to_step}'")

    target_snapshot_id = snapshot_id_by_step[to_step]
    target_checkpoints = accumulated_checkpoints[to_step]

    backend = _get_mutation_engine(step, mutation_engines)
    actual_snapshot_id = backend.rollback_to_snapshot(namespace, name, target_snapshot_id)

    assertions: list[AssertionResult] = [
        AssertionResult.equal("snapshot_id", target_snapshot_id, actual_snapshot_id),
    ]
    post_hashes: dict[str, int] = {}

    for engine_name, engine in engines:
        post = engine.capture_checkpoint(warehouse_path, namespace, name)
        target = target_checkpoints[engine_name]

        assertions.append(AssertionResult.row_count(target.row_count, post.row_count, prefix=engine_name))
        assertions.append(AssertionResult.content_hash(target.content_hash, post.content_hash, prefix=engine_name))
        post_hashes[engine_name] = post.content_hash

    assertions.extend(_cross_engine_assertions(post_hashes))
    return OperationResult.success(assertions)


def _process_table(
    workflow_def: Workflow,
    warehouse_path: str,
    namespace: str,
    table_name: str,
    output_dir: str,
    engines: list[tuple[str, ValidationEngine]],
    dry_run: bool = False,
    seed: int | None = None,
) -> TableResult:
    """Process a single table through all workflow steps."""
    workflow_name = workflow_def.name

    result = TableResult(table_name=table_name, namespace=namespace)

    placeholders = {
        'table_name': table_name,
        'namespace': namespace,
        'warehouse': warehouse_path,
    }

    backends_needed = {step['config']['backend'] for step in workflow_def.steps if 'backend' in step.get('config', {})}
    mutation_engines = _init_mutation_engines(warehouse_path, backends_needed, seed=seed) if not dry_run else {}

    rollback_targets: set[str] = {
        step['config']['to_step'] for step in workflow_def.steps
        if step['operation'] == 'rollback'
    }

    snapshot_id_by_step: dict[StepId, SnapshotId] = {}
    accumulated_checkpoints: dict[StepId, dict[EngineName, Checkpoint]] = {}

    try:
        for step in workflow_def.steps:
            step_id = step['id']
            operation = step['operation']

            try:
                if operation == 'replace':
                    op_result = _handle_replace(
                        step, engines, warehouse_path, namespace, table_name, placeholders, output_dir, workflow_name, dry_run
                    )
                elif operation == 'append':
                    op_result = _handle_append(
                        step, engines, warehouse_path, namespace, table_name, mutation_engines, dry_run
                    )
                elif operation == 'delete':
                    op_result = _handle_delete(
                        step, engines, warehouse_path, namespace, table_name, mutation_engines, dry_run
                    )
                elif operation == 'rollback':
                    op_result = _handle_rollback(
                        step, engines, warehouse_path, namespace, table_name,
                        mutation_engines, snapshot_id_by_step, accumulated_checkpoints, dry_run
                    )
                else:
                    op_result = OperationResult.failed(f"Unknown operation '{operation}' at step '{step_id}'")
            except Exception as e:
                op_result = OperationResult.failed(f"Step '{step_id}' failed: {e}")

            result.step_assertions[step_id] = op_result.assertions

            if op_result.error:
                result.error = op_result.error
                return result

            if op_result.snapshot_id is not None:
                snapshot_id_by_step[step_id] = op_result.snapshot_id

            if step_id in rollback_targets and not dry_run:
                checkpoints_by_engine: dict[EngineName, Checkpoint] = {}
                for engine_name, engine in engines:
                    checkpoints_by_engine[engine_name] = engine.capture_checkpoint(
                        warehouse_path, namespace, table_name
                    )
                accumulated_checkpoints[step_id] = checkpoints_by_engine
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
    seed: int | None = None,
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
            ("duckdb", DuckDBValidation(warehouse_path)),
            ("pyspark", PySparkValidation(warehouse_path)),
        ]

    try:
        result = _process_table(workflow_def, warehouse_path, namespace, table_name, output_dir, engines, dry_run, seed=seed)
    finally:
        for _, engine in engines:
            engine.close()

    if not dry_run:
        _print_table_result(result)

    return result if not dry_run else None
