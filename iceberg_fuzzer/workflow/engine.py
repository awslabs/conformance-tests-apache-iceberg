"""
Workflow engine for iceberg-fuzzer.

Runs multi-step workflows against existing Iceberg tables.
"""

import importlib
import re
import shlex
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from string import Template

from iceberg_fuzzer.backend import AssertionResult, Checkpoint, ColTypeStr, ColumnName, ValidationEngine, is_s3_tables_warehouse
from iceberg_fuzzer.backend.point_query import validate_point_queries
from iceberg_fuzzer.checks import TableCheck, iceberg_metadata_checks
from iceberg_fuzzer.workflow.loader import Workflow, load_workflow

StepId = str
EngineName = str
SnapshotId = int

# Seconds a replace step's command may run when the step sets no timeout.
DEFAULT_STEP_TIMEOUT = 300


@dataclass
class OperationResult:
    """Result of a single operation handler."""
    assertions: list[AssertionResult]
    error: str | None = None
    snapshot_id: SnapshotId | None = None
    appended_ids: set[int] | None = None
    deleted_ids: set[int] | None = None

    @classmethod
    def success(cls, assertions: list[AssertionResult], snapshot_id: SnapshotId | None = None, appended_ids: set[int] | None = None, deleted_ids: set[int] | None = None):
        return cls(assertions=assertions, snapshot_id=snapshot_id, appended_ids=appended_ids, deleted_ids=deleted_ids)

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


# Writer backends a workflow step can name, as "module:class". Classes are imported only when a
# workflow uses them, so a backend's dependencies are needed only by the workflows that use it.
MUTATION_BACKENDS: dict[str, str] = {
    "pyspark": "iceberg_fuzzer.backend.pyspark.mutation:PySparkMutation",
    "pyiceberg": "iceberg_fuzzer.backend.pyiceberg.mutation:PyIcebergMutation",
    "duckdb": "iceberg_fuzzer.backend.duckdb.mutation:DuckDBMutation",
}


def register_mutation_backend(name: str, target: str) -> None:
    """Make a writer backend available to workflow steps. target is "module:class", and the
    class takes the warehouse path and a seed keyword argument, like PySparkMutation."""
    MUTATION_BACKENDS[name] = target


# Readers `run_workflow` can validate with, as "module:class". Arrow readers take a function that
# loads the table and row filters; the others take the warehouse path.
ARROW_READERS: dict[str, str] = {
    "pyiceberg": "iceberg_fuzzer.backend.pyiceberg.validation:PyIcebergValidation",
    "polars": "iceberg_fuzzer.backend.polars.validation:PolarsValidation",
    "daft": "iceberg_fuzzer.backend.daft.validation:DaftValidation",
    "iceberg_rust": "iceberg_fuzzer.backend.iceberg_rust.validation:IcebergRustValidation",
}
WAREHOUSE_READERS: dict[str, str] = {
    "duckdb": "iceberg_fuzzer.backend.duckdb:DuckDBValidation",
    "pyspark": "iceberg_fuzzer.backend.pyspark.validation:PySparkValidation",
}
DEFAULT_READERS = ("duckdb", "pyspark")


def _load(target: str):
    module, _, cls = target.partition(":")
    return getattr(importlib.import_module(module), cls)


def _local_table_loader(warehouse_path: str):
    from iceberg_fuzzer.backend.pyiceberg.mutation import HadoopLayoutCatalog
    catalog = HadoopLayoutCatalog(warehouse_path)
    return lambda namespace, table_name: catalog.load_table((namespace, table_name))


def build_readers(warehouse_path: str, names=DEFAULT_READERS, row_filters: tuple[str, ...] = ()) -> list[tuple[EngineName, ValidationEngine]]:
    """The named readers. Arrow readers load local warehouses only, through the Hadoop layout."""
    unknown = [n for n in names if n not in ARROW_READERS and n not in WAREHOUSE_READERS]
    if unknown:
        raise ValueError(f"Unknown readers: {', '.join(unknown)}. Known: {', '.join([*WAREHOUSE_READERS, *ARROW_READERS])}")
    arrow = [n for n in names if n in ARROW_READERS]
    if arrow and is_s3_tables_warehouse(warehouse_path):
        raise ValueError(f"Readers {', '.join(arrow)} read local warehouses only")
    load_table = _local_table_loader(warehouse_path) if arrow else None
    return [
        (n, _load(ARROW_READERS[n])(load_table, row_filters) if n in ARROW_READERS else _load(WAREHOUSE_READERS[n])(warehouse_path))
        for n in names
    ]


def _init_mutation_engines(
    warehouse_path: str, backends: set[str], seed: int | None = None
) -> dict:
    """Create mutation engine instances for the requested backends."""
    engines = {}
    for name in backends:
        if name not in MUTATION_BACKENDS:
            raise ValueError(f"Unknown backend: {name}")
        engines[name] = _load(MUTATION_BACKENDS[name])(warehouse_path, seed=seed)
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
    return [AssertionResult.of(
        "cross_engine", all_agree, "all engines agree",
        dict(post_hashes) if not all_agree else f"agreed ({values[0]})", problem="content_hash",
    )]


def _snapshot_engine(engines) -> ValidationEngine:
    for _, engine in engines:
        if engine.reports_snapshots:
            return engine
    raise ValueError("A replace step with 'expect' needs a validation engine that reports snapshots")


def _snapshot_assertions(expect: str, before: tuple, after: tuple) -> list[AssertionResult]:
    """Check the current snapshot after a replace step against its declared expectation."""
    (before_id, _), (after_id, after_operation) = before, after
    if expect == 'replace_or_unchanged':
        return [AssertionResult.of(
            "snapshot", after_id == before_id or after_operation == 'replace',
            f"snapshot {before_id} unchanged, or a new replace snapshot", f"{after_id} ({after_operation})",
            problem="replace_or_unchanged",
        )]
    if expect == 'replace':
        return [
            AssertionResult.of("snapshot", after_id != before_id, f"a snapshot other than {before_id}", after_id, problem="new"),
            AssertionResult.equal("snapshot.operation", "replace", after_operation),
        ]
    return [AssertionResult.equal("snapshot.unchanged", before_id, after_id)]


def _error_message(error: BaseException) -> str:
    """One line naming the error. PySpark exceptions carry their message in `desc` and print as
    empty, and Py4J errors put the Java cause after a generic first line."""
    message = str(error) or str(getattr(error, 'desc', '')).strip()
    lines = [line.strip() for line in message.splitlines() if line.strip()]
    cause = next((line for line in lines[1:] if re.search(r"(Exception|Error)\b", line) and not line.startswith("at ")), None)
    first = lines[0] if lines else ""
    return f"{type(error).__name__}: {cause if cause and 'An error occurred while calling' in first else first}"


def _reraise_interrupt(error: BaseException) -> None:
    """Readers built on Rust, such as Polars, raise a panic as a BaseException, which counts as a
    failed check like any other error. Anything else that is not an Exception, such as Ctrl-C,
    still stops the run."""
    if not isinstance(error, Exception) and type(error).__name__ != "PanicException":
        raise error


def _read_failure(engine_name: EngineName, error: BaseException, check: str = "read") -> AssertionResult:
    return AssertionResult.of(check, False, "the reader reads the table", _error_message(error), reader=engine_name)


def _capture(engines, warehouse_path, namespace, name) -> tuple[dict[EngineName, Checkpoint], list[AssertionResult]]:
    """Each engine's checkpoint. An engine that cannot read the table fails its own read check,
    and the others carry on, so one reader's limits do not hide what the rest see."""
    checkpoints: dict[EngineName, Checkpoint] = {}
    failures: list[AssertionResult] = []
    for engine_name, engine in engines:
        try:
            checkpoints[engine_name] = engine.capture_checkpoint(warehouse_path, namespace, name)
        except BaseException as error:
            _reraise_interrupt(error)
            failures.append(_read_failure(engine_name, error))
    return checkpoints, failures


def _first_row_count(engines, warehouse_path, namespace, name) -> int:
    """The row count from the first engine that reads the table."""
    failures = []
    for engine in engines:
        checkpoints, failures = _capture([engine], warehouse_path, namespace, name)
        if checkpoints:
            return next(iter(checkpoints.values())).row_count
    raise RuntimeError(f"No engine could read the table: {failures[0].actual if failures else 'no engines'}")


def _agreement(post: dict[EngineName, Checkpoint], expected_rows: int) -> list[AssertionResult]:
    """Each engine reads the expected row count, and all engines read the same content."""
    assertions = [AssertionResult.row_count(expected_rows, cp.row_count, prefix=n) for n, cp in post.items()]
    return assertions + _cross_engine_assertions({n: cp.content_hash for n, cp in post.items()})


def _run_command(step, resolved: str, output_dir, workflow_name, namespace, name) -> tuple[int | None, str]:
    """Run a replace step's command. Returns its exit code, or None if it timed out, and its
    stdout and stderr."""
    timeout = step['config'].get('timeout', DEFAULT_STEP_TIMEOUT)
    out_dir = Path(output_dir) / workflow_name
    out_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = out_dir / f"{namespace}.{name}.{step['id']}.stdout"
    stderr_path = out_dir / f"{namespace}.{name}.{step['id']}.stderr"
    with open(stdout_path, 'w') as out_f, open(stderr_path, 'w') as err_f:
        try:
            returncode = subprocess.run(
                shlex.split(resolved), shell=False, stdout=out_f, stderr=err_f,
                timeout=timeout,
            ).returncode
        except subprocess.TimeoutExpired:
            err_f.write(f"\nOperation timed out after {timeout}s\n")
            returncode = None
    return returncode, stdout_path.read_text(errors="replace") + stderr_path.read_text(errors="replace")


def _declined(step, output: str) -> bool:
    """A non-zero exit is a decline only when the step expects one may happen and the command's
    output matches its decline_output pattern. Any other non-zero exit, such as a crash, fails."""
    pattern = step['config'].get('decline_output')
    expect = step['config'].get('expect')
    return expect in ('unchanged', 'replace_or_unchanged') and bool(pattern) and re.search(pattern, output) is not None


def _handle_replace(step, engines, warehouse_path, namespace, name, placeholders, output_dir, workflow_name, dry_run=False) -> OperationResult:
    """Handle a 'replace' step: checkpoint with all engines, run command, validate all."""
    step_id = step['id']
    resolved = Template(step['config']['command']).safe_substitute(placeholders)

    if dry_run:
        print(f"    {step_id} [replace]: {resolved.strip()}")
        return OperationResult.dry_run()

    checkpoints, read_failures = _capture(engines, warehouse_path, namespace, name)
    expect = step['config'].get('expect')
    snapshot_engine = _snapshot_engine(engines) if expect else None
    before = snapshot_engine.current_snapshot(warehouse_path, namespace, name) if snapshot_engine else None

    returncode, output = _run_command(step, resolved, output_dir, workflow_name, namespace, name)
    if returncode is None:
        return OperationResult.failed(f"Step '{step_id}' timed out after {step['config'].get('timeout', DEFAULT_STEP_TIMEOUT)}s")
    if returncode != 0 and not _declined(step, output):
        return OperationResult.failed(f"Step '{step_id}' exited with code {returncode} without a recognized decline")

    assertions: list[AssertionResult] = list(read_failures)
    if snapshot_engine:
        after = snapshot_engine.current_snapshot(warehouse_path, namespace, name)
        assertions.extend(_snapshot_assertions('unchanged' if returncode else expect, before, after))

    post_hashes: dict[str, int] = {}
    reference = next(iter(checkpoints.values()), None)
    for engine_name, engine in engines:
        if engine_name not in checkpoints:
            # The reader could not read the input, but it must still read the output, which is
            # compared with the row count the other readers saw before and with their content.
            try:
                post = engine.capture_checkpoint(warehouse_path, namespace, name)
            except BaseException as error:
                _reraise_interrupt(error)
                assertions.append(_read_failure(engine_name, error))
                continue
            if reference is not None:
                assertions.append(AssertionResult.row_count(reference.row_count, post.row_count, prefix=engine_name))
            post_hashes[engine_name] = post.content_hash
            continue
        try:
            validated = engine.validate(checkpoints[engine_name], warehouse_path, namespace, name)
        except BaseException as error:
            _reraise_interrupt(error)
            # The reader read the table before the command, so the command's output is what it cannot read.
            assertions.append(_read_failure(engine_name, error, "read_after"))
            continue
        for a in validated:
            assertions.append(a.for_reader(engine_name))
            if a.check == 'content_hash' and a.row_filter is None:
                post_hashes[engine_name] = a.actual
    assertions.extend(_cross_engine_assertions(post_hashes))
    return OperationResult.success(assertions)


def _handle_append(step, engines, warehouse_path, namespace, name, mutation_engines, dry_run=False) -> OperationResult:
    """Handle an 'append' step: append rows, validate row count with all engines."""
    num_rows = step['config']['num_rows']
    if dry_run:
        print(f"    {step['id']} [append]: {num_rows} rows via {step['config']['backend']}")
        return OperationResult.dry_run()

    pre_count = _first_row_count(engines, warehouse_path, namespace, name)
    append_result = _get_mutation_engine(step, mutation_engines).append_rows(namespace, name, num_rows)
    post, read_failures = _capture(engines, warehouse_path, namespace, name)
    assertions = read_failures + _agreement(post, pre_count + num_rows)
    return OperationResult.success(assertions, snapshot_id=append_result.snapshot_id, appended_ids=append_result.appended_ids)


def _handle_delete(step, engines, warehouse_path, namespace, name, mutation_engines, dry_run=False) -> OperationResult:
    """Handle a 'delete' step: delete random rows, validate row count."""
    num_rows = step['config']['num_rows']
    if dry_run:
        print(f"    {step['id']} [delete]: {num_rows} rows via {step['config']['backend']}")
        return OperationResult.dry_run()

    pre_count = _first_row_count(engines, warehouse_path, namespace, name)
    delete_result = _get_mutation_engine(step, mutation_engines).delete_rows(namespace, name, num_rows)
    post, read_failures = _capture(engines, warehouse_path, namespace, name)
    assertions = read_failures + _agreement(post, pre_count - num_rows)
    return OperationResult.success(assertions, snapshot_id=delete_result.snapshot_id, deleted_ids=delete_result.deleted_ids)


def _handle_sql(step, engines, warehouse_path, namespace, name, mutation_engines, dry_run=False) -> OperationResult:
    """Handle a 'sql' step: run the statements, then check that all engines agree."""
    statements = step['config']['statements']
    if dry_run:
        print(f"    {step['id']} [sql]: {len(statements)} statements via {step['config']['backend']}")
        return OperationResult.dry_run()

    sql_result = _get_mutation_engine(step, mutation_engines).run_sql(namespace, name, statements)
    # Arbitrary SQL has no predicted row count, so every engine must match what the writer counts.
    post, read_failures = _capture(engines, warehouse_path, namespace, name)
    return OperationResult.success(read_failures + _agreement(post, sql_result.row_count), snapshot_id=sql_result.snapshot_id)


def _handle_rollback(step, engines, warehouse_path, namespace, name, mutation_engines,
                     snapshot_id_by_step, accumulated_checkpoints, dry_run=False) -> OperationResult:
    """Handle a 'rollback' step: roll back to a prior step's snapshot, then validate."""
    step_id = step['id']
    to_step = step['config']['to_step']

    if dry_run:
        print(f"    {step_id} [rollback]: to step '{to_step}' via {step['config']['backend']}")
        return OperationResult.dry_run()
    if to_step not in snapshot_id_by_step:
        return OperationResult.failed(f"Step '{step_id}': no snapshot_id recorded for step '{to_step}'")
    if to_step not in accumulated_checkpoints:
        return OperationResult.failed(f"Step '{step_id}': no checkpoint stored for step '{to_step}'")

    target_snapshot_id = snapshot_id_by_step[to_step]
    targets = accumulated_checkpoints[to_step]
    actual_snapshot_id = _get_mutation_engine(step, mutation_engines).rollback_to_snapshot(namespace, name, target_snapshot_id)

    post, read_failures = _capture(engines, warehouse_path, namespace, name)
    post = {n: cp for n, cp in post.items() if n in targets}
    assertions = [AssertionResult.equal("snapshot_id", target_snapshot_id, actual_snapshot_id), *read_failures]
    for engine_name, cp in post.items():
        assertions.append(AssertionResult.row_count(targets[engine_name].row_count, cp.row_count, prefix=engine_name))
        assertions.append(AssertionResult.content_hash(targets[engine_name].content_hash, cp.content_hash, prefix=engine_name))
    assertions.extend(_cross_engine_assertions({n: cp.content_hash for n, cp in post.items()}))
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
    point_queries: bool = True,
    table_checks: Sequence[TableCheck] = (),
) -> TableResult:
    """Process a single table through all workflow steps.

    table_checks run around every step they apply to, see iceberg_fuzzer.checks.
    """
    workflow_name = workflow_def.name

    result = TableResult(table_name=table_name, namespace=namespace)

    placeholders = {
        'table_name': table_name,
        'namespace': namespace,
        'warehouse': warehouse_path,
    }

    backends_needed = {step['config']['backend'] for step in workflow_def.steps if 'backend' in step.get('config', {})}
    if workflow_def.setup:
        backends_needed.add(workflow_def.setup['backend'])
    mutation_engines: dict = {}

    # Point queries predict each value from its row id, as generated tables are filled, so they
    # cannot check a table whose rows were written by SQL.
    if point_queries and (workflow_def.setup or any(s['operation'] == 'sql' for s in workflow_def.steps)):
        print("  Point queries skipped: the workflow writes rows with SQL")
        point_queries = False

    rollback_targets: set[str] = {
        step['config']['to_step'] for step in workflow_def.steps
        if step['operation'] == 'rollback'
    }

    snapshot_id_by_step: dict[StepId, SnapshotId] = {}
    accumulated_checkpoints: dict[StepId, dict[EngineName, Checkpoint]] = {}

    # Point query state: track alive IDs and queryable columns
    alive_ids: set[int] = set()
    alive_ids_by_step: dict[StepId, set[int]] = {}
    queryable_columns: list[tuple[ColumnName, ColTypeStr]] = []

    try:
        try:
            if not dry_run:
                mutation_engines = _init_mutation_engines(warehouse_path, backends_needed, seed=seed)
            if workflow_def.setup and not dry_run:
                mutation_engines[workflow_def.setup['backend']].run_sql(
                    namespace, table_name, workflow_def.setup['statements']
                )
            if point_queries and not dry_run and engines:
                _, first_engine = engines[0]
                alive_ids = first_engine.get_row_ids(warehouse_path, namespace, table_name)
                queryable_columns = first_engine.get_columns(warehouse_path, namespace, table_name)
        except BaseException as e:
            _reraise_interrupt(e)
            result.error = f"Workflow setup failed: {e}"
            return result

        for step in workflow_def.steps:
            step_id = step['id']
            operation = step['operation']

            checks = [c for c in table_checks if c.applies_to(operation)] if not dry_run else []
            check_states = []
            check_failures = []
            for check in checks:
                try:
                    check_states.append((check, check.before(namespace, table_name)))
                except BaseException as e:
                    _reraise_interrupt(e)
                    check_failures.append(AssertionResult.of(
                        check.name, False, "check completes before the step", _error_message(e), problem="error_before_step",
                    ))
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
                elif operation == 'sql':
                    op_result = _handle_sql(
                        step, engines, warehouse_path, namespace, table_name, mutation_engines, dry_run
                    )
                elif operation == 'rollback':
                    op_result = _handle_rollback(
                        step, engines, warehouse_path, namespace, table_name,
                        mutation_engines, snapshot_id_by_step, accumulated_checkpoints, dry_run
                    )
                else:
                    op_result = OperationResult.failed(f"Unknown operation '{operation}' at step '{step_id}'")
            except BaseException as e:
                _reraise_interrupt(e)
                op_result = OperationResult.failed(f"Step '{step_id}' failed: {_error_message(e)}")

            if op_result.error:
                result.error = op_result.error
                return result

            op_result.assertions.extend(check_failures)
            for check, state in check_states:
                try:
                    op_result.assertions.extend(check.after(state, namespace, table_name))
                except BaseException as e:
                    _reraise_interrupt(e)
                    op_result.assertions.append(AssertionResult.of(
                        check.name, False, "check completes", _error_message(e), problem="error",
                    ))

            # Update alive_ids and run point query validation.
            if point_queries and not dry_run:
                step_config = step.get('config', {})
                match operation:
                    case 'append':
                        if op_result.appended_ids is not None:
                            alive_ids = alive_ids | op_result.appended_ids
                    case 'delete':
                        if op_result.deleted_ids is not None:
                            alive_ids = alive_ids - op_result.deleted_ids
                    case 'rollback':
                        to_step = step_config['to_step']
                        if to_step not in alive_ids_by_step:
                            raise RuntimeError(f"No saved alive_ids for rollback target '{to_step}'")
                        alive_ids = alive_ids_by_step[to_step].copy()
                    case 'replace':
                        # A correct compaction preserves the logical row set, so keeping our
                        # prediction unchanged lets point queries detect rows incorrectly
                        # dropped or duplicated by a buggy compactor.
                        pass
                    case _:
                        raise NotImplementedError(f"No alive_ids tracking for operation '{operation}'")

                pq_assertions = validate_point_queries(
                    engines, warehouse_path, namespace, table_name,
                    alive_ids, queryable_columns, seed=seed,
                )
                op_result.assertions.extend(pq_assertions)

            result.step_assertions[step_id] = op_result.assertions

            if op_result.snapshot_id is not None:
                snapshot_id_by_step[step_id] = op_result.snapshot_id

            if step_id in rollback_targets and not dry_run:
                accumulated_checkpoints[step_id] = _capture(engines, warehouse_path, namespace, table_name)[0]
                if point_queries:
                    alive_ids_by_step[step_id] = alive_ids.copy()
    finally:
        for me in mutation_engines.values():
            me.close()

    return result


def _outcome(a: AssertionResult) -> str:
    return "FAIL" if not a.passed else "PASS" if a.applicable else "N/A"


def _print_table_result(result: TableResult):
    """Print per-table result."""
    prefix = "✅" if result.passed else "❌"
    qualified = f"{result.namespace}.{result.table_name}"

    if result.error:
        # Show any steps that completed before the error
        for step_id, assertions in result.step_assertions.items():
            summary = ", ".join(f"{a.name}={_outcome(a)}" for a in assertions)
            print(f"  ✅ {qualified} [{step_id}]: {summary}")
        print(f"  {prefix} {qualified}: FAILED - {result.error}")
        return

    for step_id, assertions in result.step_assertions.items():
        summary = ", ".join(f"{a.name}={_outcome(a)}" for a in assertions)
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
    point_queries: bool = True,
    readers=DEFAULT_READERS,
    table_checks: list[str] | None = None,
    row_filters: tuple[str, ...] = (),
) -> TableResult | None:
    """Run a workflow against a single table.

    Validates each step with the named readers, DuckDB and PySpark by default, and asserts
    cross-engine content hash agreement. table_checks names checks from
    iceberg_fuzzer.checks.TABLE_CHECKS to run around the steps; row_filters are predicates the
    Arrow readers also read the table through.

    Returns a TableResult, or None if dry_run is True.
    """
    workflow_def = load_workflow(workflow_file)

    print(f"\n{'='*60}")
    print(f"WORKFLOW: {workflow_def.name}")
    print(f"{'='*60}")

    if dry_run:
        print(f"  {namespace}.{table_name}:")
        engines = []
        checks = []
    else:
        if table_checks and is_s3_tables_warehouse(warehouse_path):
            raise ValueError("Table checks read local warehouses only")
        engines = build_readers(warehouse_path, readers, row_filters)
        checks = iceberg_metadata_checks(_local_table_loader(warehouse_path), table_checks) if table_checks else []

    try:
        result = _process_table(
            workflow_def, warehouse_path, namespace, table_name, output_dir, engines, dry_run, seed=seed,
            point_queries=point_queries, table_checks=checks,
        )
    finally:
        for _, engine in engines:
            engine.close()

    if not dry_run:
        _print_table_result(result)

    return result if not dry_run else None
