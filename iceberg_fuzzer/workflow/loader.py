"""Workflow definition loading and validation."""

from dataclasses import dataclass

import yaml


_OPERATION_REQUIRED_CONFIG_FIELDS = {
    'replace': ['command'],
    'append': ['backend', 'num_rows'],
    'delete': ['backend', 'num_rows'],
    'rollback': ['backend', 'to_step'],
    'sql': ['backend', 'statements'],
}

# What a replace step may require of the table afterwards: a new replace snapshot, no new
# snapshot because there was nothing to rewrite, no change because the command declined, or
# either a replace snapshot or no change, for generated tables a compactor may refuse.
REPLACE_EXPECTATIONS = ('replace', 'none', 'unchanged', 'replace_or_unchanged')

# Operations whose snapshot a later rollback step may return to.
_ROLLBACK_TARGET_OPERATIONS = ('append', 'delete', 'sql')


@dataclass
class Workflow:
    """A parsed and validated workflow definition.

    `setup`, when present, holds a backend and SQL statements that run once before the first
    step, for example to create the table the steps act on.
    """
    name: str
    steps: list[dict]
    setup: dict | None = None


def _validate_statements(owner: str, config: dict) -> None:
    statements = config.get('statements')
    if not isinstance(statements, list) or not statements or not all(isinstance(s, str) for s in statements):
        raise ValueError(f"{owner} 'statements' must be a non-empty list of SQL strings")


def load_workflow(path: str) -> Workflow:
    """Load and validate a workflow definition from a YAML file."""
    with open(path, 'r') as f:
        workflow_def = yaml.safe_load(f)

    if 'name' not in workflow_def:
        raise ValueError(f"Workflow YAML missing required 'name' field: {path}")
    if 'steps' not in workflow_def or not workflow_def['steps']:
        raise ValueError(f"Workflow YAML missing or empty 'steps' field: {path}")

    setup = workflow_def.get('setup')
    if setup is not None:
        if 'backend' not in setup:
            raise ValueError(f"Workflow setup missing required 'backend' field: {path}")
        _validate_statements("Workflow setup", setup)

    for i, step in enumerate(workflow_def['steps']):
        if 'id' not in step:
            raise ValueError(f"Step {i} missing required 'id' field")
        if 'operation' not in step:
            raise ValueError(f"Step '{step['id']}' missing required 'operation' field")

        op = step['operation']
        if op not in _OPERATION_REQUIRED_CONFIG_FIELDS:
            raise ValueError(f"Step '{step['id']}' has unknown operation '{op}'")

        if 'config' not in step:
            raise ValueError(f"Step '{step['id']}' ({op}) missing required 'config' field")

        config = step['config']
        for field_name in _OPERATION_REQUIRED_CONFIG_FIELDS[op]:
            if field_name not in config:
                raise ValueError(
                    f"Step '{step['id']}' ({op}) missing required config field '{field_name}'"
                )

        if op == 'replace' and 'expect' in config and config['expect'] not in REPLACE_EXPECTATIONS:
            raise ValueError(
                f"Step '{step['id']}' (replace) has expect '{config['expect']}', "
                f"expected one of {', '.join(REPLACE_EXPECTATIONS)}"
            )

        if op == 'sql':
            _validate_statements(f"Step '{step['id']}' (sql)", config)

        if op == 'rollback':
            to_step = config['to_step']
            prior_mutation_ids = {
                s['id'] for s in workflow_def['steps'][:i]
                if s['operation'] in _ROLLBACK_TARGET_OPERATIONS
            }
            if to_step not in prior_mutation_ids:
                raise ValueError(
                    f"Step '{step['id']}' (rollback) references '{to_step}' "
                    f"which is not a prior append/delete/sql step"
                )

    return Workflow(name=workflow_def['name'], steps=workflow_def['steps'], setup=setup)
