"""Workflow definition loading and validation."""

from dataclasses import dataclass

import yaml


_OPERATION_REQUIRED_CONFIG_FIELDS = {
    'replace': ['command'],
    'append': ['backend', 'num_rows'],
    'delete': ['backend', 'num_rows'],
}


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

    return Workflow(name=workflow_def['name'], steps=workflow_def['steps'])
