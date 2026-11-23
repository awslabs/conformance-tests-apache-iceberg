"""What a shape config commits, as the sample reads it: the snapshots that commit, each file's commit
among them, and the data files the builder pads. Reading the sample imports neither the builder nor
Spark."""

from typing import Any, Dict, List


def padded_file_ids(config: Dict[str, Any]) -> List[str]:
    """The data files the builder pads past the target size with rows the config does not list: the
    sentinel, where the config has one."""
    sentinel = config.get('sentinel_in_file')
    return [sentinel] if sentinel else []


def committed_snapshots(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """The config's snapshots that commit a data or delete file, in commit order."""
    return [s for s in config['snapshots'] if s.get('data_files') or s.get('delete_files')]


def file_commits(config: Dict[str, Any]) -> Dict[str, int]:
    """Each data and delete file's index among the committed snapshots."""
    return {file_id: index for index, snapshot in enumerate(committed_snapshots(config))
            for key in ('data_files', 'delete_files') for file_id in snapshot.get(key, [])}
