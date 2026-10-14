"""Unit tests for replace step snapshot expectations."""

import pytest

from iceberg_fuzzer.workflow.engine import _snapshot_assertions, _snapshot_engine


def _passed(expect, before, after):
    return {a.name: a.passed for a in _snapshot_assertions(expect, before, after)}


def test_replace_requires_new_replace_snapshot():
    assert _passed("replace", (1, "append"), (2, "replace")) == {"snapshot.new": True, "snapshot.operation": True}
    assert _passed("replace", (1, "append"), (1, "append")) == {"snapshot.new": False, "snapshot.operation": False}
    assert _passed("replace", (1, "append"), (2, "overwrite"))["snapshot.operation"] is False


def test_none_and_unchanged_require_same_snapshot():
    for expect in ("none", "unchanged"):
        assert _passed(expect, (1, "append"), (1, "append")) == {"snapshot.unchanged": True}
        assert _passed(expect, (1, "append"), (2, "replace")) == {"snapshot.unchanged": False}


def test_snapshot_engine_is_the_first_that_reports_snapshots():
    class Reader:
        def __init__(self, reports):
            self.reports_snapshots = reports

    plain, reporting = Reader(False), Reader(True)
    assert _snapshot_engine([("a", plain), ("b", reporting)]) is reporting
    with pytest.raises(ValueError, match="reports snapshots"):
        _snapshot_engine([("a", plain)])
