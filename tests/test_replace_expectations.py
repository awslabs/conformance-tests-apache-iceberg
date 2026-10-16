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


def test_replace_or_unchanged_accepts_either_outcome():
    assert _passed("replace_or_unchanged", (1, "append"), (2, "replace")) == {"snapshot.replace_or_unchanged": True}
    assert _passed("replace_or_unchanged", (1, "append"), (1, "append")) == {"snapshot.replace_or_unchanged": True}
    assert _passed("replace_or_unchanged", (1, "append"), (2, "overwrite")) == {"snapshot.replace_or_unchanged": False}


def test_a_reader_that_cannot_read_fails_its_own_check():
    from iceberg_fuzzer.backend import Checkpoint
    from iceberg_fuzzer.workflow.engine import _capture

    class Reads:
        def capture_checkpoint(self, *args):
            return Checkpoint(row_count=3, content_hash=7)

    class Fails:
        def capture_checkpoint(self, *args):
            raise OSError("cannot decode")

    checkpoints, failures = _capture([("good", Reads()), ("bad", Fails())], "w", "ns", "t")
    assert list(checkpoints) == ["good"]
    assert [(f.name, f.passed, f.actual) for f in failures] == [("bad.read", False, "OSError: cannot decode")]


def test_read_failure_names_the_phase():
    from iceberg_fuzzer.workflow.engine import _read_failure
    assert _read_failure("daft", OSError("x")).name == "daft.read"
    assert _read_failure("daft", OSError("x"), "read_after").name == "daft.read_after"


class _Table:
    """A reader over a fake table whose rows and snapshot the test sets."""

    reports_snapshots = True

    def __init__(self, rows=3, content=7, readable=True):
        self.rows, self.content, self.readable, self.snapshot = rows, content, readable, (1, "append")

    def capture_checkpoint(self, *args):
        from iceberg_fuzzer.backend import Checkpoint
        if not self.readable:
            raise OSError("cannot decode")
        return Checkpoint(row_count=self.rows, content_hash=self.content)

    def validate(self, checkpoint, *args):
        from iceberg_fuzzer.backend import AssertionResult
        post = self.capture_checkpoint()
        return [AssertionResult.row_count(checkpoint.row_count, post.row_count),
                AssertionResult(name="content_hash", passed=post.content_hash == checkpoint.content_hash,
                                expected=checkpoint.content_hash, actual=post.content_hash)]

    def current_snapshot(self, *args):
        return self.snapshot


def _replace(tmp_path, script, expect="unchanged", decline_output="declined: unsupported", engines=None):
    from iceberg_fuzzer.workflow.engine import _handle_replace
    config = {"command": f"sh -c '{script}'", "expect": expect}
    if decline_output:
        config["decline_output"] = decline_output
    return _handle_replace({"id": "compact_1", "config": config}, engines or [("a", _Table())],
                           "w", "ns", "t", {}, str(tmp_path), "wf")


@pytest.mark.parametrize("script", ["exit 1", "exit 101", "kill -9 $$", "echo declined: unsupported; exit 1"])
def test_a_crash_is_not_a_decline(tmp_path, script):
    for expect, decline_output in (("unchanged", None), ("replace", "declined: unsupported")):
        assert "without a recognized decline" in _replace(tmp_path, script, expect, decline_output).error
    if "declined" not in script:
        assert "without a recognized decline" in _replace(tmp_path, script).error


@pytest.mark.parametrize("expect", ["unchanged", "replace_or_unchanged"])
def test_a_recognized_decline_passes_when_the_table_is_unchanged(tmp_path, expect):
    result = _replace(tmp_path, "echo declined: unsupported >&2; exit 1", expect)
    assert result.error is None and all(a.passed for a in result.assertions)


def test_a_reader_that_could_not_read_the_input_still_checks_the_output(tmp_path):
    result = _replace(tmp_path, "true", engines=[("a", _Table()), ("late", _Table(readable=False))])
    assert {a.name for a in result.assertions if not a.passed} == {"late.read"}

    class ReadsOnlyTheOutput(_Table):
        def capture_checkpoint(self, *args):
            readable, self.readable = self.readable, True
            if not readable:
                raise OSError("cannot decode")
            return super().capture_checkpoint(*args)

    late = ReadsOnlyTheOutput(rows=4, content=9, readable=False)
    result = _replace(tmp_path, "true", engines=[("a", _Table()), ("late", late)])
    assert {a.name for a in result.assertions if not a.passed} == {"late.read", "late.row_count", "cross_engine.content_hash"}


def test_a_reader_panic_is_a_failed_read_but_an_interrupt_stops_the_run():
    from iceberg_fuzzer.workflow.engine import _capture

    class PanicException(BaseException):
        pass

    class Panics:
        def capture_checkpoint(self, *args):
            raise PanicException("assertion failed")

    class Interrupted:
        def capture_checkpoint(self, *args):
            raise KeyboardInterrupt

    _, failures = _capture([("polars", Panics())], "w", "ns", "t")
    assert [f.name for f in failures] == ["polars.read"]
    with pytest.raises(KeyboardInterrupt):
        _capture([("polars", Interrupted())], "w", "ns", "t")
