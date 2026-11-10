"""Public API routing contracts; real SQL acceptance is a separate bridge."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from dblib.db_api import DBToolSuite, Ref
from dblib.matrixone import MatrixOneToolSuite
from dblib.matrixone_native_rebase import NativeRebaseMatrixOneToolSuite
from dblib.matrixone_rebase import RebaseAttemptUnknown
from dblib.result_collector import ResultCollector


@pytest.fixture
def suite(tmp_path):
    value = object.__new__(NativeRebaseMatrixOneToolSuite)
    DBToolSuite.__init__(value, Mock(), ResultCollector(output_dir=str(tmp_path)))
    value._prepare_native_rebase_attempt = Mock(return_value=SimpleNamespace(attempt_id="a"))
    value._request_native_rebase_snapshots = Mock()
    value._publish_native_rebase_attempt = Mock(return_value={
        "attempt_id": "a", "native_summary": {"conflict_keys": 1,
        "upstream_changed_keys": 2, "feature_changed_keys": 3}})
    return value


def test_default_route_stays_simulated():
    assert MatrixOneToolSuite.implementation()["rebase"] == "simulated"
    assert NativeRebaseMatrixOneToolSuite.implementation()["rebase"] == "native"


@pytest.mark.parametrize("policy", ["ours", "theirs"])
def test_public_api_reports_native_path_and_counts(suite, policy):
    result = suite.rebase("feature", "main", on_conflict=policy)
    assert result.ok, result.error
    assert result.execution_paths == ["native_rebase_publication"]
    assert result.fallback_reasons == []
    assert (result.value["conflicts"], result.value["ours_changes"], result.value["theirs_changes"]) == (1, 2, 3)
    suite._prepare_native_rebase_attempt.assert_called_once_with(Ref("feature"), Ref("main"), policy)
    suite._request_native_rebase_snapshots.assert_called_once_with("a")
    suite._publish_native_rebase_attempt.assert_called_once_with("a")


@pytest.mark.parametrize("source,upstream,policy", [
    ("feature@c", "main", "ours"), ("feature", "main@c", "theirs"),
    ("feature", "main", lambda *args: None),
])
def test_unsupported_rejected_before_preparation(suite, source, upstream, policy):
    result = suite.rebase(source, upstream, on_conflict=policy)
    assert result.unsupported
    suite._prepare_native_rebase_attempt.assert_not_called()
    suite._request_native_rebase_snapshots.assert_not_called()
    suite._publish_native_rebase_attempt.assert_not_called()


@pytest.mark.parametrize("stage", ["prepare", "request", "publish"])
def test_unknown_never_retries_or_falls_back(suite, stage):
    method = {"prepare": suite._prepare_native_rebase_attempt,
              "request": suite._request_native_rebase_snapshots,
              "publish": suite._publish_native_rebase_attempt}[stage]
    method.side_effect = RebaseAttemptUnknown("a")
    result = suite.rebase("feature", "main")
    assert result.failed
    assert result.execution_paths == ["native_rebase_publication"]
    assert result.fallback_reasons == []
    assert method.call_count == 1
    if stage == "prepare":
        suite._request_native_rebase_snapshots.assert_not_called()
    if stage != "publish":
        suite._publish_native_rebase_attempt.assert_not_called()
