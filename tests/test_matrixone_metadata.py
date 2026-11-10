"""Metadata correctness and round-trip bounds without a running MO server."""

from unittest.mock import Mock, call

import pymysql
import pytest

from dblib import matrixone as mo
from dblib.db_api import Ref, UnsupportedOperation


def suite():
    s = object.__new__(mo.MatrixOneToolSuite)
    s._merge_summary_supported = False
    s._database_branch_supported = {"diff": False, "merge": False}
    return s


def fork_row(seq, kind="fork", parent="parent"):
    row = dict.fromkeys(mo.MatrixOneToolSuite._LOG_COLS)
    row.update(seq=seq, id=f"c{seq}", kind=kind, db="child", snap=f"s{seq}",
               prev_db=parent, prev_snap="base", schema_json="{}")
    return row


@pytest.mark.parametrize("child_is_feature", [True, False])
def test_native_intent_binds_base_direction_and_resolved_generations(child_is_feature):
    s = suite()
    s.db_name = "bench"
    source, upstream = ("bench__feature__g3", 3), ("bench__upstream__g4", 4)
    s._registry_row = Mock(side_effect=[source, upstream])
    child = source[0] if child_is_feature else upstream[0]
    parent = "bench__upstream__g1" if child_is_feature else "bench__feature__g2"
    row = fork_row(123, "rebase", parent)
    row.update(db=child, snap="fixed_child_base", prev_snap="fixed_parent_base")
    root = fork_row(10, "root")
    s._base_rows = Mock(return_value=[row, root] if child_is_feature else [root, row])
    store = Mock()
    s._rebase_attempt_store = Mock(return_value=store)

    assert s._prepare_native_rebase_attempt(Ref("feature"), Ref("upstream")) == store.prepare.return_value
    child_base, parent_base = (child, "fixed_child_base"), (parent, "fixed_parent_base")
    feature_base, upstream_base = ((child_base, parent_base) if child_is_feature
                                   else (parent_base, child_base))
    store.prepare.assert_called_once_with(
        "feature", "upstream", source_registration=source, upstream_registration=upstream,
        feature_base=feature_base, upstream_base=upstream_base, base_sequence=123,
        on_conflict="fail")
    s._base_rows.assert_called_once_with(source[0], upstream[0])


def test_native_intent_rejects_unrecorded_common_base_without_snapshot_or_prepare():
    s = suite()
    s.db_name = "bench"
    s._registry_row = Mock(side_effect=[("bench__feature", 0), ("bench", 0)])
    s._base_rows = Mock(return_value=[fork_row(1, "root"), fork_row(2, "root")])
    s._rebase_attempt_store = Mock()
    with pytest.raises(UnsupportedOperation, match="recorded common base"):
        s._prepare_native_rebase_attempt(Ref("feature"), Ref("main"))
    s._rebase_attempt_store.assert_not_called()


def test_native_recovery_delegates_only_to_original_attempt():
    s = suite()
    store = Mock()
    s._rebase_attempt_store = Mock(return_value=store)
    attempt_id = "0123456789abcdef0123456789abcdef"
    assert s.recover_rebase(attempt_id) == store.recover.return_value
    store.recover.assert_called_once_with(attempt_id)
    assert store.method_calls == [call.recover(attempt_id)]


def test_relation_resolves_retired_and_current_native_generations_together():
    from tests.test_matrixone_rebase_generations import candidate
    s = suite()
    s.db_name = "bench"
    feature = candidate("feature", 3)
    old_upstream = candidate("upstream", 2, "e" * 32)
    current_upstream = candidate("upstream", 4, "f" * 32)
    fork = fork_row(123, "rebase", old_upstream)
    # A native base names the actual fixed U snapshot, not a nonexistent
    # pre-replay snapshot on the candidate database.
    fork.update(db=old_upstream, snap="fixed_u", prev_snap="fixed_u")
    root = fork_row(1, "root")
    store = Mock()
    store.generation_branches.return_value = {
        feature: "feature", old_upstream: "upstream", current_upstream: "upstream"}
    s._rebase_attempt_store = Mock(return_value=store)
    assert s._relation(feature, current_upstream, [fork, root]) == (feature, current_upstream, fork)
    store.generation_branches.assert_called_once_with(feature, current_upstream, old_upstream)


def test_native_generation_unknown_cannot_fall_back_to_name_parsing():
    from dblib.matrixone_rebase import RebaseAttemptUnknown
    from tests.test_matrixone_rebase_generations import ATTEMPT, candidate
    s = suite()
    s.db_name = "bench"
    store = Mock()
    store.generation_branches.side_effect = RebaseAttemptUnknown(ATTEMPT)
    s._rebase_attempt_store = Mock(return_value=store)
    with pytest.raises(RebaseAttemptUnknown):
        s._relation(candidate(), "bench", [fork_row(2, "fork", "bench"), fork_row(1, "root")])


def test_legacy_relation_needs_no_native_metadata_query():
    s = suite()
    s.db_name = "bench"
    s._rebase_attempt_store = Mock(side_effect=AssertionError("unnecessary native metadata"))
    row = fork_row(2, "fork", "bench")
    assert s._relation("bench__feature", "bench", [row, fork_row(1, "root")]) == (
        "bench__feature", "bench", row)
    s._rebase_attempt_store.assert_not_called()


def test_a_native_shaped_root_name_is_still_the_legacy_root():
    from tests.test_matrixone_rebase_generations import candidate
    s = suite()
    s.db_name = candidate()
    s._rebase_attempt_store = Mock(side_effect=AssertionError("root is not a native generation"))
    child = s.db_name + "__feature"
    row = fork_row(2, "fork", s.db_name)
    assert s._relation(child, s.db_name, [row, fork_row(1, "root")]) == (child, s.db_name, row)
    s._rebase_attempt_store.assert_not_called()


@pytest.mark.parametrize("source,upstream", [(None, ("bench", 0)),
                                               (("bench__feature", 0), None),
                                               (("bench", 0), ("bench", 0))])
def test_native_intent_requires_both_distinct_registrations(source, upstream):
    s = suite()
    s._registry_row = Mock(side_effect=[source, upstream])
    s._base_rows = Mock()
    s._rebase_attempt_store = Mock()
    with pytest.raises(ValueError, match="registered branch heads"):
        s._prepare_native_rebase_attempt(Ref("feature"), Ref("main"))
    s._base_rows.assert_not_called()
    s._rebase_attempt_store.assert_not_called()


def test_base_rows_preserve_input_order_and_independent_alias_results():
    s = suite()
    first, second = fork_row(10), fork_row(20, "rebase")
    s._execute = Mock(return_value=[(1, *second.values()), (2, *first.values()), (0, *first.values())])
    rows = s._base_rows("quoted`db", "other", "quoted`db")
    assert rows == [first, second, first]
    rows[0]["snap"] = "changed"
    assert rows[2]["snap"] == first["snap"]
    s._execute.assert_called_once()
    assert "`quoted``db`.`_bb_commits`" in s._execute.call_args.args[0]


def test_base_rows_without_inputs_do_not_query():
    s = suite()
    s._execute = Mock()
    assert s._base_rows() == []
    s._execute.assert_not_called()


@pytest.mark.parametrize("missing", [0, 1])
def test_base_rows_name_the_endpoint_with_no_fork_point(missing):
    s = suite()
    s._execute = Mock(return_value=[(1 - missing, *fork_row(10).values())])
    with pytest.raises(ValueError, match=f"{('left', 'right')[missing]} has no fork point"):
        s._base_rows("left", "right")
    s._execute.assert_called_once()


def test_base_rows_propagate_server_errors_without_retry():
    s = suite()
    s._execute = Mock(side_effect=pymysql.OperationalError(1142, "access denied"))
    with pytest.raises(pymysql.OperationalError, match="access denied"):
        s._base_rows("left", "right")
    s._execute.assert_called_once()


@pytest.mark.parametrize("left_seq,right_seq,left_kind,right_kind,expected", [
    (10, 20, "fork", "rebase", "right"),
    (30, 20, "rebase", "fork", "left"),
    (10, 10, "fork", "fork", "left"),
    (30, 20, "root", "fork", "right"),
    (30, 20, "root", "root", None),
])
def test_relation_selects_latest_matching_generation(left_seq, right_seq, left_kind, right_kind, expected):
    s = suite()
    s.db_name = "bench"
    left, right = mo.gen_db_name("bench", "left", 3), mo.gen_db_name("bench", "right", 4)
    bases = [fork_row(left_seq, left_kind, mo.gen_db_name("bench", "right", 1)),
             fork_row(right_seq, right_kind, mo.gen_db_name("bench", "left", 2))]
    s._base_rows = Mock(return_value=bases)
    relation = s._relation(left, right)
    assert relation == (None if expected is None else
                        (left, right, bases[0]) if expected == "left" else (right, left, bases[1]))
    s._base_rows.assert_called_once_with(left, right)
    s._base_rows.reset_mock()
    assert s._relation(left, right, bases) == relation
    s._base_rows.assert_not_called()


def test_relation_does_not_retain_unrelated_or_old_fork_points():
    s = suite()
    s.db_name = "bench"
    left, right = mo.gen_db_name("bench", "left", 1), mo.gen_db_name("bench", "right", 1)
    unrelated = [fork_row(1, parent="unrelated"), fork_row(2, parent="unrelated")]
    related = [fork_row(3, parent=right), unrelated[1]]
    s._base_rows = Mock(side_effect=[unrelated, related])
    assert s._relation(left, right) is None
    assert s._relation(left, right) == (left, right, related[0])
    assert s._base_rows.call_count == 2


def test_catalog_preserves_pk_order_and_column_defaults():
    s = suite()
    s._execute = Mock(return_value=[
        (0, "t", "a", "int", "NO", None, 2),
        (0, "t", "b", "int", "NO", None, 1),
        (0, "t", "v", "varchar(8)", "YES", "'hello'", None),
        (0, "t", "__mo_cpkey_col", "varchar", "NO", None, 1),
        (0, "heap", "v", "int", "YES", None, None),
    ])
    catalog = s._catalog("MixedCase")
    assert catalog["t"]["pk"] == ["b", "a"]
    assert catalog["t"]["columns"] == [
        ("a", "int", "NO", None), ("b", "int", "NO", None),
        ("v", "varchar(8)", "YES", "'hello'"),
    ]
    assert catalog["heap"]["pk"] == []
    assert s._execute.call_count == 1
    assert s._execute.call_args.args[1] == ("MixedCase", r"\_bb\_%", "MixedCase")


def test_catalogs_keep_same_named_tables_and_empty_databases_separate():
    s = suite()
    s._execute = Mock(return_value=[
        (0, "t", "id", "int", "NO", None, 1),
        (0, "t", "v", "int", "YES", None, None),
        (1, "t", "id", "bigint", "NO", None, 1),
        (1, "t", "v", "varchar(8)", "YES", "'hello'", None),
    ])
    ours, theirs, empty = s._catalogs("Ours", "Theirs", "Empty")
    assert ours["t"]["columns"][1] == ("v", "int", "YES", None)
    assert theirs["t"]["columns"][1] == ("v", "varchar(8)", "YES", "'hello'")
    assert ours["t"]["pk"] == theirs["t"]["pk"] == ["id"]
    assert empty == {}
    s._execute.assert_called_once()


@pytest.mark.parametrize("dbs", [("MixedCase", "mixedcase"), ("quoted'`\\%_name", "other")])
def test_catalogs_bind_names_and_keep_alias_results_independent(dbs):
    s = suite()
    s._execute = Mock(return_value=[
        (i, "t", "id", "int", "NO", None, 1) for i in range(2)
    ])
    first, second = s._catalogs(*dbs)
    sql, args = s._execute.call_args.args
    assert args == (*dbs, r"\_bb\_%", *dbs)
    assert all(db not in sql for db in dbs)
    first["t"]["columns"].append(("extra", "int", "YES", None))
    first["t"]["pk"].clear()
    assert second == {"t": {"columns": [("id", "int", "NO", None)], "pk": ["id"]}}


def test_no_requested_catalogs_issue_no_sql():
    s = suite()
    s._execute = Mock()
    assert s._catalogs() == []
    s._execute.assert_not_called()


def test_reconcile_fetches_both_missing_catalogs_together():
    s = suite()
    s._catalogs = Mock(return_value=[{}, {}])
    s._catalog = Mock(side_effect=AssertionError("unexpected separate read"))
    assert s._reconcile_schema("ours", mo._State("theirs")) == {
        "added_tables": [], "added_columns": [],
    }
    s._catalogs.assert_called_once_with("ours", "theirs")


@pytest.mark.parametrize("missing", ["ours", "theirs"])
def test_reconcile_preserves_a_supplied_empty_catalog(missing):
    s = suite()
    s._catalogs = Mock(side_effect=AssertionError("must preserve supplied metadata"))
    s._catalog = Mock(return_value={})
    s._reconcile_schema("ours", mo._State("theirs"),
                        None if missing == "ours" else {}, None if missing == "theirs" else {})
    s._catalog.assert_called_once_with(missing)


def test_combined_catalog_failure_propagates_before_schema_writes():
    s = suite()
    error = pymysql.OperationalError(1105, "catalog unavailable")
    s._execute = Mock(side_effect=error)
    with pytest.raises(pymysql.OperationalError, match="catalog unavailable"):
        s._reconcile_schema("ours", mo._State("theirs"))
    s._execute.assert_called_once()
    assert s._execute.call_args.args[0].startswith("SELECT ")


def test_schema_conflict_is_detected_before_any_ddl():
    s = suite()
    s._execute = Mock()
    s._readable = Mock(return_value=True)
    ours = {
        "a": {"columns": [("id", "int", "NO", None)], "pk": ["id"]},
        "z": {"columns": [("id", "int", "NO", None)], "pk": ["id"]},
    }
    theirs = {
        "a": {"columns": [("id", "int", "NO", None), ("v", "int", "YES", None)], "pk": ["id"]},
        "z": {"columns": [("id", "int", "NO", None)], "pk": []},
    }
    with pytest.raises(RuntimeError, match="primary keys differ"):
        s._reconcile_schema("ours", mo._State("theirs"), ours, theirs)
    s._execute.assert_not_called()
    assert len(ours["a"]["columns"]) == 1


@pytest.mark.parametrize("table_count", [1, 12])
@pytest.mark.parametrize("mode", ["legacy", "table", "database"])
def test_merge_catalog_queries_are_constant_and_refreshed_between_calls(monkeypatch, table_count, mode):
    monkeypatch.setattr(mo, "MO_NATIVE_MERGE_AS_IS", False)
    monkeypatch.setattr(mo, "MO_COUNT_CONFLICTS", True)
    s = suite()
    native_summary = mode != "legacy"
    s._merge_summary_supported = native_summary
    if mode == "database":
        s._database_branch_supported["merge"] = None
    queries = []
    version = [0]

    def execute(sql, args=None):
        queries.append((sql, args))
        if "information_schema.columns c" in sql:
            assert args == ("ours", "theirs", r"\_bb\_%", "ours", "theirs")
            result = []
            for index in range(2):
                cols = [("id", "int", "NO", None, 1), ("v", "int", "YES", None, None)]
                # A source column added between calls must be discovered.
                if index == 1 and version[0]:
                    cols.append(("extra", "int", "YES", "7", None))
                result.extend((index, f"t{i}", *col) for i in range(table_count) for col in cols)
            return result
        if sql.startswith("SHOW CREATE TABLE"):
            return [("t", "CREATE TABLE `t` (`id` int, `v` int, `extra` int DEFAULT 7)")]
        if sql.startswith("DATA BRANCH MERGE DATABASE"):
            return [(f"t{i}", 1, 1, 1, 0, 0, 1, 0, 0, 1) for i in range(table_count)]
        if sql.endswith("OUTPUT SUMMARY"):
            return [(1, 0, 0, 1, 0, 0, 1)]
        if sql.startswith(("SHOW", "SELECT")):
            raise AssertionError(f"unexpected per-table metadata query: {sql}")
        return None

    s._execute = execute
    s._on = Mock(return_value="ours")
    s._state = Mock(return_value=mo._State("theirs"))
    s._relation = Mock(return_value=("theirs", "ours", {"prev_db": "ours", "prev_snap": "base"}))
    s._readable = Mock(return_value=True)
    s._summary = Mock(return_value=((1, 0, 0), (1, 0, 0)))
    s._delta = lambda head, base, pk: {(1,): ("UPDATE", {"id": 1, "v": 2 if "theirs" in head else 3})}
    s._head = Mock(return_value={"id": "c", "kind": "commit", "snap": "s", "db": "ours"})
    s._copy_log = Mock()
    s._add_commit = Mock(return_value="merged")

    first = s._merge_impl(Ref("ours"), Ref("theirs"), "merge")
    assert first["conflicts"] == table_count
    assert first["schema_changes"]["added_columns"] == []
    version[0] = 1
    second = s._merge_impl(Ref("ours"), Ref("theirs"), "merge again")
    assert second["schema_changes"]["added_columns"] == [f"t{i}.extra" for i in sorted(range(table_count), key=lambda i: f"t{i}")]
    catalog_queries = [sql for sql, _ in queries if "information_schema.columns c" in sql]
    assert len(catalog_queries) == 2  # one combined read per operation, at either scale
    assert sum(sql.startswith("ALTER TABLE") for sql, _ in queries) == table_count
    assert sum(sql.startswith("DATA BRANCH MERGE") for sql, _ in queries) == (
        2 if mode == "database" else 2 * table_count)
    if native_summary:
        s._summary.assert_not_called()
        s._readable.assert_not_called()
    else:
        assert s._summary.call_count == 2 * table_count
        # Only historical bases need visibility probes, once per table
        # per operation; the two current head catalogs are sufficient.
        assert s._readable.call_count == 2 * table_count


def test_commit_index_migration_handles_concurrent_initialization():
    s = suite()
    s._execute = Mock(side_effect=[[], [(1,)]])
    s._execute_ddl = Mock(side_effect=pymysql.MySQLError(1061, "duplicate index"))
    s._ensure_commit_index("db")
    assert s._execute_ddl.call_count == 1


def test_commit_index_migration_does_not_hide_failure():
    s = suite()
    s._execute = Mock(return_value=[])
    s._execute_ddl = Mock(side_effect=pymysql.MySQLError(1105, "catalog unavailable"))
    with pytest.raises(pymysql.MySQLError, match="catalog unavailable"):
        s._ensure_commit_index("db")


def test_ddl_defaults_preserve_literals_expressions_and_quoted_identifiers():
    ddl = r"""CREATE TABLE `t` (
      `id` int NOT NULL,
      `a` varchar(50) DEFAULT 'NULL' COMMENT 'DEFAULT ignored',
      `b``x` varchar(50) DEFAULT 'it''s, (fine) \\ DEFAULT',
      `c` timestamp DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
      `d` int DEFAULT (-7 + (3 * 2)),
      `e` varchar(20) DEFAULT '',
      PRIMARY KEY (`id`)
    )"""
    assert mo._ddl_defaults(ddl) == {
        "a": "'NULL'", "b`x": r"'it''s, (fine) \\ DEFAULT'",
        "c": "CURRENT_TIMESTAMP(6)", "d": "(-7 + (3 * 2))", "e": "''",
    }


def test_snapshot_schema_checks_visibility_even_when_current_catalog_has_table():
    s = suite()
    s._readable = Mock(return_value=False)
    s._execute = Mock()
    past = mo._State("source", "old_snapshot")
    current_catalog = {"new_table": {"pk": ["id"], "columns": [("id", "int", "NO", None)]}}
    assert s._reconcile_schema("destination", past, {}, current_catalog) == {
        "added_tables": [], "added_columns": [],
    }
    s._readable.assert_called_once_with(past, "new_table")
    s._execute.assert_not_called()
