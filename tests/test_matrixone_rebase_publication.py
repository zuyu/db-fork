"""Offline publisher ownership/SQL contracts, not real transaction acceptance."""

from dataclasses import replace
import json
import unittest

from dblib.matrixone_rebase import NATIVE_REBASE_COLUMNS, RebaseAttemptUnknown, schema_fingerprint_rows
from tests.test_matrixone_rebase_attempts import ATTEMPT, base_record, intent, record
from tests import test_matrixone_rebase_snapshots as fixtures
from tests.test_matrixone_rebase_snapshots import receipt, snapshot


class PublicationTest(unittest.TestCase):
    setUp = fixtures.SnapshotRequestTest.setUp
    add_connection = fixtures.SnapshotRequestTest.add_connection

    def flow(self, policy="fail", terminal=None):
        initial = intent()
        self.value = replace(initial, candidate_db=self.store._candidate_database(
            initial.branch, initial.new_generation, ATTEMPT), on_conflict=policy)
        self.resources = [receipt(confirmed=True), receipt("upstream", True)]
        self.add_connection([[record(value=self.value, state=terminal or "PREPARING",
                                    result={} if terminal == "COMMITTED" else None)], self.resources])
        self.claimer, self.claim_cursor = self.add_connection([
            [record(value=self.value, state=terminal or "PREPARING",
                    result={} if terminal == "COMMITTED" else None)], self.resources])
        self.fixed_before = [[base_record(self.value.feature_base)],
                             [base_record(self.value.upstream_base)],
                             [snapshot()], [snapshot("upstream")]]
        self.fixed_after = [list(rows) for rows in self.fixed_before]
        self.candidate_rows = []
        self.table_rows = [("_bb_commits",), ("t",)]
        self.summary_rows = [(self.value.candidate_db, 99, "bench", 123456789, 2, 2, 1, 0, 1, 1, 0)]
        self.maximum = [(100,)]
        self.own = [(90, 2)]
        self.schema = [("t", "id", "INT", "PRI"), ("t", "v", "int", "")]
        self.publisher, self.cursor = self.add_connection([
            [record(value=self.value, state="PUBLISHING")], self.resources, *self.fixed_before,
            self.candidate_rows, self.table_rows, self.summary_rows, *self.fixed_after,
            self.maximum, self.own, self.schema])
        self.aborter, self.abort_cursor = self.add_connection([
            [record(value=self.value, state="PUBLISHING")], self.resources])
        self.failure = None
        self.affected = {}

        def execute(query, params=None):
            self.events.append((2, "execute", query, params))
            operation = None
            if query.startswith("DATA BRANCH REBASE"):
                operation = "native"
                self.cursor.description = [(name,) for name in NATIVE_REBASE_COLUMNS]
            elif query.startswith("INSERT"):
                operation = "copy_history" if "row_number()" in query else "marker"
            elif query.startswith("UPDATE"):
                operation = "pointer" if ".`branches`" in query else "success"
            elif query.startswith("DROP"):
                operation = "retire"
            self.cursor.rowcount = self.affected.get(operation, 2 if operation == "copy_history" else 1)
            if operation is not None and operation == self.failure:
                raise RuntimeError("injected " + operation)
        self.cursor.execute.side_effect = execute

    def statements(self):
        return [event[2:] for event in self.events if len(event) > 2 and event[0] == 2]

    def assert_rolled_back(self, claimed=True):
        (self.publisher if claimed else self.claimer).rollback.assert_called_once()
        self.publisher.commit.assert_not_called()
        for conn in self.connections[:self.connect.call_count]:
            conn.close.assert_called_once()
        self.assertEqual(self.connect.call_count, 4 if self.aborter.commit.called else 3 if claimed else 2)

    def test_same_owner_publishes_marker_history_cas_retirement_and_success(self):
        self.flow()
        result = self.store.publish(ATTEMPT)
        statements = self.statements()
        self.assertIn("branches", statements[1][0])
        self.assertIn("rebase_attempts", statements[2][0])
        self.assertTrue(all("FOR UPDATE" in statements[i][0] for i in (1, 2)))
        native = next(item for item in statements if item[0].startswith("DATA BRANCH REBASE"))
        self.assertIn("TABLES (`t`) FOR UPDATE EXPECT (17,3)", native[0])
        self.assertEqual(native[1], (self.value.feature_snapshot, self.value.upstream_snapshot,
                                     self.value.feature_base.snapshot, self.value.upstream_base.snapshot))
        marker = next(item for item in statements if item[0].startswith("INSERT") and "row_number" not in item[0])
        self.assertEqual(marker[1][:2], (101, ATTEMPT))
        self.assertEqual(marker[1][3:7], (self.value.upstream_snapshot, "bench", self.value.upstream_snapshot, "bench"))
        self.assertEqual(json.loads(marker[1][-1]), {"t": {"cols": {"id": "int", "v": "int"}, "pk": ["id"]}})
        copied = next(item for item in statements if "row_number()" in item[0])
        self.assertIn("snap,db,prev_snap,prev_db,ts,schema_json", copied[0])
        self.assertEqual(copied[1], (101, self.value.feature_snapshot, 42))
        pointer = next(i for i, item in enumerate(statements) if item[0].startswith("UPDATE `bench_meta`.`branches`"))
        self.assertEqual(statements[pointer][1], (self.value.candidate_db, 1, "feature", "bench_feature", 0))
        self.assertEqual(statements[pointer+1], ("DROP DATABASE `bench_feature`", None))
        self.assertEqual(statements[pointer+2][1][::2], ("COMMITTED", ATTEMPT))
        persisted = json.loads(statements[-1][1][1])
        self.assertEqual(persisted, result)
        self.assertEqual(result["upstream_timestamp"], 123456789)
        self.assertGreaterEqual(result["native_sql_elapsed_sec"], 0)
        self.publisher.begin.assert_called_once()
        self.publisher.commit.assert_called_once()
        self.publisher.rollback.assert_not_called()
        self.claimer.commit.assert_called_once()
        self.aborter.begin.assert_not_called()
        claim = self.claim_cursor.execute.call_args.args
        self.assertEqual(claim[1], ("PUBLISHING", ATTEMPT, "PREPARING"))
        self.connections[0].commit.assert_not_called()
        self.assertFalse(any(q.startswith(("CREATE SNAPSHOT", "DROP SNAPSHOT", "DELETE")) for q, _ in statements))
        for conn in self.connections[:self.connect.call_count]:
            conn.close.assert_called_once()

    def test_policy_comes_from_durable_intent_not_a_retry_argument(self):
        for policy, sql in (("fail", "fail"), ("ours", "skip"), ("theirs", "accept")):
            with self.subTest(policy=policy):
                self.setUp()
                self.flow(policy)
                self.store.publish(ATTEMPT)
                native = next(q for q, _ in self.statements() if q.startswith("DATA BRANCH REBASE"))
                self.assertTrue(native.endswith("WHEN CONFLICT " + sql))

    def test_terminal_attempt_must_recover_without_republication(self):
        for state in ("COMMITTED", "ABORTED"):
            with self.subTest(state=state):
                self.setUp()
                self.flow(terminal=state)
                with self.assertRaisesRegex(ValueError, "recover a terminal"):
                    self.store.publish(ATTEMPT)
                self.assertEqual(self.connect.call_count, 1)
                self.publisher.begin.assert_not_called()

    def test_unconfirmed_snapshot_and_existing_candidate_are_retained(self):
        for change in ("pending_snapshot", "candidate_exists"):
            with self.subTest(change=change):
                self.setUp()
                self.flow()
                if change == "pending_snapshot":
                    self.resources[0] = receipt()
                else:
                    self.candidate_rows.append((99,))
                with self.assertRaises(RebaseAttemptUnknown):
                    self.store.publish(ATTEMPT)
                self.assert_rolled_back(claimed=change != "pending_snapshot")
                self.assertFalse(any(q.startswith(("DATA BRANCH", "DROP", "INSERT", "UPDATE")) for q, _ in self.statements()))

    def test_snapshot_name_rebound_before_or_during_admission_rolls_back(self):
        for phase in ("before", "after"):
            for endpoint in range(4):
                with self.subTest(phase=phase, endpoint=endpoint):
                    self.setUp()
                    self.flow()
                    snapshots = self.fixed_before if phase == "before" else self.fixed_after
                    old = snapshots[endpoint][0]
                    snapshots[endpoint][0] = ("22345678-1234-1234-1234-123456789abc", *old[1:])
                    with self.assertRaisesRegex(ValueError, "snapshot identity changed"):
                        self.store.publish(ATTEMPT)
                    self.assert_rolled_back()
                    self.assertFalse(any(q.startswith(("INSERT", "UPDATE", "DROP")) for q, _ in self.statements()))

    def test_invalid_summary_and_history_shape_fail_before_metadata_publish(self):
        changes = {
            "wrong_candidate": lambda: self.summary_rows.__setitem__(0, ("other", *self.summary_rows[0][1:])),
            "wrong_base": lambda: self.summary_rows.__setitem__(0, (*self.summary_rows[0][:2], "other", *self.summary_rows[0][3:])),
            "missing_table": lambda: self.table_rows.remove(("_bb_commits",)),
            "unexpected_metadata": lambda: self.table_rows.append(("_bb_unknown",)),
            "missing_schema": self.schema_clear,
            "overflow": lambda: self.maximum.__setitem__(0, (2**63-2,)),
        }
        for name, mutate in changes.items():
            with self.subTest(name=name):
                self.setUp()
                self.flow()
                mutate()
                with self.assertRaises(ValueError):
                    self.store.publish(ATTEMPT)
                self.assert_rolled_back()
                self.assertFalse(any(q.startswith(("INSERT", "UPDATE", "DROP")) for q, _ in self.statements()))

    def schema_clear(self):
        self.schema.clear()

    def test_every_mutation_failure_rolls_back_without_cleanup_or_retries(self):
        for phase in ("native", "marker", "copy_history", "pointer", "retire", "success"):
            with self.subTest(phase=phase):
                self.setUp()
                self.flow()
                self.failure = phase
                with self.assertRaisesRegex(RuntimeError, "injected " + phase):
                    self.store.publish(ATTEMPT)
                self.assert_rolled_back()
                self.assertFalse(any(q.startswith(("CREATE SNAPSHOT", "DROP SNAPSHOT", "DELETE")) for q, _ in self.statements()))

    def test_wrong_affected_rows_cannot_commit(self):
        for phase in ("marker", "copy_history", "pointer", "success"):
            with self.subTest(phase=phase):
                self.setUp()
                self.flow()
                self.affected[phase] = 0
                with self.assertRaises((ValueError, RuntimeError)):
                    self.store.publish(ATTEMPT)
                self.assert_rolled_back()

    def test_commit_response_loss_is_unknown_without_rollback_or_replay(self):
        self.flow()
        self.publisher.commit.side_effect = OSError("response lost after commit send")
        with self.assertRaises(RebaseAttemptUnknown) as caught:
            self.store.publish(ATTEMPT)
        self.assertEqual(caught.exception.attempt_id, ATTEMPT)
        self.publisher.commit.assert_called_once()
        self.publisher.rollback.assert_not_called()
        self.assertEqual(self.connect.call_count, 3)
        self.assertEqual(sum(q.startswith("DATA BRANCH REBASE") for q, _ in self.statements()), 1)
        self.aborter.begin.assert_not_called()

    def test_claim_response_loss_cannot_send_native_sql(self):
        self.flow()
        self.claimer.commit.side_effect = OSError("claim commit response lost")
        with self.assertRaises(RebaseAttemptUnknown):
            self.store.publish(ATTEMPT)
        self.claimer.rollback.assert_not_called()
        self.publisher.begin.assert_not_called()
        self.aborter.begin.assert_not_called()
        self.assertEqual(self.connect.call_count, 2)
        self.assertFalse(self.statements())

    def test_already_claimed_or_unknown_attempt_cannot_publish_again(self):
        for state in ("PUBLISHING", "UNKNOWN"):
            with self.subTest(state=state):
                self.setUp()
                self.flow(terminal=state)
                with self.assertRaises(RebaseAttemptUnknown):
                    self.store.publish(ATTEMPT)
                self.claimer.rollback.assert_called_once()
                self.publisher.begin.assert_not_called()
                self.assertFalse(self.statements())

    def test_known_rollback_records_abort_only_after_acknowledgement(self):
        self.flow()
        self.failure = "native"
        with self.assertRaisesRegex(RuntimeError, "injected native"):
            self.store.publish(ATTEMPT)
        rollback = self.events.index((2, "rollback"))
        begin_abort = self.events.index((3, "begin"))
        self.assertLess(rollback, begin_abort)
        query, params = self.abort_cursor.execute.call_args.args
        self.assertIn("UPDATE", query)
        self.assertEqual(params, ("ABORTED", '{"error_type": "RuntimeError"}', ATTEMPT, "PUBLISHING"))
        self.aborter.commit.assert_called_once()
        self.assertFalse(any(event[2].startswith(("DELETE", "DROP SNAPSHOT"))
                             for event in self.events if len(event)>2))

    def test_unknown_rollback_or_abort_commit_remains_unknown(self):
        for stage in ("rollback", "abort_commit"):
            with self.subTest(stage=stage):
                self.setUp()
                self.flow()
                self.failure = "native"
                if stage == "rollback":
                    self.publisher.rollback.side_effect = OSError("rollback acknowledgement lost")
                else:
                    self.aborter.commit.side_effect = OSError("abort acknowledgement lost")
                with self.assertRaises(RebaseAttemptUnknown):
                    self.store.publish(ATTEMPT)
                if stage == "rollback":
                    self.aborter.begin.assert_not_called()
                else:
                    self.aborter.rollback.assert_not_called()
                self.assertEqual(sum(q.startswith("DATA BRANCH REBASE") for q, _ in self.statements()), 1)

    def test_no_feature_commit_append_needs_no_copy_statement(self):
        self.flow()
        self.own[:] = [(0, 0)]
        self.store.publish(ATTEMPT)
        self.assertFalse(any("row_number()" in q for q, _ in self.statements()))

    def test_shared_schema_decoder_preserves_primary_order_and_hides_internal_columns(self):
        rows = [("t", "b", "BIGINT", "PRI"), ("t", "a", "INT", "PRI"),
                ("t", "__mo_rowid", "binary", ""), ("u", "v", "VARCHAR(8)", "")]
        self.assertEqual(schema_fingerprint_rows(rows), {
            "t": {"cols": {"b": "bigint", "a": "int"}, "pk": ["b", "a"]},
            "u": {"cols": {"v": "varchar(8)"}, "pk": []}})
