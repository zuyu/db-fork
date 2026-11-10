"""Offline request-owner checks, not real snapshot or process-recovery proof."""

import unittest
from unittest.mock import MagicMock, Mock

from dblib.matrixone_rebase import RebaseAttemptStore, RebaseAttemptUnknown
from tests.test_matrixone_rebase_attempts import ATTEMPT, base_record, intent, record


def receipt(role="feature", confirmed=False):
    fixed = intent()
    name = fixed.feature_snapshot if role == "feature" else fixed.upstream_snapshot
    database_id = fixed.source_id if role == "feature" else fixed.upstream_id
    return (role, "CONFIRMED" if confirmed else "REQUESTED", name, database_id,
            "12345678-1234-1234-1234-123456789abc" if confirmed else None,
            123456789 if confirmed else None)


def snapshot(role="feature"):
    value = receipt(role, True)
    return value[4], value[5], "database", "bench_feature" if role == "feature" else "bench", value[3]


class SnapshotRequestTest(unittest.TestCase):
    def setUp(self):
        self.connections = []
        self.cursors = []
        self.events = []
        self.connect = Mock(side_effect=lambda: self.connections[self.connect.call_count - 1])
        self.store = RebaseAttemptStore(self.connect, "bench_meta")

    def add_connection(self, results, branch=("bench_feature", 0)):
        index = len(self.connections)
        conn = MagicMock()
        conn.get_autocommit.return_value = True
        cur = conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = branch
        cur.fetchall.side_effect = results
        cur.rowcount = 1
        cur.execute.side_effect = lambda query, params=None: self.events.append((index, "execute", query, params))
        conn.begin.side_effect = lambda: self.events.append((index, "begin"))
        conn.commit.side_effect = lambda: self.events.append((index, "commit"))
        conn.rollback.side_effect = lambda: self.events.append((index, "rollback"))
        self.connections.append(conn)
        self.cursors.append(cur)
        return conn, cur

    def add_inspection(self, resources=()):
        return self.add_connection([[record()], list(resources)])

    def add_reservation(self, role="feature", branch=("bench_feature", 0)):
        value = intent()
        rows = [[record()], []]
        if role == "upstream":
            rows.append([receipt(confirmed=True)])
        rows.extend([[base_record(value.feature_base)], [base_record(value.upstream_base)]])
        return self.add_connection(rows, branch)

    def add_full_flow(self):
        self.add_inspection()
        self.add_reservation()
        self.add_connection([[snapshot()]])
        self.add_connection([[record()], [receipt()]])
        self.add_reservation("upstream")
        self.add_connection([[snapshot("upstream")]])
        self.add_connection([[record()], [receipt("upstream")]])

    def snapshot_statements(self):
        return [event for event in self.events if len(event) > 2 and event[2].startswith("CREATE SNAPSHOT")]

    def assert_no_publication_or_cleanup(self):
        for event in self.events:
            if len(event) > 2:
                self.assertFalse(event[2].startswith(("REBASE", "DROP", "CREATE DATABASE", "DELETE")))
                self.assertFalse(event[2].startswith("UPDATE `bench_meta`.`branches`"))

    def test_each_durable_reservation_precedes_exactly_one_request_and_confirmation(self):
        self.add_full_flow()
        result = self.store.request_snapshots(ATTEMPT)
        self.assertEqual(result["state"], "UNKNOWN", "two prepared snapshots are not a published Rebase")
        self.assertEqual([row["role"] for row in result["snapshots"]], ["feature", "upstream"])
        self.assertTrue(all(row["state"] == "CONFIRMED" for row in result["snapshots"]))
        self.assertEqual(len(self.snapshot_statements()), 2)
        for reserve, request, confirm in ((1, 2, 3), (4, 5, 6)):
            self.assertLess(self.events.index((reserve, "commit")),
                            next(i for i, event in enumerate(self.events) if event[:2] == (request, "execute")))
            statements = [call.args[0] for call in self.cursors[reserve].execute.call_args_list]
            self.assertIn("branches", statements[1])
            self.assertIn("rebase_attempts", statements[2])
            self.connections[confirm].commit.assert_called_once()
        for index in (2, 5):
            self.connections[index].begin.assert_not_called()
            self.connections[index].commit.assert_not_called()
            self.assertIn("account_name=current_account_name()", self.cursors[index].execute.call_args.args[0])
        for conn in self.connections:
            conn.close.assert_called_once()
        self.assert_no_publication_or_cleanup()

    def test_unknown_reservation_commit_never_sends_request(self):
        self.add_inspection()
        conn, _ = self.add_reservation()
        conn.commit.side_effect = OSError("commit response lost")
        with self.assertRaises(RebaseAttemptUnknown):
            self.store.request_snapshots(ATTEMPT)
        self.assertEqual(self.connect.call_count, 2)
        conn.rollback.assert_not_called()
        self.assertEqual(self.snapshot_statements(), [])
        self.assert_no_publication_or_cleanup()

    def test_failed_reservation_rolls_back_and_sends_nothing(self):
        self.add_inspection()
        conn, cur = self.add_reservation()
        cur.rowcount = 0
        with self.assertRaises(RuntimeError):
            self.store.request_snapshots(ATTEMPT)
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
        self.assertEqual(self.snapshot_statements(), [])

    def test_requested_record_cannot_be_reissued(self):
        self.add_inspection([receipt()])
        conn, _ = self.add_connection([[record()], [receipt()]])
        with self.assertRaises(RebaseAttemptUnknown):
            self.store.request_snapshots(ATTEMPT)
        self.assertEqual(self.connect.call_count, 2)
        conn.rollback.assert_called_once()
        self.assertEqual(self.snapshot_statements(), [])
        self.assert_no_publication_or_cleanup()

    def test_both_confirmed_records_are_reused_without_request(self):
        self.add_inspection([receipt(confirmed=True), receipt("upstream", True)])
        self.add_connection([[record()], [receipt(confirmed=True)]])
        self.add_connection([[record()], [receipt("upstream", True)]])
        result = self.store.request_snapshots(ATTEMPT)
        self.assertEqual(result["state"], "UNKNOWN")
        self.assertEqual(len(result["snapshots"]), 2)
        self.assertEqual(self.snapshot_statements(), [])
        self.assert_no_publication_or_cleanup()

    def test_request_disconnect_or_interruption_does_not_confirm_retry_or_delete(self):
        for failure in (OSError("response lost"), KeyboardInterrupt()):
            with self.subTest(failure=type(failure).__name__):
                self.setUp()
                self.add_inspection()
                self.add_reservation()
                conn, cur = self.add_connection([])
                cur.execute.side_effect = failure
                with self.assertRaises(KeyboardInterrupt if isinstance(failure, KeyboardInterrupt) else RebaseAttemptUnknown):
                    self.store.request_snapshots(ATTEMPT)
                self.assertEqual(self.connect.call_count, 3)
                self.assertEqual(cur.execute.call_count, 1)
                self.assertTrue(cur.execute.call_args.args[0].startswith("CREATE SNAPSHOT"))
                conn.close.assert_called_once()
                self.assert_no_publication_or_cleanup()

    def test_invalid_catalog_receipt_leaves_request_unknown(self):
        bad_rows = [[], [snapshot(), snapshot()],
                    [("bad_uuid", 10, "database", "bench_feature", 17)],
                    [(snapshot()[0], 10, "database", "bench_feature", 18)],
                    [(snapshot()[0], 10, "table", "bench_feature", 17)],
                    [(snapshot()[0], 10, "database", "renamed", 17)],
                    [(snapshot()[0], 0, "database", "bench_feature", 17)]]
        for rows in bad_rows:
            with self.subTest(rows=rows):
                self.setUp()
                self.add_inspection()
                self.add_reservation()
                self.add_connection([rows])
                with self.assertRaises(RebaseAttemptUnknown):
                    self.store.request_snapshots(ATTEMPT)
                self.assertEqual(self.connect.call_count, 3)
                self.assertEqual(len(self.snapshot_statements()), 1)
                self.assert_no_publication_or_cleanup()

    def test_confirmation_commit_unknown_stops_before_upstream_request(self):
        self.add_inspection()
        self.add_reservation()
        self.add_connection([[snapshot()]])
        conn, _ = self.add_connection([[record()], [receipt()]])
        conn.commit.side_effect = OSError("confirmation response lost")
        with self.assertRaises(RebaseAttemptUnknown):
            self.store.request_snapshots(ATTEMPT)
        self.assertEqual(self.connect.call_count, 4)
        self.assertEqual(len(self.snapshot_statements()), 1)
        conn.rollback.assert_not_called()
        self.assert_no_publication_or_cleanup()

    def test_confirmation_can_record_original_receipt_after_branch_advances(self):
        self.add_full_flow()
        self.cursors[3].fetchone.return_value = ("later_generation", 2)
        self.store.request_snapshots(ATTEMPT)
        self.connections[3].commit.assert_called_once()
        self.assert_no_publication_or_cleanup()

    def test_stale_registration_blocks_new_requests(self):
        for branch in (None, ("replacement", 0), ("bench_feature", 1)):
            with self.subTest(branch=branch):
                self.setUp()
                self.add_inspection()
                conn, _ = self.add_reservation(branch=branch)
                with self.assertRaises(ValueError):
                    self.store.request_snapshots(ATTEMPT)
                self.assertEqual(self.snapshot_statements(), [])
                conn.rollback.assert_called_once()

    def test_upstream_cannot_start_before_feature_confirmation(self):
        conn, _ = self.add_connection([[record()], [], [receipt()]])
        with self.assertRaises(RebaseAttemptUnknown):
            self.store._reserve_snapshot_request(intent(), "upstream")
        self.assertFalse(any(call.args[0].startswith("INSERT") for call in self.cursors[0].execute.call_args_list))
        conn.rollback.assert_called_once()

    def test_corrupt_and_duplicate_receipts_are_not_evidence(self):
        malformed = [receipt(), receipt()]
        for rows in (malformed, [receipt(), receipt("upstream"), receipt()],
                     [("feature", "CONFIRMED", "other_snapshot", 17, receipt(confirmed=True)[4], 10)],
                     [("feature", "REQUESTED", intent().feature_snapshot, 17, "unexpected", None)]):
            with self.subTest(rows=rows):
                self.setUp()
                self.add_inspection(rows)
                with self.assertRaises(ValueError):
                    self.store.recover(ATTEMPT)
                self.assertEqual(self.connect.call_count, 1)
                self.assertIn("LIMIT 3", self.cursors[0].execute.call_args.args[0])
                self.assert_no_publication_or_cleanup()

    def test_confirmation_missing_or_conflicting_record_cannot_overwrite(self):
        for rows in ([], [receipt(confirmed=True)]):
            with self.subTest(rows=rows):
                self.setUp()
                conn, _ = self.add_connection([[record()], rows])
                value = self.store._decode_snapshot_request(intent(), receipt(confirmed=True))
                value["snapshot_ts"] += 1
                with self.assertRaises((RebaseAttemptUnknown, ValueError)):
                    self.store._confirm_snapshot_request(intent(), value)
                self.assertFalse(any(call.args[0].startswith("UPDATE") for call in self.cursors[0].execute.call_args_list))
                conn.rollback.assert_called_once()

    def test_pending_confirmed_receipts_do_not_imply_publication(self):
        self.add_inspection([receipt(confirmed=True), receipt("upstream", True)])
        self.add_connection([[("feature",)], [record()],
                             [receipt(confirmed=True), receipt("upstream", True)]])
        outcome = self.store.recover(ATTEMPT)
        self.assertEqual(outcome["state"], "UNKNOWN")
        self.assertIsNone(outcome["result"])
        self.assertEqual(len(outcome["snapshots"]), 2)
        self.assertTrue(all(call.args[0].startswith("SELECT") for call in self.cursors[0].execute.call_args_list))

    def test_snapshot_connection_must_be_autocommit(self):
        self.add_inspection()
        self.add_reservation()
        conn, cur = self.add_connection([])
        conn.get_autocommit.return_value = False
        with self.assertRaises(RebaseAttemptUnknown):
            self.store.request_snapshots(ATTEMPT)
        cur.execute.assert_not_called()
        conn.close.assert_called_once()

    def test_invalid_and_terminal_attempts_do_not_create_resources(self):
        with self.assertRaises(ValueError):
            self.store.request_snapshots("bad")
        self.connect.assert_not_called()
        self.add_connection([[record("COMMITTED", result={"generation": 1})], []])
        with self.assertRaises(ValueError):
            self.store.request_snapshots(ATTEMPT)
        self.assertEqual(self.connect.call_count, 1)
        self.assert_no_publication_or_cleanup()

    def test_replaced_base_identity_blocks_request_before_reservation_insert(self):
        fixed = intent()
        original = base_record(fixed.feature_base)
        changed = [("22345678-1234-1234-1234-123456789abc", *original[1:]),
                   (original[0], original[1] + 1, *original[2:]),
                   (*original[:4], original[4] + 1)]
        for actual in changed:
            with self.subTest(actual=actual):
                self.setUp()
                self.add_inspection()
                conn, cur = self.add_connection([[record()], [], [actual]])
                with self.assertRaisesRegex(ValueError, "base snapshot identity changed"):
                    self.store.request_snapshots(ATTEMPT)
                conn.rollback.assert_called_once()
                conn.commit.assert_not_called()
                self.assertEqual(cur.execute.call_args.args[1], (fixed.feature_base.snapshot,))
                self.assertFalse(any(call.args[0].startswith("INSERT") for call in cur.execute.call_args_list))
                self.assertEqual(self.snapshot_statements(), [])
                self.assert_no_publication_or_cleanup()

    def test_lost_base_between_requests_retains_confirmed_feature_without_upstream_send(self):
        fixed = intent()
        self.add_inspection()
        self.add_reservation()
        self.add_connection([[snapshot()]])
        confirmed, _ = self.add_connection([[record()], [receipt()]])
        stopped, cur = self.add_connection([[record()], [], [receipt(confirmed=True)],
                                            [base_record(fixed.feature_base)], []])
        with self.assertRaisesRegex(ValueError, "missing or mismatched Rebase base snapshot"):
            self.store.request_snapshots(ATTEMPT)
        confirmed.commit.assert_called_once()
        stopped.rollback.assert_called_once()
        stopped.commit.assert_not_called()
        self.assertEqual(len(self.snapshot_statements()), 1)
        self.assertFalse(any(call.args[0].startswith("INSERT") for call in cur.execute.call_args_list))
        self.assert_no_publication_or_cleanup()

    def test_base_read_error_does_not_create_abort_or_delete_resources(self):
        self.add_inspection()
        conn, cur = self.add_reservation()
        def execute(query, params=None):
            if "FROM mo_catalog.mo_snapshots" in query:
                raise OSError("snapshot metadata read failed")
        cur.execute.side_effect = execute
        with self.assertRaisesRegex(OSError, "metadata read failed"):
            self.store.request_snapshots(ATTEMPT)
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
        self.assertFalse(any(call.args[0].startswith(("INSERT", "UPDATE", "DELETE"))
                             for call in cur.execute.call_args_list))
        self.assertEqual(self.snapshot_statements(), [])
        self.assert_no_publication_or_cleanup()


if __name__ == "__main__":
    unittest.main()
