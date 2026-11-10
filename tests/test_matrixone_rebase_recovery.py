"""Locked recovery ordering, not actual process-loss or transaction evidence."""

from dataclasses import replace
import unittest

from pymysql.err import OperationalError

from tests.test_matrixone_rebase_attempts import ATTEMPT, intent, record
from tests import test_matrixone_rebase_snapshots as fixtures


class LockedRecoveryTest(unittest.TestCase):
    setUp = fixtures.SnapshotRequestTest.setUp
    add_connection = fixtures.SnapshotRequestTest.add_connection
    assert_no_publication_or_cleanup = fixtures.SnapshotRequestTest.assert_no_publication_or_cleanup

    def add_recovery(self, observed=None, current=None, registrations=None):
        observed = observed or record()
        current = current or observed
        self.add_connection([[observed], []])
        return self.add_connection([
            [("feature",)] if registrations is None else registrations, [current], []])

    def test_branch_then_attempt_lock_and_reread_precedes_return(self):
        result = {"candidate_db": intent().candidate_db, "generation": 1}
        conn, cur = self.add_recovery(current=record("COMMITTED", result=result))
        outcome = self.store.recover(ATTEMPT)
        self.assertEqual(outcome["state"], "COMMITTED")
        self.assertEqual(outcome["result"], result)
        statements = [call.args for call in cur.execute.call_args_list]
        self.assertIn("READ COMMITTED", statements[0][0])
        self.assertIn("branches", statements[1][0])
        self.assertIn("FOR UPDATE NOWAIT", statements[1][0])
        self.assertEqual(statements[1][1], ("feature",))
        self.assertIn("rebase_attempts", statements[2][0])
        self.assertIn("FOR UPDATE NOWAIT", statements[2][0])
        self.assertEqual(statements[2][1], (ATTEMPT,))
        conn.begin.assert_called_once()
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
        for connection in self.connections:
            connection.close.assert_called_once()
        self.assert_no_publication_or_cleanup()

    def test_terminal_success_survives_missing_or_advanced_branch(self):
        result = {"candidate_db": intent().candidate_db, "generation": 1}
        for registrations in ([], (), [("feature",)], (("feature",),)):
            with self.subTest(registrations=registrations):
                self.setUp()
                self.add_recovery(record("COMMITTED", result=result),
                                  registrations=registrations)
                outcome = self.store.recover(ATTEMPT)
                self.assertEqual(outcome["state"], "COMMITTED")
                self.assertEqual(outcome["result"], result)
                self.assertFalse(any("SELECT db, gen" in call.args[0]
                                     for call in self.cursors[1].execute.call_args_list))

    def test_pending_stays_unknown_even_with_both_confirmed_snapshots(self):
        conn, cur = self.add_recovery()
        cur.fetchall.side_effect = [[("feature",)], [record()],
                                   [fixtures.receipt(confirmed=True), fixtures.receipt("upstream", True)]]
        outcome = self.store.recover(ATTEMPT)
        self.assertEqual(outcome["state"], "UNKNOWN")
        self.assertIsNone(outcome["result"])
        self.assertEqual(len(outcome["snapshots"]), 2)
        conn.commit.assert_called_once()
        self.assert_no_publication_or_cleanup()

    def test_claimed_publication_stays_unknown_without_replay_or_abort(self):
        self.add_recovery(record("PUBLISHING"))
        outcome = self.store.recover(ATTEMPT)
        self.assertEqual(outcome["state"], "UNKNOWN")
        self.assertIsNone(outcome["result"])
        self.assert_no_publication_or_cleanup()

    def test_lock_wait_and_deadlock_are_unknown_not_aborted(self):
        for code in (1205, 1213, 3572):
            for relation in ("branches", "rebase_attempts"):
                with self.subTest(code=code, relation=relation):
                    self.setUp()
                    conn, cur = self.add_recovery()
                    def execute(sql, params=None):
                        if relation in sql and "FOR UPDATE" in sql:
                            raise OperationalError(code, "lock not acquired")
                    cur.execute.side_effect = execute
                    outcome = self.store.recover(ATTEMPT)
                    self.assertEqual(outcome["state"], "UNKNOWN")
                    self.assertIsNone(outcome["result"])
                    conn.rollback.assert_called_once()
                    conn.commit.assert_not_called()
                    self.assert_no_publication_or_cleanup()

    def test_nonlock_server_failure_propagates(self):
        conn, cur = self.add_recovery()
        def execute(sql, params=None):
            if "FOR UPDATE" in sql:
                raise OperationalError(2013, "connection lost")
        cur.execute.side_effect = execute
        with self.assertRaises(OperationalError):
            self.store.recover(ATTEMPT)
        conn.rollback.assert_called_once()

    def test_uncertain_release_keeps_unknown_without_retry(self):
        for operation in ("commit", "rollback"):
            with self.subTest(operation=operation):
                self.setUp()
                conn, cur = self.add_recovery()
                getattr(conn, operation).side_effect = OSError("response lost")
                if operation == "rollback":
                    cur.execute.side_effect = OperationalError(1205, "lock wait")
                outcome = self.store.recover(ATTEMPT)
                self.assertEqual(outcome["state"], "UNKNOWN")
                self.assertEqual(self.connect.call_count, 2)
                self.assert_no_publication_or_cleanup()

    def test_identity_or_terminal_regression_is_rejected(self):
        result = {"candidate_db": intent().candidate_db, "generation": 1}
        cases = [(record(), record(value=replace(intent(), source_epoch=4))),
                 (record("COMMITTED", result=result), record()),
                 (record("COMMITTED", result=result), record("ABORTED")),
                 (record("COMMITTED", result=result),
                  record("COMMITTED", result={"candidate_db": "different"}))]
        for observed, current in cases:
            with self.subTest(current=current):
                self.setUp()
                conn, _ = self.add_recovery(observed, current)
                with self.assertRaises(ValueError):
                    self.store.recover(ATTEMPT)
                conn.rollback.assert_called_once()
                conn.commit.assert_not_called()
                self.assert_no_publication_or_cleanup()

    def test_missing_or_ambiguous_locked_attempt_cannot_confirm_success(self):
        for rows in ([], [record(), record()]):
            with self.subTest(rows=rows):
                self.setUp()
                conn, cur = self.add_recovery()
                cur.fetchall.side_effect = [[("feature",)], rows]
                if rows:
                    with self.assertRaises(ValueError):
                        self.store.recover(ATTEMPT)
                    conn.rollback.assert_called_once()
                else:
                    self.assertEqual(self.store.recover(ATTEMPT)["state"], "UNKNOWN")
                    conn.commit.assert_called_once()

    def test_ambiguous_branch_registration_is_not_success_evidence(self):
        for registrations in ([("other",)], [("feature",), ("feature",)]):
            with self.subTest(registrations=registrations):
                self.setUp()
                conn, _ = self.add_recovery(registrations=registrations)
                with self.assertRaisesRegex(ValueError, "branch registration"):
                    self.store.recover(ATTEMPT)
                conn.rollback.assert_called_once()
                self.assert_no_publication_or_cleanup()


if __name__ == "__main__":
    unittest.main()
