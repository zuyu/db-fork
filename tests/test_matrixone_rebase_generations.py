"""Durable generation aliases, without a database or publication claim."""

from dataclasses import replace
import hashlib
import unittest
from unittest.mock import MagicMock, Mock

from dblib.matrixone_rebase import RebaseAttemptStore, RebaseAttemptUnknown
from tests.test_matrixone_rebase_attempts import ATTEMPT, intent, record


def candidate(branch="feature", generation=1, attempt=ATTEMPT, meta="bench_meta"):
    namespace = f"`{meta}`.`rebase_attempts`\x00{branch}"
    prefix = hashlib.sha256(namespace.encode()).hexdigest()[:12]
    return f"bb_r2_{prefix}_{generation}_{attempt}"


def published(attempt=ATTEMPT, generation=1, branch="feature"):
    value = replace(intent(attempt), branch=branch, old_generation=generation - 1,
                    new_generation=generation, candidate_db=candidate(branch, generation, attempt))
    return value, record("COMMITTED", value,
                         {"candidate_db": value.candidate_db, "generation": generation})


class GenerationIdentityTest(unittest.TestCase):
    def setUp(self):
        self.conn = MagicMock()
        self.cur = self.conn.cursor.return_value.__enter__.return_value
        self.connect = Mock(return_value=self.conn)
        self.store = RebaseAttemptStore(self.connect, "bench_meta")

    def test_non_native_or_empty_lookup_does_not_open_connection(self):
        self.assertEqual(self.store.generation_branches(), {})
        self.assertEqual(self.store.generation_branches("bench", "bench__feature__g2"), {})
        self.connect.assert_not_called()

    def test_lookup_is_bounded_and_validates_names_before_opening(self):
        for values in (("x",) * 5, (None,), ("x" * 65,)):
            with self.subTest(values=values), self.assertRaises(ValueError):
                self.store.generation_branches(*values)
        self.connect.assert_not_called()

    def test_generations_share_logical_branch_without_current_pointer(self):
        first, first_row = published()
        second, second_row = published("f" * 32, 2)
        self.cur.fetchall.return_value = [second_row, first_row]
        outcome = self.store.generation_branches(second.candidate_db, "bench",
                                                 first.candidate_db, first.candidate_db.upper())
        self.assertEqual(outcome, {second.candidate_db: "feature", first.candidate_db: "feature",
                                   first.candidate_db.upper(): "feature"})
        sql, params = self.cur.execute.call_args.args
        self.assertIn("attempt_id IN (%s,%s)", sql)
        self.assertEqual(params, (ATTEMPT, "f" * 32, 3))
        self.assertNotIn("branches", sql)
        self.assertNotIn("FOR UPDATE", sql)
        self.assertNotIn("json_extract", sql.lower())
        self.cur.execute.assert_called_once()
        self.conn.begin.assert_not_called()
        self.conn.commit.assert_not_called()
        self.conn.rollback.assert_not_called()
        self.conn.close.assert_called_once()

    def test_absent_and_pending_identities_stay_unknown(self):
        value, _ = published()
        for rows in ([], [record(value=value)], [record("UNKNOWN", value)]):
            with self.subTest(rows=rows):
                self.cur.fetchall.return_value = rows
                with self.assertRaises(RebaseAttemptUnknown) as caught:
                    self.store.generation_branches(value.candidate_db)
                self.assertEqual(caught.exception.attempt_id, ATTEMPT)

    def test_abort_is_not_a_generation_and_never_authorizes_cleanup(self):
        value, _ = published()
        self.cur.fetchall.return_value = [record("ABORTED", value)]
        with self.assertRaisesRegex(ValueError, "aborted"):
            self.store.generation_branches(value.candidate_db)
        self.assertTrue(all(call.args[0].startswith("SELECT") for call in self.cur.execute.call_args_list))

    def test_wrong_name_generation_or_metadata_namespace_fails(self):
        value, row = published()
        for name in (candidate(generation=2), candidate(meta="other_meta")):
            with self.subTest(name=name):
                self.cur.fetchall.return_value = [row]
                with self.assertRaisesRegex(ValueError, "original resource"):
                    self.store.generation_branches(name)
        foreign = replace(value, candidate_db=candidate(meta="other_meta"))
        self.cur.fetchall.return_value = [record("COMMITTED", foreign,
            {"candidate_db": foreign.candidate_db, "generation": foreign.new_generation})]
        with self.assertRaisesRegex(ValueError, "original resource"):
            self.store.generation_branches(foreign.candidate_db)

    def test_result_cannot_override_fixed_generation(self):
        value, _ = published()
        for result in ({}, {"candidate_db": "other", "generation": 1},
                       {"candidate_db": value.candidate_db, "generation": 2},
                       {"candidate_db": value.candidate_db, "generation": True}):
            with self.subTest(result=result):
                self.cur.fetchall.return_value = [record("COMMITTED", value, result)]
                with self.assertRaisesRegex(ValueError, "contradicts"):
                    self.store.generation_branches(value.candidate_db)

    def test_duplicate_or_unrequested_attempt_is_rejected(self):
        value, row = published()
        _, other_row = published("f" * 32, 2)
        for rows in ([row, row], [other_row]):
            with self.subTest(rows=rows):
                self.cur.fetchall.return_value = rows
                with self.assertRaisesRegex(ValueError, "ambiguous"):
                    self.store.generation_branches(value.candidate_db)

    def test_read_failure_does_not_guess_an_alias(self):
        self.cur.execute.side_effect = OSError("server unavailable")
        with self.assertRaises(OSError):
            self.store.generation_branches(candidate())
        self.conn.close.assert_called_once()
