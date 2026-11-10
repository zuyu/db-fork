"""Tiny real-client acceptance against the exclusive native-Rebase SQL fixture.

Only the fixture's private databases/snapshots are touched. Failure preserves
them; successful verification removes the explicit resource list. Not a bench.
"""

import argparse
import hashlib
import json
from pathlib import Path
import signal
import subprocess
import sys
import time
import traceback

import pymysql

from dblib import matrixone_rebase as native
from tests.native_rebase_wire_gate import RebaseWireGate


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n")
    temporary.replace(path)


def private_connection(port, cursorclass=pymysql.cursors.Cursor):
    return pymysql.connect(host="127.0.0.1", port=port, user="dump", password="111",
                           autocommit=True, connect_timeout=10, read_timeout=30, write_timeout=30,
                           cursorclass=cursorclass)


def child_task(mode, port, metadata, attempt):
    queries = []
    class Observed(pymysql.cursors.Cursor):
        def execute(self, query, args=None):
            queries.append(query)
            return super().execute(query, args)
    store = native.RebaseAttemptStore(lambda: private_connection(port, Observed), metadata)
    action = {"snapshots": store.request_snapshots, "publish": store.publish, "recover": store.recover}[mode]
    return {"value": action(attempt), "queries": queries}


def execute(bridge):
    config = json.loads((bridge / "ready.json").read_text())
    if set(config) != {"port"} or type(config["port"]) is not int or not 0 < config["port"] < 65536:
        raise ValueError("invalid private fixture endpoint")
    query_log = (bridge / "adapter-sql.jsonl").open("x")
    native_queries = 0

    class ObservedCursor(pymysql.cursors.Cursor):
        def execute(self, query, args=None):
            nonlocal native_queries
            native_queries += int(query.startswith("DATA BRANCH REBASE DATABASE "))
            query_log.write(json.dumps({"sql": query, "params": args}, default=str) + "\n")
            query_log.flush()
            return super().execute(query, args)

    def connect():
        return private_connection(config["port"], ObservedCursor)

    try:
        conn = connect()
    except BaseException:
        query_log.close()
        raise
    snapshots = []
    databases = []
    prefix = "nr_adapter_pub"
    upstream, feature, metadata = prefix + "_u", prefix + "_f", prefix + "_meta"
    store = native.RebaseAttemptStore(connect, metadata)

    def sql(query, params=None):
        with conn.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else cur.rowcount

    def snapshot(database, name):
        sql(f"CREATE SNAPSHOT {native._identifier(name)} FOR DATABASE {native._identifier(database)}")
        snapshots.append(name)
        return name

    def state(database, fixed=None):
        expression = native._identifier(database) + ".`items`" + (" {snapshot=%s}" if fixed else "")
        return sql("SELECT id,v FROM " + expression + " ORDER BY id", (fixed,) if fixed else None)

    def check(actual, expected, context):
        if actual != expected:
            raise AssertionError(f"{context}: expected {expected!r}, got {actual!r}")

    def history(database, seq, kind, name, previous=None):
        sql(f"INSERT INTO {native._identifier(database)}.`_bb_commits` "
            "(seq,id,kind,message,snap,db,prev_snap,prev_db,ts,schema_json) "
            "VALUES (%s,%s,%s,'fixture',%s,%s,%s,%s,NOW(6),NULL)", (
                seq, "fixture_" + str(seq), kind, name, database,
                previous[1] if previous else None, previous[0] if previous else None))

    def create_history(database):
        sql(f"CREATE TABLE {database}._bb_commits("
            "seq bigint primary key,id varchar(40) not null,kind varchar(8) not null,message text,"
            "snap varchar(64) not null,db varchar(64) not null,prev_snap varchar(64),prev_db varchar(64),"
            "ts datetime(6),schema_json text,key _bb_commit_id(id))")

    def subprocess_command(mode, attempt, port):
        return [sys.executable, str(Path(__file__).resolve()), "--bridge", str(bridge),
                "--child", mode, "--attempt", attempt, "--metadata", metadata, "--port", str(port)]

    def fresh_recovery(attempt, label):
        with (bridge / (label + "-recovery-error.log")).open("x") as errors:
            child = subprocess.Popen(subprocess_command("recover", attempt, config["port"]),
                                     stdout=subprocess.PIPE, stderr=errors)
            try:
                output, _ = child.communicate(timeout=20)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=10)
        check(child.returncode, 0, "fresh recovery process")
        outcome = json.loads(output)
        if any(not query.upper().startswith(("SELECT ", "SET SESSION ")) for query in outcome["queries"]):
            raise AssertionError("recovery unexpectedly attempted publication or cleanup")
        save(bridge / (label + "-recovery.json"), {"pid": child.pid, "returncode": child.returncode, **outcome})
        return outcome["value"]

    def wait_publication_locks_released(branch, attempt):
        deadline = time.monotonic() + 10
        while True:
            probe = connect()
            try:
                probe.begin()
                with probe.cursor() as cur:
                    cur.execute(f"SELECT name FROM {metadata}.branches WHERE name=%s FOR UPDATE NOWAIT", (branch,))
                    check(cur.fetchall(), ((branch,),), "post-exit branch lock identity")
                    cur.execute(f"SELECT state FROM {metadata}.rebase_attempts WHERE attempt_id=%s FOR UPDATE NOWAIT", (attempt,))
                    rows = cur.fetchall()
                probe.rollback()
                return rows
            except pymysql.err.OperationalError as exc:
                probe.rollback()
                if not exc.args or exc.args[0] not in (1205, 1213, 3572) or time.monotonic() >= deadline:
                    raise
                # Observe asynchronous disconnect rollback, not retry a publish.
                time.sleep(0.02)
            finally:
                probe.close()

    def private_case(name, conflict=False):
        case_u, case_f = prefix + "_" + name + "_u", prefix + "_" + name + "_f"
        databases.extend((case_u, case_f))
        sql(f"CREATE DATABASE {case_u}")
        create_history(case_u)
        for table in (("a_ok", "z_bad") if conflict else ("items",)):
            sql(f"CREATE TABLE {case_u}.{table}(id int primary key,v int)")
            sql(f"INSERT INTO {case_u}.{table} VALUES(1,10)")
        ubase, fbase = prefix + "_" + name + "_bu", prefix + "_" + name + "_bf"
        history(case_u, 10, "root", ubase)
        snapshot(case_u, ubase)
        sql(f"DATA BRANCH CREATE DATABASE {case_f} FROM {case_u} {{snapshot=%s}}", (ubase,))
        history(case_f, 20, "fork", fbase, (case_u, ubase))
        snapshot(case_f, fbase)
        for table in (("a_ok", "z_bad") if conflict else ("items",)):
            sql(f"UPDATE {case_f}.{table} SET v=11 WHERE id=1")
        if conflict:
            sql(f"UPDATE {case_u}.z_bad SET v=12 WHERE id=1")
        source_branch, target_branch = "case_" + name, "up_" + name
        sql(f"INSERT INTO {metadata}.branches VALUES(%s,%s,0),(%s,%s,0)",
            (source_branch, case_f, target_branch, case_u))
        intent = store.prepare(source_branch, target_branch, source_registration=(case_f, 0),
                               upstream_registration=(case_u, 0), feature_base=(case_f, fbase),
                               upstream_base=(case_u, ubase), base_sequence=20)
        databases.append(intent.candidate_db)
        return intent, source_branch, case_u, case_f

    def check_unpublished(intent, branch, source):
        check(sql(f"SELECT db,gen FROM {metadata}.branches WHERE name=%s", (branch,)), ((source, 0),), "unpublished pointer")
        check(sql("SELECT epoch FROM mo_catalog.mo_database_inventory WHERE account_id=current_account_id() AND database_id=%s", (intent.source_id,)),
              ((intent.source_epoch,),), "unpublished source epoch")
        check(sql("SELECT count(*) FROM mo_catalog.mo_database WHERE datname=%s", (intent.candidate_db,)), ((0,),), "unpublished candidate")

    def failure_and_kill_cases():
        failed, branch, case_u, case_f = private_case("rollback", conflict=True)
        store.request_snapshots(failed.attempt_id)
        snapshots.extend((failed.feature_snapshot, failed.upstream_snapshot))
        try:
            store.publish(failed.attempt_id)
        except pymysql.MySQLError as exc:
            if "rebase conflict in table z_bad" not in str(exc).lower():
                raise
        else:
            raise AssertionError("late-table conflict unexpectedly published")
        check_unpublished(failed, branch, case_f)
        for database, table, value in ((case_f, "a_ok", 11), (case_f, "z_bad", 11), (case_u, "a_ok", 10), (case_u, "z_bad", 12)):
            check(sql(f"SELECT id,v FROM {database}.{table} ORDER BY id"), ((1, value),), "multi-table rollback")
        rollback = fresh_recovery(failed.attempt_id, "rollback")
        check(rollback["state"], "ABORTED", "acknowledged rollback and confirmed requests permit durable abort")
        check([item["state"] for item in rollback["snapshots"]], ["CONFIRMED", "CONFIRMED"], "failed transaction receipts")
        replacement = store.prepare(branch, "up_rollback", source_registration=(case_f, 0),
                                    upstream_registration=(case_u, 0),
                                    feature_base=(failed.feature_base.database, failed.feature_base.snapshot),
                                    upstream_base=(failed.upstream_base.database, failed.upstream_base.snapshot),
                                    base_sequence=20)
        databases.append(replacement.candidate_db)
        check(replacement.source_id, failed.source_id, "proven abort permits a fresh intent, not replay of old attempt")
        kills = {}
        for mode in ("snapshot", "before_commit", "after_commit"):
            intent, branch, case_u, case_f = private_case(mode)
            if mode != "snapshot":
                store.request_snapshots(intent.attempt_id)
                snapshots.extend((intent.feature_snapshot, intent.upstream_snapshot))
            gate = RebaseWireGate(config["port"], mode, bridge / (mode + "-wire.jsonl"),
                                  snapshot_prefix=f"CREATE SNAPSHOT {native._identifier(intent.feature_snapshot)} ",
                                  success_prefix=f"UPDATE {native._identifier(metadata)}.`rebase_attempts` SET state=")
            publisher = None
            receipt = {"attempt_id": intent.attempt_id, "mode": mode, "status": "FAIL"}
            saved_success = None
            try:
                with (bridge / (mode + "-publisher.log")).open("x") as log:
                    publisher = subprocess.Popen(subprocess_command("snapshots" if mode == "snapshot" else "publish",
                                                                    intent.attempt_id, gate.port), stdout=log, stderr=subprocess.STDOUT)
                    receipt["publisher_pid"] = publisher.pid
                    receipt["barrier"] = gate.wait_held(publisher)
                    save(bridge / (mode + "-kill-receipt.json"), receipt)
                    if mode == "snapshot":
                        check(sql("SELECT count(*) FROM mo_catalog.mo_snapshots WHERE account_name=current_account_name() AND sname=%s", (intent.feature_snapshot,)), ((1,),), "server snapshot OK proof")
                        receipt["server_snapshot_verified"] = True
                        snapshots.append(intent.feature_snapshot)
                    elif mode == "before_commit":
                        busy = fresh_recovery(intent.attempt_id, mode + "_busy")
                        check(busy["state"], "UNKNOWN", "active publication is unknown")
                        if "snapshots" in busy:
                            raise AssertionError("recovery did not observe the active publication lock")
                        receipt["busy_recovery_verified"] = True
                    else:
                        committed = sql(f"SELECT state,result_json FROM {metadata}.rebase_attempts WHERE attempt_id=%s", (intent.attempt_id,))
                        check(len(committed), 1, "server COMMIT result cardinality")
                        check(committed[0][0], "COMMITTED", "server committed before child response")
                        saved_success = json.loads(committed[0][1])
                        receipt["server_committed_verified"] = True
                    check(publisher.poll(), None, "publisher still blocked before SIGKILL")
                    publisher.kill()
                    check(publisher.wait(timeout=10), -signal.SIGKILL, "exact publisher killed")
                    receipt["publisher_returncode"] = publisher.returncode
            finally:
                if publisher is not None and publisher.poll() is None:
                    publisher.kill()
                    publisher.wait(timeout=10)
                gate.close()
                receipt["native_sql_requests"] = sum(query.startswith("DATA BRANCH REBASE DATABASE ") for query in gate.queries)
                save(bridge / (mode + "-kill-receipt.json"), receipt)
            lock_state = wait_publication_locks_released(branch, intent.attempt_id)
            receipt["post_exit_lock_state"] = lock_state
            recovered = fresh_recovery(intent.attempt_id, mode + "_after_exit")
            if mode == "after_commit":
                check(lock_state, (("COMMITTED",),), "committed lock state")
                check(recovered["state"], "COMMITTED", "fresh process confirms durable success")
                check(recovered["result"], saved_success, "exact original durable result")
                check(state(intent.candidate_db), ((1, 11),), "committed candidate")
                check(sql("SELECT count(*) FROM mo_catalog.mo_database WHERE datname=%s", (case_f,)), ((0,),), "committed source retired")
                check(sql(f"SELECT db,gen FROM {metadata}.branches WHERE name=%s", (branch,)), ((intent.candidate_db, 1),), "no repeated pointer publication")
            else:
                check(lock_state, (("PREPARING" if mode == "snapshot" else "PUBLISHING",),), "uncommitted durable intent")
                check(recovered["state"], "UNKNOWN", "pending resources must remain unknown")
                check_unpublished(intent, branch, case_f)
                check(state(case_f), ((1, 11),), "source unchanged after killed child")
                expected_receipts = ["REQUESTED"] if mode == "snapshot" else ["CONFIRMED", "CONFIRMED"]
                check([item["state"] for item in recovered["snapshots"]], expected_receipts, "pending receipts preserved")
            check(state(case_u), ((1, 10),), "upstream unchanged by killed child")
            check(receipt["native_sql_requests"], 0 if mode == "snapshot" else 1, "no duplicate native replay")
            before_native = native_queries
            try:
                store.publish(intent.attempt_id)
            except native.RebaseAttemptUnknown as exc:
                check(exc.attempt_id, intent.attempt_id, "single-send fence returns original attempt")
                if mode == "after_commit":
                    raise
            except ValueError:
                if mode != "after_commit":
                    raise
            else:
                raise AssertionError("a killed or terminal publication was replayed")
            check(native_queries, before_native, "retry cannot send a second native SQL")
            if mode != "after_commit":
                try:
                    store.prepare(branch, "up_" + mode, source_registration=(case_f, 0),
                                  upstream_registration=(case_u, 0),
                                  feature_base=(intent.feature_base.database, intent.feature_base.snapshot),
                                  upstream_base=(intent.upstream_base.database, intent.upstream_base.snapshot),
                                  base_sequence=20)
                except native.RebaseAttemptUnknown as exc:
                    check(exc.attempt_id, intent.attempt_id, "new attempt cannot bypass pending result")
                else:
                    raise AssertionError("pending branch admitted a replacement publication")
                check(sql(f"SELECT count(*) FROM {metadata}.rebase_attempts WHERE branch=%s", (branch,)), ((1,),), "only original attempt retained")
            for name in ((intent.feature_snapshot,) if mode == "snapshot" else (intent.feature_snapshot, intent.upstream_snapshot)):
                check(sql("SELECT count(*) FROM mo_catalog.mo_snapshots WHERE account_name=current_account_name() AND sname=%s", (name,)), ((1,),), "recovery must retain snapshots")
            receipt.update(status="PASS", recovered_state=recovered["state"], backend_and_relay_closed=True)
            save(bridge / (mode + "-kill-receipt.json"), receipt)
            kills[mode] = recovered["state"]
        return kills

    def prepare_and_snapshot(source, generation, feature_base, upstream_base, sequence):
        intent = store.prepare("feature", "main", source_registration=(source, generation),
                               upstream_registration=(upstream, 0), feature_base=feature_base,
                               upstream_base=upstream_base, base_sequence=sequence)
        receipts = store.request_snapshots(intent.attempt_id)
        check([r["state"] for r in receipts["snapshots"]], ["CONFIRMED", "CONFIRMED"], "snapshot receipts")
        snapshots.extend((intent.feature_snapshot, intent.upstream_snapshot))
        databases.append(intent.candidate_db)
        return intent

    try:
        for database in (upstream, feature, metadata):
            databases.append(database)
        sql(f"CREATE DATABASE {upstream}")
        sql(f"CREATE TABLE {upstream}.items(id int primary key,v int)")
        sql(f"CREATE TABLE {upstream}.history(v int)")
        create_history(upstream)
        sql(f"INSERT INTO {upstream}.items VALUES(1,10)")
        sql(f"INSERT INTO {upstream}.history VALUES(1),(1),(NULL)")
        base_u, base_f = prefix + "_bu", prefix + "_bf"
        history(upstream, 10, "root", base_u)
        snapshot(upstream, base_u)
        sql(f"DATA BRANCH CREATE DATABASE {feature} FROM {upstream} {{snapshot=%s}}", (base_u,))
        history(feature, 20, "fork", base_f, (upstream, base_u))
        snapshot(feature, base_f)
        sql(f"UPDATE {feature}.items SET v=11 WHERE id=1")
        sql(f"INSERT INTO {feature}.items VALUES(4,40)")
        historical_f = prefix + "_feature_commit"
        history(feature, 30, "commit", historical_f)
        snapshot(feature, historical_f)
        sql(f"INSERT INTO {upstream}.items VALUES(2,20)")
        history(upstream, 40, "commit", prefix + "_upstream_commit")
        snapshot(upstream, prefix + "_upstream_commit")
        sql(f"CREATE DATABASE {metadata}")
        sql(f"CREATE TABLE {metadata}.branches(name varchar(64) primary key,db varchar(64),gen int)")
        sql(f"INSERT INTO {metadata}.branches VALUES('main',%s,0),('feature',%s,0)", (upstream, feature))
        first = prepare_and_snapshot(feature, 0, (feature, base_f), (upstream, base_u), 20)
        sql(f"INSERT INTO {upstream}.items VALUES(3,30)")
        one = store.publish(first.attempt_id)
        check(state(one["candidate_db"]), ((1, 11), (2, 20), (4, 40)), "first native result")
        check(state(upstream), ((1, 10), (2, 20), (3, 30)), "later upstream write")
        check(state(feature, historical_f), ((1, 11), (4, 40)), "retired original history")
        check(store.recover(first.attempt_id)["result"], one, "first durable result")
        check(sql(f"SELECT gen,db FROM {metadata}.branches WHERE name='feature'"),
              ((1, one["candidate_db"]),), "first pointer")
        check(sql("SELECT count(*) FROM mo_catalog.mo_database WHERE datname=%s", (feature,)), ((0,),), "source retirement")
        marker_rows = sql(f"SELECT seq,db,snap,prev_db,prev_snap FROM {native._identifier(one['candidate_db'])}._bb_commits WHERE kind='rebase'")
        check(len(marker_rows), 1, "first base marker cardinality")
        marker, marker_db, marker_snap, previous_db, previous_snap = marker_rows[0]
        check((marker_db, marker_snap, previous_db, previous_snap),
              (upstream, first.upstream_snapshot, upstream, first.upstream_snapshot), "actual U base")
        current = one["candidate_db"]
        sql(f"UPDATE {native._identifier(current)}.items SET v=41 WHERE id=4")
        historical_second = prefix + "_feature_commit2"
        history(current, 50, "commit", historical_second)
        snapshot(current, historical_second)
        sql(f"INSERT INTO {upstream}.items VALUES(5,50)")
        fixed_base = (upstream, first.upstream_snapshot)
        second = prepare_and_snapshot(current, 1, fixed_base, fixed_base, marker)
        two = store.publish(second.attempt_id)
        expected = ((1, 11), (2, 20), (3, 30), (4, 41), (5, 50))
        check(state(two["candidate_db"]), expected, "second native result")
        check(store.recover(first.attempt_id)["result"], one, "old success survives advanced pointer")
        check(store.recover(second.attempt_id)["result"], two, "second durable result")
        check(state(current, historical_second), ((1, 11), (2, 20), (4, 41)), "retired second history")
        check(sql(f"SELECT id FROM {native._identifier(two['candidate_db'])}._bb_commits "
                  "WHERE kind='commit' ORDER BY seq"),
              (("fixture_40",), ("fixture_30",), ("fixture_50",)), "preserved commit identities")
        sql(f"DATA BRANCH MERGE {native._identifier(two['candidate_db'])}.items INTO {upstream}.items WHEN CONFLICT FAIL")
        check(state(upstream), expected, "merge after second rebase")
        check(sql(f"SELECT count(*),sum(v=1),sum(v is null) FROM {native._identifier(two['candidate_db'])}.history"),
              ((3, 2, 1),), "duplicate/NULL bag retained")
        kills = failure_and_kill_cases()
        # All fixed/historical reads and durable confirmations precede cleanup.
        for name in reversed(snapshots):
            sql(f"DROP SNAPSHOT {native._identifier(name)}")
        for database in reversed(databases):
            sql(f"DROP DATABASE IF EXISTS {native._identifier(database)}")
        remaining = sql("SELECT datname FROM mo_catalog.mo_database WHERE datname IN (" +
                        ",".join("%s" for _ in databases) + ")", tuple(databases))
        check(remaining, (), "precise database cleanup")
        check(sql("SELECT sname FROM mo_catalog.mo_snapshots WHERE account_name=current_account_name() AND sname IN (" +
                  ",".join("%s" for _ in snapshots) + ")", tuple(snapshots)), (), "precise snapshot cleanup")
        return {"status": "PASS_REAL_NATIVE_ADAPTER_PUBLICATION", "publications": 3, "main_path_publications": 2,
                "rollback_verified": True, "kill_cases": 3, "killed_client_recovery": kills,
                "single_send_verified": True, "known_abort_verified": True, "pending_branch_fence_verified": True,
                "historical_reads": 2, "merge_verified": True, "cleanup_complete": True,
                "adapter_sha256": hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest()}
    finally:
        conn.close()
        query_log.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bridge", type=Path, required=True)
    parser.add_argument("--child", choices=("snapshots", "publish", "recover"))
    parser.add_argument("--attempt")
    parser.add_argument("--metadata")
    parser.add_argument("--port", type=int)
    args = parser.parse_args()
    bridge = args.bridge
    if args.child:
        value = child_task(args.child, args.port, args.metadata, args.attempt)
        print(json.dumps(value, sort_keys=True))
        return 0
    try:
        result = execute(bridge)
    except BaseException as exc:
        traceback.print_exc(file=sys.stderr)
        save(bridge / "result.json", {"status": "FAIL", "category": type(exc).__name__, "cleanup_complete": False})
        return 1
    save(bridge / "result.json", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
