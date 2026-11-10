"""Bounded same-build old/native Rebase comparison, never normal CI timing.

The exclusive Go bridge owns the server. Use the upstream Rebase fixture recipe
(commit one disjoint insert on each side),16 spine branches,seed42,one untimed
warm-up and ten measured calls. Setup/checks/cleanup are outside verb latency.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import re
import sqlite3
import statistics
import time
import traceback

import pymysql


def save(path, value):
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, sort_keys=True) + "\n")
    temporary.replace(path)


def seed_statements(path):
    """Read the pinned generator's DDL prefix and one-INSERT-per-line format."""
    prefix = []
    inserting = False
    with path.open() as stream:
        for line in stream:
            if not inserting and not line.startswith("INSERT INTO "):
                prefix.append(line)
                if sqlite3.complete_statement("".join(prefix)):
                    yield "".join(prefix)
                    prefix.clear()
                continue
            if not inserting:
                if "".join(prefix).strip():
                    raise ValueError("seed has incomplete DDL")
                inserting = True
            if not line.startswith("INSERT INTO ") or not line.rstrip().endswith(";"):
                raise ValueError("seed is not the pinned generator's statement format")
            yield line
    if not inserting:
        raise ValueError("seed has no generated inserts")


def execute(bridge, seed_dir):
    ready = json.loads((bridge / "ready.json").read_text())
    if set(ready) != {"port"} or type(ready["port"]) is not int or not 0 < ready["port"] < 65536:
        raise ValueError("invalid private bridge endpoint")
    os.environ.update(MO_HOST="127.0.0.1", MO_PORT=str(ready["port"]), MO_USER="dump", MO_PASSWORD="111")
    from dblib import matrixone as mo
    from dblib import matrixone_rebase as native
    from dblib.matrixone_native_rebase import NativeRebaseMatrixOneToolSuite
    from dblib.result_collector import ResultCollector

    seed_file, metadata_file = seed_dir / "ch-w1-common.sql", seed_dir / "ch-w1-common.json"
    metadata = json.loads(metadata_file.read_text())
    if (metadata.get("scale") != "W=1" or metadata.get("seed") != 42
            or hashlib.sha256(seed_file.read_bytes()).hexdigest() != metadata.get("sql_sha256")):
        raise ValueError("W1 seed identity differs")

    log = (bridge / "adapter-sql.jsonl").open("x")
    operations = None
    original_connect = pymysql.connect
    phase, sql_count, native_times = "init", 0, []
    databases, snapshots = set(), set()
    raw, suite_connection = None, None
    groups = []

    class Observed(pymysql.cursors.Cursor):
        def execute(self, query, args=None):
            nonlocal sql_count
            sql_count += 1
            if sql_count > 25000:
                raise RuntimeError("comparison SQL budget exceeded")
            # Preserve full verb SQL but do not duplicate77MB seed bodies.
            record = {"phase": phase, "sql": query, "params": args}
            if phase == "seed" and query.startswith("INSERT INTO "):
                record = {"phase": phase, "seed_statement_sha256": hashlib.sha256(query.encode()).hexdigest()}
            log.write(json.dumps(record, default=str) + "\n")
            log.flush()
            started = time.perf_counter()
            result = super().execute(query, args)
            if phase == "verb" and query.startswith("DATA BRANCH REBASE DATABASE "):
                native_times.append(time.perf_counter() - started)
            match = re.match(r"^CREATE SNAPSHOT (`(?:``|[^`])+`|[a-zA-Z0-9_]+)\s+FOR DATABASE ", query, re.I)
            if match:
                name = match[1]
                snapshots.add(name[1:-1].replace("``", "`") if name.startswith("`") else name)
            return result

    def observed_connect(*args, **kwargs):
        kwargs.setdefault("cursorclass", Observed)
        return original_connect(*args, **kwargs)

    def sql(query, params=None):
        with raw.cursor() as cur:
            cur.execute(query, params)
            return cur.fetchall() if cur.description else cur.rowcount

    def check(actual, expected, context):
        if actual != expected:
            raise AssertionError(f"{context}: expected {expected!r}, got {actual!r}")

    def verb(result):
        result.raise_for_status()
        if not result.ok:
            raise AssertionError("unsupported result cannot become a performance sample")
        return result.value

    def change(suite, ref, statement):
        for result in suite.exec([statement], refs=[ref], timed=False, raise_on_error=True):
            verb(result)

    pymysql.connect = observed_connect
    try:
        operations = (bridge / "comparison-operations.jsonl").open("x")
        raw = mo.connect(None, read_timeout=90, write_timeout=90)
        for scale, order in (("small", ("legacy", "native")), ("W1", ("native", "legacy"))):
            for mode in order:
                phase = "seed"
                started = time.perf_counter()
                database = f"nr_compare_{scale.lower()}_{mode}"
                databases.update((database, mo.meta_db_name(database)))
                sql(f"CREATE DATABASE {native._identifier(database)}")
                sql(f"USE {native._identifier(database)}")
                if scale == "small":
                    sql("CREATE TABLE items(id int primary key,v int)")
                    sql("INSERT INTO items VALUES(1,10),(2,20)")
                    table, columns, initial_count = "items", "id,v", 2
                else:
                    for statement in seed_statements(seed_file):
                        sql(statement)
                    for table_name, count in metadata["counts"].items():
                        check(sql(f"SELECT count(*) FROM {native._identifier(table_name)}"), ((count,),), "W1 seed row count")
                    table, columns, initial_count = "new_order", "no_o_id,no_d_id,no_w_id", metadata["counts"]["new_order"]
                load_wall = time.perf_counter() - started
                phase = "prepare"
                started = time.perf_counter()
                collector = ResultCollector(run_id=f"rebase-{scale}-{mode}", output_dir=str(bridge / f"collector-{scale}-{mode}"))
                suite_connection = mo.connect(None, read_timeout=90, write_timeout=90)
                suite_type = NativeRebaseMatrixOneToolSuite if mode == "native" else mo.MatrixOneToolSuite
                suite = suite_type(suite_connection, collector, database)
                verb(suite.commit("main", "seed", timed=False, raise_on_error=True))
                parent = "main"
                branches = [f"spine{i}" for i in range(16)]
                for branch in branches:
                    verb(suite.branch(branch, parent, timed=False, raise_on_error=True))
                    databases.add(suite._db_for(branch))
                    parent = branch
                spine_wall = time.perf_counter() - started
                rnd = random.Random(42)
                schedule = [rnd.choice(branches) for _ in range(11)]
                additions = {name: [] for name in branches}
                records = []
                for index, parent in enumerate(schedule):
                    phase = "prepare"
                    prepared = time.perf_counter()
                    child = f"operation{index}"
                    verb(suite.branch(child, parent, timed=False, raise_on_error=True))
                    databases.add(suite._db_for(child))
                    child_key, parent_key = 1000000 + 2 * index, 1000001 + 2 * index
                    values = lambda key: (key, key) if scale == "small" else (key, 1, 1)
                    render = lambda key: ",".join(str(value) for value in values(key))
                    change(suite, child, f"INSERT INTO {table}({columns}) VALUES({render(child_key)})")
                    verb(suite.commit(child, "child insert", timed=False, raise_on_error=True))
                    change(suite, parent, f"INSERT INTO {table}({columns}) VALUES({render(parent_key)})")
                    verb(suite.commit(parent, "parent insert", timed=False, raise_on_error=True))
                    additions[parent].append(values(parent_key))
                    prep_wall = time.perf_counter() - prepared
                    phase = "verb"
                    before, before_native = sql_count, len(native_times)
                    result = suite.rebase(child, parent, timed=index != 0, storage=False, raise_on_error=True)
                    outcome = verb(result)
                    statement_times = native_times[before_native:]
                    count = sql_count - before
                    if mode == "native":
                        check(result.execution_paths, ["native_rebase_publication"], "native performance route")
                        check(len(statement_times), 1, "exactly one native replay")
                        databases.add(outcome["candidate_db"])
                    else:
                        check(len(statement_times), 0, "legacy comparator must not use native replay")
                    phase = "verify"
                    candidate = suite._db_for(child)
                    wanted = tuple(sorted(additions[parent] + [values(child_key)]))
                    check(sql(f"SELECT {columns} FROM {native._identifier(candidate)}.{table} WHERE "
                              f"{columns.split(',')[0]}>=1000000 ORDER BY {columns.split(',')[0]}"), wanted, "replayed disjoint keys")
                    check(sql(f"SELECT count(*) FROM {native._identifier(candidate)}.{table}"),
                          ((initial_count + len(wanted),),), "replayed row count")
                    if mode == "native":
                        recovered = suite.recover_rebase(outcome["attempt_id"])
                        check(recovered["state"], "COMMITTED", "measured publication durable success")
                    phase = "cleanup"
                    cleaned = time.perf_counter()
                    verb(suite.delete(child, timed=False, raise_on_error=True))
                    cleanup_wall = time.perf_counter() - cleaned
                    records.append({"index": index, "warmup": index == 0, "parent": parent,
                                    "adapter_elapsed_sec": result.latency, "verb_sql_count": count,
                                    "native_sql_client_sec": statement_times[0] if statement_times else None,
                                    "prepare_wall_sec": prep_wall, "cleanup_wall_sec": cleanup_wall})
                    operations.write(json.dumps({"scale": scale, "mode": mode, **records[-1]}) + "\n")
                    operations.flush()
                phase = "cleanup"
                registered = sql("SELECT db FROM " + native._identifier(suite._meta_db) + ".branches")
                databases.update(row[0] for row in registered)
                databases.update(suite._temp_dbs.values())
                suite.close_connection()
                suite_connection = None
                measured = [record for record in records if not record["warmup"]]
                check(len(collector.results), 10, "one warmup is outside recorded operations")
                groups.append({"scale": scale, "mode": mode, "load_wall_sec": load_wall,
                               "spine_prepare_wall_sec": spine_wall, "records": records,
                               "measured_operations": len(measured), "warmup_operations": 1,
                               "median_adapter_sec": statistics.median(record["adapter_elapsed_sec"] for record in measured)})
                save(bridge / "comparison-progress.json", {"groups": groups})
        phase = "cleanup"
        for name in sorted(snapshots):
            sql(f"DROP SNAPSHOT {native._identifier(name)}")
        for name in sorted(databases, reverse=True):
            sql(f"DROP DATABASE IF EXISTS {native._identifier(name)}")
        for relation, column, resources in (("mo_catalog.mo_database", "datname", databases),
                                            ("mo_catalog.mo_snapshots", "sname", snapshots)):
            check(sql(f"SELECT {column} FROM {relation} WHERE {column} IN (" +
                      ",".join("%s" for _ in resources) + ")", tuple(sorted(resources))), (), "exact successful fixture cleanup")
        return {"status": "PASS_REAL_REBASE_SMALL_AND_W1_COMPARISON", "groups": groups,
                "seed_sha256": metadata["sql_sha256"], "workers": 1, "spine_branches": 16,
                "random_seed": 42, "cleanup_complete": True, "production_enabled": False,
                "adapter_sha256": hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),
                "suite_sha256": hashlib.sha256(Path(mo.__file__).read_bytes()).hexdigest()}
    finally:
        if suite_connection is not None:
            suite_connection.close()
        if raw is not None:
            raw.close()
        pymysql.connect = original_connect
        if operations is not None:
            operations.close()
        log.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bridge", type=Path, required=True)
    parser.add_argument("--seed-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        value = execute(args.bridge, args.seed_dir)
    except BaseException as exc:
        traceback.print_exc()
        save(args.bridge / "result.json", {"status": "FAIL", "category": type(exc).__name__, "cleanup_complete": False})
        return 1
    save(args.bridge / "result.json", value)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
