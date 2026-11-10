"""MatrixOne backend.

MatrixOne is MySQL-compatible and ships "git for data" primitives: database
and table branches with recorded lineage (``DATA BRANCH CREATE``), a
three-way ``DATA BRANCH DIFF`` / ``DATA BRANCH MERGE`` that finds the lowest
common ancestor of two tables itself, named snapshots with time-travel
reads (``t{snapshot = 'x'}``) and ``RESTORE DATABASE`` from a snapshot.
Branches are whole databases, as on SeekDB:

    main branch      -> <db_name>
    branch "X"       -> <db_name>__X
    after N rebases  -> <db_name>__X__gN   (main: <db_name>__main__gN)

A rebase re-forks the branch into a new database (its next *generation*)
and drops the old one, so the branch name -> database mapping lives in a
registry table, ``<db_name>___bb_meta.branches``, that every worker reads
(a point lookup, ~0.2 ms). The generation suffix keeps old snapshot names
unambiguous: a commit made before a rebase is still read through the
database it was taken on, which stays readable by name after it is dropped.

A commit is a database snapshot. Its message and ordering live in the
branch database's ``_bb_commits`` table (branches inherit it, since a
branch copies every table), and each row names the snapshot (and the
database it was taken on) that holds the committed state. Snapshots stay
readable after their database is dropped, so a merged-and-deleted branch's
commits can still be diffed and reverted.

Verb -> MatrixOne primitives (see ``IMPLEMENTATION`` for the summary the
report uses):

    branch     CREATE SNAPSHOT on the parent (the fork point) and
               DATA BRANCH CREATE DATABASE <branch> FROM <parent> {snapshot}
    commit     CREATE SNAPSHOT FOR DATABASE <branch> + a _bb_commits row
    log        SELECT FROM _bb_commits
    diff       DATA BRANCH DIFF DATABASE ... OUTPUT SUMMARY (table fallback)
    merge      DATA BRANCH MERGE DATABASE <src> INTO <dst> TABLES (...)
               WHEN CONFLICT SKIP|ACCEPT OUTPUT SUMMARY ("ours" | "theirs");
               older servers and custom resolvers use a pre-merge summary
               and two native diffs against the fork snapshot, with
               conflict rows joined in Python
    rebase     re-fork: the branch's next generation is cloned from the
               upstream's head snapshot and snapshotted at once (the new
               base), then receives the branch's delta since its previous
               base: the keys a native diff of the branch's own two
               snapshots lists (plus rows with a value in a column the
               branch added, which that diff does not see) are copied in
               with SQL from the head snapshot's time-travel view (never
               DATA BRANCH PICK, see the limitations; in native-only mode
               rebase is unsupported); conflicts with the upstream's delta are resolved
               per on_conflict; the registry is repointed and the old
               generation dropped (the lineage now starts at the upstream
               head, so a later merge back sees only newer changes)
    revert     DATA BRANCH REVERT DATABASE between the commit's snapshot
               and its predecessor; legacy row replay for older servers
               and schema-changing commits
    reset      DATA BRANCH RESET DATABASE from the commit's snapshot,
               restoring rows in place and preserving table identities;
               normal mode reconciles schema and can use legacy row replay;
               native-only mode falls back to RESTORE DATABASE on older servers
    delete     DATA BRANCH DELETE DATABASE (DROP DATABASE as fallback)

A multi-ref script addresses another branch through its current
database (`<db>__<branch>[__gN]`.`<table>`, resolved by the adapter) and a
commit as `<db>`.`<table>`{snapshot = '<name>'}.

Conflict semantics follow git: in a merge "ours" is the target branch,
in a rebase "ours" is the upstream (the branch being rebased onto) and
"theirs" the branch's own commits.

Known limitations (MatrixOne v4.2.5):

- DATA BRANCH PICK ... BETWEEN SNAPSHOT, the engine's replay of a
  branch's delta and the only candidate for a native rebase, is not
  reliable. In a long S4 run, from roughly 500 s in, about every other
  rebase's PICK carries none of the branch's rows even though a
  time-travel read of the same snapshots shows them all; the branch then
  silently loses that backfill. Not reproduced standalone (spine load,
  delays, flushes, checkpoints, intermediate commits, composite keys,
  migrations in the window, prior ALTERs and sibling branch churn were all
  ruled out). It also refuses a table whose columns differ between the
  sides ("column has different identity"). The rebase therefore copies
  the rows from the snapshot with SQL, is reported as simulated, and is
  unsupported in native-only mode (2026-10-15).
- PICK and MERGE resolve conflicts per row, so "theirs" replaces the
  upstream's whole row, including columns only the upstream changed.
- DATA BRANCH DIFF does not report a change confined to a column added
  since the base, and diffing a clone against an older snapshot of its
  ancestor drops the ancestor's updates and deletes (see
  tarLCASnapshot/baseLCASnapshot in pkg/frontend/data_branch_types.go).
"""

import csv
import io
import json
import os
import re
import time
import random

import aiomysql
import pymysql
from pymysql.constants import CLIENT

from dblib.db_api import DBToolSuite, Ref, native_only
from dblib import mysql_common
import dblib.result_collector as rc
import dblib.util as dbutil

MO_USER = os.environ.get("MO_USER", "root")
MO_PASSWORD = os.environ.get("MO_PASSWORD", "111")
MO_HOST = os.environ.get("MO_HOST") or os.environ.get("DBFORK_SERVER_HOST", "127.0.0.1")
MO_PORT = int(os.environ.get("MO_PORT", "6001"))
MO_DATA_DIR = os.path.expanduser(os.environ.get("MO_DATA_DIR", "~/mo/matrixone/mo-data"))

# Report conflicts for "ours"/"theirs". On older servers this requires two
# native diffs and a Python join per table both sides changed.
MO_COUNT_CONFLICTS = os.environ.get("MO_COUNT_CONFLICTS", "1").lower() not in ("0", "false", "no")
# Set to 0 for a controlled comparison with the legacy multi-statement path.
# Unsupported servers are detected once per connection by a parser error.
MO_MERGE_SUMMARY = os.environ.get("MO_MERGE_SUMMARY", "1").lower() not in ("0", "false", "no")
MO_DATABASE_BRANCH = os.environ.get("MO_DATABASE_BRANCH", "1").lower() not in ("0", "false", "no")
MO_DATABASE_RESTORE = os.environ.get("MO_DATABASE_RESTORE", "1").lower() not in ("0", "false", "no")

# Native-only mode (DBFORK_NATIVE_ONLY=1) uses the server's data-only RESET
# without client schema reconciliation. Older servers fall back to their
# RESTORE DATABASE (which replaces identities and affects later lineage
# diffs). Normal mode can also use the legacy diff-driven row restore.
# Rebase has no native counterpart that meets the contract
# (DATA BRANCH PICK, see the limitations), so it is simulated in every
# mode and unsupported in native-only mode.
MO_NATIVE_RESET = native_only()
# Native-only mode: merge skips the adapter's schema reconciliation (added
# tables/columns replayed on the target before the per-table engine merge).
MO_NATIVE_MERGE_AS_IS = native_only()
# Native-only mode: a diff the engine refuses stays refused (no SQL fallback).
MO_NATIVE_DIFF_ONLY = native_only()

MAIN_BRANCH = "main"
_BRANCH_SEP = "__"
_TEMP_PREFIX = "tmp_"
_GEN_SEP = "__g"
_META_NAME = "_bb_meta"
BRANCHES_TABLE = "branches"
_MAX_DB_NAME_LEN = 64
# Snapshot names are silently truncated to 64 characters.
_MAX_SNAPSHOT_LEN = 64

META_PREFIX = "_bb_"
COMMITS_TABLE = "_bb_commits"

_COMMITS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    seq BIGINT NOT NULL,
    id VARCHAR(40) NOT NULL,
    kind VARCHAR(8) NOT NULL,
    message TEXT,
    snap VARCHAR(64) NOT NULL,
    db VARCHAR(64) NOT NULL,
    prev_snap VARCHAR(64),
    prev_db VARCHAR(64),
    ts DATETIME(6),
    schema_json TEXT,
    PRIMARY KEY (seq),
    KEY _bb_commit_id (id)
)
"""

_BRANCHES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    name VARCHAR(64) NOT NULL,
    db VARCHAR(64) NOT NULL,
    gen INT NOT NULL,
    PRIMARY KEY (name)
)
"""



def connect(db_name: str = None, autocommit: bool = True, **kwargs):
    """Open a PyMySQL connection to the MatrixOne server."""
    return pymysql.connect(
        host=MO_HOST, port=MO_PORT, user=MO_USER, password=MO_PASSWORD,
        database=db_name, autocommit=autocommit, **kwargs,
    )


async def create_pool_async(db_name: str, size: int, autocommit: bool = True):
    return await aiomysql.create_pool(
        minsize=size, maxsize=size, host=MO_HOST, port=MO_PORT, user=MO_USER,
        password=MO_PASSWORD, db=db_name, autocommit=autocommit,
    )


def _quote(ident: str) -> str:
    return "`" + ident.replace("`", "``") + "`"


def _qt(db: str, table: str) -> str:
    return f"{_quote(db)}.{_quote(table)}"


def _ddl_defaults(ddl: str) -> dict:
    """Read SQL default expressions from MO's canonical SHOW CREATE output.

    COLUMN_DEFAULT is a value on newer MO releases and SQL text on older
    ones; quoting it heuristically loses expressions or literal quotes.
    Tokenize quoted strings/identifiers intact and split at table depth.
    """
    tokens = list(re.finditer(r"'(?:[^'\\]|\\.|'')*'|\"(?:[^\"\\]|\\.|\"\")*\"|"
                             r"`(?:[^`]|``)*`|[\w$]+|[^\s]", ddl))
    out, clause, depth = {}, [], 0

    def read_column(parts):
        if not parts or not parts[0].group().startswith("`"):
            return
        name = parts[0].group()[1:-1].replace("``", "`")
        level, start = 0, None
        for token in parts[1:]:
            word = token.group().upper()
            if start is not None and level == 0 and word in {
                "COMMENT", "ON", "AUTO_INCREMENT", "NOT", "PRIMARY", "UNIQUE", "REFERENCES", "CHECK"
            }:
                out[name] = ddl[start:token.start()].strip()
                return
            if word == "DEFAULT" and level == 0:
                start = token.end()
            level += (word == "(") - (word == ")")
        if start is not None:
            out[name] = ddl[start:parts[-1].end()].strip()

    for token in tokens:
        word = token.group()
        if (word == "," and depth == 1) or (word == ")" and depth == 1):
            read_column(clause)
            clause = []
        elif depth >= 1:
            clause.append(token)
        depth += (word == "(") - (word == ")")
    return out


def branch_db_name(db_name: str, branch_name: str) -> str:
    """The database of a branch's first generation."""
    return gen_db_name(db_name, branch_name, 0)


def gen_db_name(db_name: str, branch_name: str, gen: int) -> str:
    """The database that holds generation ``gen`` of a branch (a rebase
    moves a branch to its next generation)."""
    if gen == 0 and branch_name == MAIN_BRANCH:
        return db_name
    name = f"{db_name}{_BRANCH_SEP}{branch_name}"
    if gen:
        name += f"{_GEN_SEP}{int(gen)}"
    if len(name) > _MAX_DB_NAME_LEN:
        raise ValueError(f"MatrixOne database name '{name}' is longer than {_MAX_DB_NAME_LEN}")
    return name


def branch_of_db(db_name: str, db: str) -> str:
    """Inverse of gen_db_name: the branch a (possibly old) database held."""
    if db == db_name:
        return MAIN_BRANCH
    prefix = db_name + _BRANCH_SEP
    name = db[len(prefix):] if db.startswith(prefix) else db
    m = re.match(rf"^(.*){re.escape(_GEN_SEP)}\d+$", name)
    return m.group(1) if m else name


# MatrixOne stores database names in lowercase (SHOW DATABASES and
# information_schema report them so) while accepting any case in
# statements, so catalog lookups compare case-insensitively.
def meta_db_name(db_name: str) -> str:
    return f"{db_name}{_BRANCH_SEP}{_META_NAME}"


def snapshot_prefix(db_name: str) -> str:
    """Every snapshot of a run starts with this (drop_database uses it)."""
    return f"bb_{db_name[:24]}_"


def _branch_databases(cur, db_name: str) -> list:
    prefix = db_name + _BRANCH_SEP
    cur.execute("SHOW DATABASES;")
    return [name for (name,) in cur.fetchall() if name.startswith(prefix)]


def load_sql_dump(db_name: str, sql_path: str) -> None:
    conn = connect(db_name, client_flag=CLIENT.MULTI_STATEMENTS)
    try:
        mysql_common.load_sql_dump(conn, sql_path)
    finally:
        conn.close()


def setup_database(db_name: str, sql_path: str = None) -> None:
    """(Re)create db_name with no branches and load sql_path (if given)."""
    drop_database(db_name)
    conn = connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE DATABASE {_quote(db_name)};")
        print("Database created successfully.")
    finally:
        conn.close()
    if sql_path:
        load_sql_dump(db_name, sql_path)


def drop_database(db_name: str) -> None:
    """Drop db_name, every branch database and every snapshot of the run."""
    conn = connect()
    try:
        with conn.cursor() as cur:
            for branch_db in _branch_databases(cur, db_name):
                cur.execute(f"DROP DATABASE IF EXISTS {_quote(branch_db)};")
            cur.execute(f"DROP DATABASE IF EXISTS {_quote(db_name)};")
            prefix = snapshot_prefix(db_name)
            cur.execute("SHOW SNAPSHOTS;")
            names = [r[0] for r in cur.fetchall() if str(r[0]).startswith(prefix)]
            for name in names:
                try:
                    cur.execute(f"DROP SNAPSHOT {_quote(name)};")
                except Exception:
                    pass
        print(f"Database '{db_name}' deleted successfully ({len(names)} snapshots dropped).")
    finally:
        conn.close()


class _State:
    """A ref resolved to a database and, for a commit, the snapshot name."""

    __slots__ = ("db", "snap", "seq")

    def __init__(self, db: str, snap: str = None, seq: int = None):
        self.db = db
        self.snap = snap
        self.seq = seq

    def expr(self, table: str) -> str:
        e = _qt(self.db, table)
        if self.snap:
            e += f"{{snapshot = '{self.snap}'}}"
        return e


class MatrixOneToolSuite(DBToolSuite):
    BACKEND_NAME = "matrixone"
    STORAGE_SCOPE = "server"
    SUPPORTS_COMMIT_REFS = True
    SCHEMA_DIALECT = "mysql"
    SUPPORTS_MULTI_REF_EXEC = True
    IMPLEMENTATION = {
        "branch": "native",
        "commit": "native",
        "diff": "native",
        "log": "simulated",
        "merge": "native",
        "rebase": "simulated",   # PICK loses rows and refuses schema differences
        "revert": "native" if MO_DATABASE_RESTORE else "simulated",
        "reset": "native" if MO_NATIVE_RESET or MO_DATABASE_RESTORE else "simulated",
        "delete": "native",
        "commit_refs": "native",
        "multi_ref_exec": "native",
    }
    IMPLEMENTATION_NOTES = {
        "branch": "CREATE SNAPSHOT on the parent + DATA BRANCH CREATE DATABASE FROM {snapshot}",
        "commit": "CREATE SNAPSHOT FOR DATABASE; message/order in a _bb_commits row",
        "diff": "DATA BRANCH DIFF DATABASE ... OUTPUT SUMMARY (native LCA; table fallback)",
        "log": "SELECT from the _bb_commits bookkeeping table",
        "merge": "DATA BRANCH MERGE DATABASE ... WHEN CONFLICT SKIP|ACCEPT OUTPUT SUMMARY "
                 "returns pre-merge logical change/conflict counts in the same request; the optimized "
                 "server collects them in one diff traversal per table. Older servers fall back to "
                 "table-level MERGE ... OUTPUT SUMMARY, then legacy separate DIFF summaries and "
                 "two diffs against the fork snapshot for conflict counts. Custom resolvers use "
                 "the legacy row path and write over the SKIP result. Added tables/columns are "
                 "reconciled first except in native-only mode. Database data apply is atomic; "
                 "schema and commit-log updates are separate. Actual paths and fallback reasons "
                 "are recorded per operation",
        "rebase": ("re-fork: the branch's next generation is cloned from the upstream's head "
                   "snapshot and snapshotted (the new base); the branch's delta since its previous "
                   "base (a native diff of its own two snapshots) is copied in with SQL from "
                   "its head snapshot's time-travel view (DATA BRANCH PICK loses rows and refuses "
                   "schema differences); conflicts from two native diffs; the registry is "
                   "repointed and the old generation dropped"),
        "revert": "DATA BRANCH REVERT DATABASE using after/before snapshots; "
                  "legacy row replay for older servers or schema-changing commits",
        "reset": "DATA BRANCH RESET DATABASE restores rows in place and preserves lineage; "
                 "normal mode reconciles schema and retains a legacy row restore; "
                 "native-only mode falls back to RESTORE DATABASE on older servers",
        "delete": "DATA BRANCH DELETE DATABASE (DROP DATABASE fallback)",
        "commit_refs": "time-travel reads t{snapshot = ...}",
        "multi_ref_exec": "cross-database queries",
    }

    @classmethod
    def get_default_connection_uri(cls) -> str:
        return cls.get_initial_connection_uri("")

    @classmethod
    def get_initial_connection_uri(cls, db_name: str) -> str:
        return f"mysql://{MO_USER}:{MO_PASSWORD}@{MO_HOST}:{MO_PORT}/{db_name}"

    @classmethod
    def init_for_bench(cls, collector: rc.ResultCollector, db_name: str,
                       default_branch_name: str = MAIN_BRANCH,
                       measure_storage: bool = False):
        # No default database: main's own database is replaced by its next
        # generation when main is rebased, so a connection must not depend
        # on it; every statement names its database or follows a USE.
        return cls(connect(None), collector, db_name, default_branch_name,
                   measure_storage)

    def __init__(self, connection, collector: rc.ResultCollector, db_name: str,
                 default_branch_name: str = MAIN_BRANCH, measure_storage: bool = False):
        super().__init__(connection, collector, measure_storage)
        self.db_name = db_name
        self.default_branch = default_branch_name
        self._current_db = None
        self._temp_dbs: dict = {}
        self._warned = set()
        self._merge_summary_supported = None if MO_MERGE_SUMMARY else False
        self._database_branch_supported = {
            "diff": None if MO_DATABASE_BRANCH else False,
            "merge": None if MO_DATABASE_BRANCH and MO_MERGE_SUMMARY else False,
        }
        self._database_restore_supported = {
            "reset": None if MO_DATABASE_RESTORE else False,
            "revert": None if MO_DATABASE_RESTORE else False,
        }
        self._meta_db = meta_db_name(db_name)
        self._ensure_registry()
        self._ensure_meta(self._db_for(default_branch_name))
        self._connect_impl(Ref(default_branch_name))
        self._current_ref = Ref(default_branch_name)

    # ------------------------------------------------------------------
    # Names and connection
    # ------------------------------------------------------------------

    # MatrixOne reports a catalog deadlock when several workers run DDL at
    # once; the statements below are safe to retry.
    _ER_DEADLOCK = 20701
    _DDL_RETRIES = 5

    def _execute_ddl(self, sql: str) -> None:
        for attempt in range(self._DDL_RETRIES):
            try:
                self._execute(sql)
                return
            except pymysql.MySQLError as e:
                if e.args and e.args[0] == self._ER_DEADLOCK and attempt < self._DDL_RETRIES - 1:
                    time.sleep(0.2 * (attempt + 1))
                    continue
                raise

    def _ensure_registry(self) -> None:
        """The branch -> database registry; main is its first row."""
        self._execute_ddl(f"CREATE DATABASE IF NOT EXISTS {_quote(self._meta_db)};")
        self._execute_ddl(_BRANCHES_DDL.format(table=_qt(self._meta_db, BRANCHES_TABLE)))
        self._execute(f"INSERT IGNORE INTO {_qt(self._meta_db, BRANCHES_TABLE)} (name, db, gen) "
                      "VALUES (%s, %s, 0)", (MAIN_BRANCH, self.db_name))

    def _rebase_attempt_store(self):
        from .matrixone_rebase import RebaseAttemptStore
        return RebaseAttemptStore(lambda: connect(None), self._meta_db)

    def _prepare_native_rebase_attempt(self, ref: Ref, onto: Ref, on_conflict="fail"):
        """Prepare durable intent only; no snapshot or publication is sent."""
        if ref.commit or onto.commit:
            raise self._unsupported("rebase", "native publication requires branch heads")
        source, upstream = self._registry_row(ref.branch), self._registry_row(onto.branch)
        if source is None or upstream is None or source[0] == upstream[0]:
            raise ValueError("native Rebase requires two registered branch heads")
        bases = self._base_rows(source[0], upstream[0])
        relation = self._relation(source[0], upstream[0], bases)
        if relation is None:
            raise self._unsupported("rebase", "native publication requires a recorded common base")
        row = relation[2]
        child_base = (row["db"], row["snap"])
        parent_base = (row["prev_db"], row["prev_snap"])
        feature_base, upstream_base = ((child_base, parent_base) if relation[0] == source[0]
                                       else (parent_base, child_base))
        return self._rebase_attempt_store().prepare(
            ref.branch, onto.branch, source_registration=source, upstream_registration=upstream,
            feature_base=feature_base, upstream_base=upstream_base, base_sequence=row["seq"],
            on_conflict=on_conflict)

    def _request_native_rebase_snapshots(self, attempt_id: str):
        """Disabled-path preparation only; never publishes or retries an attempt."""
        return self._rebase_attempt_store().request_snapshots(attempt_id)

    def _publish_native_rebase_attempt(self, attempt_id: str):
        """Explicit candidate path only; the ordinary benchmark path stays off."""
        result = self._rebase_attempt_store().publish(attempt_id)
        self._current_db = None
        return result

    def list_rebase_attempts(self, branch: str, after=None, limit=100):
        """MO-only bounded inspection; does not create or retry publications."""
        return self._rebase_attempt_store().list_attempts(branch, after, limit)

    def recover_rebase(self, attempt_id: str):
        """Confirm an original attempt under locks; UNKNOWN retains resources."""
        return self._rebase_attempt_store().recover(attempt_id)

    def _registry_row(self, branch: str):
        rows = self._execute(f"SELECT db, gen FROM {_qt(self._meta_db, BRANCHES_TABLE)} "
                             "WHERE name = %s", (branch,))
        return (rows[0][0], int(rows[0][1])) if rows else None

    def _register(self, branch: str, db: str, gen: int) -> None:
        self._execute(f"INSERT INTO {_qt(self._meta_db, BRANCHES_TABLE)} (name, db, gen) "
                      "VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE db = VALUES(db), gen = VALUES(gen)",
                      (branch, db, int(gen)))

    def _unregister(self, branch: str) -> None:
        self._execute(f"DELETE FROM {_qt(self._meta_db, BRANCHES_TABLE)} WHERE name = %s", (branch,))

    def _db_for(self, branch: str) -> str:
        row = self._registry_row(branch)
        if row is None:
            raise ValueError(f"unknown branch '{branch}' on {self.db_name}")
        return row[0]

    def _use(self, db: str) -> None:
        self._execute(f"USE {_quote(db)};")
        self._current_db = db

    def _on(self, ref: Ref) -> str:
        db = self._db_for(ref.branch)
        if self._current_db != db:
            self._use(db)
        self._current_ref = Ref(ref.branch)
        return db

    def _warn(self, key, msg):
        if key not in self._warned:
            self._warned.add(key)
            print(f"Warning: {msg}")

    def _new_snapshot_name(self) -> str:
        tag = f"{time.time_ns():x}{random.randrange(256):02x}"
        return (snapshot_prefix(self.db_name) + tag)[:_MAX_SNAPSHOT_LEN]

    def _snapshot(self, db: str, name: str = None) -> str:
        name = name or self._new_snapshot_name()
        self._execute(f"CREATE SNAPSHOT {_quote(name)} FOR DATABASE {_quote(db)};")
        return name

    def _get_table_columns(self, table_name: str) -> list:
        rows = self._execute(mysql_common.TABLE_COLUMNS_QUERY, (table_name,))
        rows = [r for r in rows or [] if not str(r[0]).startswith("__mo_")]
        return mysql_common.normalize_column_types(rows)

    def list_branches(self) -> list:
        rows = self._execute(f"SELECT name FROM {_qt(self._meta_db, BRANCHES_TABLE)} "
                             "ORDER BY name") or []
        names = [r[0] for r in rows if r[0] != MAIN_BRANCH]
        return [MAIN_BRANCH] + names

    # ------------------------------------------------------------------
    # Commit log
    # ------------------------------------------------------------------

    def _ensure_meta(self, db: str) -> None:
        self._execute(_COMMITS_DDL.format(table=_qt(db, COMMITS_TABLE)))
        self._ensure_commit_index(db)
        n = self._execute(f"SELECT COUNT(*) FROM {_qt(db, COMMITS_TABLE)};")[0][0]
        if not n:
            self._add_commit(db, "root", "initial state", None, db, None, None,
                             take_snapshot=True)

    def _ensure_commit_index(self, db: str) -> None:
        """Upgrade existing logs too; multiple workers may initialize together.

        The index is deliberately non-unique: a merge can record the source
        commit's id after copying that same commit into the destination log.
        """
        def exists():
            return bool(self._execute(
                "SELECT 1 FROM information_schema.statistics "
                "WHERE LOWER(table_schema) = LOWER(%s) AND table_name = %s "
                "AND index_name = '_bb_commit_id' LIMIT 1", (db, COMMITS_TABLE)))

        if not exists():
            try:
                self._execute_ddl(f"CREATE INDEX _bb_commit_id ON {_qt(db, COMMITS_TABLE)} (id)")
            except pymysql.MySQLError:
                if not exists():
                    raise

    def _schema_at(self, ref: Ref) -> dict:
        """A commit's schema is the fingerprint recorded with its log row;
        a branch head is read from the catalog."""
        if ref.commit:
            row = self._find_commit(self._db_for(ref.branch), ref.commit)
            if row.get("schema_json"):
                fp = json.loads(row["schema_json"])
                return {t: dict(spec["cols"]) for t, spec in fp.items()}
        return super()._schema_at(ref)

    def _fingerprint(self, db: str) -> dict:
        """{table: {"cols": {column: type}, "pk": [columns]}} of db's data
        tables, one catalog query; recorded with every log row so that a
        reset can put the schema back (types, dropped tables included)."""
        rows = self._execute(
            "SELECT c.table_name, c.column_name, c.column_type, c.column_key "
            "FROM information_schema.columns c JOIN information_schema.tables t "
            "ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
            "WHERE LOWER(c.table_schema) = LOWER(%s) AND t.table_type = 'BASE TABLE' AND c.table_name NOT LIKE %s "
            "ORDER BY c.table_name, c.ordinal_position",
            (db, META_PREFIX.replace("_", r"\_") + "%")) or []
        from .matrixone_rebase import schema_fingerprint_rows
        return schema_fingerprint_rows(rows)

    def _add_commit(self, db: str, kind: str, message: str, snap, snap_db: str,
                    prev_snap, prev_db, commit_id: str = None, take_snapshot: bool = False,
                    seq: int = None, schema_json: str = None) -> str:
        """Append a log row. With ``take_snapshot`` the snapshot named in
        the row is created after the row, so it contains its own entry.
        ``schema_json`` is the schema fingerprint of the state the row
        records (captured from snap_db when not given)."""
        seq = seq or time.time_ns()
        commit_id = commit_id or f"{seq:x}"
        snap = snap or self._new_snapshot_name()
        if schema_json is None:
            try:
                schema_json = json.dumps(self._fingerprint(snap_db))
            except pymysql.MySQLError as e:
                self._warn(("fingerprint", snap_db), f"schema fingerprint of {snap_db} failed: {e}")
        for _ in range(8):
            try:
                self._execute(
                    f"INSERT INTO {_qt(db, COMMITS_TABLE)} "
                    "(seq, id, kind, message, snap, db, prev_snap, prev_db, ts, schema_json) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW(6), %s)",
                    (seq, commit_id, kind, message or "", snap, snap_db, prev_snap, prev_db, schema_json))
                break
            except pymysql.err.IntegrityError:
                seq += 1
        else:
            raise RuntimeError(f"could not record commit on {db}")
        if take_snapshot:
            self._snapshot(snap_db, snap)
        return commit_id

    _LOG_COLS = ("seq", "id", "kind", "message", "snap", "db", "prev_snap", "prev_db", "ts", "schema_json")

    def _log_rows(self, db: str, limit: int = None, max_seq: int = None) -> list:
        sql = f"SELECT {', '.join(self._LOG_COLS)} FROM {_qt(db, COMMITS_TABLE)}"
        args = []
        if max_seq is not None:
            sql += " WHERE seq <= %s"
            args.append(int(max_seq))
        sql += " ORDER BY seq DESC"
        if limit:
            sql += f" LIMIT {int(limit)}"
        rows = self._execute(sql, tuple(args) if args else None) or []
        return [dict(zip(self._LOG_COLS, r)) for r in rows]

    def _find_commit(self, db: str, commit_id: str) -> dict:
        rows = self._execute(
            f"SELECT {', '.join(self._LOG_COLS)} FROM {_qt(db, COMMITS_TABLE)} "
            "WHERE id = %s ORDER BY seq DESC LIMIT 1", (str(commit_id),))
        if not rows:
            raise ValueError(f"unknown commit '{commit_id}' on {db}")
        return dict(zip(self._LOG_COLS, rows[0]))

    def _head(self, db: str) -> dict:
        return self._log_rows(db, limit=1)[0]

    def _base_row(self, db: str) -> dict:
        """The branch's latest fork/rebase/root row: ``snap`` is the branch's
        own snapshot of that state, ``prev_snap``/``prev_db`` the parent's
        (upstream's) snapshot it was cloned from."""
        return self._base_rows(db)[0]

    def _base_rows(self, *dbs: str) -> list[dict]:
        """Read each endpoint's latest fork point in one statement, in input order.

        Each LIMIT applies to its own log. Input tags preserve order even when
        UNION execution interleaves rows, or two inputs name the same database.
        Results belong to this operation; a later merge/rebase reads them again.
        """
        if not dbs:
            return []
        columns = ", ".join(self._LOG_COLS)
        sql = " UNION ALL ".join(
            f"SELECT {index} AS endpoint, {columns} FROM "
            f"(SELECT {columns} FROM {_qt(db, COMMITS_TABLE)} "
            "WHERE kind IN ('fork', 'rebase', 'root') ORDER BY seq DESC LIMIT 1) AS base_row"
            for index, db in enumerate(dbs))
        out = [None] * len(dbs)
        for index, *values in self._execute(sql) or []:
            out[index] = dict(zip(self._LOG_COLS, values))
        for db, row in zip(dbs, out):
            if row is None:
                raise ValueError(f"{db} has no fork point recorded")
        return out

    def _relation(self, x_db: str, y_db: str, base_rows: list[dict] = None):
        """(child_db, parent_db, base_row) for the most recent fork/rebase
        that made one of the two databases a clone of the other, or None
        when neither descends from the other."""
        x_base, y_base = self._base_rows(x_db, y_db) if base_rows is None else base_rows
        relevant = [x_db, y_db] + [row["prev_db"] for row in (x_base, y_base)
                                   if row["kind"] in ("fork", "rebase")]
        branches = {db: branch_of_db(self.db_name, db) for db in relevant}
        from .matrixone_rebase import native_generation_attempt
        native_names = [db for db in relevant if db.lower() != self.db_name.lower()
                        and native_generation_attempt(db) is not None]
        if native_names:
            branches.update(self._rebase_attempt_store().generation_branches(*native_names))
        cands = []
        for child, parent, row in ((x_db, y_db, x_base), (y_db, x_db, y_base)):
            # prev_db names the parent's generation at fork/rebase time.
            if (row["kind"] in ("fork", "rebase")
                    and branches[row["prev_db"]] == branches[parent]):
                cands.append((child, parent, row))
        if not cands:
            return None
        return max(cands, key=lambda c: int(c[2]["seq"]))

    def _state(self, ref: Ref) -> _State:
        db = self._db_for(ref.branch)
        if not ref.commit:
            return _State(db)
        row = self._find_commit(db, ref.commit)
        return _State(row["db"], row["snap"], int(row["seq"]))

    def _copy_log(self, into_db: str, from_db: str, max_seq: int = None) -> None:
        # Separate equality anti-joins let MO hash the two keys. A single
        # OR predicate can compare every source row with the destination log.
        sql = (f"INSERT INTO {_qt(into_db, COMMITS_TABLE)} "
               f"({', '.join(self._LOG_COLS)}) "
               f"SELECT {', '.join('s.' + c for c in self._LOG_COLS)} "
               f"FROM {_qt(from_db, COMMITS_TABLE)} s WHERE s.kind IN ('commit', 'merge') "
               f"AND NOT EXISTS (SELECT 1 FROM {_qt(into_db, COMMITS_TABLE)} d "
               "WHERE d.seq = s.seq) "
               f"AND NOT EXISTS (SELECT 1 FROM {_qt(into_db, COMMITS_TABLE)} d "
               "WHERE d.id = s.id)")
        if max_seq is not None:
            sql += f" AND s.seq <= {int(max_seq)}"
        self._execute(sql)

    # ------------------------------------------------------------------
    # Table metadata
    # ------------------------------------------------------------------

    def _catalog(self, db: str) -> dict:
        """Read one current database's table/column/PK metadata."""
        return self._catalogs(db)[0]

    def _catalogs(self, *dbs: str) -> list[dict]:
        """Read current catalogs together, in input order, with one query.

        Callers own these per-operation dictionaries. Never retain them across
        operations: another worker can change the schema or rebase a branch.
        PK order comes from the index, which need not match column order.
        Snapshot readability is checked separately by the caller.
        """
        if not dbs:
            return []
        # Route rows by input position, leaving case-insensitive database-name
        # matching to the server. Even aliases of one database get independent
        # dictionaries, and a database with no data tables keeps its empty slot.
        requested = " UNION ALL ".join(
            f"SELECT {i} AS catalog_id, %s AS db_name" for i in range(len(dbs)))
        schema_filter = ",".join("LOWER(%s)" for _ in dbs)
        rows = self._execute(
            "SELECT requested.catalog_id, c.table_name, c.column_name, c.column_type, c.is_nullable, "
            "c.column_default, s.seq_in_index "
            "FROM information_schema.columns c JOIN information_schema.tables t "
            "ON t.table_schema = c.table_schema AND t.table_name = c.table_name "
            "LEFT JOIN information_schema.statistics s ON s.table_schema = c.table_schema "
            "AND s.table_name = c.table_name AND s.column_name = c.column_name "
            "AND s.index_name = 'PRIMARY' "
            f"JOIN ({requested}) requested ON LOWER(c.table_schema) = LOWER(requested.db_name) "
            "WHERE t.table_type = 'BASE TABLE' AND c.table_name NOT LIKE %s "
            # Keep a constant schema filter available to the planner as well
            # as the join that assigns rows to requested catalogs.
            f"AND LOWER(c.table_schema) IN ({schema_filter}) "
            "ORDER BY requested.catalog_id, c.table_name, c.ordinal_position",
            (*dbs, META_PREFIX.replace("_", r"\_") + "%", *dbs)) or []
        out = [{} for _ in dbs]
        for index, table, name, ctype, nullable, default, pk_pos in rows:
            spec = out[index].setdefault(table, {"columns": [], "pk": []})
            if str(name).startswith("__mo_"):
                continue
            spec["columns"].append((name, ctype, nullable, default))
            if pk_pos is not None:
                spec["pk"].append((int(pk_pos), name))
        for catalog in out:
            for spec in catalog.values():
                spec["pk"] = [name for _, name in sorted(spec["pk"])]
        return out

    def _tables(self, db: str) -> list:
        rows = self._execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE LOWER(table_schema) = LOWER(%s) AND table_type = 'BASE TABLE' "
            "AND table_name NOT LIKE %s ORDER BY table_name",
            (db, META_PREFIX.replace("_", r"\_") + "%"))
        return [r[0] for r in rows or []]

    def _columns(self, db: str, table: str) -> list:
        """[(name, column_type, is_nullable, column_default)]"""
        rows = self._execute(
            "SELECT column_name, column_type, is_nullable, column_default "
            "FROM information_schema.columns WHERE LOWER(table_schema) = LOWER(%s) AND table_name = %s "
            "ORDER BY ordinal_position", (db, table))
        return [(r[0], r[1], r[2], r[3]) for r in rows or [] if not str(r[0]).startswith("__mo_")]

    def _pk(self, db: str, table: str) -> list:
        rows = self._execute(f"SHOW KEYS FROM {_qt(db, table)};") or []
        pk = sorted((int(r[3]), r[4]) for r in rows
                    if r[2] == "PRIMARY" and not str(r[4]).startswith("__mo_"))
        return [name for _, name in pk]

    # "table does not exist" (also for a table absent at the snapshot).
    _ER_NO_TABLE = (1064, 1146)  # older MO parse error; newer MySQL-compatible code

    def _readable(self, state: _State, table: str) -> bool:
        """Does the table exist at ``state``? Any other error (unknown
        database or snapshot, a server fault) is raised: silently treating
        it as "absent" would drop the table from a merge or rebase."""
        try:
            self._execute(f"SELECT 1 FROM {state.expr(table)} LIMIT 0")
            return True
        except pymysql.MySQLError as e:
            if e.args and e.args[0] in self._ER_NO_TABLE and "does not exist" in str(e.args[1]):
                return False
            raise

    @staticmethod
    def _join(a: str, b: str, pk: list) -> str:
        return " AND ".join(f"{a}.{_quote(c)} = {b}.{_quote(c)}" for c in pk)

    @staticmethod
    def _same(a: str, b: str, cols: list) -> str:
        if not cols:
            return "TRUE"
        return " AND ".join(f"{a}.{_quote(c)} <=> {b}.{_quote(c)}" for c in cols)

    @staticmethod
    def _cols(alias: str, cols: list) -> str:
        return ", ".join(f"{alias}.{_quote(c)}" for c in cols)

    def _diff_rows(self, target: str, base: str) -> tuple:
        """Rows of ``DATA BRANCH DIFF target AGAINST base`` fetched to the
        client: (column names, [(flag, values)]). The OUTPUT AS form would
        keep them on the server but fails on tables altered since the base
        (MatrixOne emits a hidden ROWID column in the scratch DDL)."""
        with self.conn.cursor() as cur:
            cur.execute(f"DATA BRANCH DIFF {target} AGAINST {base}")
            cols = [d[0] for d in cur.description][2:]
            rows = cur.fetchall()
        return cols, [(r[1], r[2:]) for r in rows]

    def _keyed(self, cols: list, rows: list, pk: list) -> dict:
        """{key tuple: (flag, {col: value})} for diff rows."""
        idx = {c: i for i, c in enumerate(cols)}
        kpos = [idx[c] for c in pk]
        out = {}
        for flag, vals in rows:
            out[tuple(vals[i] for i in kpos)] = (flag, dict(zip(cols, vals)))
        return out

    def _in_keys(self, pk: list, keys: list) -> str:
        """``(k1, k2) IN ((..), (..))`` for a chunk of key tuples."""
        esc = self.conn.escape
        if len(pk) == 1:
            return f"{_quote(pk[0])} IN ({', '.join(esc(k[0]) for k in keys)})"
        tuples = ", ".join("(" + ", ".join(esc(v) for v in k) + ")" for k in keys)
        return f"({', '.join(map(_quote, pk))}) IN ({tuples})"

    _KEY_CHUNK = 500

    def _chunks(self, keys: list):
        for i in range(0, len(keys), self._KEY_CHUNK):
            yield keys[i:i + self._KEY_CHUNK]

    # ------------------------------------------------------------------
    # Hooks
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Seed bulk load (untimed setup only)
    # ------------------------------------------------------------------

    SEED_LOAD_CHUNK_ROWS = 20000

    def seed_bulk_insert(self, db, table: str, columns: list, rows, batch_rows: int = 500) -> int:
        """Load seed rows with ``LOAD DATA INLINE`` instead of multi-row
        INSERTs. MatrixOne's INSERT path costs about 1.4 ms per row even in
        500-row batches (the CH seed took 14 min); its bulk loader takes CSV
        text in the statement. Used by ``macrobench.datagen.ch.insert_rows``
        for the untimed seed only, never for measured operations. NULL is
        backslash-N, strings are quoted with doubled quotes, and ``batch_rows``
        is ignored in favour of SEED_LOAD_CHUNK_ROWS."""
        cols = ", ".join(f"`{c}`" for c in columns)
        stmt = (f"LOAD DATA INLINE FORMAT='csv', DATA=%s INTO TABLE `{table}` "
                f"FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' ({cols})")

        def cell(v):
            if v is None:
                return "\\N"
            if isinstance(v, bool):
                return "1" if v else "0"
            return v

        total = 0
        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL, doublequote=True)
        n = 0
        for row in rows:
            writer.writerow([cell(v) for v in row])
            n += 1
            if n >= self.SEED_LOAD_CHUNK_ROWS:
                db.sql(stmt, (buf.getvalue(),))
                total += n
                buf = io.StringIO()
                writer = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL, doublequote=True)
                n = 0
        if n:
            db.sql(stmt, (buf.getvalue(),))
            total += n
        return total

    def _storage_bytes(self) -> int:
        """Size of the server's data directory (object store plus log
        service): MatrixOne has no per-database directory."""
        return dbutil.get_directory_size_bytes(MO_DATA_DIR, ssh=dbutil.ssh_target("MO_SSH"))

    def _gc_impl(self) -> None:
        # No GC command; a forced checkpoint flushes memtables to objects.
        self._execute("SELECT mo_ctl('dn', 'checkpoint', '');")

    def _connect_impl(self, ref: Ref) -> None:
        if ref.commit:
            self._use(self._temp_db_for(ref))
            return
        self._use(self._db_for(ref.branch))

    def _temp_db_for(self, ref: Ref) -> str:
        key = str(ref)
        db = self._temp_dbs.get(key)
        if db:
            return db
        state = self._state(ref)
        db = branch_db_name(
            self.db_name, f"{_TEMP_PREFIX}{ref.commit[-10:]}_{os.getpid() % 10000}_{len(self._temp_dbs)}")
        self._execute(f"DROP DATABASE IF EXISTS {_quote(db)};")
        self._execute(f"DATA BRANCH CREATE DATABASE {_quote(db)} FROM {_quote(state.db)} "
                      f"{{snapshot = '{state.snap}'}};")
        self._temp_dbs[key] = db
        return db

    def _branch_impl(self, name: str, from_ref: Ref) -> None:
        src = self._db_for(from_ref.branch)
        db = branch_db_name(self.db_name, name)
        if from_ref.commit:
            row = self._find_commit(src, from_ref.commit)
            snap, snap_db, seq = row["snap"], row["db"], int(row["seq"])
        else:
            snap, snap_db, seq = self._snapshot(src), src, None
        self._execute(f"DATA BRANCH CREATE DATABASE {_quote(db)} FROM {_quote(snap_db)} "
                      f"{{snapshot = '{snap}'}};")
        if seq is not None:
            self._execute(f"DELETE FROM {_qt(db, COMMITS_TABLE)} WHERE seq > %s", (seq,))
        # The fork row holds the branch's own snapshot of its starting state
        # (snap, taken after the row so it includes it) and the parent's
        # snapshot it was cloned from (prev_*): the base of later merges.
        self._add_commit(db, "fork", f"fork from {from_ref}", None, db, snap, snap_db,
                         take_snapshot=True)
        self._register(name, db, 0)

    def _commit_impl(self, ref: Ref, message: str) -> str:
        db = self._on(ref)
        head = self._head(db)
        return self._add_commit(db, "commit", message, None, db, head["snap"], head["db"],
                                take_snapshot=True)

    def _log_impl(self, ref: Ref, limit: int) -> list:
        db = self._db_for(ref.branch)
        max_seq = int(self._find_commit(db, ref.commit)["seq"]) if ref.commit else None
        return [
            {"commit_hash": r["id"], "kind": r["kind"], "committer": "matrixone",
             "email": "", "date": r["ts"], "message": r["message"], "snapshot": r["snap"]}
            for r in self._log_rows(db, limit=limit, max_seq=max_seq)
        ]

    # -- diff ------------------------------------------------------------

    def _summary(self, target: str, base: str) -> tuple:
        """DATA BRANCH DIFF target AGAINST base OUTPUT SUMMARY ->
        ((ins, del, upd) on the target side, (ins, del, upd) on the base side)."""
        rows = self._execute(f"DATA BRANCH DIFF {target} AGAINST {base} OUTPUT SUMMARY") or []
        t = {"INSERTED": 0, "DELETED": 0, "UPDATED": 0}
        b = dict(t)
        for metric, x, y in rows:
            t[str(metric).upper()] = int(x or 0)
            b[str(metric).upper()] = int(y or 0)
        return ((t["INSERTED"], t["DELETED"], t["UPDATED"]),
                (b["INSERTED"], b["DELETED"], b["UPDATED"]))

    def _diff_impl(self, ref_a: Ref, ref_b: Ref) -> dict:
        a, b = self._state(ref_a), self._state(ref_b)
        try:
            summary = self._database_summary("diff", b, a)
        except pymysql.MySQLError as exc:
            # DIFF is read-only: retain the existing schema-compatibility
            # fallback when historical column identities cannot be compared.
            if "schema compatibility check" not in str(exc) or MO_NATIVE_DIFF_ONLY:
                raise
            self._record_execution(fallback_reason="database_diff_summary: incompatible schema")
            summary = None
        if summary is not None:
            out = []
            for table, (exists_b, exists_a, counts) in sorted(summary.items()):
                if table.startswith(META_PREFIX):
                    continue
                bi, bd, bu, ai, ad, au, _ = counts
                item = {"table_name": table, "rows_added": bi + ad,
                        "rows_deleted": bd + ai, "rows_modified": bu + au}
                if exists_a and exists_b:
                    item.update(b_side=[bi, bd, bu], a_side=[ai, ad, au])
                out.append(item)
            return {"tables": out,
                    **{key: sum(t[key] for t in out)
                       for key in ("rows_added", "rows_deleted", "rows_modified")}}
        tables_a = {t for t in self._tables(a.db) if self._readable(a, t)}
        tables_b = {t for t in self._tables(b.db) if self._readable(b, t)}
        if not (tables_a or tables_b):
            self._record_execution("no_data_tables")
        out = []
        for table in sorted(tables_a | tables_b):
            if table in tables_a and table not in tables_b:
                self._record_execution("table_diff_counts")
                n = self._execute(f"SELECT COUNT(*) FROM {a.expr(table)}")[0][0]
                out.append({"table_name": table, "rows_added": 0, "rows_deleted": int(n),
                            "rows_modified": 0})
                continue
            if table in tables_b and table not in tables_a:
                self._record_execution("table_diff_counts")
                n = self._execute(f"SELECT COUNT(*) FROM {b.expr(table)}")[0][0]
                out.append({"table_name": table, "rows_added": int(n), "rows_deleted": 0,
                            "rows_modified": 0})
                continue
            try:
                (bi, bd, bu), (ai, ad, au) = self._summary(b.expr(table), a.expr(table))
            except Exception as e:
                # The engine refuses a diff whose column sets are not the
                # same objects (a column re-created by a reset, a retyped
                # column: "schema compatibility check"). Simulate it then:
                # anti-joins over the common columns through time-travel
                # reads, as the single-server adapters do.
                if "schema compatibility check" not in str(e) or MO_NATIVE_DIFF_ONLY:
                    self._record_execution("table_diff_summary")
                    raise
                self._record_execution("sql_diff", fallback_reason="table_diff_summary: incompatible schema")
                added, deleted, modified = self._sql_summary(a, b, table)
                out.append({"table_name": table, "rows_added": added, "rows_deleted": deleted,
                            "rows_modified": modified, "simulated": True})
                continue
            self._record_execution("table_diff_summary")
            out.append({"table_name": table, "rows_added": bi + ad, "rows_deleted": bd + ai,
                        "rows_modified": bu + au,
                        "b_side": [bi, bd, bu], "a_side": [ai, ad, au]})
        return {
            "tables": out,
            "rows_added": sum(t["rows_added"] for t in out),
            "rows_deleted": sum(t["rows_deleted"] for t in out),
            "rows_modified": sum(t["rows_modified"] for t in out),
        }

    def _expr_cols(self, expr: str) -> list:
        """Column names a time-travel or plain table expression returns
        (the snapshot's own columns, not the database's current ones)."""
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {expr} LIMIT 0")
            return [d[0] for d in cur.description if not str(d[0]).startswith("__mo_")]

    def _sql_summary(self, a: "_State", b: "_State", table: str) -> tuple:
        """(added, deleted, modified) rows of ``table`` from a to b by SQL:
        anti-joins on the primary key over the columns both sides share
        (a keyless table is compared by whole row, added/deleted only)."""
        ea, eb = a.expr(table), b.expr(table)
        cols_b = set(self._expr_cols(eb))
        cols = [c for c in self._expr_cols(ea) if c in cols_b]
        pk = [c for c in self._pk(b.db, table) if c in cols] or [c for c in self._pk(a.db, table) if c in cols]
        if not cols:
            return 0, 0, 0
        key = pk or cols
        on = " AND ".join(f"x.{_quote(c)} = y.{_quote(c)}" for c in key)
        first = _quote(key[0])

        def count(sql):
            return int(self._execute(sql)[0][0] or 0)
        added = count(f"SELECT COUNT(*) FROM {eb} x LEFT JOIN {ea} y ON {on} WHERE y.{first} IS NULL")
        deleted = count(f"SELECT COUNT(*) FROM {ea} x LEFT JOIN {eb} y ON {on} WHERE y.{first} IS NULL")
        rest = [c for c in cols if c not in key]
        modified = 0
        if pk and rest:
            differ = " OR ".join(f"NOT (x.{_quote(c)} = y.{_quote(c)} OR (x.{_quote(c)} IS NULL AND y.{_quote(c)} IS NULL))"
                                 for c in rest)
            modified = count(f"SELECT COUNT(*) FROM {ea} x JOIN {eb} y ON {on} WHERE {differ}")
        return added, deleted, modified

    # -- schema reconciliation (merge, rebase) -----------------------------

    def _reconcile_schema(self, ours_db: str, theirs: _State,
                          ours_catalog: dict = None, theirs_catalog: dict = None,
                          readable_tables=None) -> dict:
        """Give ours_db the tables and columns theirs has and it lacks; a
        differing primary key is a schema conflict (raised before any
        change)."""
        if ours_catalog is None and theirs_catalog is None:
            ours_catalog, theirs_catalog = self._catalogs(ours_db, theirs.db)
        else:
            ours_catalog = self._catalog(ours_db) if ours_catalog is None else ours_catalog
            theirs_catalog = self._catalog(theirs.db) if theirs_catalog is None else theirs_catalog
        ours = set(ours_catalog)
        # The current catalog already establishes visibility at the head.
        # Historical refs still need a probe: a current table may not have
        # existed at the requested snapshot.
        theirs_tables = (readable_tables if readable_tables is not None else
                         {t for t in theirs_catalog if not theirs.snap or self._readable(theirs, t)})
        new_tables = sorted(set(theirs_tables) - ours)
        plan_cols = []
        for table in sorted(ours & set(theirs_tables)):
            if ours_catalog[table]["pk"] != theirs_catalog[table]["pk"]:
                raise RuntimeError(f"schema conflict on {table}: primary keys differ "
                                   f"between {ours_db} and {theirs.db}")
            have = {c[0]: c[1] for c in ours_catalog[table]["columns"]}
            for name, ctype, nullable, default in theirs_catalog[table]["columns"]:
                if name not in have:
                    plan_cols.append((table, name, ctype, default))
                elif str(ctype).lower() != str(have[name]).lower():
                    raise RuntimeError(f"schema conflict on {table}.{name}: types differ "
                                       f"(ours {have[name]}, theirs {ctype})")
        # Fetch canonical SQL only for tables whose new columns have
        # defaults; ordinary merges keep their constant metadata cost.
        defaults = {}
        for table in sorted({t for t, _, _, d in plan_cols if d is not None}):
            ddl = self._execute(f"SHOW CREATE TABLE {_qt(theirs.db, table)}")[0][1]
            defaults[table] = _ddl_defaults(ddl)
        for table, name, _, default in plan_cols:
            if default is not None and name not in defaults[table]:
                raise RuntimeError(f"Cannot read default expression for {table}.{name}")
        for table in new_tables:
            self._execute(f"DATA BRANCH CREATE TABLE {_qt(ours_db, table)} FROM {theirs.expr(table)};")
        for table, name, ctype, default in plan_cols:
            ddl = f"ALTER TABLE {_qt(ours_db, table)} ADD COLUMN {_quote(name)} {ctype} NULL"
            if default is not None:
                ddl += f" DEFAULT {defaults[table][name]}"
            self._execute(ddl)
            ours_catalog[table]["columns"].append((name, ctype, "YES", default))
        return {"added_tables": new_tables,
                "added_columns": [f"{t}.{c}" for t, c, _, _ in plan_cols]}

    # -- deltas and conflicts ---------------------------------------------
    #
    # MatrixOne's diff is reliable for two snapshots of one table, for a
    # clone (or chain of clones) against its ancestor's head, and for a
    # clone against the exact snapshot it was cloned from. It drops the
    # ancestor's updates/deletes when a clone is compared with an *older*
    # snapshot of the ancestor, so every delta below is one of the
    # reliable shapes.

    def _delta(self, head: str, base: str, pk: list) -> dict:
        cols, rows = self._diff_rows(head, base)
        return self._keyed(cols, rows, pk)

    def _conflict_keys(self, a: dict, b: dict, rest: list) -> list:
        """Keys both deltas changed differently (same change = no conflict)."""
        out = []
        for key in a.keys() & b.keys():
            af, av = a[key]
            bf, bv = b[key]
            if af == "DELETE" and bf == "DELETE":
                continue
            if af != "DELETE" and bf != "DELETE" and all(av.get(c) == bv.get(c) for c in rest):
                continue
            out.append(key)
        return out

    def _conflict_dicts(self, keys: list, ours: dict, theirs: dict, base_expr: str,
                        pk: list, cols: list) -> list:
        """Rows in Dolt's dolt_conflicts_<table> shape: base_*, our_*,
        their_* columns plus our_diff_type / their_diff_type."""
        base_rows = {}
        kpos = [cols.index(c) for c in pk]
        for chunk in self._chunks(keys):
            got = self._execute(
                f"SELECT {', '.join(map(_quote, cols))} FROM {base_expr} "
                f"WHERE {self._in_keys(pk, chunk)}") or []
            for r in got:
                base_rows[tuple(r[i] for i in kpos)] = dict(zip(cols, r))
        out = []
        for key in keys:
            tf, tv = theirs[key]
            of, ov = ours[key]
            b = base_rows.get(key)
            d = {}
            for c in cols:
                d[f"base_{c}"] = b.get(c) if b else None
                d[f"our_{c}"] = None if of == "DELETE" else ov.get(c)
                d[f"their_{c}"] = None if tf == "DELETE" else tv.get(c)

            def diff_type(flag):
                if flag == "DELETE":
                    return "removed"
                return "modified" if b else "added"
            d["our_diff_type"] = diff_type(of)
            d["their_diff_type"] = diff_type(tf)
            out.append(d)
        return out

    def _restore_keys(self, dst: str, src_expr: str, delta: dict, keys: list,
                      pk: list, cols: list) -> None:
        """Put ``src_expr``'s version of ``keys`` back into ``dst``: rows the
        source side deleted are deleted, the rest upserted."""
        deleted = [k for k in keys if delta[k][0] == "DELETE"]
        kept = [k for k in keys if delta[k][0] != "DELETE"]
        for chunk in self._chunks(deleted):
            self._execute(f"DELETE FROM {dst} WHERE {self._in_keys(pk, chunk)}")
        for chunk in self._chunks(kept):
            self._execute(self._upsert(
                dst, cols, pk,
                f"SELECT {self._cols('s', cols)} FROM {src_expr} s WHERE {self._in_keys(pk, chunk)}"))

    def _apply_nopk_delta(self, dst: str, cols: list, diff) -> int:
        """Apply a key-less table's diff rows to dst: INSERT rows are
        inserted, DELETE rows delete one matching row each."""
        dcols, rows = diff
        pos = [dcols.index(c) for c in cols]
        n = 0
        with self.conn.cursor() as cur:
            inserts = [tuple(vals[i] for i in pos) for flag, vals in rows if flag == "INSERT"]
            for chunk in self._chunks(inserts):
                cur.executemany(
                    f"INSERT INTO {dst} ({', '.join(map(_quote, cols))}) "
                    f"VALUES ({', '.join(['%s'] * len(cols))})", chunk)
            n += len(inserts)
            for flag, vals in rows:
                if flag != "DELETE":
                    continue
                where = " AND ".join(f"{_quote(c)} <=> %s" for c in cols)
                cur.execute(f"DELETE FROM {dst} WHERE {where} LIMIT 1",
                            tuple(vals[i] for i in pos))
                n += 1
        return n

    def _common_cols(self, db_a: str, db_b: str, table: str) -> list:
        cols_a = [c[0] for c in self._columns(db_a, table)]
        cols_b = {c[0] for c in self._columns(db_b, table)}
        return [c for c in cols_a if c in cols_b]

    def _fk_checks(self, on: bool) -> None:
        try:
            self._execute(f"SET SESSION foreign_key_checks = {1 if on else 0}")
        except Exception:
            pass

    # -- merge --------------------------------------------------------------

    def _native_path_unavailable(self, path: str, *, parser_rejected=False) -> None:
        # Keep the original reason after capability negotiation disables a
        # path, so subsequent operations do not misreport it as configured off.
        reasons = self.__dict__.setdefault("_native_path_reasons", {})
        if parser_rejected:
            reasons[path] = f"{path}: server rejected syntax"
            self._warn(("native_path", path),
                       f"{path} is unsupported by this server; using the fallback path "
                       "for this connection (see execution_paths and fallback_reasons)")
        self._record_execution(fallback_reason=reasons.get(path, f"{path}: disabled"))

    @staticmethod
    def _database_expr(state: _State) -> str:
        expr = _quote(state.db)
        if state.snap:
            expr += "{snapshot=" + pymysql.converters.escape_str(state.snap) + "}"
        return expr

    def _database_summary(self, operation: str, source: _State, destination: _State,
                          tables=None, policy=None):
        """Return all table summaries in one request; never replay a failed write."""
        supported = self._database_branch_supported
        path = f"database_{operation}_summary"
        if supported[operation] is False:
            self._native_path_unavailable(path)
            return None
        if tables is not None and not tables:
            self._record_execution("no_data_tables")
            return {}
        join = "AGAINST" if operation == "diff" else "INTO"
        sql = (f"DATA BRANCH {operation.upper()} DATABASE {self._database_expr(source)} "
               f"{join} {self._database_expr(destination)}")
        if tables is None:
            sql += f" IGNORE TABLES ({_quote(COMMITS_TABLE)})"
        else:
            sql += " TABLES (" + ", ".join(map(_quote, tables)) + ")"
        if policy is not None:
            sql += " WHEN CONFLICT " + policy
        try:
            rows = self._execute(sql + " OUTPUT SUMMARY")
        except Exception as exc:
            if (isinstance(exc, pymysql.MySQLError) and supported[operation] is None
                    and exc.args and exc.args[0] == 1064
                    and re.search(r'syntax error at .*near [\'\"]\s*database\b', str(exc), re.I)):
                supported[operation] = False
                self._native_path_unavailable(path, parser_rejected=True)
                return None
            # Read-only schema rejection is handled by _diff_impl's SQL
            # fallback; do not count that rejected probe as a native result.
            if operation != "diff" or MO_NATIVE_DIFF_ONLY or "schema compatibility check" not in str(exc):
                self._record_execution(path)
            raise
        self._record_execution(path)
        result = {}
        try:
            if rows is None:
                raise ValueError("missing result")
            for row in rows:
                if len(row) != 10:
                    raise ValueError("wrong column count")
                name, src, dst, *counts = row
                counts = tuple(int(n) for n in counts)
                if (not isinstance(name, str) or name in result or src not in (0, 1)
                        or dst not in (0, 1) or not (src or dst) or any(n < 0 for n in counts)):
                    raise ValueError("invalid table summary")
                if operation == "merge" and not (src and dst):
                    raise ValueError("missing merge endpoint")
                result[name] = (bool(src), bool(dst), counts)
            if tables is not None and set(result) != set(tables):
                raise ValueError("incomplete table results")
        except (ValueError, TypeError) as exc:
            raise RuntimeError("Invalid MatrixOne database summary; operation was not retried") from exc
        supported[operation] = True
        return result

    def _merge_with_summary(self, source: str, target: str, policy: str):
        """Merge and return native pre-merge counts, or None if unsupported.

        Only a syntax rejection at OUTPUT can select the legacy path.
        A transport/apply/result error may follow a committed mutation and
        must propagate without replaying the operation.
        """
        if self._merge_summary_supported is False:
            self._native_path_unavailable("table_merge_summary")
            return None
        try:
            result = self._execute(f"DATA BRANCH MERGE {source} INTO {target} "
                                   f"WHEN CONFLICT {policy} OUTPUT SUMMARY")
        except Exception as exc:
            if (isinstance(exc, pymysql.MySQLError) and self._merge_summary_supported is None
                    and exc.args and exc.args[0] == 1064
                    and re.search(r'syntax error at .*near [\'"]\s*output\b', str(exc), re.I)):
                self._merge_summary_supported = False
                self._native_path_unavailable("table_merge_summary", parser_rejected=True)
                return None
            self._record_execution("table_merge_summary")
            raise
        self._record_execution("table_merge_summary")
        if not result or len(result) != 1 or len(result[0]) != 7:
            raise RuntimeError("Invalid MatrixOne MERGE OUTPUT SUMMARY result; merge was not retried")
        counts = tuple(int(n) for n in result[0])
        if any(n < 0 for n in counts):
            raise RuntimeError("Negative MatrixOne merge counts; merge was not retried")
        self._merge_summary_supported = True
        return counts[:3], counts[3:6], counts[6]

    def _merge_impl(self, into: Ref, source: Ref, message: str, on_conflict="ours") -> dict:
        """Database MERGE with a table-level fallback. One side is a clone of the
        other here (the LCA MatrixOne finds is the base recorded in the
        clone's fork/rebase row), so its three-way result matches ours;
        counts come from MERGE OUTPUT SUMMARY where supported. A callable
        or older server uses both sides' deltas against that base."""
        ours_db = self._on(into)
        theirs = self._state(source)
        if theirs.db == ours_db:
            raise ValueError(f"Cannot merge branch '{source.branch}' into itself")
        rel = self._relation(ours_db, theirs.db)
        info = {"conflicts": 0, "conflict_tables": [], "merged_tables": [],
                "ours_changes": 0, "theirs_changes": 0, "two_way": rel is None,
                "merge_summary": "none"}
        summary_modes = set()
        if rel is None:
            self._warn(("two_way", ours_db, theirs.db),
                       f"{theirs.db} and {ours_db} share no fork point: merging two-way")
        want_rows = callable(on_conflict)
        policy = "SKIP" if (want_rows or on_conflict == "ours") else "ACCEPT"
        ours_catalog, theirs_catalog = self._catalogs(ours_db, theirs.db)
        readable_tables = {t for t in theirs_catalog if not theirs.snap or self._readable(theirs, t)}
        if MO_NATIVE_MERGE_AS_IS:
            # Native-only mode: the engine's merge as it is. A column the
            # target lacks is left out of the result, a table only the
            # source has is not merged, and a retyped column makes the
            # engine refuse (a failed merge, caught by the checks).
            schema = {"added_tables": [], "added_columns": []}
        else:
            schema = self._reconcile_schema(ours_db, theirs, ours_catalog, theirs_catalog,
                                            readable_tables=readable_tables)
        conflicts = []
        self._fk_checks(False)
        try:
            # Added tables were already cloned, including their snapshot data.
            tables = sorted((ours_catalog.keys() & readable_tables) - set(schema["added_tables"]))
            if not tables:
                self._record_execution("no_data_tables")
            if want_rows and tables:
                self._record_execution(fallback_reason="merge: custom resolver requires conflict rows")
            database_summary = None if want_rows else self._database_summary(
                "merge", theirs, _State(ours_db), tables=tables, policy=policy)
            for table in tables:
                # Native summaries keep conflict rows on the server. A
                # custom resolver still needs the legacy row-level path.
                if database_summary is not None:
                    counts = database_summary[table][2]
                    summary = counts[:3], counts[3:6], counts[6]
                else:
                    summary = None if want_rows else self._merge_with_summary(
                        theirs.expr(table), _qt(ours_db, table), policy)
                if summary is not None:
                    summary_modes.add("native")
                    source_counts, target_counts, n_conf = summary
                    info["ours_changes"] += sum(target_counts)
                    info["theirs_changes"] += sum(source_counts)
                    if sum(source_counts):
                        info["merged_tables"].append(table)
                    if n_conf and rel is not None and MO_COUNT_CONFLICTS:
                        info["conflicts"] += n_conf
                        info["conflict_tables"].append(table)
                    continue
                summary_modes.add("legacy")
                self._record_execution("legacy_table_merge")
                (ti, td, tu), (oi, od, ou) = self._summary(theirs.expr(table), _qt(ours_db, table))
                info["ours_changes"] += oi + od + ou
                info["theirs_changes"] += ti + td + tu
                if not (ti + td + tu):
                    continue
                n_conf, rows = 0, []
                pk = ours_catalog[table]["pk"]
                if rel is not None and (oi + od + ou) and pk and (want_rows or MO_COUNT_CONFLICTS):
                    child, parent, row = rel
                    base = _State(row["prev_db"], row["prev_snap"])
                    theirs_cols = {c[0] for c in theirs_catalog[table]["columns"]}
                    cols = [c[0] for c in ours_catalog[table]["columns"] if c[0] in theirs_cols]
                    if self._readable(base, table):
                        d_t = self._delta(theirs.expr(table), base.expr(table), pk)
                        d_o = self._delta(_qt(ours_db, table), base.expr(table), pk)
                        rest = [c for c in cols if c not in pk]
                        keys = self._conflict_keys(d_o, d_t, rest)
                        n_conf = len(keys)
                        if n_conf and want_rows:
                            rows = self._conflict_dicts(keys, d_o, d_t, base.expr(table), pk, cols)
                self._execute(f"DATA BRANCH MERGE {theirs.expr(table)} INTO {_qt(ours_db, table)} "
                              f"WHEN CONFLICT {policy}")
                info["merged_tables"].append(table)
                if n_conf:
                    info["conflicts"] += n_conf
                    info["conflict_tables"].append(table)
                    if want_rows:
                        conflicts.append({"table": table, "rows": rows})
        finally:
            self._fk_checks(True)
        if conflicts:
            self._record_execution("custom_resolver")
            info["resolved"] = "custom"
            info["resolution"] = on_conflict(self._conflict_session(into), conflicts)
        elif info["conflicts"]:
            info["resolved"] = on_conflict
        head = self._head(ours_db)  # before the source's commits join the log
        self._copy_log(ours_db, theirs.db, max_seq=theirs.seq)
        src_head = self._find_commit(theirs.db, source.commit) if theirs.snap else self._head(theirs.db)
        commit_id = src_head["id"] if src_head["kind"] == "commit" else None
        info["hash"] = self._add_commit(
            ours_db, "merge", message or f"Merge {source.branch} into {into.branch}",
            None, ours_db, head["snap"], head["db"], commit_id=commit_id, take_snapshot=True)
        # Fast-forward: the target has not moved since the base, or (already
        # up to date) the source has nothing beyond it.
        info["fast_forward"] = info["ours_changes"] == 0 or info["theirs_changes"] == 0
        info["schema_changes"] = schema
        info["merge_summary"] = (next(iter(summary_modes)) if len(summary_modes) == 1
                                 else "mixed" if summary_modes else "none")
        return info

    # -- rebase -------------------------------------------------------------

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict="ours") -> dict:
        """Re-fork: the branch's next generation is cloned from the
        upstream's head snapshot and snapshotted at once (that snapshot is
        the new base, so a later rebase's delta window starts before the
        work replayed now), the branch's delta since its previous base
        (a native diff of the branch's own two snapshots) is replayed into
        it table by table, by SQL from the head snapshot's time-travel
        view, or by DATA BRANCH PICK ... BETWEEN SNAPSHOT in native-only
        mode, conflicts with the upstream's
        delta over the same period are resolved per on_conflict ("ours" =
        upstream, "theirs" = the branch, as in git), the registry is
        repointed and the old generation dropped. The lineage now starts
        at the upstream head, so MatrixOne's LCA for a later merge is that
        head."""
        branch_db = self._on(ref)
        onto_db = self._db_for(onto.branch)
        if onto_db == branch_db:
            raise ValueError(f"Cannot rebase branch '{ref.branch}' onto itself")
        # The common base: the branch's fork/rebase of the upstream, or (as
        # when a spine is rebased onto one of its own branches) the
        # upstream's fork of the branch. Either way s_b0 is the branch's
        # own snapshot of that state and up_base the upstream's.
        base_rows = self._base_rows(branch_db, onto_db)
        rel = self._relation(branch_db, onto_db, base_rows)
        base_row = base_rows[0]
        if rel is None:
            self._warn(("rebase_two_way", branch_db),
                       f"{branch_db} and {onto_db} share no fork point: upstream conflicts not detected")
            s_b0 = base_row["snap"]
            up_base = _State(onto_db, None)
        elif rel[0] == branch_db:
            s_b0 = rel[2]["snap"]
            up_base = _State(rel[2]["prev_db"], rel[2]["prev_snap"])
        else:
            s_b0 = rel[2]["prev_snap"]
            up_base = _State(rel[2]["db"], rel[2]["snap"])
        base_seq = int(rel[2]["seq"]) if rel else int(base_row["seq"])
        upstream = self._state(onto) if onto.commit else _State(onto_db, self._snapshot(onto_db))
        info = {"conflicts": 0, "conflict_tables": [], "merged_tables": [],
                "ours_changes": 0, "theirs_changes": 0, "up_to_date": False}
        gen = self._registry_row(ref.branch)[1] + 1
        new_db = gen_db_name(self.db_name, ref.branch, gen)
        self._execute_ddl(f"DROP DATABASE IF EXISTS {_quote(new_db)};")
        self._execute(f"DATA BRANCH CREATE DATABASE {_quote(new_db)} FROM {_quote(upstream.db)} "
                      f"{{snapshot = '{upstream.snap}'}};")
        want_rows = callable(on_conflict)
        conflicts = []
        base_snap = self._new_snapshot_name()
        try:
            # The new base: the branch's own snapshot of the upstream head,
            # taken before anything is replayed into it; the rebase row goes
            # in first so that the snapshot contains it.
            now = time.time_ns()
            self._add_commit(new_db, "rebase", f"rebase onto {onto}", base_snap, new_db,
                             upstream.snap, upstream.db, seq=now, take_snapshot=True)
            # PICK needs the source to have every column of the target, so
            # the branch (dropped after the swap) first gets the upstream's
            # new columns; then the new generation gets the branch's.
            up_schema = self._reconcile_schema(branch_db, _State(new_db))
            s_now = self._snapshot(branch_db)
            branch_now = _State(branch_db, s_now)
            schema = self._reconcile_schema(new_db, branch_now)
            schema["added_tables"] = sorted(set(schema["added_tables"]) - set(up_schema["added_tables"]))
            upstream_moved = bool(up_schema["added_tables"] or up_schema["added_columns"])
            self._fk_checks(False)
            try:
                branch_base = _State(branch_db, s_b0)
                for table in sorted(set(self._tables(new_db)) & set(self._tables(branch_db))):
                    if table in schema["added_tables"] or table in up_schema["added_tables"]:
                        continue
                    if not self._readable(branch_now, table):
                        # The table exists now, so its snapshot must show it.
                        raise RuntimeError(f"{table} is unreadable at {branch_db}'s snapshot {s_now}")
                    if not self._readable(branch_base, table):
                        # Created on both sides since the fork: nothing to replay.
                        self._warn(("rebase_new_both", table),
                                   f"{table} was created on both {branch_db} and {onto_db}; "
                                   "the upstream's version is kept")
                        continue
                    pk = self._pk(new_db, table)
                    cols = self._common_cols(new_db, branch_db, table)
                    rest = [c for c in cols if c not in pk]
                    if not pk:
                        # PICK needs a primary key: replay the delta row by row.
                        n = self._apply_nopk_delta(
                            _qt(new_db, table), cols,
                            self._diff_rows(branch_now.expr(table), branch_base.expr(table)))
                        info["theirs_changes"] += n
                        if n:
                            info["merged_tables"].append(table)
                        continue
                    # The branch's delta since its base and the upstream's over
                    # the same period, for the conflict count. The native diff
                    # does not list a row whose only change is in a column
                    # added since the base, so an empty delta cannot mean
                    # "nothing to replay": every table is considered.
                    d_b = self._delta(branch_now.expr(table), branch_base.expr(table), pk)
                    d_u = {}
                    if up_base.snap and up_base.db == upstream.db and self._readable(up_base, table):
                        d_u = self._delta(upstream.expr(table), up_base.expr(table), pk)
                    info["theirs_changes"] += len(d_b)
                    info["ours_changes"] += len(d_u)
                    if d_u:
                        upstream_moved = True
                    new_cols = [c.split(".", 1)[1] for c in schema["added_columns"]
                                if c.startswith(table + ".")]
                    keys = self._replayed_keys(d_b, branch_now, table, pk, new_cols)
                    # Replay: the branch's deletes, then its inserted/updated
                    # rows copied from the head snapshot's time-travel view
                    # (server-side; only keys travel).
                    deleted = [k for k, (flag, _) in d_b.items() if flag == "DELETE"]
                    for chunk in self._chunks(deleted):
                        self._execute(f"DELETE FROM {_qt(new_db, table)} WHERE {self._in_keys(pk, chunk)}")
                    for chunk in self._chunks(keys):
                        self._execute(self._upsert(
                            _qt(new_db, table), cols, pk,
                            f"SELECT {self._cols('s', cols)} FROM {branch_now.expr(table)} s "
                            f"WHERE {self._in_keys(pk, chunk)}"))
                    if keys or any(flag == "DELETE" for flag, _ in d_b.values()):
                        info["merged_tables"].append(table)
                    if not d_u:
                        continue
                    keys = self._conflict_keys(d_u, d_b, rest)
                    if not keys:
                        continue
                    info["conflicts"] += len(keys)
                    info["conflict_tables"].append(table)
                    if on_conflict == "theirs":
                        continue  # the branch's version, already applied
                    # "ours": the upstream's version wins; a callable starts from it.
                    self._restore_keys(_qt(new_db, table), upstream.expr(table), d_u, keys, pk, cols)
                    if want_rows:
                        conflicts.append({"table": table, "rows": self._conflict_dicts(
                            keys, d_u, d_b, up_base.expr(table), pk, cols)})
            finally:
                self._fk_checks(True)
            if not upstream_moved and not self._upstream_moved(base_seq, upstream):
                info["up_to_date"] = True
                self._execute_ddl(f"DROP DATABASE {_quote(new_db)};")
                self._drop_snapshot(base_snap)
                return info
            if conflicts:
                info["resolved"] = "custom"
                self._use(new_db)
                info["resolution"] = on_conflict(self._conflict_session(ref), conflicts)
                self._use(branch_db)
            elif info["conflicts"]:
                info["resolved"] = on_conflict
            # Log: upstream history (cloned), the rebase marker (the new
            # base), then the branch's own commits replayed on top. Their
            # snapshots were taken on the old generation, which stays
            # readable by name after it is dropped.
            own = [r for r in reversed(self._log_rows(branch_db))
                   if r["kind"] in ("commit", "merge") and int(r["seq"]) > base_seq]
            for i, r in enumerate(own):
                self._add_commit(new_db, r["kind"], r["message"], r["snap"], r["db"],
                                 r["prev_snap"], r["prev_db"], commit_id=r["id"], seq=now + 1 + i,
                                 schema_json=r.get("schema_json"))
        except BaseException:
            self._execute_ddl(f"DROP DATABASE IF EXISTS {_quote(new_db)};")
            self._drop_snapshot(base_snap)
            raise
        # Swap: the branch name now points at the new generation; the old
        # one goes once every worker can resolve the new.
        self._register(ref.branch, new_db, gen)
        self._current_db = None
        self._use(new_db)
        self._drop_branch_db(branch_db)
        info["schema_changes"] = schema
        info["generation"] = gen
        return info

    def _drop_snapshot(self, name: str) -> None:
        try:
            self._execute(f"DROP SNAPSHOT {_quote(name)};")
        except pymysql.MySQLError:
            pass

    def _replayed_keys(self, delta: dict, src: _State, table: str, pk: list, new_cols: list) -> list:
        """Keys the branch changed since its base: those the native delta
        lists (minus deletions) plus every row with a value in a column
        the branch added, which the native diff does not list."""
        keys = {k for k, (flag, _) in delta.items() if flag != "DELETE"}
        for col in new_cols:
            rows = self._execute(f"SELECT {', '.join(map(_quote, pk))} FROM {src.expr(table)} "
                                 f"WHERE {_quote(col)} IS NOT NULL") or []
            keys.update(tuple(r) for r in rows)
        return list(keys)

    def _upstream_moved(self, base_seq: int, upstream: _State) -> bool:
        """Has the upstream any commit after the common base?"""
        rows = self._execute(
            f"SELECT COUNT(*) FROM {_qt(upstream.db, COMMITS_TABLE)} WHERE seq > %s "
            "AND kind IN ('commit', 'merge')", (base_seq,))
        return bool(rows and int(rows[0][0]) > 0)

    # -- reset / revert ---------------------------------------------------

    def _reset_impl(self, ref: Ref, to: str) -> None:
        db = self._on(ref)
        row = self._find_commit(db, to)
        fp = json.loads(row["schema_json"]) if row.get("schema_json") else None
        restored = False
        if self._database_restore_supported["reset"] is not False:
            if MO_NATIVE_RESET:
                restored = self._database_restore("reset", db, _State(row["db"], row["snap"]))
            elif fp is not None:
                self._fk_checks(False)
                try:
                    retyped = []
                    same_schema = self._fingerprint(db) == fp
                    # Do not alter schema before an unknown server rejects
                    # the syntax: the legacy path needs its original schema
                    # differences to choose the correct restore strategy.
                    if same_schema or self._database_restore_supported["reset"] is True:
                        if not same_schema:
                            retyped = self._reconcile_to_fingerprint(db, fp)[2]
                        restored = self._database_restore(
                            "reset", db, _State(row["db"], row["snap"]), tables=sorted(fp))
                    else:
                        self._record_execution(fallback_reason="database_reset: schema differs and support is unknown")
                    if restored:
                        for table, col, ctype in retyped:
                            self._execute_ddl(f"ALTER TABLE {_qt(db, table)} MODIFY {_quote(col)} {ctype}")
                finally:
                    self._fk_checks(True)
            else:
                self._record_execution(fallback_reason="database_reset: missing schema metadata")
        else:
            self._native_path_unavailable("database_reset")
        if restored:
            sql = f"DELETE FROM {_qt(db, COMMITS_TABLE)} WHERE seq > %s"
            if row["db"] != db:
                sql += " AND kind NOT IN ('fork', 'rebase', 'root')"
            self._execute(sql, (int(row["seq"]),))
            return
        if row["db"] == db and MO_NATIVE_RESET:
            self._record_execution("restore_database")
            self._execute(f"RESTORE DATABASE {_quote(db)} {{snapshot = '{row['snap']}'}};")
            self._current_db = None
            self._use(db)
            # The snapshot was taken right after its own row, so the log is
            # already cut at the commit; make sure of it anyway.
            self._execute(f"DELETE FROM {_qt(db, COMMITS_TABLE)} WHERE seq > %s", (int(row["seq"]),))
            return
        if row["db"] == db:
            self._record_execution("legacy_reset")
            self._undo_since(db, _State(db, row["snap"]), fp)
            self._execute(f"DELETE FROM {_qt(db, COMMITS_TABLE)} WHERE seq > %s", (int(row["seq"]),))
            return
        if MO_NATIVE_RESET:
            # RESTORE DATABASE restores a database to its own snapshot; a
            # commit made on another generation of the branch (before a
            # rebase) has none for this database, and the SQL restore is a
            # simulation.
            raise self._unsupported("reset", f"commit made on {row['db']}, another generation of the branch; "
                                             "RESTORE DATABASE needs the database's own snapshot")
        self._warn(("reset_sql", db), f"reset of {db} to a commit made on {row['db']}: rows restored with SQL")
        self._record_execution("legacy_reset")
        self._restore_sql(db, _State(row["db"], row["snap"]), fp)
        self._execute(
            f"DELETE FROM {_qt(db, COMMITS_TABLE)} WHERE seq > %s "
            "AND kind NOT IN ('fork', 'rebase', 'root')", (int(row["seq"]),))

    def _snapshot_columns(self, state: _State, table: str) -> list:
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {state.expr(table)} LIMIT 0")
            return [d[0] for d in cur.description if not str(d[0]).startswith("__mo_")]

    def _reconcile_to_fingerprint(self, db: str, fp: dict) -> list:
        """Give db exactly the tables and columns of the fingerprint:
        tables not in it dropped, missing ones created (empty), extra
        columns dropped, missing or retyped columns (re)added with the
        recorded type (the row restore that follows refills them).
        Returns (tables (re)created, tables whose columns changed, retyped
        columns as (table, column, type) to MODIFY once rows are back: a
        MODIFY keeps the column's identity, so the lineage diff still
        works and the snapshot's values are restored before the type is
        narrowed back)."""
        now = set(self._tables(db))
        created, altered, retyped = [], [], []
        for table in sorted(now - set(fp)):
            self._execute_ddl(f"DROP TABLE {_qt(db, table)}")
        for table in sorted(set(fp) - now):
            spec = fp[table]
            defs = [f"{_quote(c)} {t}" for c, t in spec["cols"].items()]
            if spec.get("pk"):
                defs.append(f"PRIMARY KEY ({', '.join(map(_quote, spec['pk']))})")
            self._execute_ddl(f"CREATE TABLE {_qt(db, table)} ({', '.join(defs)})")
            created.append(table)
        for table in sorted(now & set(fp)):
            spec = fp[table]
            cols_now = {c: str(t).lower() for c, t, _, _ in self._columns(db, table)}
            pk = set(spec.get("pk") or self._pk(db, table))
            changed = False
            for col in list(cols_now):
                if col not in spec["cols"]:
                    self._execute_ddl(f"ALTER TABLE {_qt(db, table)} DROP COLUMN {_quote(col)}")
                    cols_now.pop(col)
                    changed = True
            for col, ctype in spec["cols"].items():
                if col in cols_now and cols_now[col] != ctype and col not in pk:
                    # MODIFY changed the column's identity, which the lineage
                    # diff refuses: the table's rows are restored by SQL.
                    retyped.append((table, col, ctype))
                    changed = True
                elif col not in cols_now:
                    self._execute_ddl(f"ALTER TABLE {_qt(db, table)} ADD COLUMN {_quote(col)} {ctype}")
                    changed = True
            if changed:
                altered.append(table)
        return created, altered, retyped

    def _undo_since(self, db: str, target: _State, fp: dict = None) -> None:
        """Make db's tables equal to its own earlier snapshot ``target``
        by undoing what a native diff of head vs snapshot lists. The
        schema comes first: from the commit's fingerprint when the log
        row has one (tables and columns dropped, created, retyped), else
        tables and columns created since are dropped."""
        self._fk_checks(False)
        try:
            recreated, altered, retyped = [], [], []
            if fp is not None:
                recreated, altered, retyped = self._reconcile_to_fingerprint(db, fp)
            here = set(self._tables(db))
            there = {t for t in here if self._readable(target, t)}
            for table in sorted(here - there):
                if table in recreated:
                    continue  # its snapshot cannot be read; left empty
                self._execute(f"DROP TABLE {_qt(db, table)}")
            for table in sorted(there):
                cols_now = [c[0] for c in self._columns(db, table)]
                cols_then = self._snapshot_columns(target, table)
                if fp is None:
                    for col in cols_now:
                        if col not in cols_then:
                            self._execute(f"ALTER TABLE {_qt(db, table)} DROP COLUMN {_quote(col)}")
                cols = [c for c in cols_now if c in cols_then]
                if table in recreated:
                    # A table dropped since the commit: fill it from the snapshot.
                    self._execute(f"INSERT INTO {_qt(db, table)} ({', '.join(map(_quote, cols))}) "
                                  f"SELECT {self._cols('s', cols)} FROM {target.expr(table)} s")
                    continue
                if table in altered:
                    # A re-added column has a new identity, which the lineage
                    # diff refuses: restore the table's rows by SQL instead.
                    self._restore_table_sql(db, table, target.expr(table), cols)
                    continue
                pk = [c for c in self._pk(db, table) if c in cols]
                dst = _qt(db, table)
                diff = self._diff_rows(dst, target.expr(table))
                if not diff[1]:
                    continue
                if not pk:
                    # Undo = apply the diff with the flags inverted.
                    inv = (diff[0], [("DELETE" if f == "INSERT" else "INSERT", v) for f, v in diff[1]])
                    self._apply_nopk_delta(dst, cols, inv)
                    continue
                keyed = self._keyed(diff[0], diff[1], pk)
                added = [k for k, (flag, _) in keyed.items() if flag == "INSERT"]
                changed = [k for k, (flag, _) in keyed.items() if flag != "INSERT"]
                for chunk in self._chunks(added):
                    self._execute(f"DELETE FROM {dst} WHERE {self._in_keys(pk, chunk)}")
                for chunk in self._chunks(changed):
                    self._execute(self._upsert(
                        dst, cols, pk,
                        f"SELECT {self._cols('s', cols)} FROM {target.expr(table)} s "
                        f"WHERE {self._in_keys(pk, chunk)}"))
            for table, col, ctype in retyped:
                self._execute_ddl(f"ALTER TABLE {_qt(db, table)} MODIFY {_quote(col)} {ctype}")
        finally:
            self._fk_checks(True)

    def _restore_table_sql(self, db: str, table: str, src: str, cols: list) -> None:
        """Make one table equal to ``src`` (a time-travel expression) on
        ``cols``: rows absent from src deleted, the rest upserted."""
        pk = [c for c in self._pk(db, table) if c in cols]
        dst = _qt(db, table)
        if not pk:
            self._execute(f"DELETE FROM {dst}")
            self._execute(f"INSERT INTO {dst} ({', '.join(map(_quote, cols))}) "
                          f"SELECT {self._cols('s', cols)} FROM {src} s")
            return
        # The source is the same table's time-travel view, which MatrixOne
        # does not allow in a DELETE's subquery (error 1093): the keys to
        # delete are computed here instead.
        keys_now = {tuple(r) for r in self._execute(f"SELECT {', '.join(map(_quote, pk))} FROM {dst}") or []}
        keys_then = {tuple(r) for r in self._execute(f"SELECT {', '.join(map(_quote, pk))} FROM {src}") or []}
        for chunk in self._chunks(sorted(keys_now - keys_then)):
            self._execute(f"DELETE FROM {dst} WHERE {self._in_keys(pk, chunk)}")
        self._execute(self._upsert(dst, cols, pk, f"SELECT {self._cols('s', cols)} FROM {src} s"))

    def _restore_sql(self, db: str, target: _State, fp: dict = None) -> None:
        """Make db's tables equal to target's (common columns; tables
        missing on either side are created from / dropped); with a
        fingerprint the schema is reconciled to it first."""
        retyped = []
        if fp is not None:
            self._fk_checks(False)
            try:
                retyped = self._reconcile_to_fingerprint(db, fp)[2]
            finally:
                self._fk_checks(True)
        here = set(self._tables(db))
        there = {t for t in self._tables(target.db) if self._readable(target, t)}
        try:
            self._execute("SET SESSION foreign_key_checks = 0")
        except Exception:
            pass
        try:
            for table in sorted(here - there):
                self._execute(f"DROP TABLE {_qt(db, table)}")
            for table in sorted(there - here):
                self._execute(f"DATA BRANCH CREATE TABLE {_qt(db, table)} FROM {target.expr(table)};")
            for table in sorted(here & there):
                cols_here = [c[0] for c in self._columns(db, table)]
                cols_there = {c[0] for c in self._columns(target.db, table)}
                cols = [c for c in cols_here if c in cols_there]
                pk = [c for c in self._pk(db, table) if c in cols]
                dst, src = _qt(db, table), target.expr(table)
                if not pk:
                    self._execute(f"DELETE FROM {dst}")
                    self._execute(f"INSERT INTO {dst} ({', '.join(map(_quote, cols))}) "
                                  f"SELECT {self._cols('s', cols)} FROM {src} s")
                    continue
                self._execute(
                    f"DELETE FROM {dst} WHERE NOT EXISTS (SELECT 1 FROM {src} s "
                    f"WHERE {self._join('s', dst, pk)})")
                self._execute(self._upsert(dst, cols, pk, f"SELECT {self._cols('s', cols)} FROM {src} s"))
            for table, col, ctype in retyped:
                self._execute_ddl(f"ALTER TABLE {_qt(db, table)} MODIFY {_quote(col)} {ctype}")
        finally:
            try:
                self._execute("SET SESSION foreign_key_checks = 1")
            except Exception:
                pass

    @staticmethod
    def _upsert(dst: str, cols: list, pk: list, select: str) -> str:
        rest = [c for c in cols if c not in pk]
        head = f"INSERT INTO {dst} ({', '.join(map(_quote, cols))}) {select}"
        if not rest:
            return head.replace("INSERT INTO", "INSERT IGNORE INTO", 1)
        updates = ", ".join(f"{_quote(c)} = VALUES({_quote(c)})" for c in rest)
        return f"{head} ON DUPLICATE KEY UPDATE {updates}"

    def _revert_impl(self, ref: Ref, commit: str) -> None:
        db = self._on(ref)
        row = self._find_commit(db, commit)
        if not row["prev_snap"]:
            raise ValueError(f"commit {commit} has no predecessor to revert to")
        after = _State(row["db"], row["snap"])
        before = _State(row["prev_db"], row["prev_snap"])
        restored = False
        if self._database_restore_supported["revert"] is not False:
            if native_only():
                restored = self._database_restore("revert", db, after, before=before)
            elif row.get("schema_json"):
                after_schema = json.loads(row["schema_json"])
                rows = self._execute(f"SELECT schema_json FROM {before.expr(COMMITS_TABLE)} "
                                     "ORDER BY seq DESC LIMIT 1") or []
                before_schema = json.loads(rows[0][0]) if rows and rows[0][0] else None
                if before_schema == after_schema == self._fingerprint(db):
                    # The legacy adapter only reverts primary-key tables.
                    tables = sorted(t for t, spec in after_schema.items() if spec.get("pk"))
                    self._fk_checks(False)
                    try:
                        restored = self._database_restore("revert", db, after, before=before, tables=tables)
                    finally:
                        self._fk_checks(True)
                else:
                    self._record_execution(fallback_reason="database_revert: schema differs or predecessor metadata is missing")
            else:
                self._record_execution(fallback_reason="database_revert: missing schema metadata")
        else:
            self._native_path_unavailable("database_revert")
        if not restored:
            if native_only():
                raise self._unsupported("revert", "server does not support DATA BRANCH REVERT DATABASE")
            self._record_execution("legacy_revert")
            self._revert_rows(db, after, before)
        head = self._head(db)
        self._add_commit(db, "commit", f"revert {commit}", None, db, head["snap"], head["db"],
                         take_snapshot=True)

    def _database_restore(self, operation: str, db: str, source: _State,
                          before: _State = None, tables=None) -> bool:
        """Invoke an in-place server restore; only parser rejection permits replay."""
        supported = self._database_restore_supported
        path = f"database_{operation}"
        if supported[operation] is False:
            self._native_path_unavailable(path)
            return False
        if tables is not None and not tables:
            self._record_execution("no_data_tables")
            return True
        sql = f"DATA BRANCH {operation.upper()} DATABASE {_quote(db)} FROM {self._database_expr(source)}"
        if before is not None:
            sql += " AGAINST " + self._database_expr(before)
        if tables is None:
            sql += f" IGNORE TABLES ({_quote(COMMITS_TABLE)})"
        else:
            sql += " TABLES (" + ", ".join(map(_quote, tables)) + ")"
        try:
            self._execute(sql)
        except Exception as exc:
            if (isinstance(exc, pymysql.MySQLError) and supported[operation] is None
                    and exc.args and exc.args[0] == 1064
                    and re.search(r'syntax error at .*near [\'\"]\s*(?:reset|revert|database)\b', str(exc), re.I)):
                supported[operation] = False
                self._native_path_unavailable(path, parser_rejected=True)
                return False
            self._record_execution(path)
            raise
        self._record_execution(path)
        supported[operation] = True
        return True

    def _revert_rows(self, db: str, after: _State, before: _State) -> None:
        """Compatibility path for older servers and schema-changing commits."""
        try:
            self._execute("SET SESSION foreign_key_checks = 0")
        except Exception:
            pass
        try:
            for table in self._tables(db):
                if not (self._readable(after, table) and self._readable(before, table)):
                    continue
                cols = [c[0] for c in self._columns(db, table)]
                there = {c[0] for c in self._columns(after.db, table)}
                cols = [c for c in cols if c in there]
                pk = [c for c in self._pk(db, table) if c in cols]
                if not pk:
                    continue
                dcols, drows = self._diff_rows(after.expr(table), before.expr(table))
                if not drows:
                    continue
                keyed = self._keyed(dcols, drows, pk)
                added = [k for k, (flag, _) in keyed.items() if flag == "INSERT"]
                changed = [k for k, (flag, _) in keyed.items() if flag != "INSERT"]
                dst = _qt(db, table)
                # Rows the commit added: delete them.
                for chunk in self._chunks(added):
                    self._execute(f"DELETE FROM {dst} WHERE {self._in_keys(pk, chunk)}")
                # Rows it deleted or modified: put the earlier version back.
                for chunk in self._chunks(changed):
                    self._execute(self._upsert(
                        dst, cols, pk,
                        f"SELECT {self._cols('s', cols)} FROM {before.expr(table)} s "
                        f"WHERE {self._in_keys(pk, chunk)}"))
        finally:
            try:
                self._execute("SET SESSION foreign_key_checks = 1")
            except Exception:
                pass

    # -- delete ------------------------------------------------------------

    def _drop_branch_db(self, db: str) -> None:
        try:
            self._execute_ddl(f"DATA BRANCH DELETE DATABASE {_quote(db)};")
            return
        except pymysql.MySQLError:
            pass
        self._execute_ddl(f"DROP DATABASE IF EXISTS {_quote(db)};")

    def _delete_impl(self, ref: Ref) -> None:
        db = self._db_for(ref.branch)
        if db == self._current_db:
            self._use(self._db_for(self.default_branch))
            self._current_ref = Ref(self.default_branch)
        self._drop_branch_db(db)
        self._unregister(ref.branch)

    def _qualified_table(self, ref: Ref, table: str) -> str:
        if ref.commit:
            return self._state(ref).expr(table)
        return _qt(self._db_for(ref.branch), table)

    def close_connection(self) -> None:
        if self.conn and self._temp_dbs:
            for db in list(self._temp_dbs.values()):
                try:
                    self._drop_branch_db(db)
                except Exception:
                    pass
            self._temp_dbs.clear()
        super().close_connection()

    # ------------------------------------------------------------------
    # Async
    # ------------------------------------------------------------------

    async def open_async_pool(self, size: int) -> None:
        self.async_pool = await create_pool_async(None, size)

    def _pool_connection(self):
        return mysql_common.aiomysql_pool_connection(self.async_pool)

    async def close_async_pool(self) -> None:
        if self.async_pool:
            self.async_pool.close()
            await self.async_pool.wait_closed()
            self.async_pool = None

    async def _connect_impl_async(self, conn, ref: Ref):
        db = self._temp_db_for(ref) if ref.commit else self._db_for(ref.branch)
        if getattr(conn, "_mo_db", None) != db:
            async with conn.cursor() as cur:
                await cur.execute(f"USE {_quote(db)};")
            conn._mo_db = db
        return conn
