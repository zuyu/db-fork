"""Durable intent, snapshots, disabled publication and native MO recovery.

The optional snapshot stage does not publish, retire or delete anything. Normal
benchmark Rebase is unchanged. Publication integration remains disabled until
kernel and real-process acceptance pass; pending intent is not rollback proof.
"""

from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass
import hashlib
import json
import re
import time
import uuid

from pymysql.err import OperationalError


ATTEMPTS_TABLE = "rebase_attempts"
ATTEMPTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    attempt_id VARCHAR(32) NOT NULL PRIMARY KEY,
    branch VARCHAR(64) NOT NULL,
    state VARCHAR(16) NOT NULL,
    intent_json TEXT NOT NULL,
    result_json TEXT
)
"""
REQUESTS_TABLE = "rebase_snapshot_requests"
REQUESTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    attempt_id VARCHAR(32) NOT NULL,
    role VARCHAR(8) NOT NULL,
    state VARCHAR(16) NOT NULL,
    snapshot_name VARCHAR(64) NOT NULL,
    database_id BIGINT UNSIGNED NOT NULL,
    snapshot_uuid VARCHAR(36),
    snapshot_ts BIGINT UNSIGNED,
    PRIMARY KEY (attempt_id, role)
)
"""

NATIVE_REBASE_COLUMNS = (
    "database", "database_id", "base_database", "base_ts", "tables",
    "feature_changed_keys", "upstream_changed_keys", "conflict_keys",
    "applied_insert_rows", "applied_update_rows", "applied_delete_rows",
)


def schema_fingerprint_rows(rows):
    """Decode the shared history fingerprint's catalog projection."""
    out = {}
    for table, column, column_type, key in rows:
        if str(column).startswith("__mo_"):
            continue
        entry = out.setdefault(str(table), {"cols": {}, "pk": []})
        entry["cols"][str(column)] = str(column_type).lower()
        if str(key or "").upper() == "PRI":
            entry["pk"].append(str(column))
    return out


class RebaseAttemptUnknown(RuntimeError):
    """A request or COMMIT may have succeeded; inspect this ID, never reissue it."""

    def __init__(self, attempt_id):
        self.attempt_id = attempt_id
        super().__init__(f"Rebase attempt {attempt_id} has an unknown outcome")


def _identifier(value):
    if not isinstance(value, str) or not value or len(value) > 64 or "\x00" in value:
        raise ValueError("invalid Rebase metadata identifier")
    return "`" + value.replace("`", "``") + "`"


def _attempt_id(value):
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{32}", value):
        raise ValueError("invalid Rebase attempt ID")
    return value


def native_generation_attempt(database):
    """Recognize our allocated namespace, not an arbitrary generation suffix."""
    _identifier(database)
    match = re.fullmatch(r"bb_r2_[0-9a-f]{12}_[1-9][0-9]*_([0-9a-f]{32})",
                         database, re.IGNORECASE)
    return match[1].lower() if match else None


@dataclass(frozen=True)
class RebaseBase:
    database: str
    snapshot: str
    database_id: int
    snapshot_uuid: str
    snapshot_ts: int

    def __post_init__(self):
        _identifier(self.database)
        _identifier(self.snapshot)
        if (type(self.database_id) is not int or not 0 < self.database_id < 2 ** 64
                or type(self.snapshot_ts) is not int or not 0 < self.snapshot_ts < 2 ** 63
                or not isinstance(self.snapshot_uuid, str)
                or str(uuid.UUID(self.snapshot_uuid)) != self.snapshot_uuid):
            raise ValueError("invalid Rebase base snapshot identity")


@dataclass(frozen=True)
class RebaseIntent:
    attempt_id: str
    branch: str
    source_db: str
    source_id: int
    source_epoch: int
    old_generation: int
    candidate_db: str
    new_generation: int
    upstream_db: str
    upstream_id: int
    feature_snapshot: str
    upstream_snapshot: str
    feature_base: RebaseBase
    upstream_base: RebaseBase
    base_sequence: int
    protocol: int = 1
    on_conflict: str = "fail"

    def __post_init__(self):
        _attempt_id(self.attempt_id)
        for field in ("feature_base", "upstream_base"):
            base = getattr(self, field)
            if isinstance(base, dict):
                base = RebaseBase(**base)
                object.__setattr__(self, field, base)
            if not isinstance(base, RebaseBase):
                raise ValueError("missing fixed Rebase base")
        for value in (self.branch, self.source_db, self.candidate_db, self.upstream_db,
                      self.feature_snapshot, self.upstream_snapshot):
            _identifier(value)
        for value in (self.source_id, self.source_epoch, self.upstream_id):
            if type(value) is not int or not 0 < value < 2 ** 64:
                raise ValueError("invalid Rebase identity or epoch")
        if self.source_epoch == 2 ** 64 - 1 or self.source_id == self.upstream_id:
            raise ValueError("exhausted epoch or identical database identity")
        if (type(self.old_generation) is not int or not 0 <= self.old_generation < 2 ** 31 - 1
                or type(self.new_generation) is not int
                or self.new_generation != self.old_generation + 1):
            raise ValueError("invalid Rebase generation")
        if (type(self.protocol) is not int or self.protocol != 1
                or type(self.base_sequence) is not int or not 0 < self.base_sequence < 2 ** 63
                or self.candidate_db in (self.source_db, self.upstream_db)
                or self.source_db == self.upstream_db
                or self.feature_snapshot == self.upstream_snapshot):
            raise ValueError("invalid Rebase intent protocol or candidate")
        if not isinstance(self.on_conflict, str) or self.on_conflict not in ("fail", "ours", "theirs"):
            raise ValueError("unsupported native Rebase conflict policy")


class RebaseAttemptStore:
    """MO-specific metadata owner, using fresh DB-API connections per operation."""

    def __init__(self, connect, meta_db):
        self._connect = connect
        quoted = _identifier(meta_db)
        self._attempts = f"{quoted}.`{ATTEMPTS_TABLE}`"
        self._branches = f"{quoted}.`branches`"
        self._requests = f"{quoted}.`{REQUESTS_TABLE}`"

    def _candidate_database(self, branch, generation, attempt_id):
        prefix = hashlib.sha256((self._attempts + "\x00" + branch).encode()).hexdigest()[:12]
        return f"bb_r2_{prefix}_{generation}_{attempt_id}"

    def generation_branches(self, *databases):
        """Resolve up to four native physical names using committed identities."""
        if len(databases) > 4:
            raise ValueError("native generation lookup accepts at most four databases")
        requested = {}
        for database in databases:
            attempt = native_generation_attempt(database)
            if attempt is not None:
                requested.setdefault(attempt, []).append(database)
        if not requested:
            return {}
        ids = sorted(requested)
        placeholders = ",".join("%s" for _ in ids)
        with closing(self._connect()) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT attempt_id,state,intent_json,result_json FROM {self._attempts} "
                        f"WHERE attempt_id IN ({placeholders}) ORDER BY attempt_id LIMIT %s",
                        (*ids, len(ids) + 1))
            rows = cur.fetchall()
        seen, branches = set(), {}
        for row in rows:
            outcome = self._decode(row)
            attempt = outcome["attempt_id"]
            if attempt not in requested or attempt in seen:
                raise ValueError("ambiguous native generation attempt")
            seen.add(attempt)
            intent = RebaseIntent(**outcome["intent"])
            expected = self._candidate_database(intent.branch, intent.new_generation, attempt)
            if (intent.candidate_db != expected
                    or any(database.lower() != expected for database in requested[attempt])):
                raise ValueError("native generation does not match its original resource")
            if outcome["state"] == "ABORTED":
                raise ValueError("aborted Rebase attempt is not a published generation")
            if outcome["state"] != "COMMITTED":
                raise RebaseAttemptUnknown(attempt)
            result = outcome["result"]
            if (result.get("candidate_db") != expected
                    or type(result.get("generation")) is not int
                    or result["generation"] != intent.new_generation):
                raise ValueError("native generation result contradicts its intent")
            for database in requested[attempt]:
                branches[database] = intent.branch
        for attempt in ids:
            if attempt not in seen:
                raise RebaseAttemptUnknown(attempt)
        return branches

    @contextmanager
    def _transaction(self, attempt_id, initialize=False, rolled_back=None):
        with closing(self._connect()) as conn:
            if not conn.get_autocommit():
                raise ValueError("Rebase metadata requires a fresh autocommit connection")
            with conn.cursor() as cur:
                if initialize:
                    cur.execute(ATTEMPTS_DDL.format(table=self._attempts))
                    cur.execute(REQUESTS_DDL.format(table=self._requests))
                cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
            conn.begin()
            commit_started = False
            try:
                with conn.cursor() as cur:
                    yield cur
                commit_started = True
                try:
                    conn.commit()
                except Exception as exc:
                    raise RebaseAttemptUnknown(attempt_id) from exc
            except BaseException:
                if not commit_started:
                    try:
                        conn.rollback()
                        if rolled_back is not None:
                            rolled_back.append(True)
                    except Exception as exc:
                        raise RebaseAttemptUnknown(attempt_id) from exc
                raise

    @staticmethod
    def _read_base(cur, endpoint):
        database, snapshot = endpoint
        cur.execute("SELECT cast(snapshot_id as char(36)),ts,level,database_name,obj_id "
                    "FROM mo_catalog.mo_snapshots "
                    "WHERE sname=%s AND account_name=current_account_name()", (snapshot,))
        rows = cur.fetchall()
        if len(rows) != 1 or rows[0][2:4] != ("database", database):
            raise ValueError("missing or mismatched Rebase base snapshot")
        snapshot_uuid, ts, _, _, database_id = rows[0]
        return RebaseBase(database, snapshot, database_id, snapshot_uuid, ts)

    def prepare(self, branch, upstream_branch, attempt_id=None, *, source_registration,
                upstream_registration, feature_base, upstream_base, base_sequence,
                on_conflict="fail"):
        """Persist fixed token and resource names before any snapshot request.

        This is not a publish operation. Its returned intent must be passed to
        the later snapshot/publication protocol without generating new names.
        """
        _identifier(branch)
        _identifier(upstream_branch)
        if not isinstance(on_conflict, str) or on_conflict not in ("fail", "ours", "theirs"):
            raise ValueError("unsupported native Rebase conflict policy")
        attempt_id = _attempt_id(attempt_id if attempt_id is not None else uuid.uuid4().hex)
        for registration in (source_registration, upstream_registration):
            if not isinstance(registration, tuple) or len(registration) != 2:
                raise ValueError("invalid Rebase source registration")
            _identifier(registration[0])
            if type(registration[1]) is not int or not 0 <= registration[1] < 2 ** 31 - 1:
                raise ValueError("invalid Rebase registration generation")
        for endpoint in (feature_base, upstream_base):
            if not isinstance(endpoint, tuple) or len(endpoint) != 2:
                raise ValueError("invalid Rebase base endpoint")
            for name in endpoint:
                _identifier(name)
        if type(base_sequence) is not int or not 0 < base_sequence < 2 ** 63:
            raise ValueError("invalid Rebase base sequence")
        with self._transaction(attempt_id, initialize=True) as cur:
            cur.execute(f"SELECT db, gen FROM {self._branches} WHERE name=%s FOR UPDATE", (branch,))
            source = cur.fetchone()
            if source is None:
                raise ValueError("unknown source branch")
            if source != source_registration:
                raise ValueError("Rebase source generation changed during base resolution")
            cur.execute(f"SELECT attempt_id FROM {self._attempts} WHERE branch=%s "
                        "AND state IN (%s,%s,%s) ORDER BY attempt_id LIMIT 1",
                        (branch, "PREPARING", "PUBLISHING", "UNKNOWN"))
            pending = cur.fetchall()
            if pending:
                if len(pending) != 1 or len(pending[0]) != 1:
                    raise ValueError("ambiguous pending Rebase attempt")
                raise RebaseAttemptUnknown(_attempt_id(pending[0][0]))
            cur.execute(f"SELECT db, gen FROM {self._branches} WHERE name=%s", (upstream_branch,))
            upstream = cur.fetchone()
            if upstream is None or source[0] == upstream[0]:
                raise ValueError("unknown upstream or identical branch")
            if upstream != upstream_registration:
                raise ValueError("Rebase upstream generation changed during base resolution")
            cur.execute("SELECT d.dat_id, i.epoch FROM mo_catalog.mo_database d "
                        "JOIN mo_catalog.mo_database_inventory i "
                        "ON i.account_id=d.account_id AND i.database_id=d.dat_id "
                        "WHERE d.account_id=current_account_id() AND d.datname=%s", (source[0],))
            rows = cur.fetchall()
            if len(rows) != 1:
                raise ValueError("missing or ambiguous source inventory")
            source_id, epoch = rows[0]
            cur.execute("SELECT dat_id FROM mo_catalog.mo_database "
                        "WHERE account_id=current_account_id() AND datname=%s", (upstream[0],))
            rows = cur.fetchall()
            if len(rows) != 1:
                raise ValueError("missing or ambiguous upstream identity")
            gen = source[1]
            intent = RebaseIntent(
                attempt_id, branch, source[0], source_id, epoch, gen,
                self._candidate_database(branch, gen + 1, attempt_id), gen + 1,
                upstream[0], rows[0][0], f"bb_r2_f_{attempt_id}", f"bb_r2_u_{attempt_id}",
                self._read_base(cur, feature_base), self._read_base(cur, upstream_base), base_sequence,
                on_conflict=on_conflict)
            cur.execute(f"INSERT INTO {self._attempts} (attempt_id,branch,state,intent_json) "
                        "VALUES (%s,%s,%s,%s)",
                        (attempt_id, branch, "PREPARING", json.dumps(asdict(intent), sort_keys=True)))
            if cur.rowcount != 1:
                raise RuntimeError("Rebase intent was not inserted exactly once")
        return intent

    @staticmethod
    def _snapshot_spec(intent, role):
        if role == "feature":
            return intent.feature_snapshot, intent.source_db, intent.source_id
        if role == "upstream":
            return intent.upstream_snapshot, intent.upstream_db, intent.upstream_id
        raise ValueError("invalid Rebase snapshot role")

    def _locked_intent(self, cur, intent, require_current, expected_state="PREPARING"):
        # Every writer observes branch -> attempt order. Confirmation only
        # records a completed request; it never publishes a stale generation.
        cur.execute(f"SELECT db, gen FROM {self._branches} WHERE name=%s FOR UPDATE", (intent.branch,))
        branch = cur.fetchone()
        cur.execute(f"SELECT attempt_id,state,intent_json,result_json FROM {self._attempts} "
                    "WHERE attempt_id=%s FOR UPDATE", (intent.attempt_id,))
        rows = cur.fetchall()
        if len(rows) != 1:
            raise RebaseAttemptUnknown(intent.attempt_id)
        record = self._decode(rows[0])
        if record["intent"] != asdict(intent):
            raise ValueError("Rebase intent identity changed")
        if record["state"] in ("PUBLISHING", "UNKNOWN") and record["state"] != expected_state:
            raise RebaseAttemptUnknown(intent.attempt_id)
        if record["state"] != expected_state:
            raise ValueError("Rebase intent is changed or no longer preparing")
        if require_current and branch != (intent.source_db, intent.old_generation):
            raise ValueError("Rebase source registration changed before snapshot request")

    def _read_snapshot_requests(self, cur, intent, role=None):
        predicate = "attempt_id=%s" + (" AND role=%s" if role else "")
        params = (intent.attempt_id, role) if role else (intent.attempt_id,)
        cur.execute(f"SELECT role,state,snapshot_name,database_id,snapshot_uuid,snapshot_ts "
                    f"FROM {self._requests} WHERE {predicate} ORDER BY role LIMIT 3", params)
        rows = cur.fetchall()
        receipts = [self._decode_snapshot_request(intent, row) for row in rows]
        roles = [receipt["role"] for receipt in receipts]
        if len(roles) != len(set(roles)) or len(roles) > 2 or (role and roles not in ([], [role])):
            raise ValueError("ambiguous Rebase snapshot receipts")
        return receipts

    @classmethod
    def _decode_snapshot_request(cls, intent, row):
        role, state, name, database_id, snapshot_id, ts = row
        expected_name, _, expected_id = cls._snapshot_spec(intent, role)
        if name != expected_name or type(database_id) is not int or database_id != expected_id:
            raise ValueError("snapshot receipt belongs to a different resource")
        if state == "CONFIRMED":
            if (not isinstance(snapshot_id, str) or str(uuid.UUID(snapshot_id)) != snapshot_id
                    or type(ts) is not int or not 0 < ts < 2 ** 63):
                raise ValueError("invalid confirmed snapshot identity")
        elif state != "REQUESTED" or snapshot_id is not None or ts is not None:
            raise ValueError("invalid snapshot request state")
        return {"role": role, "state": state, "snapshot_name": name,
                "database_id": database_id, "snapshot_uuid": snapshot_id, "snapshot_ts": ts}

    def _reserve_snapshot_request(self, intent, role):
        name, _, database_id = self._snapshot_spec(intent, role)
        with self._transaction(intent.attempt_id) as cur:
            self._locked_intent(cur, intent, require_current=True)
            receipts = self._read_snapshot_requests(cur, intent, role)
            if receipts:
                if receipts[0]["state"] == "REQUESTED":
                    raise RebaseAttemptUnknown(intent.attempt_id)
                return receipts[0]
            if role == "upstream":
                feature = self._read_snapshot_requests(cur, intent, "feature")
                if not feature or feature[0]["state"] != "CONFIRMED":
                    raise RebaseAttemptUnknown(intent.attempt_id)
            # Reject already-stale inputs before allocating another resource.
            # This check is not the publication transaction's required GC pin.
            for fixed in (intent.feature_base, intent.upstream_base):
                if self._read_base(cur, (fixed.database, fixed.snapshot)) != fixed:
                    raise ValueError("Rebase base snapshot identity changed before request")
            cur.execute(f"INSERT INTO {self._requests} "
                        "(attempt_id,role,state,snapshot_name,database_id) VALUES (%s,%s,%s,%s,%s)",
                        (intent.attempt_id, role, "REQUESTED", name, database_id))
            if cur.rowcount != 1:
                raise RuntimeError("snapshot request was not inserted exactly once")
        return None

    def _confirm_snapshot_request(self, intent, receipt):
        with self._transaction(intent.attempt_id) as cur:
            self._locked_intent(cur, intent, require_current=False)
            existing = self._read_snapshot_requests(cur, intent, receipt["role"])
            if not existing:
                raise RebaseAttemptUnknown(intent.attempt_id)
            if existing[0]["state"] == "CONFIRMED":
                if existing[0] != receipt:
                    raise ValueError("confirmed snapshot identity changed")
                return
            cur.execute(f"UPDATE {self._requests} SET state=%s,snapshot_uuid=%s,snapshot_ts=%s "
                        "WHERE attempt_id=%s AND role=%s AND state=%s", (
                            "CONFIRMED", receipt["snapshot_uuid"], receipt["snapshot_ts"],
                            intent.attempt_id, receipt["role"], "REQUESTED"))
            if cur.rowcount != 1:
                raise RebaseAttemptUnknown(intent.attempt_id)

    def request_snapshots(self, attempt_id):
        """Prepare the original two resources, without publishing or reissuing."""
        outcome = self._inspect(attempt_id)
        if "intent" not in outcome:
            raise RebaseAttemptUnknown(attempt_id)
        if outcome["state"] != "UNKNOWN":
            raise ValueError("terminal Rebase attempt cannot prepare new snapshots")
        intent = RebaseIntent(**outcome["intent"])
        receipts = []
        for role in ("feature", "upstream"):
            existing = self._reserve_snapshot_request(intent, role)
            if existing is not None:
                receipts.append(existing)
                continue
            name, database, database_id = self._snapshot_spec(intent, role)
            try:
                with closing(self._connect()) as conn:
                    if not conn.get_autocommit():
                        raise ValueError("snapshot request requires a fresh autocommit connection")
                    with conn.cursor() as cur:
                        cur.execute(f"CREATE SNAPSHOT {_identifier(name)} FOR DATABASE {_identifier(database)}")
                        cur.execute("SELECT cast(snapshot_id as char(36)),ts,level,database_name,obj_id "
                                    "FROM mo_catalog.mo_snapshots "
                                    "WHERE sname=%s AND account_name=current_account_name()", (name,))
                        rows = cur.fetchall()
                    if len(rows) != 1 or rows[0][2:4] != ("database", database):
                        raise ValueError("missing or mismatched snapshot identity")
                    snapshot_id, ts, _, _, actual_id = rows[0]
                    receipt = self._decode_snapshot_request(intent, (
                        role, "CONFIRMED", name, actual_id, snapshot_id, ts))
                    if actual_id != database_id:
                        raise ValueError("snapshot database identity changed")
                self._confirm_snapshot_request(intent, receipt)
                receipts.append(receipt)
            except Exception as exc:
                raise RebaseAttemptUnknown(attempt_id) from exc
        return {"attempt_id": attempt_id, "state": "UNKNOWN", "snapshots": receipts}

    def _check_publication_snapshots(self, cur, intent, receipts):
        if {item["role"] for item in receipts} != {"feature", "upstream"} or any(
                item["state"] != "CONFIRMED" for item in receipts):
            raise RebaseAttemptUnknown(intent.attempt_id)
        fixed = [intent.feature_base, intent.upstream_base]
        for receipt in receipts:
            _, database, _ = self._snapshot_spec(intent, receipt["role"])
            fixed.append(RebaseBase(database, receipt["snapshot_name"], receipt["database_id"],
                                    receipt["snapshot_uuid"], receipt["snapshot_ts"]))
        for endpoint in fixed:
            if self._read_base(cur, (endpoint.database, endpoint.snapshot)) != endpoint:
                raise ValueError("Rebase publication snapshot identity changed")
        return next(endpoint for endpoint in fixed[2:] if endpoint.database == intent.upstream_db)

    def publish(self, attempt_id):
        """Explicit disabled-path publisher; never retries or activates the server.

        Normal benchmark Rebase does not call this until end-to-end acceptance.
        A terminal/uncertain attempt must be recovered, not published again.
        """
        observed = self._inspect(attempt_id)
        if "intent" not in observed:
            raise RebaseAttemptUnknown(attempt_id)
        if observed["state"] != "UNKNOWN":
            raise ValueError("recover a terminal Rebase attempt instead of republishing")
        intent = RebaseIntent(**observed["intent"])
        if intent.candidate_db != self._candidate_database(intent.branch, intent.new_generation, attempt_id):
            raise ValueError("Rebase candidate does not match its allocated identity")
        self._reserve_publication(intent)
        rolled_back = []
        try:
            return self._publish_reserved(intent, rolled_back)
        except RebaseAttemptUnknown:
            raise
        except Exception as exc:
            if not rolled_back:
                raise RebaseAttemptUnknown(attempt_id) from exc
            try:
                self._abort_rolled_back_publication(intent, type(exc).__name__)
            except Exception as abort_error:
                raise RebaseAttemptUnknown(attempt_id) from abort_error
            raise

    def _reserve_publication(self, intent):
        with self._transaction(intent.attempt_id) as cur:
            self._locked_intent(cur, intent, require_current=True)
            receipts = self._read_snapshot_requests(cur, intent)
            if (len(receipts) != 2 or {r["role"] for r in receipts} != {"feature", "upstream"}
                    or any(r["state"] != "CONFIRMED" for r in receipts)):
                raise RebaseAttemptUnknown(intent.attempt_id)
            cur.execute(f"UPDATE {self._attempts} SET state=%s WHERE attempt_id=%s AND state=%s",
                        ("PUBLISHING", intent.attempt_id, "PREPARING"))
            if cur.rowcount != 1:
                raise RebaseAttemptUnknown(intent.attempt_id)

    def _abort_rolled_back_publication(self, intent, error_type):
        # Only the original publisher calls this after its rollback response.
        # Inspection/recovery has no such evidence and must never call it.
        with self._transaction(intent.attempt_id) as cur:
            self._locked_intent(cur, intent, require_current=False, expected_state="PUBLISHING")
            receipts = self._read_snapshot_requests(cur, intent)
            if len(receipts) != 2 or any(r["state"] != "CONFIRMED" for r in receipts):
                raise RebaseAttemptUnknown(intent.attempt_id)
            cur.execute(f"UPDATE {self._attempts} SET state=%s,result_json=%s "
                        "WHERE attempt_id=%s AND state=%s",
                        ("ABORTED", json.dumps({"error_type": error_type}, sort_keys=True),
                         intent.attempt_id, "PUBLISHING"))
            if cur.rowcount != 1:
                raise RebaseAttemptUnknown(intent.attempt_id)

    def _publish_reserved(self, intent, rolled_back):
        attempt_id = intent.attempt_id
        with self._transaction(attempt_id, rolled_back=rolled_back) as cur:
            self._locked_intent(cur, intent, require_current=True, expected_state="PUBLISHING")
            receipts = self._read_snapshot_requests(cur, intent)
            upstream = self._check_publication_snapshots(cur, intent, receipts)
            cur.execute("SELECT dat_id FROM mo_catalog.mo_database "
                        "WHERE account_id=current_account_id() AND datname=%s", (intent.candidate_db,))
            if cur.fetchall():
                raise RebaseAttemptUnknown(attempt_id)
            cur.execute("SELECT table_name FROM information_schema.tables "
                        "WHERE LOWER(table_schema)=LOWER(%s) AND table_type='BASE TABLE' "
                        "ORDER BY table_name", (intent.source_db,))
            tables = [row[0] for row in cur.fetchall()]
            if (not tables or len(tables) != len(set(tables)) or "_bb_commits" not in tables
                    or any(name.startswith("_bb_") and name != "_bb_commits" for name in tables)):
                raise ValueError("native Rebase requires only its known history metadata")
            replay = sorted(name for name in tables if name != "_bb_commits")
            if not replay:
                raise ValueError("native Rebase requires at least one data table")
            selection = ",".join(_identifier(name) for name in replay)
            policy = {"fail": "fail", "ours": "skip", "theirs": "accept"}[intent.on_conflict]
            query = (f"DATA BRANCH REBASE DATABASE {_identifier(intent.candidate_db)} "
                     f"FROM {_identifier(intent.source_db)} {{snapshot=%s}} "
                     f"AGAINST {_identifier(intent.upstream_db)} {{snapshot=%s}} "
                     f"BETWEEN {_identifier(intent.feature_base.database)} {{snapshot=%s}} "
                     f"AND {_identifier(intent.upstream_base.database)} {{snapshot=%s}} "
                     f"TABLES ({selection}) FOR UPDATE EXPECT ({intent.source_id},{intent.source_epoch}) "
                     f"WHEN CONFLICT {policy}")
            started = time.perf_counter()
            cur.execute(query, (intent.feature_snapshot, intent.upstream_snapshot,
                                intent.feature_base.snapshot, intent.upstream_base.snapshot))
            rows = cur.fetchall()
            native_elapsed = time.perf_counter() - started
            columns = tuple(column[0] for column in cur.description or ())
            if columns != NATIVE_REBASE_COLUMNS or len(rows) != 1 or len(rows[0]) != len(columns):
                raise ValueError("invalid native Rebase summary shape")
            summary = dict(zip(columns, rows[0]))
            numeric = [value for name, value in summary.items() if name not in ("database", "base_database")]
            if (any(type(value) is not int or not 0 <= value < 2 ** 64 for value in numeric)
                    or summary["database"] != intent.candidate_db or summary["database_id"] == 0
                    or summary["base_database"] != upstream.database or summary["base_ts"] != upstream.snapshot_ts
                    or summary["tables"] != len(tables)):
                raise ValueError("native Rebase summary contradicts its fixed inputs")
            # Native admission now owns snapshot protection in this transaction.
            # A name rebound before admission must not publish another snapshot.
            self._check_publication_snapshots(cur, intent, receipts)
            candidate_log = f"{_identifier(intent.candidate_db)}.`_bb_commits`"
            source_log = f"{_identifier(intent.source_db)}.`_bb_commits` {{snapshot=%s}}"
            cur.execute(f"SELECT cast(coalesce(max(seq),0) as signed) FROM {candidate_log}")
            maximum = cur.fetchall()
            cur.execute(f"SELECT cast(coalesce(max(seq),0) as signed),count(*) FROM {source_log} "
                        "WHERE seq>%s AND kind IN ('commit','merge')",
                        (intent.feature_snapshot, intent.base_sequence))
            own = cur.fetchall()
            if (len(maximum) != 1 or len(maximum[0]) != 1 or len(own) != 1 or len(own[0]) != 2
                    or any(type(value) is not int or value < 0 for value in (*maximum[0], *own[0]))):
                raise ValueError("invalid Rebase history bounds")
            marker = max(maximum[0][0], own[0][0], intent.base_sequence) + 1
            count = own[0][1]
            if marker + count >= 2 ** 63:
                raise ValueError("Rebase history sequence exhausted")
            cur.execute("SELECT c.table_name,c.column_name,c.column_type,c.column_key "
                        "FROM information_schema.columns c JOIN information_schema.tables t "
                        "ON t.table_schema=c.table_schema AND t.table_name=c.table_name "
                        "WHERE LOWER(c.table_schema)=LOWER(%s) AND t.table_type='BASE TABLE' "
                        "AND c.table_name NOT LIKE %s ORDER BY c.table_name,c.ordinal_position",
                        (intent.candidate_db, r"\_bb\_%"))
            schema = schema_fingerprint_rows(cur.fetchall())
            if set(schema) != set(replay):
                raise ValueError("native Rebase history schema inventory changed")
            cur.execute(f"INSERT INTO {candidate_log} "
                        "(seq,id,kind,message,snap,db,prev_snap,prev_db,ts,schema_json) "
                        "VALUES (%s,%s,'rebase',%s,%s,%s,%s,%s,NOW(6),%s)", (
                            marker, attempt_id, "native rebase onto fixed upstream", upstream.snapshot,
                            upstream.database, upstream.snapshot, upstream.database, json.dumps(schema, sort_keys=True)))
            if cur.rowcount != 1:
                raise RuntimeError("Rebase base marker was not inserted exactly once")
            if count:
                cur.execute(f"INSERT INTO {candidate_log} "
                            "(seq,id,kind,message,snap,db,prev_snap,prev_db,ts,schema_json) "
                            f"SELECT %s+row_number() over(order by seq),id,kind,message,snap,db,prev_snap,prev_db,ts,schema_json "
                            f"FROM {source_log} WHERE seq>%s AND kind IN ('commit','merge')",
                            (marker, intent.feature_snapshot, intent.base_sequence))
                if cur.rowcount != count:
                    raise RuntimeError("Rebase historical commits were not copied exactly once")
            cur.execute(f"UPDATE {self._branches} SET db=%s,gen=%s WHERE name=%s AND db=%s AND gen=%s", (
                intent.candidate_db, intent.new_generation, intent.branch, intent.source_db, intent.old_generation))
            if cur.rowcount != 1:
                raise ValueError("Rebase branch generation changed during publication")
            cur.execute(f"DROP DATABASE {_identifier(intent.source_db)}")
            result = {"attempt_id": attempt_id, "candidate_db": intent.candidate_db,
                      "database_id": summary["database_id"], "generation": intent.new_generation,
                      "upstream_database": upstream.database, "upstream_snapshot": upstream.snapshot,
                      "upstream_timestamp": upstream.snapshot_ts, "native_summary": summary,
                      "native_sql_elapsed_sec": native_elapsed}
            cur.execute(f"UPDATE {self._attempts} SET state=%s,result_json=%s "
                        "WHERE attempt_id=%s AND state=%s", (
                            "COMMITTED", json.dumps(result, sort_keys=True), attempt_id, "PUBLISHING"))
            if cur.rowcount != 1:
                raise ValueError("Rebase success record changed during publication")
        return result

    def list_attempts(self, branch, after=None, limit=100):
        """Return a bounded page, including its explicit continuation cursor."""
        _identifier(branch)
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("attempt page limit must be between 1 and 100")
        if after is not None:
            _attempt_id(after)
        predicate = "branch=%s" + (" AND attempt_id>%s" if after else "")
        params = (branch, after, limit + 1) if after else (branch, limit + 1)
        with closing(self._connect()) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT attempt_id,state,intent_json,result_json FROM {self._attempts} "
                        f"WHERE {predicate} ORDER BY attempt_id LIMIT %s", params)
            rows = cur.fetchall()
        page = [self._decode(row) for row in rows[:limit]]
        if any(item["intent"]["branch"] != branch for item in page):
            raise ValueError("persisted attempt belongs to a different branch")
        return {"attempts": page, "next": page[-1]["attempt_id"] if len(rows) > limit else None}

    def _inspect(self, attempt_id):
        """Read identities for preparation/recovery; never infer rollback."""
        _attempt_id(attempt_id)
        with closing(self._connect()) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT attempt_id,state,intent_json,result_json FROM {self._attempts} "
                        "WHERE attempt_id=%s", (attempt_id,))
            rows = cur.fetchall()
            if not rows:
                return {"attempt_id": attempt_id, "state": "UNKNOWN", "result": None}
            if len(rows) != 1:
                raise ValueError("ambiguous Rebase attempt identity")
            outcome = self._decode(rows[0])
            outcome["snapshots"] = self._read_snapshot_requests(cur, RebaseIntent(**outcome["intent"]))
        if outcome["state"] not in ("COMMITTED", "ABORTED"):
            outcome["state"] = "UNKNOWN"
        return outcome

    def recover(self, attempt_id):
        """Confirm the original attempt under the publisher's lock order.

        The first read only discovers its immutable branch identity. A current
        branch pointer is not proof of an earlier attempt's success or failure.
        No pending record is aborted and no request or publication is reissued.
        """
        observed = self._inspect(attempt_id)
        if "intent" not in observed:
            return observed
        intent = RebaseIntent(**observed["intent"])
        unknown = {"attempt_id": attempt_id, "state": "UNKNOWN", "result": None,
                   "intent": asdict(intent)}
        try:
            with self._transaction(attempt_id) as cur:
                cur.execute(f"SELECT name FROM {self._branches} WHERE name=%s FOR UPDATE NOWAIT",
                            (intent.branch,))
                # A terminal attempt remains valid after its branch advances or
                # is removed. Only the original durable result is authoritative.
                registrations = cur.fetchall()
                if len(registrations) > 1 or (registrations and registrations[0] != (intent.branch,)):
                    raise ValueError("ambiguous Rebase branch registration")
                cur.execute(f"SELECT attempt_id,state,intent_json,result_json FROM {self._attempts} "
                            "WHERE attempt_id=%s FOR UPDATE NOWAIT", (attempt_id,))
                rows = cur.fetchall()
                if not rows:
                    return unknown
                if len(rows) != 1:
                    raise ValueError("ambiguous Rebase attempt identity")
                outcome = self._decode(rows[0])
                if outcome["intent"] != observed["intent"]:
                    raise ValueError("Rebase intent changed during recovery")
                if (observed["state"] in ("COMMITTED", "ABORTED")
                        and (outcome["state"], outcome["result"])
                        != (observed["state"], observed["result"])):
                    raise ValueError("Rebase terminal evidence changed during recovery")
                outcome["snapshots"] = self._read_snapshot_requests(cur, intent)
                if outcome["state"] not in ("COMMITTED", "ABORTED"):
                    outcome["state"] = "UNKNOWN"
            return outcome
        except RebaseAttemptUnknown:
            return unknown
        except OperationalError as exc:
            # MySQL lock-wait/deadlock errors prove neither publisher rollback
            # nor snapshot termination. Other read/server failures stay errors.
            if exc.args and exc.args[0] in (1205, 1213, 3572):
                return unknown
            raise

    @staticmethod
    def _decode(row):
        attempt_id, state, raw_intent, raw_result = row
        try:
            intent = RebaseIntent(**json.loads(raw_intent))
        except (TypeError, ValueError) as exc:
            raise ValueError("invalid persisted Rebase intent; fixed bases are required") from exc
        if attempt_id != intent.attempt_id or state not in ("PREPARING", "PUBLISHING", "UNKNOWN", "COMMITTED", "ABORTED"):
            raise ValueError("invalid persisted Rebase attempt")
        result = json.loads(raw_result) if raw_result is not None else None
        if state == "COMMITTED" and not isinstance(result, dict):
            raise ValueError("committed Rebase attempt has no result")
        if state not in ("COMMITTED", "ABORTED") and result is not None:
            raise ValueError("pending Rebase attempt has a terminal result")
        return {"attempt_id": attempt_id, "state": state, "intent": asdict(intent), "result": result}
