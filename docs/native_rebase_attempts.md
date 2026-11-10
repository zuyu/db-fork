# Restricted Native Rebase

The normal `MatrixOneToolSuite` and registered benchmark backend remain
unchanged. `NativeRebaseMatrixOneToolSuite` is an explicit candidate subclass;
it requires the kernel's exclusive test-only EXPECT admission. No production
switch, fallback to legacy SQL or automatic retry is added.

Support same-tenant, same-schema ordinary primary-key tables. Unchanged no-PK
tables require full visible multiset equality in the kernel. Changed no-PK data,
unsupported schema, pinned endpoint requests and custom conflict callbacks are
rejected. `ours` keeps fixed upstream; `theirs` keeps feature. The public API
inherits the existing timer, collector and error reporting and labels only the
candidate subclass as native.

## Durable Objects And Owners

`RebaseAttemptStore` owns a fresh connection per call. The benchmark's existing
metadata database owns attempts and snapshot request receipts. No credentials,
process-local result or external branch platform is used as publication truth.

An immutable intent binds a 128-bit attempt ID, logical branch, source DB ID,
old/new generation, source epoch, upstream identity, original base sequence and
all four endpoint identities. Snapshot identities include name, UUID, timestamp
and database ID. Candidate/snapshot names are allocated once from the attempt
and stay within the server's identifier limit. New attempts reject overflow.

Resolve the legacy lineage once and pass both observed registrations into
`prepare`. Under the branch lock, revalidate registrations and original base
snapshots before inserting PREPARING. Missing or rebound endpoints fail; there
is no two-way/no-base fallback. Old unreleased incomplete intents cannot invent
bases during recovery. A PREPARING, PUBLISHING or UNKNOWN attempt fences new
attempt preparation for that logical branch.

All metadata transactions lock branch registration before attempt. A helper
must not call autonomous `_add_commit`, `_register`, snapshot or DROP operations
inside publication. Unknown COMMIT responses retain the original attempt ID.

## Snapshot Request Protocol

The intent commits before either independent snapshot request. There are at
most two receipts, keyed by attempt ID and role (`feature`/`upstream`).

1. Lock branch then attempt, verify its generation and both original bases.
2. Insert REQUESTED with the preallocated resource identity and commit it.
3. Only the caller that acknowledged that reservation sends CREATE SNAPSHOT.
4. After its successful response, verify the current account's UUID/TS/DB
   identity and commit CONFIRMED under the same lock order.

An unknown reservation commit sends no request. Existing REQUESTED is UNKNOWN,
not permission to issue it again. A matching CONFIRMED receipt can be reused
without another request. Upstream preparation requires confirmed feature.
Confirmation does not require the branch still to be current; publication must
check that separately. Errors or client death after REQUESTED retain resources.
Catalog absence cannot distinguish never-sent from still-running requests.
Endpoint lifetime is protected by kernel publication admission, not this early
catalog check.

## Exactly-One Sender And Atomic Publication

PREPARING alone cannot prove whether a publication was already sent. A separate
branch-then-attempt transaction checks both CONFIRMED receipts and consumes
PREPARING as PUBLISHING. Only its acknowledged claimant sends native SQL once.
A second caller cannot consume PUBLISHING, including after client death.

The fresh Pessimistic-RC publication owner then:

1. Locks branch then attempt; checks PUBLISHING, source ID/generation, candidate
   identity and both confirmed receipts. Existing candidates are never dropped
   as a retry strategy.
2. Resolves data tables, omitting only known `_bb_commits`; rejects unknown
   reserved metadata. Calls native Rebase with fixed four snapshots, table
   selection and `FOR UPDATE EXPECT (source_id, source_epoch)`.
3. Checks the complete typed summary and rechecks snapshot identities after
   native admission has acquired protection. The source epoch and separate
   table-data dependencies protect directory and DML races respectively.
4. Records the actual fixed U snapshot as the new base. Appends feature
   commit/merge history after the original base using server-side INSERT SELECT
   and ROW_NUMBER, preserving original commit IDs and historical endpoints.
   Sequence allocation and affected counts are checked.
5. CAS-updates the branch DB/generation, logically retires the old source and
   writes COMMITTED with the result, all in that same owner.

Acknowledged COMMIT is publication success. The stored result includes attempt,
candidate ID/generation, exact U database/snapshot/timestamp, native summary and
client-observed native SQL time. The public result maps upstream/feature/conflict
key counts without losing that durable identity.

On acknowledged publication rollback, and only with both CONFIRMED requests,
the original publisher may record ABORTED in a new branch-then-attempt owner.
An unknown claim, publication COMMIT, rollback or abort COMMIT stays UNKNOWN.
Neither ABORTED nor success authorizes speculative physical cleanup.

## Inspection And Recovery

- `list_rebase_attempts(branch, after=None, limit=100)` reads a bounded ID-ordered
  page and explicit continuation cursor. It creates no tables.
- `recover_rebase(attempt_id)` opens a fresh RC connection, locks branch then
  attempt with NOWAIT and rereads durable state/at most two receipts.
- Busy publisher, lock timeout/deadlock or uncertain recovery completion returns
  UNKNOWN. Other read errors propagate. No publication is sent by recovery.
- A matching COMMITTED result is returned even after the branch advances or its
  current registration disappears. Current-pointer inference is not evidence of
  an earlier success. Corrupt immutable/terminal state is rejected.
- Pending state, snapshot visibility, elapsed time and disconnect are not rollback
  evidence. Recovery cannot automatically mark them ABORTED or delete resources.

Native generation names are opaque to legacy name parsing. Resolve at most the
four relevant names by their embedded attempt IDs and primary-key reads. Only
matching immutable intent plus COMMITTED result establishes logical identity.
Pending/absent state remains UNKNOWN; corrupt/aborted identity fails. Legacy
names and the known benchmark root keep their existing behavior.

## Retirement And Resource Lifetime

The old source DROP is logical retirement in the publication transaction.
Historical snapshots keep earlier generations readable. Unknown resources,
historical references and imported objects are retained until safe reclamation
can be proved; there is no production sweeper or cleanup inference from a dead
client. Successful integration fixtures may remove their exact private objects
after independently observing terminal requests and closing all clients. That
teardown is not production reference-safe GC qualification.

## Verification

Offline DB-API tests cover transaction ownership, branch/attempt order, exact
identity/counts, SQL quoting, corrupted records, single send, overflow, rollback,
both uncertain COMMIT boundaries and connection release. They do not establish
storage conflicts or actual process recovery.

Real integration scripts use the exclusive kernel SQL fixture:

- Publication: two consecutive Rebase operations, exact new bases/history,
  Merge, both conflict policies and multi-table late failure rollback.
- Public Suite: the actual constructor, collector, branch/commit/rebase/history
  path, reverse main direction and unsupported no-SQL rejection.
- Process recovery: actual adapter SIGKILL at three deterministic packet barriers:
  snapshot OK held before delivery, publication COMMIT held before forwarding,
  and server COMMIT OK held before delivery. Fresh interpreters receive only the
  metadata DB and original attempt ID. Independently prove server commit in the
  last case; a Python exception after receiving COMMIT is not equivalent.

The bounded loopback test relay has packet/connection limits, never records
authentication and joins all owned workers on failure. Receipt observation is
not sleep-based synchronization of competing database transactions. Pending
kill cases remain UNKNOWN; confirmed post-commit death recovers COMMITTED once.

Kernel catalog-producer, data-race, cross-CN, WAL/checkpoint, upgrade and snapshot
protection evidence is separate. Selected client PASS is not automatic kernel,
production or cleanup acceptance. Qualification results and identities stay in
external artifacts, not an append-only history in this design.

## Performance Recipe

`tests/integration_native_rebase_benchmark.py` compares the actual old/native
public methods against the same qualified server build and frozen adapter.
Use small legacy/native followed by W1 native/legacy, one serial worker,
16 spine branches, seed42, one excluded warmup and ten measured calls per arm.
Each call prepares a child and commits one disjoint INSERT on each side, matching
the upstream fixture recipe. W1 preserves all twelve tables and pinned seed.

Check table counts, exact changed-key results and native durable outcomes. This
is not a full-content digest of every W1 branch. Report adapter verb latency,
native SQL client time, actual timed SQL requests, seed/spine/operation setup
and cleanup separately. Flush operation evidence outside timing. Failures and
unsupported calls are not samples. One pair on a shared serial server, including
retained metadata/cache from earlier arms, cannot establish significance or a
single-patch causal speedup. Fewer SQL requests alone is not a performance win.
