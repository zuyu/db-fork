"""Explicit opt-in to restricted native Rebase; not a registered backend.

Requires the qualified candidate server and its test-only EXPECT gate. The
ordinary MatrixOneToolSuite remains unchanged and never selects this route.
"""

from .db_api import Ref
from .matrixone import MatrixOneToolSuite


class NativeRebaseMatrixOneToolSuite(MatrixOneToolSuite):
    IMPLEMENTATION = {**MatrixOneToolSuite.IMPLEMENTATION, "rebase": "native"}
    IMPLEMENTATION_NOTES = {
        **MatrixOneToolSuite.IMPLEMENTATION_NOTES,
        "rebase": "Restricted native replay and atomic publication with durable attempt recovery; "
                  "explicit opt-in only, no legacy fallback or automatic retry",
    }

    def _rebase_impl(self, ref: Ref, onto: Ref, on_conflict):
        if callable(on_conflict) or on_conflict not in ("ours", "theirs"):
            raise self._unsupported("rebase", "native publication has no custom conflict callback")
        if ref.commit or onto.commit:
            raise self._unsupported("rebase", "native publication requires branch heads")
        self._record_execution("native_rebase_publication")
        intent = self._prepare_native_rebase_attempt(ref, onto, on_conflict)
        self._request_native_rebase_snapshots(intent.attempt_id)
        result = self._publish_native_rebase_attempt(intent.attempt_id)
        summary = result["native_summary"]
        return {**result, "conflicts": summary["conflict_keys"],
                "ours_changes": summary["upstream_changed_keys"],
                "theirs_changes": summary["feature_changed_keys"]}
