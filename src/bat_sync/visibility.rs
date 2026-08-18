use crate::bat_record_model::tx_stamp::{TxStamp, WorkerId};
use crate::bat_record_model::version_info::Version;
use crate::bat_sync::commit_log::CommitLog;

/// A worker's memo of the last `LCB` result computed against each foreign
/// worker, indexed by that foreign worker's id — the paper's "thread-local
/// snapshot cache" (Listing 1). Each entry is only valid for the reader
/// `ts_start` it was computed against, and is refreshed lazily the next time
/// a newer `ts_start` queries it.
///
/// Plain fields, no interior mutability: every real caller reaches this
/// through `bat_sync::worker::with_snapshot_cache`, which already holds a
/// `&mut SnapshotCache` by the time it hands one to `is_visible` — an inner
/// `RefCell`/`SafeCell` here would just be a second, redundant layer on top
/// of that.
pub struct SnapshotCache {
    // Structure-of-arrays: snapshot identity and LCB values are scanned or
    // refreshed independently and no longer force 16-byte tuple traffic.
    // This also leaves each stream densely packed for worker-indexed access.
    snapshot_versions: Box<[Version]>,
    lcb: Box<[Version]>,
}

impl SnapshotCache {
    pub fn new(max_workers: usize) -> Self {
        Self {
            snapshot_versions: vec![0; max_workers].into_boxed_slice(),
            lcb: vec![0; max_workers].into_boxed_slice(),
        }
    }
}

/// Direct port of the paper's Listing 1 `isVisible`: a version stamped by
/// `stamp.worker_id` at `stamp.ts_start` is visible to a reader on
/// `reader_worker` with snapshot `reader_ts_start` iff it's the reader's own
/// write (from this-or-an-earlier transaction on the same worker) or
/// `LCB(stamp.worker_id, reader_ts_start) > stamp.ts_start`.
///
/// Checked *before* either of those: a stamp marked invalid (its writing
/// transaction aborted — see `TxStamp::is_invalid`'s doc) is never visible,
/// to anyone, including the writer's own later transactions on the same
/// worker — deliberately ahead of the same-worker fast path below, since
/// that fast path is exactly what would otherwise keep an aborted write
/// visible to its own writer forever (it doesn't consult the commit log at
/// all, so an uncommitted write's absence from it never mattered there).
pub fn is_visible(
    commit_logs: &[CommitLog],
    cache: &mut SnapshotCache,
    reader_worker: WorkerId,
    reader_ts_start: Version,
    stamp: TxStamp,
) -> bool {
    if stamp.is_invalid() {
        return false;
    }

    if stamp.worker_id() == reader_worker {
        // A worker's own transactions are strictly serialized in time, so
        // any write by "me" is visible to "my" current transaction — but
        // not to a deliberately historical/point-in-time snapshot (a
        // smaller `reader_ts_start` than the write's own `ts_start`), which
        // BatStore supports as a first-class feature distinct from the paper's
        // "current snapshot only" model.
        return stamp.ts_start() <= reader_ts_start;
    }

    let index = stamp.worker_id() as usize;

    if cache.lcb[index] > stamp.ts_start() {
        return true; // cache hit: already known-visible
    }

    if cache.snapshot_versions[index] < reader_ts_start {
        // Cache is stale for this reader_ts_start (or never queried this
        // worker before) — refresh via a real (locked) LCB query.
        cache.lcb[index] = commit_logs[index].lcb(reader_ts_start);
        cache.snapshot_versions[index] = reader_ts_start;
    }

    cache.lcb[index] > stamp.ts_start()
}
