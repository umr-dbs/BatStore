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
        return stamp.ts_start() <= reader_ts_start;
    }

    let index = stamp.worker_id() as usize;

    if cache.snapshot_versions[index] > reader_ts_start {
        cache.lcb[index] = commit_logs[index].lcb(reader_ts_start);
        cache.snapshot_versions[index] = reader_ts_start;
    } else if cache.lcb[index] > stamp.ts_start() {
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
