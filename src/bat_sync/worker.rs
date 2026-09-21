use crate::bat_record_model::tx_stamp::WorkerId;
use crate::bat_sync::safe_cell::SafeCell;
use crate::bat_sync::visibility::SnapshotCache;
use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};

/// A tree's fixed, bounded pool of OSIC workers (§3.1): each worker gets its
/// own `CommitLog`/`SnapshotCache` slot, so the pool size directly bounds the
/// memory OSIC needs and the cost of a commit-log prune. Workers are handed
/// out once per (tree, thread) and never returned — matching the paper's
/// model of a fixed set of long-lived worker threads, one per core, rather
/// than one per arbitrary short-lived task.
pub struct WorkerRegistry {
    /// A process-wide unique id for the tree this registry belongs to — see
    /// `worker_id_for` for why this, and not the tree's address, is what the
    /// per-thread cache must be keyed by.
    db_uid: u64,
    max_workers: usize,
    next: AtomicUsize,
}

/// A reserved `WorkerId` that `acquire` can never hand out (it only ever
/// returns values `< max_workers`, and this crate's every real deployment
/// keeps `max_workers` far below `u16::MAX` — bounded by `num_cpus`-scale
/// thread budgets, see `bat_tree::mvbt::default_max_workers`). Safe for a
/// thread to pass as its own `reader_worker` to `visibility::is_visible`/
/// `RangeQueryIter::new` *without ever calling `worker_id_for`* — i.e.
/// without permanently claiming a real registry slot — as long as that
/// thread is a pure reader that never writes (`register_reader_si: false`,
/// relying on some other, already-registered transaction's snapshot
/// protection — see `bat_bench::parallel_scan`'s doc):
///
///  - `is_visible`'s only use of the reader's own id is the same-worker
///    fast path (`stamp.worker_id() == reader_worker`) — since no real
///    writer is ever assigned this id, that comparison is always correctly
///    `false` for every real stamp, so every check properly falls through
///    to the real `LCB` lookup instead of a wrong same-worker shortcut.
///  - Every other per-worker structure on the read path
///    (`TxContext::commit_logs`, `SnapshotCache`) is indexed by the
///    *writer's* worker id from the stamp being checked, never by the
///    reader's own id — so this value is never used as an array index and
///    can't go out of bounds.
///  - `with_snapshot_cache`/`with_snapshot_cache_and_logs` (the only other
///    per-worker state a read touches) are keyed by thread-local storage,
///    not by `WorkerId` at all, and work for any calling thread regardless
///    of whether it ever registered one.
///
/// Used by `bat_tree::scan_pool::ScanWorkerPool`'s worker threads, which
/// exist purely to run these read-only sub-range jobs and should never
/// permanently consume a slot from a tree's fixed, never-growing
/// `WorkerRegistry` — letting a pool oversubscribe past `max_workers`
/// entirely safely, since its threads draw from this one shared constant
/// instead of the registry's counter.
pub const READ_ONLY_SCAN_WORKER_ID: WorkerId = WorkerId::MAX;

static NEXT_DB_UID: AtomicU64 = AtomicU64::new(0);

impl WorkerRegistry {
    pub fn new(max_workers: usize) -> Self {
        Self {
            db_uid: NEXT_DB_UID.fetch_add(1, Relaxed),
            max_workers,
            next: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn max_workers(&self) -> usize {
        self.max_workers
    }

    fn acquire(&self) -> WorkerId {
        let id = self.next.fetch_add(1, Relaxed);
        assert!(
            id < self.max_workers,
            "WorkerRegistry: exceeded max_workers ({}) — increase max_workers on tree construction",
            self.max_workers
        );
        id as WorkerId
    }
}

thread_local! {
    static WORKER_CACHE: Cell<(u64, WorkerId, u64, WorkerId)> =
        Cell::new((u64::MAX, 0, u64::MAX, 0));

    // Cold-path storage for threads that touch more than two databases.
    // Keeping it separate preserves WORKER_CACHE's single-comparison primary
    // hit and avoids constructing or borrowing a Vec in the normal case.
    static WORKER_CACHE_OVERFLOW: SafeCell<Vec<(u64, WorkerId)>> =
        SafeCell::new(Vec::new());
}

pub(crate) fn worker_id_for(registry: &WorkerRegistry) -> WorkerId {
    WORKER_CACHE.with(|cache| {
        let mut slot = cache.get();
        if slot.0 == registry.db_uid {
            return slot.1;
        }

        if slot.2 == registry.db_uid {
            std::mem::swap(&mut slot.0, &mut slot.2);
            std::mem::swap(&mut slot.1, &mut slot.3);
        } else {
            let requested = WORKER_CACHE_OVERFLOW.with(|overflow| {
                let overflow = overflow.get_mut();
                if let Some(index) = overflow.iter().position(|entry| entry.0 == registry.db_uid) {
                    let requested = overflow[index];
                    overflow[index] = (slot.2, slot.3);
                    requested
                } else {
                    if slot.2 != u64::MAX {
                        overflow.push((slot.2, slot.3));
                    }
                    (registry.db_uid, registry.acquire())
                }
            });
            slot.2 = slot.0;
            slot.3 = slot.1;
            slot.0 = requested.0;
            slot.1 = requested.1;
        }
        cache.set(slot);
        slot.1
    })
}

thread_local! {
    static SNAPSHOT_CACHE: SafeCell<(u64, SnapshotCache, u64, SnapshotCache)> =
        SafeCell::new((u64::MAX, SnapshotCache::new(0), u64::MAX, SnapshotCache::new(0)));

    // As above, this is never accessed on a primary or secondary inline hit.
    static SNAPSHOT_CACHE_OVERFLOW: SafeCell<Vec<(u64, SnapshotCache)>> =
        SafeCell::new(Vec::new());
}

pub(crate) fn with_snapshot_cache<R>(
    registry: &WorkerRegistry,
    f: impl FnOnce(&mut SnapshotCache) -> R,
) -> R {
    SNAPSHOT_CACHE.with(|cache| {
        let slot = cache.get_mut();

        if slot.0 != registry.db_uid {
            if slot.2 == registry.db_uid {
                std::mem::swap(&mut slot.0, &mut slot.2);
                std::mem::swap(&mut slot.1, &mut slot.3);
            } else {
                let requested = SNAPSHOT_CACHE_OVERFLOW.with(|overflow| {
                    let overflow = overflow.get_mut();
                    if let Some(index) =
                        overflow.iter().position(|entry| entry.0 == registry.db_uid)
                    {
                        let requested = overflow.swap_remove(index);
                        if slot.2 != u64::MAX {
                            overflow.push((
                                slot.2,
                                std::mem::replace(&mut slot.3, SnapshotCache::new(0)),
                            ));
                        }
                        requested
                    } else {
                        if slot.2 != u64::MAX {
                            overflow.push((
                                slot.2,
                                std::mem::replace(&mut slot.3, SnapshotCache::new(0)),
                            ));
                        }
                        (registry.db_uid, SnapshotCache::new(registry.max_workers()))
                    }
                });
                slot.2 = slot.0;
                slot.3 = std::mem::replace(&mut slot.1, requested.1);
                slot.0 = requested.0;
            }
        }

        f(&mut slot.1)
    })
}
