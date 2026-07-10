use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use crate::mv_record_model::tx_stamp::WorkerId;
use crate::mv_sync::visibility::SnapshotCache;

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
    tree_uid: u64,
    max_workers: usize,
    next: AtomicUsize,
}

static NEXT_TREE_UID: AtomicU64 = AtomicU64::new(0);

impl WorkerRegistry {
    pub fn new(max_workers: usize) -> Self {
        Self {
            tree_uid: NEXT_TREE_UID.fetch_add(1, Relaxed),
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
    // Keyed by each tree's process-wide unique `tree_uid`, *not* its address:
    // a dropped tree's stack/heap slot can be reused by an unrelated later
    // tree, and a thread that cached a WorkerId for the old occupant would
    // otherwise silently (and wrongly) reuse it for the new one — bypassing
    // that new tree's own `WorkerRegistry` counter entirely and risking two
    // different threads believing they own the same WorkerId. A `tree_uid`
    // is never reused, so this can't happen. (This also avoids the old
    // process-global `clock::THREAD_ID`/`STATE`'s cross-tree contamination bug.)
    static WORKER_CACHE: RefCell<HashMap<u64, WorkerId>> = RefCell::new(HashMap::new());
}

/// Returns this thread's `WorkerId` for `registry`'s tree, assigning one on
/// first use. Stable for the lifetime of the thread; a thread that touches
/// many trees accumulates one entry per tree (bounded by however many
/// distinct trees a process actually creates over its lifetime, not by any
/// single tree's lifetime — acceptable since each entry is one `u64 -> u16` pair).
pub(crate) fn worker_id_for(registry: &WorkerRegistry) -> WorkerId {
    WORKER_CACHE.with(|cache| {
        *cache.borrow_mut().entry(registry.tree_uid).or_insert_with(|| registry.acquire())
    })
}

thread_local! {
    // Same keying rationale as `WORKER_CACHE` above. Kept as a separate map
    // (rather than folded into `WORKER_CACHE`) since it holds a whole
    // `SnapshotCache`, not just a `u16` id.
    static SNAPSHOT_CACHES: RefCell<HashMap<u64, SnapshotCache>> = RefCell::new(HashMap::new());
}

/// Gives `f` this thread's own `SnapshotCache` for `registry`'s tree,
/// creating it (sized to `max_workers`) on first use. This is the paper's
/// "thread-local snapshot cache" (Listing 1) literally: unlike a `WorkerId`,
/// which is just a small `Copy` value, the cache is a real piece of mutable
/// per-worker state — keeping it in thread-local storage rather than in a
/// `Vec` on the tree indexed by `WorkerId` means no thread ever *could*
/// (accidentally or otherwise) touch another worker's cache, instead of
/// merely relying on every call site happening to pass its own id.
///
/// Takes `f: FnOnce(&mut SnapshotCache)`, not `&SnapshotCache`: the
/// `RefCell` around this thread-local `HashMap` (needed only because
/// `thread_local!`'s `.with()` hands out `&T`, and inserting a new tree's
/// entry needs `&mut`) already gives us a `&mut SnapshotCache` via
/// `entry().or_insert_with()` — passing that straight through means
/// `SnapshotCache` itself doesn't need its own, redundant interior mutability.
pub(crate) fn with_snapshot_cache<R>(registry: &WorkerRegistry, f: impl FnOnce(&mut SnapshotCache) -> R) -> R {
    SNAPSHOT_CACHES.with(|caches| {
        let mut caches = caches.borrow_mut();
        let cache = caches
            .entry(registry.tree_uid)
            .or_insert_with(|| SnapshotCache::new(registry.max_workers()));
        f(cache)
    })
}
