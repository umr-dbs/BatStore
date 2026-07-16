use std::cell::Cell;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};
use crate::mv_record_model::tx_stamp::WorkerId;
use crate::mv_sync::safe_cell::SafeCell;
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
    db_uid: u64,
    max_workers: usize,
    next: AtomicUsize,
}

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

// Both caches below hold exactly one tree's worth of state per thread, not a
// searchable collection of many: every real deployment of this tree (main
// benchmark harnesses — `main_load`, TPC-C, YCSB — and any production use)
// creates exactly one tree that lives for the whole process, so "does this
// call's tree match the one we already have cached" is a single comparison,
// not a scan. A thread that touches a *different* tree next (the only way
// this actually happens today: `cargo test`'s thread pool reusing one OS
// thread across many single-tree `#[test]`s, never interleaved — each test
// builds its own tree, uses it to completion, and moves on) just overwrites
// the one slot — correct either way, since `tree_uid` is a process-wide
// unique, never-reused id (see below); it only costs that thread one fresh
// `WorkerRegistry::acquire()`/`SnapshotCache::new()` the first time it comes
// back to a tree it had evicted, exactly like a genuinely new thread's first
// touch. A prior version of this cache was a 16-slot searchable array sized
// for the `#[test]`-reuse case — that's solving a test-harness inconvenience
// with production-hot-path complexity; a single slot handles it just fine.

thread_local! {
    // Keyed by each tree's process-wide unique `tree_uid`, *not* its address:
    // a dropped tree's stack/heap slot can be reused by an unrelated later
    // tree, and a thread that cached a WorkerId for the old occupant would
    // otherwise silently (and wrongly) reuse it for the new one — bypassing
    // that new tree's own `WorkerRegistry` counter entirely and risking two
    // different threads believing they own the same WorkerId. A `tree_uid`
    // is never reused, so this can't happen. (This also avoids the old
    // process-global `clock::THREAD_ID`/`STATE`'s cross-tree contamination bug.)
    //
    // Plain `Cell`, not `RefCell`: `(u64, WorkerId)` is `Copy`, so reading
    // and writing it is a bare load/store — no runtime borrow-check needed
    // at all. (`RefCell` would only earn its keep if we needed to hand out
    // `&mut` into the cell's *contents* while still holding a `&` to the
    // cell itself, the way `SNAPSHOT_CACHES` below does; a `Copy` value never
    // needs that, `get`/`set` round-trip it by value instead.) `u64::MAX`
    // marks an empty slot: real `tree_uid`s come from a `fetch_add` counter
    // starting at 0, so this value is unreachable as a genuine one.
    static WORKER_CACHE: Cell<(u64, WorkerId)> = Cell::new((u64::MAX, 0));
}

/// Returns this thread's `WorkerId` for `registry`'s tree, assigning one on
/// first use. Stable for the lifetime of the thread as long as this is still
/// the last tree this thread touched (see the module-level doc for why a
/// single cached entry, not a searchable collection, is the right shape
/// here) — a call for a *different* tree overwrites the slot and draws a
/// fresh `WorkerId` via `registry.acquire()`. That's still correct (this
/// thread's earlier writes on the evicted tree just stop qualifying for the
/// "my own write" fast path in `visibility::is_visible`, falling back to the
/// slower cross-worker check, which still gives the right answer).
pub(crate) fn worker_id_for(registry: &WorkerRegistry) -> WorkerId {
    WORKER_CACHE.with(|cache| {
        let (uid, id) = cache.get();
        if uid == registry.db_uid {
            return id;
        }

        let id = registry.acquire();
        cache.set((registry.db_uid, id));
        id
    })
}

thread_local! {
    // Same keying rationale (and same single-slot-over-searchable-collection
    // reasoning) as `WORKER_CACHE` above — see the module-level doc. Kept as
    // a separate cell (rather than folded into `WORKER_CACHE`) since it
    // holds a whole `SnapshotCache`, not just a `u16` id.
    //
    // `SafeCell`, not `RefCell`: a thread-local is by construction only ever
    // touched by the one thread that owns it — there is no real concurrent
    // aliasing to guard against here, only Rust's `thread_local!::with()`
    // API always handing out `&T`, never `&mut T`, even to that sole owner.
    // `RefCell`'s runtime borrow-check exists for a *different* hazard this
    // module doesn't have: reentrant `.with()` calls on the same thread
    // aliasing a live `&mut` — checked here by inspection instead, the same
    // way the rest of this crate already trusts `SafeCell` for its
    // concurrency-critical block/node access (see `mv_sync::safe_cell`):
    // `with_snapshot_cache`'s only two callers (`version_handle.rs`'s
    // `is_visible_stamp`/`with_visibility_checker`) pass a closure that never
    // calls back into `with_snapshot_cache` (or `worker_id_for`) itself.
    static SNAPSHOT_CACHE: SafeCell<(u64, SnapshotCache)> = SafeCell::new((u64::MAX, SnapshotCache::new(0)));
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
/// Eviction follows the same single-slot policy as `worker_id_for` — see the
/// module-level doc; the only cost of evicting a tree's `SnapshotCache` here
/// is that thread re-warming it (one `lcb` query per foreign worker it goes
/// on to touch again), not a correctness issue.
pub(crate) fn with_snapshot_cache<R>(registry: &WorkerRegistry, f: impl FnOnce(&mut SnapshotCache) -> R) -> R {
    SNAPSHOT_CACHE.with(|cache| {
        let slot = cache.get_mut();

        if slot.0 != registry.db_uid {
            *slot = (registry.db_uid, SnapshotCache::new(registry.max_workers()));
        }

        f(&mut slot.1)
    })
}
