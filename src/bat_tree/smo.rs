use crate::bat_block::block::{Block, BlockGuard};
use crate::bat_block::block_handle::BlockAllocManager;
use crate::bat_page_model::node::PageType;
use crate::bat_page_model::{BlockRef, Height};
use crate::bat_query::interval::Interval;
use crate::bat_record_model::record_point::RecordPoint;
use crate::bat_root::index_root::RootIndexGuard;
use crate::bat_root::root::Root;
use crate::bat_sync::tx_context::TxContext;
use crate::bat_test::{DIAG, VERBOSE, record_version_split};
use crate::bat_tree::mvbt::MVBTSt;
use itertools::Itertools;
use std::fmt::Display;
use std::hash::Hash;
use std::ops::Deref;

// TEMPORARY diagnostic: traces every leaf-level split/merge's source(s) and
// result so a lost key's lifecycle across pages can be reconstructed after a
// repro. Buffered in-memory (not `eprintln!`'d live) so the stderr lock
// itself doesn't perturb the race being chased; call `drain_trace_log` to
// dump it out once a failure is detected. Remove once the investigation
// concludes.
pub(crate) const TRACE_KEY_DEBUG: bool = false;

pub(crate) static TRACE_LOG: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

pub(crate) fn push_trace(s: String) {
    TRACE_LOG.lock().unwrap().push(s);
}

pub fn drain_trace_log() -> Vec<String> {
    std::mem::take(&mut *TRACE_LOG.lock().unwrap())
}

// TEMPORARY diagnostic helper.
fn diag_thread_hash() -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut hasher);
    hasher.finish()
}

/// Nearest index to `target` in `[1, items.len() - 1]` at which `key_of`
/// changes between `items[i - 1]` and `items[i]`, given `items` is already
/// key-sorted so one key's own entries are contiguous, preferring — in
/// order — a boundary that (1) doesn't tear a key *and* keeps both
/// resulting halves within `capacity`, (2) at least keeps both halves
/// within `capacity` (may tear a key), (3) at least doesn't tear a key
/// (may overflow `capacity` on one side), falling back to `target` itself
/// only if `items` is a single key's entire chain (no boundary exists at
/// all).
///
/// Two distinct hazards motivate this order. A raw `len / 2` cut can land
/// inside a single key's own multi-entry chain (e.g. a dead-but-abortable
/// predecessor next to its live successor, the exact shape
/// `record_survives_gc` preserves) and tear it across the two resulting
/// sibling blocks — whose fences are disjoint by construction — making the
/// torn-off half unreachable by any fence-routed lookup; that's what
/// tier (1) avoids in the common case. But nearest-to-`target` alone isn't
/// enough once `items` can hold *two* pages' worth of survivors combined
/// (`merge()`'s combined-then-split path, up to `2 * capacity`): dense
/// key-version clustering right at the midpoint can force every
/// tear-avoiding boundary to overflow one side or the other (e.g. 31
/// records, capacity 16: every non-tearing boundary sits at 14 or 17, both
/// leaving the other half over capacity) — confirmed as a real crash
/// (`LeafPage::bulk_push`/`bulk_push_from_slice_ref` panicking past
/// `NUM_RECORDS`) under sustained multi-table concurrent load. Whenever
/// `len <= 2 * capacity` (guaranteed for `merge()`'s combined case, since
/// neither original page can itself hold more than one page's worth), a
/// capacity-fitting position always exists in the range
/// `[len - capacity, capacity]` — tier (2) picks the one nearest `target`
/// in that range even if it tears a key, since a torn (but recoverable via
/// its own already-documented limitation) key beats a hard capacity
/// violation. Tier (3) is the original, tear-avoiding-only search, kept as
/// a fallback for callers whose `items` can exceed `2 * capacity` (none
/// currently do) or where `capacity` doesn't apply.
fn nearest_key_boundary<T, K: PartialEq>(
    items: &[T],
    target: usize,
    capacity: usize,
    key_of: impl Fn(&T) -> K,
) -> usize {
    let len = items.len();
    debug_assert!(len >= 2);
    let target = target.clamp(1, len - 1);
    let is_boundary = |i: usize| key_of(&items[i - 1]) != key_of(&items[i]);
    let fits = |i: usize| i <= capacity && len - i <= capacity;

    let search = |accept: &dyn Fn(usize) -> bool| -> Option<usize> {
        if accept(target) {
            return Some(target);
        }

        let mut lo = target;
        while lo > 1 && !accept(lo) {
            lo -= 1;
        }
        let found_lo = accept(lo).then_some(lo);

        let mut hi = target;
        while hi < len - 1 && !accept(hi) {
            hi += 1;
        }
        let found_hi = accept(hi).then_some(hi);

        match (found_lo, found_hi) {
            (Some(l), Some(h)) => Some(if target - l <= h - target { l } else { h }),
            (Some(l), None) => Some(l),
            (None, Some(h)) => Some(h),
            (None, None) => None,
        }
    };

    search(&|i| is_boundary(i) && fits(i))
        .or_else(|| search(&fits))
        .or_else(|| search(&is_boundary))
        .unwrap_or(target)
}

/// Finds a genuine key boundary whose two sides both fit. Unlike
/// `nearest_key_boundary`, this never tears a same-key version run: `None`
/// is the signal that an ordinary key split cannot converge and cold
/// offload is required.
fn fitting_key_boundary<T, K: PartialEq>(
    items: &[T],
    target: usize,
    capacity: usize,
    key_of: impl Fn(&T) -> K,
) -> Option<usize> {
    if items.len() < 2 {
        return None;
    }
    (1..items.len())
        .filter(|i| *i <= capacity && items.len() - *i <= capacity)
        .filter(|i| key_of(&items[*i - 1]) != key_of(&items[*i]))
        .min_by_key(|i| i.abs_diff(target))
}

/// A real two-key-range split recovered from an otherwise-indivisible batch
/// by finding its boundary against the write-facing (hot) subset only (see
/// `try_hot_key_split`). `left`/`right` carry every record on their side of
/// `split_key`, hot or not -- nothing is dropped.
struct HotKeySplit<Key: Ord + Copy + Hash + Default, Payload: Clone + Default> {
    split_key: Key,
    left: Vec<RecordPoint<Key, Payload>>,
    right: Vec<RecordPoint<Key, Payload>>,
}

#[repr(u8)]
pub enum BlockUnsafeDegree {
    Ok,
    Overflow,
    ActiveUnderflow,
}

impl BlockUnsafeDegree {
    #[inline(always)]
    pub const fn is_overflow(&self) -> bool {
        matches!(self, BlockUnsafeDegree::Overflow)
    }
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static,
> Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    // #[inline(always)]
    // pub const fn block_id(&self) -> BlockID {
    //     0
    // }

    /// The raw "is there room for up to 2 more entries" capacity check —
    /// `active + dead >= overflow_units_count()`, the same condition
    /// `unsafe_degree()` computes internally as its own local `is_overflow`
    /// — exposed directly rather than through `unsafe_degree()`'s
    /// `BlockUnsafeDegree::Overflow` classification, which can *hide* this
    /// exact condition: `unsafe_degree()` reclassifies it to
    /// `ActiveUnderflow` whenever `active` is also low (`<= 40%` of
    /// capacity, preferring a merge over a split for a sparse-but-cluttered
    /// page), so `BlockUnsafeDegree::is_overflow()` can return `false` for a
    /// page that is, by raw slot count, already full or one entry short of
    /// it.
    ///
    /// `on_overflow_node`/`on_underflow_node`'s pre-emptive "does `mufasa`
    /// have room for the entries I'm about to push" guard used to check
    /// `is_overflow()` (the classification) instead of this — sound for a
    /// page with a healthy active count, but silently defeated for a mostly
    /// *dead-but-still-snapshot-protected* `mufasa`: exactly the shape a
    /// sustained-transaction workload with long-held reader snapshots
    /// produces (TPC-C's default OLAP scan-sweep and multi-table
    /// transactions far more than YCSB's simpler point-op workload),
    /// confirmed as the mechanism behind `push_uncommitted` writing past
    /// `FAN_OUT` into `pointer_region`'s/`OptCell`'s adjacent memory with no
    /// bounds check ever tripping first (that guard having already, wrongly,
    /// let the push through).
    #[inline(always)]
    fn lacks_room_for_split_entries(&self) -> bool {
        let (active, dead) = self.active_dead_count();
        (active as usize) + (dead as usize) >= self.overflow_units_count()
    }

    /// Whether at least one of this leaf's dead entries is still protected
    /// by a live transaction's snapshot (`MVBTSt::record_survives_gc`'s own
    /// condition, inlined here since `Block` has no `ctx` of its own to
    /// call that method with) — i.e. whether a merge attempt here could
    /// still hit the "combined footprint can't shrink while the same
    /// transaction protects it" livelock `unsafe_degree()`'s raw-footprint
    /// check exists to avoid (see that method's doc). Short-circuits on the
    /// first protected entry found, so the case that check was originally
    /// written for (substantial *protected* garbage) stays cheap; only a
    /// leaf whose garbage is entirely unprotected pays the full scan, and
    /// only from the one narrow caller below — not on every leaf on every
    /// traversal.
    #[inline]
    fn has_protected_garbage(&self, ctx: &TxContext) -> bool {
        self.as_records().iter().any(|r| {
            let version = r.version();
            !version.is_live()
                && !version.insertion_stamp().is_invalid()
                && version
                    .deletion_stamp()
                    .is_some_and(|del| ctx.is_snapshot_live(del.ts_start()))
        })
    }

    #[inline(always)]
    pub fn unsafe_degree(&self, ctx: &TxContext) -> BlockUnsafeDegree {
        let (active, dead) = self.active_dead_count();

        let (active, dead) = (active as usize, dead as usize);

        // A leaf can also be sparse on *active* alone while its *raw*
        // footprint (active+dead) stays large, without ever having gone
        // through a cold-offloading split yet -- e.g. a run of plain,
        // never-updated single-version deletes under one still-open
        // transaction, where every record is its own key's "newest" entry
        // (see `grouped_hot_cold_records`) and so can never be shed to a
        // cold chain in the first place, not even in principle, until that
        // transaction resolves. Classifying that leaf `ActiveUnderflow` on
        // active count alone routes it into `on_underflow_node`/`merge()`,
        // which can only combine it with a sibling if their *combined* raw
        // footprint fits one page -- if the sibling is itself substantial
        // (e.g. untouched, still fully live), that combined footprint can
        // never shrink below capacity while the same transaction protects
        // it, so `merge()` falls to `KeySplit` every time and reproduces
        // the identical two fences it just replaced: zero progress, forever
        // (confirmed via `insert_then_delete_same_keys_leaves_tree_empty`'s
        // single-threaded, 100%-reproducible repro). Requiring the raw
        // footprint to also clear the underflow bar leaves such a leaf
        // `Ok` instead -- the traversal simply descends through it (still
        // well under capacity) rather than forcing a merge attempt that
        // cannot converge until the protecting transaction finishes.
        //
        // That raw-footprint check is deliberately blind to *why* `dead` is
        // large -- it can't tell "still protected by an open transaction"
        // apart from "was protected once, but that transaction committed
        // long ago and every one of these entries is now permanently,
        // safely dead." A leaf built up entirely from one-shot,
        // never-reinserted deletes (so nothing here is cold-offload
        // eligible either — see the branch above) can accumulate enough
        // such garbage to sit above the 20% bar forever, even at
        // `active == 0`, leaving it permanently exempt from underflow and
        // permanently un-mergeable — confirmed via
        // `ascending_insert_then_random_order_delete_leaves_tree_empty`/
        // `descending_insert_then_random_order_delete_leaves_tree_empty`
        // (`tests/crud_persistence_tests.rs`): every leaf across a
        // fully-deleted 5-level tree stayed `Ok`, so nothing ever merged.
        // `has_protected_garbage` resolves the ambiguity directly: if
        // `active` alone is already sparse and none of the "extra" raw
        // footprint is still protected, this leaf's true, live-at-rebuild
        // content is just its own live records (zero here) -- merging it
        // is always genuine progress, not a repeat of the same fences,
        // since a rebuild would simply drop every unprotected dead entry
        // rather than carry it forward. Only reached once the cheap
        // raw-footprint check above has already failed to classify this as
        // underflow, so a healthy, busy leaf never pays this scan.
        if self.is_leaf() {
            return if active + dead >= self.overflow_units_count() {
                BlockUnsafeDegree::Overflow
            } else if active + dead <= self.filling_20_percent() {
                BlockUnsafeDegree::ActiveUnderflow
            } else if active <= self.filling_20_percent() && !self.has_protected_garbage(ctx) {
                BlockUnsafeDegree::ActiveUnderflow
            } else {
                BlockUnsafeDegree::Ok
            };
        }

        let one_d = self.filling_20_percent();

        if active <= one_d {
            BlockUnsafeDegree::ActiveUnderflow
        } else {
            let overflow_units_count = self.overflow_units_count();

            let is_overflow = active + dead >= overflow_units_count;

            if is_overflow && active <= one_d * 2 {
                BlockUnsafeDegree::ActiveUnderflow
            } else if is_overflow {
                BlockUnsafeDegree::Overflow
            } else {
                BlockUnsafeDegree::Ok
            }
        }
    }

    #[inline(always)]
    pub fn unsafe_degree_root(&self) -> BlockUnsafeDegree {
        let (active, dead) = self.active_dead_count();

        let (active, dead) = (active as usize, dead as usize);

        let is_leaf = self.is_leaf();

        if active == 1 && !is_leaf {
            // single child
            BlockUnsafeDegree::ActiveUnderflow
        } else if active + dead >= self.overflow_units_count() {
            BlockUnsafeDegree::Overflow
        } else {
            BlockUnsafeDegree::Ok
        }
    }

    // #[inline(always)]
    // pub fn min_active_units(&self) -> usize { // 20%
    // match self.is_leaf() {
    //     true => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_records(),
    //     false => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_keys()
    // }
    // }

    // #[inline(always)]
    // pub fn max_active_units(&self) -> usize { // 80%
    //     match self.is_leaf() {
    //         true => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_records() * 2,
    //         false => BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_keys() * 2
    //     }
    // }

    #[inline(always)]
    pub fn max_units(&self) -> usize {
        // absolute units
        match self.is_leaf() {
            true => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_records(),
            false => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_keys(),
        }
    }

    #[inline(always)]
    pub fn filling_40_percent(&self) -> usize {
        // 40%
        (2 * self.max_units() + 4) / 5
    }

    #[inline(always)]
    pub fn filling_80_percent(&self) -> usize {
        // 80%
        (4 * self.max_units() + 4) / 5
    }

    #[inline(always)]
    pub fn filling_20_percent(&self) -> usize {
        // 20%
        (self.max_units() + 4) / 5
    }

    #[inline(always)]
    pub fn overflow_units_count(&self) -> usize {
        // trigger for overflow
        match self.is_leaf() {
            true => {
                BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_records_count()
            }
            false => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_keys_count(),
        }
    }

    #[inline(always)]
    pub(crate) fn active_dead_count(&self) -> (u32, u32) {
        match self.as_page_ref() {
            PageType::IndexRef(internal_page) => internal_page.active_dead_count(),
            PageType::LeafRef(leaf_page) => leaf_page.active_dead_count(),
            _ => unreachable!(),
        }
    }

    // #[inline(always)]
    // pub(crate) fn active_dead(&self) -> (usize, usize) {
    //     match self.as_ref() {
    //         Node::Index(internal_page) =>
    //             internal_page.active_dead(),
    //         Node::Leaf(leaf_page) =>
    //             leaf_page.active_dead()
    //     }
    // }
}

pub(crate) enum BlockSplit<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> {
    ByKey(
        Interval<Key>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        Interval<Key>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ),
    ByVersion(BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>),
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload>
{
}

pub(crate) enum MergeResult<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default,
> {
    Merged(
        usize,
        Interval<Key>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        // Already retired (`SmartGuard::try_retire`) by the time
        // `merge()` builds this — see that call site's doc — so this is
        // a bare cell, not a guard: there's no lock left to hold or
        // later release.
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ),
    KeySplit(
        usize,
        BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload>,
        BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ),
    Error,
}

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + 'static + Display,
    Payload: Display + Clone + Default + Sync + 'static,
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn on_overflow_node<'a>(
        &self,
        mufasa: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        child_index: usize,
    ) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()> {
        // `mufasa` may already be a `Writer` carried over from a *different*
        // child's overflow/underflow round earlier in this same traversal —
        // `SmartGuard::upgrade_write_lock` is a no-op once a guard is
        // already a `Writer` (no version-CAS re-validates capacity the way
        // it does the *first* time a `Reader` upgrades). A split always
        // needs room for up to 2 fresh entries; without this check, a
        // second round landing on an already-full `mufasa` writes past
        // `FAN_OUT` — confirmed empirically (not just in theory): this
        // raced in practice, corrupting `pointer_region`'s adjacent memory
        // before the array's own bounds check turned it into a panic
        // instead. If there isn't room, `mufasa` now needs splitting
        // itself — which a fresh `unsafe_degree()` check one level up (or,
        // if `mufasa` is the root, `retrieve_root_write_olc`'s own
        // `unsafe_degree_root()` check, reached the same way on retry)
        // will correctly detect — so bail out and force a restart rather
        // than corrupt it. Root and non-root `mufasa` share this same
        // fixed-size-array capacity constraint identically; `split_root`/
        // `merge_root` don't need the same guard because they only ever
        // push into a *freshly allocated* page, never one that could have
        // already absorbed an earlier round.
        //
        // This was written and documented but left disabled — confirmed via
        // `generate` on a large population: `rand_query`'s per-level
        // *random* child selection (unlike the key-routed write path) makes
        // a second overflow/underflow round landing on the same already-near-
        // capacity `mufasa` far more likely once the tree is large enough for
        // several of its children to be near their own thresholds at once —
        // reproduced as exactly the corruption this comment predicted:
        // `push_uncommitted`'s `index == FAN_OUT` bounds panic.
        //
        // Must be the narrow "is there literally no room for 2 more
        // entries" check (`lacks_room_for_split_entries`, not the full
        // `unsafe_degree()`) — `unsafe_degree().is_unsafe()` also trips on
        // plain `ActiveUnderflow` (`active <= one_d`, ~20% of capacity),
        // which is the *normal*, expected state for almost every internal
        // page in a small/young tree (plenty of physical room, just not
        // many children yet) — using the broad check here permanently
        // blocks every split attempt on such a `mufasa` (its "too few
        // children" never resolves on its own, and nothing above it can fix
        // it either, since the ancestor hits the identical false trip), a
        // total deadlock confirmed via `generate` stalling at ~234 keys,
        // essentially immediately.
        //
        // Originally written as `mufasa.is_overflow()` — *not* equivalent to
        // `lacks_room_for_split_entries()` despite looking narrower than
        // `unsafe_degree()`: `is_overflow()` goes through the exact same
        // `ActiveUnderflow`-reclassification `unsafe_degree()` does (see
        // that method's doc), so it silently returned `false` — letting
        // this guard through — for a `mufasa` that's mostly dead-but-still-
        // protected entries with a low active count, even at raw sum_len
        // already at/past capacity. Confirmed as the actual mechanism behind
        // a real crash under sustained TPC-C load (long-held reader
        // snapshots keep exactly this many dead-but-protected entries
        // around): `push_uncommitted` then writes past `FAN_OUT` for real,
        // corrupting `pointer_region`'s adjacent memory with no bounds check
        // ever tripping first.
        if mufasa.lacks_room_for_split_entries() {
            return Err(());
        }

        let mufasa_deref_mut = mufasa.deref_mut();

        let internal_page = mufasa_deref_mut.as_internal_page();

        let fence = internal_page.get_key(child_index).clone();

        let current_len = internal_page.sum_len();

        if DIAG && format!("{}", fence.upper) == "18446744073709551615" {
            eprintln!(
                "DIAG on_overflow_node ENTER thread={:#x} page={:p} child_index={child_index} current_len={current_len} fence=[{},{}] sum_len_before={}",
                diag_thread_hash(),
                internal_page as *const _,
                fence.lower,
                fence.upper,
                internal_page.sum_len()
            );
        }

        // `simba`'s content is fully consumed here (copied into `left`/
        // `right`, then `simba` itself is retired) — this isn't a read-only
        // traversal step that can tolerate staleness, so `simba` needs
        // genuine exclusion for the read, the same way `mufasa` already has
        // it and `merge()`'s `candidate` already gets. A version-check
        // after the fact can't substitute for this: `cell_version` is
        // pinned at one constant value for a writer's *entire* critical
        // section (only the unlock bumps it), so a before/after comparison
        // is blind to a writer that's already mid-flight when the "before"
        // sample is taken and still mid-flight at "after" — exactly the
        // race this project confirmed happening in practice.
        //
        // `try_retire()`, not `upgrade_write_lock()`: `simba` is never
        // mutated in place here (`split()` only reads it, building fresh
        // `left`/`right` pages elsewhere), and once this call decides to
        // split `simba` at all, it's unconditionally committed — `split()`
        // has no failure path, so there's no later "actually, never mind"
        // that would need `simba`'s exclusion to be reversible. That's
        // exactly the condition `try_retire`'s doc requires: a single CAS
        // straight to permanently retired, skipping the intermediate
        // locked state entirely. Bail and restart the whole traversal if
        // someone else got to `simba` first (wrote to it, or is retiring it
        // via some other path) — the same recovery a failed
        // `upgrade_write_lock` would need.
        let simba_cell = match simba.try_retire() {
            Ok(cell) => cell,
            Err(..) => return Err(()),
        };

        let version = match self.split(simba_cell.deref(), &fence) {
            BlockSplit::ByKey(left_fence, left, right_fence, right) => {
                let version = self.start_tx_commit();

                internal_page.push_uncommitted(left_fence, version, left, current_len);

                internal_page.push_uncommitted(right_fence, version, right, current_len + 1);

                internal_page.commit_delta(1, 1);
                if DIAG && format!("{}", right_fence.upper) == "18446744073709551615" {
                    eprintln!(
                        "DIAG on_overflow_node ByKey thread={:#x} page={:p} child_index={child_index} superseded, pushed left=[{},{}]@{current_len} right=[{},{}]@{}",
                        diag_thread_hash(),
                        internal_page as *const _,
                        left_fence.lower,
                        left_fence.upper,
                        right_fence.lower,
                        right_fence.upper,
                        current_len + 1
                    );
                }
                version
            }
            BlockSplit::ByVersion(fresh) => {
                let version = self.start_tx_commit();

                internal_page.push_uncommitted(fence, version, fresh, current_len);

                internal_page.commit_delta(0, 1);
                if DIAG && format!("{}", fence.upper) == "18446744073709551615" {
                    eprintln!(
                        "DIAG on_overflow_node ByVersion thread={:#x} page={:p} child_index={child_index} superseded, pushed fence=[{},{}]@{current_len}",
                        diag_thread_hash(),
                        internal_page as *const _,
                        fence.lower,
                        fence.upper
                    );
                }
                version
            }
        };

        // Registers the *new* entry's birth version as this old child's
        // death — not `internal_page.get_version(child_index)` (the old
        // child's own, much older birth version), which was the bug: an
        // active reader whose snapshot predates `version` still needs to
        // route through this now-obsoleted entry (that's exactly what makes
        // it "obsolete" rather than "gone" — see `TimeMatcher::matched`), so
        // registering the block as dead using its own birth version made it
        // eligible for GC reuse immediately, long before every such reader
        // was done with it — a premature-reclaim bug that could hand this
        // block to an unrelated concurrent writer while a reader was still
        // (or about to start) traversing into it.
        //
        // `simba_cell` is already retired (via `try_retire()` above) by the
        // time we get here — nothing left to do but hand it to the tracker.
        self.block_manager
            .register_dead(self.worker_id(), version, simba_cell);
        Ok(mufasa)
    }

    pub(crate) fn on_underflow_node<'a>(
        &self,
        mufasa: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
        index_simba: usize,
    ) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()> {
        if VERBOSE {
            println!("on_underflow_node");
        }

        // See `on_overflow_node`'s matching comment (including why this was
        // written but left disabled, why it must be the narrow
        // `lacks_room_for_split_entries()` capacity check and not the broad
        // `unsafe_degree().is_unsafe()`, why plain `is_overflow()` was *not*
        // equivalent to that narrow check either, and how `generate`
        // reproduces each failure mode in turn): a merge can need up to 2
        // fresh entries (`MergeResult::KeySplit`), and `mufasa` gets the
        // same "already a Writer from an earlier round in this traversal,
        // no re-validated capacity" exposure. Checked conservatively for
        // both outcomes before doing any of the (otherwise wasted) work
        // below, since which one `merge()` produces isn't known yet.
        if mufasa.lacks_room_for_split_entries() {
            return Err(());
        }

        let mufasa_deref_mut = mufasa.deref_mut();

        // `simba` arrives here as a plain `Reader`, same as `merge()`'s own
        // `candidate` — but unlike `candidate`, this call *can* still fail
        // below (`merge()` returns `MergeResult::Error` when
        // `compute_candidate` finds no sibling), and by the time that's
        // known, `simba` is already retired. Rather than keep `simba` on
        // the reversible `Writer` path for that reason, retire it upfront
        // and explicitly *un*-retire (`clear_retired`) on that one failure
        // exit: sound because nothing else can observe or act on a retired
        // cell before it's registered as dead (`register_dead_col`, below,
        // gated on `merge()` actually succeeding) — no CAS can ever match a
        // retired value, so no concurrent writer/retirer can race in
        // during the window before the revert. Saves an atomic op on the
        // success path (no separate `retire()` unlock-and-mark-dead store
        // needed) at the cost of an explicit revert on the one failure
        // path — if `merge()` ever grows a *second* way to fail after
        // `simba` is retired, that path needs the same `clear_retired()`
        // call, since nothing enforces it structurally the way a `Writer`
        // guard's `Drop` would.
        let simba_cell = match simba.try_retire() {
            Ok(cell) => cell,
            Err(..) => return Err(()),
        };

        match self.merge(mufasa_deref_mut, simba_cell.deref(), index_simba) {
            MergeResult::Merged(index_sibling, fence_sibling, merged_block, candidate_cell) => {
                if VERBOSE {
                    println!(
                        "MergeResult::Merged: Simba-fence: {} - Sibling-fence: {}",
                        mufasa_deref_mut.as_internal_page_ref().get_key(index_simba),
                        fence_sibling
                    );
                }
                let mufasa_internal_page = mufasa_deref_mut.as_internal_page();

                let mufasa_len = mufasa_internal_page.sum_len();

                let mut merged_fence = mufasa_internal_page.get_key(index_simba).clone();

                merged_fence.merged(&fence_sibling);

                let version = self.start_tx_commit();

                mufasa_internal_page.push_uncommitted(
                    merged_fence,
                    version,
                    merged_block,
                    mufasa_len,
                );

                mufasa_internal_page.commit_delta(-1, 2);

                if DIAG && format!("{}", merged_fence.upper) == "18446744073709551615" {
                    eprintln!(
                        "DIAG on_underflow_node Merged thread={:#x} page={:p} index_simba={index_simba} index_sibling={index_sibling} superseded, pushed merged=[{},{}]@{mufasa_len}",
                        diag_thread_hash(),
                        mufasa_internal_page as *const _,
                        merged_fence.lower,
                        merged_fence.upper
                    );
                }

                // See `on_overflow_node`'s matching comment: the new
                // `merged_block` entry's birth version (`version`) is the
                // correct death point for these two now-obsoleted entries,
                // not their own (older) birth versions. Both `simba_cell`
                // and `candidate_cell` are already retired by this point
                // (this function's own `try_retire()`, and `merge()`'s),
                // so they're used as-is.
                self.block_manager.register_dead_col(
                    self.worker_id(),
                    [(version, simba_cell), (version, candidate_cell)],
                )
            }
            MergeResult::KeySplit(
                index_sibling,
                BlockSplit::ByKey(left_interval, left, right_interval, right),
                candidate_cell,
            ) => {
                if VERBOSE {
                    unsafe {
                        println!(
                            "MergeResult::KeySplit: \
                       \tleft-fence: {}, \
                       \tright-fence: {}.\
                        \n\tSimba-fence: {} - Sibling-fence: {}\n\
                        \tsimba:\n{}",
                            left_interval,
                            right_interval,
                            mufasa_deref_mut.keys().get_unchecked(index_simba),
                            mufasa_deref_mut.keys().get_unchecked(index_sibling),
                            simba_cell.deref().node_data.as_ref()
                        );
                    }
                }
                let mufasa_internal_page = mufasa_deref_mut.as_internal_page();

                let mufasa_len = mufasa_internal_page.sum_len();

                let version = self.start_tx_commit();

                mufasa_internal_page.push_uncommitted(left_interval, version, left, mufasa_len);

                mufasa_internal_page.push_uncommitted(
                    right_interval,
                    version,
                    right,
                    mufasa_len + 1,
                );

                mufasa_internal_page.commit_delta(0, 2);

                if DIAG && format!("{}", right_interval.upper) == "18446744073709551615" {
                    eprintln!(
                        "DIAG on_underflow_node KeySplit thread={:#x} page={:p} index_simba={index_simba} index_sibling={index_sibling} superseded, pushed left=[{},{}]@{mufasa_len} right=[{},{}]@{}",
                        diag_thread_hash(),
                        mufasa_internal_page as *const _,
                        left_interval.lower,
                        left_interval.upper,
                        right_interval.lower,
                        right_interval.upper,
                        mufasa_len + 1
                    );
                }

                // See the `Merged` arm's matching comment.
                self.block_manager.register_dead_col(
                    self.worker_id(),
                    [(version, simba_cell), (version, candidate_cell)],
                )
            }
            // `merge()` failed (`compute_candidate` found no sibling) —
            // `simba_cell` was already retired above in anticipation of
            // success; since that didn't happen, undo it so `simba` is
            // exactly as live as it was before this call, for whoever
            // retries next. See this function's own doc for why this
            // revert is sound.
            _ => {
                simba_cell.clear_retired();
                return Err(());
            }
        }

        Ok(mufasa)
    }

    /// Whether a record must be carried forward by a version-split/merge's
    /// GC compaction — `is_live()` plus one more case: a record that's
    /// *deleted* (not invalidated) by a transaction whose `ts_start` is
    /// still registered as in-flight (`TxContext::is_snapshot_live`) is not
    /// safe to discard yet, even though `is_live()` already reports it
    /// dead. `is_deleted()`/`is_live()` are pure local bookkeeping with no
    /// notion of commit status — OSIC's "instant commit" model means
    /// `delete()` marks a record dead the moment `DbTransaction::update`/
    /// `delete` calls it, well before (and regardless of whether) that
    /// transaction ever actually commits. If it aborts instead (dropped
    /// without `commit()` — e.g. New-Order's own ~1%-of-transactions
    /// business-logic rollback, `bat_bench::tpcc_txn::new_order`, which can
    /// abort *after* having already updated `District`), reversing that
    /// delete (`LeafPage::apply_invalidate`/`apply_undelete`) requires the
    /// predecessor record to still be physically present to undelete. A
    /// version-split/merge racing in between — reading `is_deleted()` as
    /// `true` and discarding the record, exactly as it should for a
    /// *committed* delete — makes that reversal silently impossible
    /// instead, permanently losing the key: confirmed empirically as the
    /// root cause of `bat_bench::tpcc_txn`'s District
    /// `ZeroAffected(KeyDoesNotExist)` panic under `gc=on` at high thread
    /// counts.
    ///
    /// Deliberately does *not* extend the same protection to an invalidated
    /// `insert_stamp` (an aborted `Insert`/the new half of an `Update`):
    /// invalidation only ever happens as part of that same abort-reversal
    /// (never speculatively ahead of it, unlike a plain `delete()`), so by
    /// the time a record is invalid its owning transaction has already
    /// fully resolved — there's no future "undo" that still needs it kept
    /// around.
    ///
    /// Other readers do not require their visible predecessor to be copied
    /// into a replacement page. A reader older than the split/merge routes
    /// through the retained source block, whose replacement birth version
    /// is also its death version; `live_min_snapshot` prevents that block's
    /// reuse until the reader finishes. If the deleting transaction is
    /// still unresolved when the SMO runs, the check below copies the
    /// predecessor for abort safety and for readers born against that new
    /// structural version. Once deletion commits, future readers see it.
    #[inline]
    #[cfg(not(feature = "lightweight-gc"))]
    /// Precise GC: keep record if its deletion's snapshot ID is one of the
    /// currently-active snapshots. Uses pre-computed HashSet for O(1) lookups.
    /// Avoids keeping unnecessarily old deletions.
    pub(crate) fn record_survives_gc(
        &self,
        version: &crate::bat_record_model::version_info::VersionInfo,
        live_snapshots: &std::collections::HashSet<u64>,
    ) -> bool {
        if version.is_live() {
            return true;
        }

        !version.insertion_stamp().is_invalid()
            && version
                .deletion_stamp()
                .is_some_and(|del| live_snapshots.contains(&del.ts_start()))
    }

    #[cfg(feature = "lightweight-gc")]
    /// Lightweight GC: keep record if its deletion happened at or after the
    /// oldest live snapshot (conservative but O(1): just compares against min).
    /// May keep some dead records but avoids HashSet collection overhead.
    #[allow(unused_variables)]
    pub(crate) fn record_survives_gc(
        &self,
        version: &crate::bat_record_model::version_info::VersionInfo,
        live_snapshots: &std::collections::HashSet<u64>,
    ) -> bool {
        if version.is_live() {
            return true;
        }

        !version.insertion_stamp().is_invalid()
            && version
                .deletion_stamp()
                .is_some_and(|del| {
                    self.ctx
                        .live_min_snapshot()
                        .map_or(false, |min| del.ts_start() >= min)
                })
    }

    /// Collect all currently-live transaction snapshot IDs for efficient GC filtering.
    fn live_snapshots(&self) -> std::collections::HashSet<u64> {
        self.ctx.live_snapshots_set()
    }

    /// Returns all records owned by this leaf generation, in physical order.
    fn retained_owned_leaf_history(
        &self,
        block: &Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
    ) -> Vec<RecordPoint<Key, Payload>> {
        let live_snapshots = self.live_snapshots();
        let mut history = Vec::with_capacity(block.as_records().len());
        for record in block.as_records().iter() {
            if self.record_survives_gc(record.version(), &live_snapshots) {
                history.push(RecordPoint::clone_from_leaf(record));
            }
        }
        history
    }

    /// Identifies, per key, its write-facing "hot" record: the live record
    /// for a key, or (if no live record exists) that key's most recent
    /// version. Used only to *find* a real key-boundary below -- unlike the
    /// old cold-chain design, every input record is still written back onto
    /// a page somewhere by this method's callers; nothing here is a filter
    /// that drops records outright. A dead-but-still-protected non-newest
    /// record (e.g. a delete-after-update's own predecessor while its
    /// transaction remains uncommitted -- see `record_survives_gc`) has no
    /// copy anywhere else: it was born within this leaf's current
    /// generation, so it isn't reachable via any earlier, untouched page,
    /// and must stay physically present for `version_handle::abort_writes`
    /// to still find it.
    fn hot_records_only(
        &self,
        records: &[RecordPoint<Key, Payload>],
    ) -> Vec<usize> {
        let mut newest_valid = vec![false; records.len()];
        let mut group_start = 0;
        while group_start < records.len() {
            let key = records[group_start].key();
            let mut group_end = group_start + 1;
            while group_end < records.len() && records[group_end].key() == key {
                group_end += 1;
            }
            if let Some(i) = (group_start..group_end)
                .rev()
                .find(|&i| !records[i].version().insertion_stamp().is_invalid())
            {
                newest_valid[i] = true;
            }
            group_start = group_end;
        }

        (0..records.len())
            .filter(|&i| records[i].version().is_live() || newest_valid[i])
            .collect()
    }

    fn populate_leaf_history(
        &self,
        page: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        mut records: Vec<RecordPoint<Key, Payload>>,
    ) -> usize {
        records.sort_by_key(|record| record.key());
        self.push_records_onto(page, records)
    }

    /// Writes `records` (already in the physical order they should land in)
    /// onto `page`.
    fn push_records_onto(
        &self,
        page: BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
        records: Vec<RecordPoint<Key, Payload>>,
    ) -> usize {
        let count = records.len();
        page.unsafe_borrow_mut()
            .as_leaf_page()
            .bulk_push_owned(records);
        count
    }

    /// Retries a real key-boundary split using only the write-facing (`hot`)
    /// subset of a batch `fitting_key_boundary` already rejected outright
    /// against the raw, garbage-inflated `records`. Finding a boundary
    /// against the reduced hot-key set first can locate a split point that
    /// didn't exist before: it shrinks a key's version run down to at most
    /// one hot entry (for boundary-search purposes only) without ever
    /// removing the key itself, so a batch that looked like one indivisible
    /// run of records can resolve into a real two-key-range split once its
    /// non-hot entries are set aside from the search. Every record --
    /// hot or not -- is still written back to one side or the other of the
    /// split below; nothing here decides what physically survives.
    ///
    /// `Err` means even the fully-reduced hot-key set has no such boundary
    /// -- either the whole batch is a single key's own history, or the
    /// boundary found doesn't leave enough physical room on one side once
    /// that side's non-hot entries are added back -- in which case the
    /// caller's only remaining option is a single combined leaf.
    fn try_hot_key_split(
        &self,
        mut records: Vec<RecordPoint<Key, Payload>>,
        capacity: usize,
    ) -> Result<HotKeySplit<Key, Payload>, Vec<RecordPoint<Key, Payload>>> {
        records.sort_by_key(|record| record.key());
        let hot_indices = self.hot_records_only(&records);
        let hot_keys: Vec<Key> = hot_indices.iter().map(|&i| records[i].key()).collect();
        match fitting_key_boundary(&hot_keys, hot_keys.len() / 2, capacity, |k| *k) {
            Some(mid) => {
                let split_key = hot_keys[mid];
                let (left, right): (Vec<_>, Vec<_>) =
                    records.into_iter().partition(|r| r.key() < split_key);
                if left.len() > capacity || right.len() > capacity {
                    let mut all = left;
                    all.extend(right);
                    return Err(all);
                }
                Ok(HotKeySplit {
                    split_key,
                    left,
                    right,
                })
            }
            None => Err(records),
        }
    }

    pub(crate) fn merge<'a>(
        &self,
        mufasa: &'a Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: &Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba_index: usize,
    ) -> MergeResult<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let mufasa_internal_page = mufasa.as_internal_page_ref();

        let is_simba_leaf = simba.is_leaf();

        let simba_fence = mufasa_internal_page.get_key(simba_index);

        let simba_max_units = simba.max_units();

        let (simba_active_count, _simba_dead_count) = simba.active_dead_count();

        let (simba_active_count, _simba_dead_count) =
            (simba_active_count as usize, _simba_dead_count as usize);

        let mufasa_children = mufasa_internal_page.children();

        let mut all_candidates = mufasa_children
            .iter()
            .enumerate()
            .zip(mufasa_internal_page.versions())
            .zip(mufasa_internal_page.keys())
            .filter(|(((index, ..), ..), ..)| *index != simba_index)
            .filter(|(((index, ..), ..), ..)| mufasa_internal_page.is_slot_live(*index))
            .sorted_by_key(|(.., fence)| fence.lower())
            .map(|(((index, bro), ..), fence)| (index, bro, fence))
            .collect_vec();

        // Picking blindly between two adjacent siblings (the old
        // right-then-left-fallback order below) can land on one that's
        // already nearly full itself: merging `simba` into it then can't
        // qualify for `Merged` (single combined leaf, this fn's real,
        // child-count-reducing outcome) and instead falls through to
        // `KeySplit` — safe, but a pure repartition of two already-full
        // leaves into two other full leaves, achieving nothing `mufasa`
        // actually needed, and wasted allocation/copy work to boot. When
        // both an adjacent left and right sibling exist, peek at each
        // one's own `active_dead_count()` (already loaded elsewhere in
        // this fn for the same purpose - a single atomic read, no per-
        // record scan) and prefer whichever has more room, so a genuinely
        // emptier neighbor on the "wrong" side isn't skipped over in favor
        // of a fuller one just because of fence order. A stale peek here
        // (the candidate isn't retired for this) only risks picking the
        // still-valid but slightly worse of the two — the actual merge
        // decision below re-reads whichever one is chosen fresh, once
        // retired, exactly as before.
        let mut compute_candidate = || {
            let insertion_point =
                match all_candidates.binary_search_by_key(&simba_fence.lower, |(.., f)| f.lower) {
                    Ok(index) => return Ok(all_candidates.remove(index)),
                    Err(index) => index,
                };

            let right = (insertion_point < all_candidates.len()).then_some(insertion_point);
            let left = insertion_point.checked_sub(1);

            let chosen = match (left, right) {
                (Some(l), Some(r)) => {
                    let (l_active, l_dead) = all_candidates[l].1.active_dead_count();
                    let (r_active, r_dead) = all_candidates[r].1.active_dead_count();
                    if l_active as usize + l_dead as usize <= r_active as usize + r_dead as usize {
                        l
                    } else {
                        r
                    }
                }
                (Some(l), None) => l,
                (None, Some(r)) => r,
                (None, None) => return Err(()),
            };

            Ok(all_candidates.remove(chosen))
        };

        let (candidate_index, candidate_block, candidate_fence) = match compute_candidate() {
            Ok(triple) => triple,
            Err(()) => return MergeResult::Error,
        };

        // `try_retire()`, not `upgrade_write_lock()`: `candidate` is never
        // mutated in place below (only read, to build `combined_block`/the
        // key-split halves), and once selected here it's unconditionally
        // used — nothing past this point in `merge()` can still decide not
        // to use it. See `SmartGuard::try_retire`'s doc for why that's
        // exactly the condition that makes skipping the reversible
        // `Writer` phase sound. `simba` (the caller's, not `candidate`)
        // stays on the ordinary write-lock path precisely because *this*
        // function can still fail above (`compute_candidate` finding
        // nothing) — `simba`'s exclusion has to stay reversible for that,
        // `candidate`'s doesn't once we're here.
        //
        // A failed retire (someone else concurrently retired, or is
        // retiring, this exact candidate first) bails out to `Error` rather
        // than immediately trying the next sibling in `all_candidates`:
        // tried once (see git history), but retrying in a tight loop here
        // has no backoff at all, unlike the caller's own retry path
        // (`sched_yield`'s jittered backoff) - under real contention that
        // spins fast enough to starve every thread racing on the same
        // region instead of ever making progress, confirmed empirically as
        // several `tree_wal_consistency_tests` cases hanging for 60+
        // seconds. Bailing to `Error` costs a full traversal restart, but
        // restarts here go through the same backoff the rest of the OLC
        // retry machinery already relies on.
        let candidate_cell = match candidate_block.borrow_read().try_retire() {
            Ok(cell) => cell,
            Err(..) => return MergeResult::Error,
        };

        all_candidates.clear();

        let (candidate_active_count, _candidate_dead_count) =
            candidate_cell.deref().active_dead_count();

        let candidate_active_count = candidate_active_count as usize;

        // Leaf-only: the records a "merge into one combined leaf" below
        // actually pushes are everything `record_survives_gc` keeps —
        // every active record *and* any dead-but-still-snapshot-protected
        // one (see that fn's doc) — which can run well ahead of
        // `active_count` alone under heavy concurrent load (long-lived
        // readers/transactions keeping old deleted versions alive). The
        // 80%-active-count threshold below only bounds the *active* total,
        // so two leaves that each individually pass it can still jointly
        // overflow `NUM_RECORDS` once their protected-dead records are
        // combined — confirmed as a real, repeatable crash (`LeafPage::
        // bulk_push` panicking with 126-134 records for `NUM_RECORDS=125`)
        // under a multi-table TPC-C workload with sustained transactions
        // and OLAP scans holding snapshots open long enough to accumulate
        // exactly this. Falling through to the `KeySplit` branch instead is
        // always safe here: it already computes the real survivor count
        // (same `record_survives_gc` filter) and correctly spreads it
        // across *two* leaves via `nearest_key_boundary`, so routing there
        // whenever the single-leaf merge wouldn't fit costs nothing extra
        // in the common case (this closure only runs when `is_simba_leaf`)
        // and cannot itself overflow.
        let retained_leaf_histories = is_simba_leaf.then(|| {
            let mut records = self.retained_owned_leaf_history(simba);
            records.extend(self.retained_owned_leaf_history(candidate_cell.deref()));
            records
        });
        let leaf_merge_would_overflow = retained_leaf_histories
            .as_ref()
            .is_some_and(|records| records.len() > simba_max_units);

        if !leaf_merge_would_overflow
            && candidate_active_count + simba_active_count <= ((4 * simba_max_units) / 5)
        {
            // <= 80% ok merge
            let combined_block = match is_simba_leaf {
                false => {
                    let combined_block = self.block_manager.new_empty_index_block(&self.ctx);

                    let (keys, versions, pointers) =
                        simba.as_internal_page_ref().keys_versions_pointers();

                    let simba_internal = simba.as_internal_page_ref();

                    let (c_keys, c_versions, c_pointers) = candidate_cell
                        .deref()
                        .as_internal_page_ref()
                        .keys_versions_pointers();

                    let candidate_internal = candidate_cell.deref().as_internal_page_ref();

                    let shadow_copy = keys
                        .iter()
                        .zip(versions.iter().copied())
                        .zip(pointers.iter())
                        .enumerate()
                        .filter(|(index, ..)| simba_internal.is_slot_live(*index))
                        .map(|(_, rest)| rest)
                        .merge_by(
                            c_keys
                                .iter()
                                .zip(c_versions.iter().copied())
                                .zip(c_pointers.iter())
                                .enumerate()
                                .filter(|(index, ..)| candidate_internal.is_slot_live(*index))
                                .map(|(_, rest)| rest),
                            |((.., v0), ..), ((.., v1), ..)| v0 <= v1,
                        )
                        .collect_vec();

                    combined_block
                        .unsafe_borrow_mut()
                        .as_internal_page()
                        .bulk_push(shadow_copy);

                    combined_block
                }
                true => {
                    let combined_block = self.block_manager.new_empty_leaf(&self.ctx);

                    // Plain concatenation, not a `ts_start`-ordered
                    // `merge_by`: `simba` and `candidate` are distinct
                    // siblings in `mufasa`'s own child list, so their fences
                    // are disjoint by construction and no key can appear in
                    // both. Every physical-order-sensitive lookup downstream
                    // (`LeafPage::is_live_lineage`/`rfind`) filters on exact
                    // key match first, so it only ever compares two records
                    // of the *same* key against each other — which, since
                    // that key's whole chain necessarily comes from just one
                    // of these two sources, always stays in that source's
                    // own original relative order regardless of how the two
                    // sources are interleaved with each other here. Unlike
                    // the `KeySplit` branches' identical-looking comment
                    // (see those for the actual hazard), there's no live-
                    // ahead-of-dead reordering risk to avoid in the first
                    // place — nothing here spans more than one key's chain.
                    self.populate_leaf_history(
                        combined_block,
                        retained_leaf_histories.expect("leaf histories"),
                    );

                    if TRACE_KEY_DEBUG {
                        push_trace(format!(
                            "TRACE merge::Merged(leaf) thread={:#x} simba={:p} simba_fence={} simba_live={} candidate={:p} candidate_fence={} candidate_live={} -> combined={:p} combined_live={}",
                            diag_thread_hash(),
                            simba,
                            simba_fence,
                            simba
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            candidate_cell.deref(),
                            candidate_fence,
                            candidate_cell
                                .deref()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            combined_block.unsafe_borrow(),
                            combined_block
                                .unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(",")
                        ));
                    }

                    combined_block
                }
            };

            MergeResult::Merged(
                candidate_index,
                candidate_fence.clone(),
                combined_block,
                candidate_cell,
            )
        } else {
            // Keysplit when merged: > 80% active entries ---> redistribute the keys
            match is_simba_leaf {
                true => unsafe {
                    let candidate_records = candidate_cell.deref().as_records();
                    let simba_records = simba.as_records();
                    let records = retained_leaf_histories.expect("leaf histories");
                    let mut sorted_records = records.iter().collect_vec();
                    sorted_records.sort_by_key(|r| r.key());
                    let Some(middle) = fitting_key_boundary(
                        &sorted_records,
                        sorted_records.len() / 2,
                        simba_max_units,
                        |r| r.key(),
                    ) else {
                        // No non-tearing key split of the raw (garbage-
                        // inflated) combined history converges. Retry
                        // against just the write-facing subset -- shedding
                        // cold-eligible garbage first can free up a boundary
                        // this couldn't find (see `try_hot_key_split`).
                        return match self.try_hot_key_split(records, simba_max_units) {
                            Ok(hks) => {
                                let left_interval = Interval::new(
                                    candidate_fence.lower.min(simba_fence.lower),
                                    (self.cold.dec_key)(hks.split_key),
                                );
                                let right_interval = Interval::new(
                                    hks.split_key,
                                    candidate_fence.upper.max(simba_fence.upper),
                                );
                                let combined_block_0 = self.block_manager.new_empty_leaf(&self.ctx);
                                let combined_block_1 = self.block_manager.new_empty_leaf(&self.ctx);
                                self.push_records_onto(combined_block_0, hks.left);
                                self.push_records_onto(combined_block_1, hks.right);
                                MergeResult::KeySplit(
                                    candidate_index,
                                    BlockSplit::ByKey(
                                        left_interval,
                                        combined_block_0,
                                        right_interval,
                                        combined_block_1,
                                    ),
                                    candidate_cell,
                                )
                            }
                            Err(records) => {
                                // Truly indivisible even discounting
                                // non-hot entries: the whole batch is one
                                // key's own history. A single combined leaf
                                // is the only converging one-child merge.
                                let combined = self.block_manager.new_empty_leaf(&self.ctx);
                                self.push_records_onto(combined, records);
                                MergeResult::Merged(
                                    candidate_index,
                                    candidate_fence.clone(),
                                    combined,
                                    candidate_cell,
                                )
                            }
                        };
                    };
                    let split_key = sorted_records[middle].key();
                    let (mut first, mut second): (Vec<_>, Vec<_>) =
                        records.into_iter().partition(|r| r.key() < split_key);
                    first.sort_by_key(|r| r.key());
                    second.sort_by_key(|r| r.key());

                    let left_interval = Interval::new(
                        candidate_fence.lower.min(simba_fence.lower),
                        (self.cold.dec_key)(split_key),
                    );

                    let right_interval =
                        Interval::new(split_key, candidate_fence.upper.max(simba_fence.upper));

                    // No re-sort by `insertion_stamp().ts_start()` here (there
                    // used to be one for each half): `joined` is already in
                    // true chain order — each half's own physical write
                    // order, established purely by that write's exclusive-
                    // lock timing — via the stable `sorted_by_key(|r| r.key)`
                    // + `merge_by` above. OSIC lets a transaction's real
                    // write land arbitrarily later than the `ts_start` it
                    // drew at `begin()`, so "physically later" and "larger
                    // `ts_start`" are not the same thing — but every
                    // `rfind`/`is_live_lineage` search downstream depends on
                    // "physically later = actually supersedes". Re-sorting
                    // by `ts_start` here could silently reorder a key's live
                    // record ahead of a dead predecessor with a numerically
                    // larger `ts_start`, so `rfind` (which only ever looks at
                    // physical position) would find the stale dead entry
                    // instead of the live current one — confirmed
                    // empirically as the root cause of
                    // `bat_bench::tpcc_txn`'s Delivery `OrderLine`
                    // `ZeroAffected(KeyAlreadyDeleted)` panic (a diagnostic
                    // dump of the leaf at the panic site showed exactly this
                    // shape: an older, still-live entry sitting before a
                    // newer entry already marked dead by a still-later
                    // write).

                    let combined_block_0 = self.block_manager.new_empty_leaf(&self.ctx);

                    let combined_block_1 = self.block_manager.new_empty_leaf(&self.ctx);

                    self.populate_leaf_history(combined_block_0, first);
                    self.populate_leaf_history(combined_block_1, second);

                    if TRACE_KEY_DEBUG {
                        push_trace(format!(
                            "TRACE merge::KeySplit(leaf) thread={:#x} simba={:p} simba_fence={} simba_live={} candidate={:p} candidate_fence={} candidate_live={} -> left={:p} left_fence={} left_live={} right={:p} right_fence={} right_live={}",
                            diag_thread_hash(),
                            simba,
                            simba_fence,
                            simba_records
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            candidate_cell.deref(),
                            candidate_fence,
                            candidate_records
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            combined_block_0.unsafe_borrow(),
                            left_interval,
                            combined_block_0
                                .unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            combined_block_1.unsafe_borrow(),
                            right_interval,
                            combined_block_1
                                .unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(",")
                        ));
                    }

                    MergeResult::KeySplit(
                        candidate_index,
                        BlockSplit::ByKey(
                            left_interval,
                            combined_block_0,
                            right_interval,
                            combined_block_1,
                        ),
                        candidate_cell,
                    )
                },
                false => unsafe {
                    let candidate_internal_page = candidate_cell.deref().as_internal_page_ref();

                    let (c_keys, c_versions, c_children) =
                        candidate_internal_page.keys_versions_pointers();

                    let (s_keys, s_version, s_children) = simba.keys_versions_pointers();

                    let simba_internal = simba.as_internal_page_ref();

                    let mut joined = c_keys
                        .iter()
                        .zip(c_versions.iter().copied())
                        .zip(c_children.iter())
                        .enumerate()
                        .filter(|(index, ..)| candidate_internal_page.is_slot_live(*index))
                        .map(|(_, rest)| rest)
                        .sorted_by_key(|((k, ..), ..)| k.lower)
                        .merge_by(
                            s_keys
                                .iter()
                                .zip(s_version.iter().copied())
                                .zip(s_children.iter())
                                .enumerate()
                                .filter(|(index, ..)| simba_internal.is_slot_live(*index))
                                .map(|(_, rest)| rest)
                                .sorted_by_key(|((k, ..), ..)| k.lower),
                            |((f, ..), ..), ((s, ..), ..)| f.lower < s.lower,
                        )
                        .collect_vec();

                    let joined_len = joined.len();
                    let (first, second) = joined.split_at_mut(joined_len / 2);

                    let left_fence = Interval::new(
                        candidate_fence.lower.min(simba_fence.lower),
                        (self.cold.dec_key)(second.get_unchecked(0).0.0.lower),
                    );

                    let right_fence = Interval::new(
                        second.get_unchecked(0).0.0.lower,
                        candidate_fence.upper.max(simba_fence.upper),
                    );

                    first.sort_by_key(|((.., v), ..)| *v);
                    second.sort_by_key(|((.., v), ..)| *v);

                    let combined_block_0 = self.block_manager.new_empty_index_block(&self.ctx);

                    let combined_block_1 = self.block_manager.new_empty_index_block(&self.ctx);

                    combined_block_0
                        .unsafe_borrow_mut()
                        .as_internal_page()
                        .bulk_push_from_slice(first);

                    combined_block_1
                        .unsafe_borrow_mut()
                        .as_internal_page()
                        .bulk_push_from_slice(second);

                    MergeResult::KeySplit(
                        candidate_index,
                        BlockSplit::ByKey(
                            left_fence,
                            combined_block_0,
                            right_fence,
                            combined_block_1,
                        ),
                        candidate_cell,
                    )
                },
            }
        }
    }

    pub(crate) fn split(
        &self,
        block: &Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        fence: &Interval<Key>,
    ) -> BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload> {
        let is_leaf = block.is_leaf();

        let (active_block, _dead_block) = block.active_dead_count();

        // What a VERSION_SPLIT (the `else` branch below) would actually have
        // to push into its one single new page: every *survivor*, not just
        // the active count `filling_80_percent()` is measured against -
        // active entries for a leaf plus any dead-but-still-snapshot-
        // protected one (`record_survives_gc`, same predicate `merge()`'s
        // `leaf_merge_would_overflow` already uses for the identical
        // hazard on the merge side - see that check's doc), or every
        // current-live entry for an internal page. Under heavy
        // concurrent load with long-held reader snapshots (TPC-C's
        // signature), this can run well ahead of `active_block` alone, so
        // a block whose *active* count looks comfortably below 80%
        // (choosing VERSION_SPLIT) can still hold more survivors than the
        // new page's capacity - `bulk_push`'s bounds `assert!` would catch
        // the write, but only after already deciding on the wrong split
        // shape. Folded into the KEY_SPLIT decision below instead, exactly
        // like `merge()`'s equivalent check, so this size is never chosen
        // when it can't actually fit in one page.
        let retained_history = is_leaf.then(|| self.retained_owned_leaf_history(block));
        let survivor_count = match &retained_history {
            Some(records) => records.len(),
            None => block.as_internal_page_ref().live_count(),
        };

        let capacity = if is_leaf { NUM_RECORDS } else { FAN_OUT };

        // `>=`, not `>`: this decision exists *because* some pending write
        // needs room in whatever comes out of it (that's why `split()` was
        // called at all - see `on_overflow_node`/`split_root`, its only
        // callers) - so a `VERSION_SPLIT` is only a real fix when it leaves
        // at least one free slot for that write, not merely when survivors
        // don't yet outnumber capacity. At `survivor_count == capacity`
        // exactly, the old `>` comparison chose `VERSION_SPLIT` anyway,
        // `bulk_push`ing every survivor into a fresh page that came out
        // already 100% full - the pending write then had nowhere to go.
        // For a root-is-leaf tree (no parent to redo the overflow check on
        // the fresh result - see `retrieve_root_write_internal_olc`, whose
        // `Overflow` arm returns `split_root`'s result directly) that write
        // immediately panicked (`LeafPage::push_uncommitted`'s bounds
        // check); for a non-root leaf (whose parent *does* recheck the
        // fresh child before writing to it) it instead re-entered this same
        // decision with the identical, still-fully-protected survivor set,
        // repeating the exact same no-op `VERSION_SPLIT` forever. Neither
        // failure needed a repeated or "hot" key - confirmed via a repro
        // where every survivor was a distinct key, piled up simply because
        // one transaction happened to be slower to commit than several
        // others sharing its page (see `tests/db_transaction_abort_tests.rs`
        // and the git history around this change for the traced repro).
        // `>=` instead reserves that one slot: at the boundary, `KEY_SPLIT`
        // now runs instead, which produces two pages with real free space.
        // Transaction-level self-overwrite/reinsert reuse and first-writer-
        // wins keep an unresolved single-key chain below page capacity.
        //
        // Attempted and reverted (this session): forcing `KEY_SPLIT` here
        // whenever a `VERSION_SPLIT` wouldn't shrink a fence's survivor
        // count versus its last `VERSION_SPLIT` (tracked via a per-fence
        // watermark) — motivated by `SPLIT_CONVERGENCE_TRACE` confirming
        // this exact "same fence, same survivor count, forever" pattern as
        // one real mechanism behind `verify_concurrent_shared_keys`'s ~4%
        // livelock. Reverted for two reasons, not one: (1) it can force a
        // `KEY_SPLIT` at 0-1 survivors, which `nearest_key_boundary` can't
        // partition (asserts `len >= 2`) — confirmed as a real, reproducible
        // panic (`min > max` in `nearest_key_boundary`) across 3 unrelated
        // tests; guarding `survivor_count >= 2` stopped that panic there,
        // but (2) the same shape of panic then reappeared via `merge`'s own
        // keysplit-on-overflow fallback (its `nearest_key_boundary` call a
        // few hundred lines up), because forcing more frequent real
        // `KEY_SPLIT`s shrinks leaves more aggressively, producing smaller
        // leaves that `merge()`'s combine-then-keysplit path apparently
        // never had to handle before — a second, pre-existing edge case
        // this exposed, not one this change introduced. Also: even before
        // the panics, `SPLIT_CONVERGENCE_TRACE` confirmed the watermark
        // check correctly stopped the originally-diagnosed pattern, but the
        // test still hung at the same ~4% rate regardless — a second,
        // separate livelock (something keeps re-forming the *same* fence at
        // very high frequency regardless of which split type resolves it,
        // most likely a split/merge oscillation on tiny key ranges) survives
        // this fix untouched. Needs its own investigation before
        // reattempting; see the project memory entry on this session for
        // details.
        if active_block as usize >= block.filling_80_percent() || survivor_count >= capacity {
            // KEY_SPLIT
            match is_leaf {
                true => unsafe {
                    // LeafPage
                    if VERBOSE {
                        println!(
                            "Key Split: Leaf\n{}",
                            block.as_records().iter().join("\n\t")
                        );
                    }
                    let block_records = block.as_records();
                    let records = retained_history.expect("leaf history");
                    let mut sorted_records = records.iter().collect_vec();
                    sorted_records.sort_by_key(|r| r.key());
                    let Some(middle) = fitting_key_boundary(
                        &sorted_records,
                        sorted_records.len() / 2,
                        capacity,
                        |r| r.key(),
                    ) else {
                        // No non-tearing key split of the raw (garbage-
                        // inflated) history converges. Retry against just
                        // the write-facing subset -- shedding cold-eligible
                        // garbage first can free up a boundary this couldn't
                        // find (see `try_hot_key_split`).
                        return match self.try_hot_key_split(records, capacity) {
                            Ok(hks) => {
                                let (left, right) = (
                                    self.block_manager.new_empty_leaf(&self.ctx),
                                    self.block_manager.new_empty_leaf(&self.ctx),
                                );
                                let fence_left = Interval::new(
                                    fence.lower,
                                    (self.cold.dec_key)(hks.split_key),
                                );
                                let fence_right = Interval::new(hks.split_key, fence.upper);
                                self.push_records_onto(left, hks.left);
                                self.push_records_onto(right, hks.right);
                                BlockSplit::ByKey(fence_left, left, fence_right, right)
                            }
                            Err(records) => {
                                // Truly indivisible even discounting
                                // non-hot entries: this is the exceptional
                                // repeated-key-history case (the whole batch
                                // is one key's own history).
                                let replacement = self.block_manager.new_empty_leaf(&self.ctx);
                                let count = records.len();
                                record_version_split(fence.to_string(), count);
                                self.push_records_onto(replacement, records);
                                BlockSplit::ByVersion(replacement)
                            }
                        };
                    };
                    let split_key = sorted_records[middle].key();

                    let (left, right) = (
                        self.block_manager.new_empty_leaf(&self.ctx),
                        self.block_manager.new_empty_leaf(&self.ctx),
                    );

                    let (mut first, mut second): (Vec<_>, Vec<_>) =
                        records.into_iter().partition(|r| r.key() < split_key);
                    // Stable key sorting preserves physical order within a
                    // key while keeping range iteration's key ordering.
                    first.sort_by_key(|r| r.key());
                    second.sort_by_key(|r| r.key());

                    let fence_left = Interval::new(fence.lower, (self.cold.dec_key)(split_key));

                    // No re-sort by `insertion_stamp().ts_start()` here (there
                    // used to be one for each half) — see `merge`'s identical
                    // KeySplit leaf branch for why: `sorted_block` is already
                    // in true chain order via the stable
                    // `sorted_by_key(|r| r.key())` above, and re-sorting by
                    // `ts_start` can silently reorder a live record ahead of
                    // a dead predecessor with a numerically larger
                    // `ts_start`, corrupting the physical-order invariant
                    // `rfind`/`is_live_lineage` depend on. Confirmed as the
                    // root cause of the Delivery `OrderLine`
                    // `ZeroAffected(KeyAlreadyDeleted)` panic.
                    let fence_right = Interval::new(split_key, fence.upper);
                    self.populate_leaf_history(left, first);
                    self.populate_leaf_history(right, second);

                    if TRACE_KEY_DEBUG {
                        push_trace(format!(
                            "TRACE split::ByKey(leaf) thread={:#x} old={:p} old_fence={} old_live={} -> left={:p} left_fence={} left_live={} right={:p} right_fence={} right_live={}",
                            diag_thread_hash(),
                            block,
                            fence,
                            block_records
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            left.unsafe_borrow(),
                            fence_left,
                            left.unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            right.unsafe_borrow(),
                            fence_right,
                            right
                                .unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(",")
                        ));
                    }

                    BlockSplit::ByKey(fence_left, left, fence_right, right)
                },
                false => unsafe {
                    // KEY_SPLIT InternalPage
                    if VERBOSE {
                        println!("Key Split: Internal");
                    }
                    let (left, right) = (
                        self.block_manager.new_empty_index_block(&self.ctx),
                        self.block_manager.new_empty_index_block(&self.ctx),
                    );

                    let (key_intervals, versions, pointers) = block.keys_versions_pointers();

                    let internal = block.as_internal_page_ref();

                    let mut filtered = key_intervals
                        .iter()
                        .zip(versions.iter().copied())
                        .zip(pointers.iter())
                        .enumerate()
                        .filter(|(index, ..)| internal.is_slot_live(*index))
                        .map(|(_, rest)| rest)
                        .sorted_by_key(|((i, ..), ..)| i.lower)
                        .collect_vec();

                    let middle = filtered.len() / 2;
                    let (first, second) = filtered.split_at_mut(middle);

                    debug_assert!(!first.is_empty() && !second.is_empty());

                    let fence_left = Interval::new(
                        fence.lower,
                        (self.cold.dec_key)(second.get_unchecked(0).0.0.lower),
                    );

                    if let PageType::IndexMut(internal_page) =
                        left.unsafe_borrow_mut().as_page_mut()
                    {
                        first.sort_by_key(|((.., v), ..)| *v);
                        internal_page.bulk_push_from_slice(first)
                    }

                    let fence_right = Interval::new(second.get_unchecked(0).0.0.lower, fence.upper);

                    if let PageType::IndexMut(internal_page) =
                        right.unsafe_borrow_mut().as_page_mut()
                    {
                        second.sort_by_key(|((.., v), ..)| *v);
                        internal_page.bulk_push_from_slice(second)
                    }

                    BlockSplit::ByKey(fence_left, left, fence_right, right)
                },
            }
        } else {
            // < max_units_safe. meaning: active >= 40% and active < 80%
            // VERSION SPLIT
            match is_leaf {
                true => {
                    // LeafPage
                    if VERBOSE {
                        println!("Version Split: Leaf");
                    }
                    let new_leaf = self.block_manager.new_empty_leaf(&self.ctx);

                    let block_records = block.as_records();
                    let records = retained_history.expect("leaf history");
                    // debug_assert!(active_records.len() >= block.filling_40_percent(),
                    //               "Active records = {}, required >= {}", active_records.len(), block.filling_40_percent());

                    // if active_records.len() <=
                    //     BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_records()
                    // {
                    //     let s = "asds".to_string();
                    //     let hase = "asdfasdasdaoshufiusdjbf".to_string();
                    //     exit(1);
                    // }

                    // debug_assert!(active_records.len() <=
                    //     BlockManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::min_active_records());

                    let hot_count = self.populate_leaf_history(new_leaf, records);
                    record_version_split(fence.to_string(), hot_count);

                    if TRACE_KEY_DEBUG {
                        push_trace(format!(
                            "TRACE split::ByVersion(leaf) thread={:#x} old={:p} fence={} old_live={} -> new={:p} new_live={}",
                            diag_thread_hash(),
                            block,
                            fence,
                            block_records
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(","),
                            new_leaf.unsafe_borrow(),
                            new_leaf
                                .unsafe_borrow()
                                .as_records()
                                .iter()
                                .filter(|r| r.version().is_live())
                                .map(|r| r.key.to_string())
                                .collect_vec()
                                .join(",")
                        ));
                    }

                    BlockSplit::ByVersion(new_leaf)
                }
                false => {
                    // VERSION SPLIT InternalPage
                    if VERBOSE {
                        println!("Version Split: Internal");
                    }
                    let new_internal_page = self.block_manager.new_empty_index_block(&self.ctx);

                    let (key_intervals, versions, pointers) = block.keys_versions_pointers();

                    let internal = block.as_internal_page_ref();

                    let active_entries = key_intervals
                        .iter()
                        .zip(versions.iter().copied())
                        .zip(pointers.iter())
                        .enumerate()
                        .filter(|(index, ..)| internal.is_slot_live(*index))
                        .map(|(_, rest)| rest)
                        .collect_vec();

                    if VERBOSE {
                        let key_intervals = active_entries
                            .iter()
                            .map(|((k, ..), ..)| (k.lower, k.upper))
                            .sorted_by_key(|i| i.0)
                            .collect_vec();

                        if !key_intervals
                            .iter()
                            .zip(key_intervals.iter().skip(1))
                            .all(|((k0, k1), (k2, k3))| (self.cold.dec_key)(*k2) == *k1)
                        {
                            let s = "sdasdasdasdasln".to_string();
                        }
                    }

                    // RootSplit calls this too! Root may run under conditioned 2d
                    // debug_assert!(active_entries.len() >= block.two_d_filling(),
                    //               "Active entries = {}, required >= {}", active_entries.len(), block.two_d_filling());
                    if let PageType::IndexMut(internal_page) =
                        new_internal_page.unsafe_borrow_mut().as_page_mut()
                    {
                        internal_page.bulk_push(active_entries)
                    }

                    BlockSplit::ByVersion(new_internal_page)
                }
            }
        }
    }

    #[inline]
    pub(crate) fn merge_root<'a>(
        &self,
        master_guard: RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        root_guard: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        height: Height,
    ) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()> {
        if VERBOSE {
            println!("merge root");
        }

        // `merge_root` was reached because `unsafe_degree_root()` observed
        // exactly 1 active child — but that check happened before
        // `master_guard`'s own upgrade, and unlike `split_root` (which
        // retires `root_guard` itself), `merge_root` never retires
        // `root_guard`: it only reads `last_child()` off it below. The
        // caller (`retrieve_root_write_internal_olc`'s `ActiveUnderflow`
        // arm) already does `root_guard.upgrade_write_lock()` before
        // calling here — see that arm's doc for why this read genuinely
        // needs it (a concurrent overflow of this same single active child
        // can otherwise push a second one into root, in between the degree
        // check and this read, without ever needing root's lock itself).
        // This is the only caller `root_guard` has, so no repeat upgrade is
        // needed in this function.

        let child_ref = root_guard.as_internal_page_ref().last_child();

        let child_guard = child_ref.borrow_read();

        if VERBOSE {
            println!("Old root height = {}, new height = {}", height, height - 1);
        }

        let guard = self.split_root(master_guard, child_guard, height - 1)?;

        if VERBOSE {
            let guard_deref = guard.deref_mut();

            let (active, dead) = guard_deref.active_dead_count();

            println!("active dead count: ({} / {})", active, dead);
        }

        Ok(guard)
    }

    #[inline]
    pub(crate) fn split_root<'a>(
        &self,
        _master_guard: RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        root_guard: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        height: Height,
    ) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()> {
        // `try_retire()`, not `upgrade_write_lock()`: `root_guard` is only
        // ever read below (`split()` takes `&Block`, never mutates it in
        // place), and `split()` has no failure path — once this call is
        // reached, the old root is unconditionally superseded. Same
        // reasoning as `on_overflow_node`'s `simba`; see
        // `SmartGuard::try_retire`'s doc. Both callers (the root-overflow
        // arm in `retrieve_root_write_internal_olc`, and `merge_root`) pass
        // `root_guard`/`child_guard` in as a still-unexcluded `Reader`.
        let root_cell = match root_guard.try_retire() {
            Ok(cell) => cell,
            Err(_) => {
                if VERBOSE {
                    println!("split_root: root_guard.try_retire() failed");
                }
                return Err(());
            }
        };

        match self.split(
            root_cell.deref(),
            &Interval::new(self.cold.min_key, self.cold.max_key),
        ) {
            BlockSplit::ByKey(left_fence, left, right_fence, right) => {
                let new_root_block = self.block_manager.new_empty_index_block(&self.ctx);

                let root_internal_page = new_root_block
                    .unsafe_borrow_mut()
                    .as_mut()
                    .as_internal_page();

                let version = self.start_tx_commit();

                root_internal_page.push_uncommitted(left_fence, version, left, 0);

                root_internal_page.push_uncommitted(right_fence, version, right, 1);

                root_internal_page.commit_delta(2, 0);

                let new_root_latch = new_root_block.borrow_read();

                self.root
                    .append_root(Root::new(new_root_block, version, height + 1));

                // Registers the *new* root's birth version (`version`) as
                // the old root's death, not `_master_guard.version()` (the
                // old root's own, much older birth version) — see
                // `on_overflow_node`'s matching comment; the same
                // premature-reclaim bug applied here too, since any active
                // reader whose snapshot predates `version` still needs to
                // resolve through this now-superseded root. `root_cell` is
                // already retired (`try_retire()` above) — used as-is.
                self.block_manager
                    .register_dead(self.worker_id(), version, root_cell);

                Ok(new_root_latch)
            }
            BlockSplit::ByVersion(new_root_block) => {
                let version = self.start_tx_commit();

                let new_root_latch = new_root_block.borrow_read();

                self.root
                    .append_root(Root::new(new_root_block, version, height));

                self.block_manager
                    .register_dead(self.worker_id(), version, root_cell);

                Ok(new_root_latch)
            }
        }
    }
}
