use crate::mv_block::block::{Block, BlockGuard};
use crate::mv_block::block_handle::BlockAllocManager;
use crate::mv_page_model::node::PageType;
use crate::mv_page_model::{BlockRef, Height};
use crate::mv_query::interval::Interval;
use crate::mv_root::index_root::RootIndexGuard;
use crate::mv_root::root::Root;
use crate::mv_test::{DIAG, VERBOSE};
use crate::mv_tree::mvbt::MVBTSt;
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
/// key-sorted so one key's own entries are contiguous. A raw `len / 2` cut
/// can otherwise land inside a single key's own multi-entry chain (e.g. a
/// dead-but-abortable predecessor next to its live successor, the exact
/// shape `record_survives_gc` preserves) and tear it across the two
/// resulting sibling blocks — whose fences are disjoint by construction —
/// making the torn-off half unreachable by any fence-routed lookup. Falls
/// back to `target` only when every entry shares one key (the whole slice
/// is one key's chain): tearing is then unavoidable without duplicate-key
/// sibling support, a pre-existing structural limit this doesn't attempt
/// to fix.
fn nearest_key_boundary<T, K: PartialEq>(
    items: &[T],
    target: usize,
    key_of: impl Fn(&T) -> K,
) -> usize {
    let len = items.len();
    debug_assert!(len >= 2);
    let target = target.clamp(1, len - 1);
    let is_boundary = |i: usize| key_of(&items[i - 1]) != key_of(&items[i]);

    if is_boundary(target) {
        return target;
    }

    let mut lo = target;
    while lo > 1 && !is_boundary(lo) {
        lo -= 1;
    }
    let found_lo = is_boundary(lo).then_some(lo);

    let mut hi = target;
    while hi < len - 1 && !is_boundary(hi) {
        hi += 1;
    }
    let found_hi = is_boundary(hi).then_some(hi);

    match (found_lo, found_hi) {
        (Some(l), Some(h)) => if target - l <= h - target { l } else { h },
        (Some(l), None) => l,
        (None, Some(h)) => h,
        (None, None) => target,
    }
}

#[repr(u8)]
pub enum BlockUnsafeDegree {
    Ok,
    Overflow,
    ActiveUnderflow
}

impl BlockUnsafeDegree {
    #[inline(always)]
    pub const fn is_overflow(&self) -> bool {
        matches!(self, BlockUnsafeDegree::Overflow)
    }
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + 'static,
    Payload: Clone + Default + 'static
> Block<FAN_OUT, NUM_RECORDS, Key, Payload>
{ // #[inline(always)]
    // pub const fn block_id(&self) -> BlockID {
    //     0
    // }

    #[inline]
    fn is_overflow(&self) -> bool {
        self.unsafe_degree().is_overflow()
    }

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

    #[inline(always)]
    pub fn unsafe_degree(&self) -> BlockUnsafeDegree {
        let (active, dead)
            = self.active_dead_count();

        let (active, dead)
            = (active as usize,  dead as usize);

        let one_d
            = self.filling_20_percent();

        if active <= one_d {
            BlockUnsafeDegree::ActiveUnderflow
        }
        else {
            let overflow_units_count
                = self.overflow_units_count();

            let is_overflow
                = active + dead >= overflow_units_count;

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
        let (active, dead)
            = self.active_dead_count();

        let (active, dead)
            = (active as usize,  dead as usize);

        let is_leaf
            = self.is_leaf();

        if active == 1 && !is_leaf { // single child
            BlockUnsafeDegree::ActiveUnderflow
        }
        else if active + dead >= self.overflow_units_count() {
            BlockUnsafeDegree::Overflow
        }
        else {
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
    pub fn max_units(&self) -> usize { // absolute units
        match self.is_leaf() {
            true => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_records(),
            false => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::max_keys()
        }
    }

    #[inline(always)]
    pub fn filling_40_percent(&self) -> usize { // 40%
        (2 * self.max_units() + 4) / 5
    }

    #[inline(always)]
    pub fn filling_80_percent(&self) -> usize { // 80%
        (4 * self.max_units() + 4) / 5
    }

    #[inline(always)]
    pub fn filling_20_percent(&self) -> usize { // 20%
        (self.max_units() + 4) / 5
    }

    #[inline(always)]
    pub fn overflow_units_count(&self) -> usize { // trigger for overflow
        match self.is_leaf() {
            true => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_records_count(),
            false => BlockAllocManager::<FAN_OUT, NUM_RECORDS, Key, Payload>::overflow_keys_count()
        }
    }

    #[inline(always)]
    pub(crate) fn active_dead_count(&self) -> (u32, u32) {
        match self.as_page_ref() {
            PageType::IndexRef(internal_page) => internal_page.active_dead_count(),
            PageType::LeafRef(leaf_page) => leaf_page.active_dead_count(),
            _ => unreachable!()
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
    Payload: Clone + Default
> {
    ByKey(Interval<Key>,
          BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
          Interval<Key>,
          BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>),
    ByVersion(BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>)
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default> BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload
> { }

pub(crate) enum MergeResult<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display,
    Payload: Clone + Default
> {
    Merged(usize,
           Interval<Key>,
           BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>,
           // Already retired (`SmartGuard::try_retire`) by the time
           // `merge()` builds this — see that call site's doc — so this is
           // a bare cell, not a guard: there's no lock left to hold or
           // later release.
           BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>),
    KeySplit(usize,
             BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload>,
             BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>),
    Error,
}

impl<const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Sync + 'static + Display,
    Payload: Display + Clone + Default + Sync + 'static
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    pub(crate) fn on_overflow_node<'a>(
        &self,
        mufasa: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        child_index: usize) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
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

        let mufasa_deref_mut
            = mufasa.deref_mut();

        let internal_page
            = mufasa_deref_mut.as_internal_page();

        let fence = internal_page
            .get_key(child_index)
            .clone();

        let current_len
            = internal_page.sum_len();

        if DIAG && format!("{}", fence.upper) == "18446744073709551615" {
            eprintln!("DIAG on_overflow_node ENTER thread={:#x} page={:p} child_index={child_index} current_len={current_len} fence=[{},{}] sum_len_before={}",
                diag_thread_hash(), internal_page as *const _, fence.lower, fence.upper, internal_page.sum_len());
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
            BlockSplit::ByKey(left_fence,
                              left,
                              right_fence,
                              right
            ) => {
                let version
                    = self.start_tx_commit();

                internal_page.push_uncommitted(
                    left_fence,
                    version,
                    left,
                    current_len);

                internal_page.push_uncommitted(
                    right_fence,
                    version,
                    right,
                    current_len + 1);

                internal_page.commit_delta(1, 1);
                if DIAG && format!("{}", right_fence.upper) == "18446744073709551615" {
                    eprintln!("DIAG on_overflow_node ByKey thread={:#x} page={:p} child_index={child_index} superseded, pushed left=[{},{}]@{current_len} right=[{},{}]@{}",
                        diag_thread_hash(), internal_page as *const _, left_fence.lower, left_fence.upper, right_fence.lower, right_fence.upper, current_len + 1);
                }
                version
            }
            BlockSplit::ByVersion(fresh) => {
                let version
                    = self.start_tx_commit();

                internal_page.push_uncommitted(
                    fence,
                    version,
                    fresh,
                    current_len);

                internal_page.commit_delta(0, 1);
                if DIAG && format!("{}", fence.upper) == "18446744073709551615" {
                    eprintln!("DIAG on_overflow_node ByVersion thread={:#x} page={:p} child_index={child_index} superseded, pushed fence=[{},{}]@{current_len}",
                        diag_thread_hash(), internal_page as *const _, fence.lower, fence.upper);
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
        self.block_manager.register_dead(
            self.worker_id(),
            version,
            simba_cell);
        Ok(mufasa)
    }

    pub(crate) fn on_underflow_node<'a>(
        &self,
        mufasa: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: BlockGuard<'_, FAN_OUT, NUM_RECORDS, Key, Payload>,
        index_simba: usize) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
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

        let mufasa_deref_mut
            = mufasa.deref_mut();

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
            Err(..) => return Err(())
        };

        match self.merge(mufasa_deref_mut, simba_cell.deref(), index_simba) {
            MergeResult::Merged(
                index_sibling,
                fence_sibling,
                merged_block,
                candidate_cell
            ) => {
                if VERBOSE {
                    println!("MergeResult::Merged: Simba-fence: {} - Sibling-fence: {}",
                             mufasa_deref_mut.as_internal_page_ref().get_key(index_simba),
                             fence_sibling);
                }
                let mufasa_internal_page = mufasa_deref_mut
                    .as_internal_page();

                let mufasa_len
                    = mufasa_internal_page.sum_len();

                let mut merged_fence = mufasa_internal_page
                    .get_key(index_simba)
                    .clone();

                merged_fence.merged(&fence_sibling);

                let version
                    = self.start_tx_commit();

                mufasa_internal_page.push_uncommitted(
                    merged_fence,
                    version,
                    merged_block,
                    mufasa_len);

                mufasa_internal_page
                    .commit_delta(-1, 2);

                if DIAG && format!("{}", merged_fence.upper) == "18446744073709551615" {
                    eprintln!("DIAG on_underflow_node Merged thread={:#x} page={:p} index_simba={index_simba} index_sibling={index_sibling} superseded, pushed merged=[{},{}]@{mufasa_len}",
                        diag_thread_hash(), mufasa_internal_page as *const _, merged_fence.lower, merged_fence.upper);
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
                    [
                        (version, simba_cell),
                        (version, candidate_cell)
                    ])
            }
            MergeResult::KeySplit(
                index_sibling,
                BlockSplit::ByKey(left_interval,
                                  left,
                                  right_interval,
                                  right),
                candidate_cell
            ) => {
                if VERBOSE {
                    unsafe {
                        println!("MergeResult::KeySplit: \
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
                let mufasa_internal_page = mufasa_deref_mut
                    .as_internal_page();

                let mufasa_len
                    = mufasa_internal_page.sum_len();

                let version
                    = self.start_tx_commit();

                mufasa_internal_page.push_uncommitted(
                    left_interval,
                    version,
                    left,
                    mufasa_len);

                mufasa_internal_page.push_uncommitted(
                    right_interval,
                    version,
                    right,
                    mufasa_len + 1);

                mufasa_internal_page
                    .commit_delta(0, 2);

                if DIAG && format!("{}", right_interval.upper) == "18446744073709551615" {
                    eprintln!("DIAG on_underflow_node KeySplit thread={:#x} page={:p} index_simba={index_simba} index_sibling={index_sibling} superseded, pushed left=[{},{}]@{mufasa_len} right=[{},{}]@{}",
                        diag_thread_hash(), mufasa_internal_page as *const _, left_interval.lower, left_interval.upper, right_interval.lower, right_interval.upper, mufasa_len + 1);
                }

                // See the `Merged` arm's matching comment.
                self.block_manager.register_dead_col(
                    self.worker_id(),
                    [
                        (version, simba_cell),
                        (version, candidate_cell)
                    ])
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
    /// business-logic rollback, `mv_bench::tpcc_txn::new_order`, which can
    /// abort *after* having already updated `District`), reversing that
    /// delete (`LeafPage::apply_invalidate`/`apply_undelete`) requires the
    /// predecessor record to still be physically present to undelete. A
    /// version-split/merge racing in between — reading `is_deleted()` as
    /// `true` and discarding the record, exactly as it should for a
    /// *committed* delete — makes that reversal silently impossible
    /// instead, permanently losing the key: confirmed empirically as the
    /// root cause of `mv_bench::tpcc_txn`'s District
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
    #[inline]
    pub(crate) fn record_survives_gc(&self, version: &crate::mv_record_model::version_info::VersionInfo) -> bool {
        if version.is_live() {
            return true;
        }

        !version.insertion_stamp().is_invalid()
            && version.deletion_stamp()
                .is_some_and(|del| self.ctx.is_snapshot_live(del.ts_start()))
    }

    pub(crate) fn merge<'a>(
        &self,
        mufasa: &'a Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba: &Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        simba_index: usize,
    ) -> MergeResult<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let mufasa_internal_page
            = mufasa.as_internal_page_ref();

        let is_simba_leaf
            = simba.is_leaf();

        let simba_fence
            = mufasa_internal_page.get_key(simba_index);

        let simba_max_units
            = simba.max_units();

        let (simba_active_count, _simba_dead_count)
            = simba.active_dead_count();

        let (simba_active_count, _simba_dead_count)
            = (simba_active_count as usize, _simba_dead_count as usize);

        let mufasa_children
            = mufasa_internal_page.children();

        let live
            = mufasa_internal_page.live_mask();

        let mut all_candidates = mufasa_children
            .iter()
            .enumerate()
            .zip(mufasa_internal_page.versions())
            .zip(mufasa_internal_page.keys())
            .filter(|(((index, ..), ..), ..)|
                *index != simba_index)
            .filter(|(((index, ..), ..), ..)| live[*index])
            .sorted_by_key(|(.., fence)| fence.lower())
            .map(|(((index, bro), ..), fence)|
                (index, bro, fence))
            .collect_vec();

        let mut compute_candidate = ||
            match all_candidates.binary_search_by_key(&simba_fence.lower, |(.., f)| f.lower) {
                Ok(index) => Ok(all_candidates.remove(index)),
                Err(index) => if index < all_candidates.len() {
                    Ok(all_candidates.remove(index))
                } else if !all_candidates.is_empty() {
                    return Ok(all_candidates.pop().unwrap());
                } else {
                    return Err(());
                },
            };

        let (candidate_index,
            // candidate_guard,
            candidate_block,
            // candidate_active_count,
            candidate_fence
        ) = match compute_candidate() {
            Ok((index,
                   // mut candidate_guard,
                   block,
                   // cac,
                   cf)
            ) => (index, block, cf),
            _ => return MergeResult::Error
        };

        all_candidates.clear();

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
        let candidate_cell = match candidate_block.borrow_read().try_retire() {
            Ok(cell) => cell,
            Err(..) => return MergeResult::Error,
        };

        let (candidate_active_count, _candidate_dead_count) = candidate_cell
            .deref()
            .active_dead_count();

        let candidate_active_count
            = candidate_active_count as usize;

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
        let leaf_merge_would_overflow = is_simba_leaf && {
            let simba_survivors = simba.as_records()
                .iter()
                .filter(|r| self.record_survives_gc(r.version()))
                .count();

            let candidate_survivors = candidate_cell
                .deref()
                .as_records()
                .iter()
                .filter(|r| self.record_survives_gc(r.version()))
                .count();

            simba_survivors + candidate_survivors > simba_max_units
        };

        if !leaf_merge_would_overflow && candidate_active_count + simba_active_count <= ((4 * simba_max_units) / 5) { // <= 80% ok merge
            let combined_block = match is_simba_leaf {
                false => {
                    let combined_block = self.block_manager
                        .new_empty_index_block(&self.ctx);

                    let (keys, versions, pointers)
                        = simba.as_internal_page_ref().keys_versions_pointers();

                    let simba_live
                        = simba.as_internal_page_ref().live_mask();

                    let (c_keys, c_versions, c_pointers) = candidate_cell
                        .deref()
                        .as_internal_page_ref()
                        .keys_versions_pointers();

                    let candidate_live = candidate_cell
                        .deref()
                        .as_internal_page_ref()
                        .live_mask();

                    let shadow_copy = keys
                        .iter()
                        .zip(versions.iter().copied())
                        .zip(pointers.iter())
                        .enumerate()
                        .filter(|(index, ..)| simba_live[*index])
                        .map(|(_, rest)| rest)
                        .merge_by(c_keys.iter()
                                      .zip(c_versions.iter().copied())
                                      .zip(c_pointers.iter())
                                      .enumerate()
                                      .filter(|(index, ..)| candidate_live[*index])
                                      .map(|(_, rest)| rest),
                                  |((.., v0), ..), ((.., v1), ..)| v0 <= v1)
                        .collect_vec();

                    combined_block
                        .unsafe_borrow_mut()
                        .as_internal_page()
                        .bulk_push(shadow_copy);

                    combined_block
                }
                true => {
                    let combined_block = self.block_manager
                        .new_empty_leaf(&self.ctx);

                    combined_block
                        .unsafe_borrow_mut()
                        .as_leaf_page()
                        .bulk_push(simba
                            .as_records()
                            .iter()
                            .filter(|r| self.record_survives_gc(r.version()))
                            .merge_by(candidate_cell
                                          .deref()
                                          .as_records()
                                          .iter()
                                          .filter(|r| self.record_survives_gc(r.version())),
                                      |f, s|
                                          f.version().insertion_stamp().ts_start() <= s.version().insertion_stamp().ts_start())
                            .collect_vec());

                    if TRACE_KEY_DEBUG {
                        push_trace(format!("TRACE merge::Merged(leaf) thread={:#x} simba={:p} simba_fence={} simba_live={} candidate={:p} candidate_fence={} candidate_live={} -> combined={:p} combined_live={}",
                            diag_thread_hash(),
                            simba, simba_fence,
                            simba.as_records().iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(","),
                            candidate_cell.deref(), candidate_fence,
                            candidate_cell.deref().as_records().iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(","),
                            combined_block.unsafe_borrow(),
                            combined_block.unsafe_borrow().as_records().iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(",")));
                    }

                    combined_block
                }
            };

            MergeResult::Merged(candidate_index, candidate_fence.clone(), combined_block, candidate_cell)
        } else { // Keysplit when merged: > 80% active entries ---> redistribute the keys
            match is_simba_leaf {
                true => unsafe {
                    let candidate_records = candidate_cell.deref().as_records();
                    let simba_records = simba.as_records();

                    let mut joined = candidate_records
                        .iter()
                        .filter(|r| self.record_survives_gc(r.version()))
                        .sorted_by_key(|r| r.key)
                        .merge_by(simba_records
                                      .iter()
                                      .filter(|r| self.record_survives_gc(r.version()))
                                      .sorted_by_key(|r| r.key),
                                  |f, s|
                                      f.key() <= s.key())
                        .collect_vec();

                    let joined_len = joined.len();
                    let middle = nearest_key_boundary(&joined, joined_len / 2, |r| r.key());
                    let (first, second)
                        = joined.split_at_mut(middle);

                    let left_interval = Interval::new(
                        candidate_fence.lower.min(simba_fence.lower),
                        (self.dec_key)(second.get_unchecked(0).key()));

                    let right_interval = Interval::new(
                        second.get_unchecked(0).key(),
                        candidate_fence.upper.max(simba_fence.upper));

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
                    // `mv_bench::tpcc_txn`'s Delivery `OrderLine`
                    // `ZeroAffected(KeyAlreadyDeleted)` panic (a diagnostic
                    // dump of the leaf at the panic site showed exactly this
                    // shape: an older, still-live entry sitting before a
                    // newer entry already marked dead by a still-later
                    // write).

                    let combined_block_0 = self.block_manager
                        .new_empty_leaf(&self.ctx);

                    let combined_block_1 = self.block_manager
                        .new_empty_leaf(&self.ctx);

                    combined_block_0
                        .unsafe_borrow_mut()
                        .as_leaf_page()
                        .bulk_push_from_slice_ref(first);

                    combined_block_1
                        .unsafe_borrow_mut()
                        .as_leaf_page()
                        .bulk_push_from_slice_ref(second);

                    if TRACE_KEY_DEBUG {
                        push_trace(format!("TRACE merge::KeySplit(leaf) thread={:#x} simba={:p} simba_fence={} simba_live={} candidate={:p} candidate_fence={} candidate_live={} -> left={:p} left_fence={} left_live={} right={:p} right_fence={} right_live={}",
                            diag_thread_hash(),
                            simba, simba_fence,
                            simba_records.iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(","),
                            candidate_cell.deref(), candidate_fence,
                            candidate_records.iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(","),
                            combined_block_0.unsafe_borrow(), left_interval,
                            combined_block_0.unsafe_borrow().as_records().iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(","),
                            combined_block_1.unsafe_borrow(), right_interval,
                            combined_block_1.unsafe_borrow().as_records().iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(",")));
                    }

                    MergeResult::KeySplit(
                        candidate_index,
                        BlockSplit::ByKey(
                            left_interval,
                            combined_block_0,
                            right_interval,
                            combined_block_1),
                        candidate_cell)
                }
                false => unsafe {
                    let candidate_internal_page = candidate_cell
                        .deref()
                        .as_internal_page_ref();

                    let (c_keys, c_versions, c_children)
                        = candidate_internal_page.keys_versions_pointers();

                    let candidate_live
                        = candidate_internal_page.live_mask();

                    let (s_keys, s_version, s_children)
                        = simba.keys_versions_pointers();

                    let simba_live
                        = simba.as_internal_page_ref().live_mask();

                    let mut joined = c_keys
                        .iter()
                        .zip(c_versions.iter().copied())
                        .zip(c_children.iter())
                        .enumerate()
                        .filter(|(index, ..)| candidate_live[*index])
                        .map(|(_, rest)| rest)
                        .sorted_by_key(|((k, ..), ..)| k.lower)
                        .merge_by(s_keys.iter()
                                      .zip(s_version.iter().copied())
                                      .zip(s_children.iter())
                                      .enumerate()
                                      .filter(|(index, ..)| simba_live[*index])
                                      .map(|(_, rest)| rest)
                                      .sorted_by_key(|((k, ..), ..)| k.lower),
                                  |((f, ..), ..), ((s, ..), ..)|
                                      f.lower < s.lower)
                        .collect_vec();

                    let joined_len = joined.len();
                    let (first, second)
                        = joined.split_at_mut(joined_len / 2);

                    let left_fence = Interval::new(
                        candidate_fence.lower.min(simba_fence.lower),
                        (self.dec_key)(second.get_unchecked(0).0.0.lower));

                    let right_fence = Interval::new(
                        second.get_unchecked(0).0.0.lower,
                        candidate_fence.upper.max(simba_fence.upper));

                    first.sort_by_key(|((.., v), ..)| *v);
                    second.sort_by_key(|((.., v), ..)| *v);

                    let combined_block_0 = self.block_manager
                        .new_empty_index_block(&self.ctx);

                    let combined_block_1 = self.block_manager
                        .new_empty_index_block(&self.ctx);

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
                            combined_block_1),
                        candidate_cell)
                }
            }
        }
    }

    pub(crate) fn split(
        &self,
        block: &Block<FAN_OUT, NUM_RECORDS, Key, Payload>,
        fence: &Interval<Key>,
    ) -> BlockSplit<FAN_OUT, NUM_RECORDS, Key, Payload>
    {
        let is_leaf
            = block.is_leaf();

        let (active_block, _dead_block)
            = block.active_dead_count();

        // What a VERSION_SPLIT (the `else` branch below) would actually have
        // to push into its one single new page: every *survivor*, not just
        // the active count `filling_80_percent()` is measured against -
        // active entries for a leaf plus any dead-but-still-snapshot-
        // protected one (`record_survives_gc`, same predicate `merge()`'s
        // `leaf_merge_would_overflow` already uses for the identical
        // hazard on the merge side - see that check's doc), or every
        // `live_mask()`-live entry for an internal page. Under heavy
        // concurrent load with long-held reader snapshots (TPC-C's
        // signature), this can run well ahead of `active_block` alone, so
        // a block whose *active* count looks comfortably below 80%
        // (choosing VERSION_SPLIT) can still hold more survivors than the
        // new page's capacity - `bulk_push`'s bounds `assert!` would catch
        // the write, but only after already deciding on the wrong split
        // shape. Folded into the KEY_SPLIT decision below instead, exactly
        // like `merge()`'s equivalent check, so this size is never chosen
        // when it can't actually fit in one page.
        let survivor_count = match is_leaf {
            true => block.as_records()
                .iter()
                .filter(|r| self.record_survives_gc(r.version()))
                .count(),
            false => block.as_internal_page_ref()
                .live_mask()
                .iter()
                .filter(|live| **live)
                .count(),
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
        // now runs instead, which always produces two pages with real free
        // space (short of the separate, still-open same-key-tearing
        // limitation this doesn't touch - see `nearest_key_boundary`'s doc).
        if active_block as usize >= block.filling_80_percent() || survivor_count >= capacity {
            // KEY_SPLIT
            match is_leaf {
                true => unsafe { // LeafPage
                    if VERBOSE {
                        println!("Key Split: Leaf\n{}", block.as_records().iter().join("\n\t"));

                    }
                    let (left, right) =
                        (self.block_manager
                             .new_empty_leaf(&self.ctx),
                         self.block_manager
                             .new_empty_leaf(&self.ctx));

                    let block_records = block.as_records();

                    let mut sorted_block = block_records
                        .iter()
                        .filter(|r| self.record_survives_gc(r.version()))
                        .sorted_by_key(|r| r.key())
                        .collect_vec();

                    let middle = nearest_key_boundary(&sorted_block, sorted_block.len() / 2, |r| r.key());
                    let (first, second) = sorted_block
                        .split_at_mut(middle);

                    let fence_left = Interval::new(
                        fence.lower,
                        (self.dec_key)(second.get_unchecked(0).key));

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
                    if let PageType::LeafMut(leaf_page) = left.unsafe_borrow_mut().as_page_mut() {
                        leaf_page.bulk_push_from_slice_ref(first);
                    }

                    let fence_right = Interval::new(
                        second.get_unchecked(0).key,
                        fence.upper);

                    if let PageType::LeafMut(leaf_page) = right.unsafe_borrow_mut().as_page_mut() {
                        leaf_page.bulk_push_from_slice_ref(second)
                    }

                    if TRACE_KEY_DEBUG {
                        push_trace(format!("TRACE split::ByKey(leaf) thread={:#x} old={:p} old_fence={} old_live={} -> left={:p} left_fence={} left_live={} right={:p} right_fence={} right_live={}",
                            diag_thread_hash(),
                            block, fence,
                            block_records.iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(","),
                            left.unsafe_borrow(), fence_left,
                            left.unsafe_borrow().as_records().iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(","),
                            right.unsafe_borrow(), fence_right,
                            right.unsafe_borrow().as_records().iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(",")));
                    }

                    BlockSplit::ByKey(fence_left, left, fence_right, right)
                }
                false => unsafe { // KEY_SPLIT InternalPage
                    if VERBOSE {
                        println!("Key Split: Internal");
                    }
                    let (left, right) =
                        (self.block_manager
                             .new_empty_index_block(&self.ctx),
                         self.block_manager
                             .new_empty_index_block(&self.ctx));

                    let (key_intervals, versions, pointers) = block
                        .keys_versions_pointers();

                    let live
                        = block.as_internal_page_ref().live_mask();

                    let mut filtered = key_intervals
                        .iter()
                        .zip(versions.iter().copied())
                        .zip(pointers.iter())
                        .enumerate()
                        .filter(|(index, ..)| live[*index])
                        .map(|(_, rest)| rest)
                        .sorted_by_key(|((i, ..), ..)| i.lower)
                        .collect_vec();

                    let middle = filtered.len() / 2;
                    let (first, second)
                        = filtered.split_at_mut(middle);

                    debug_assert!(!first.is_empty() && !second.is_empty());

                    let fence_left = Interval::new(
                        fence.lower,
                        (self.dec_key)(second.get_unchecked(0).0.0.lower));

                    if let PageType::IndexMut(internal_page) = left.unsafe_borrow_mut().as_page_mut() {
                        first.sort_by_key(|((.., v), ..)| *v);
                        internal_page.bulk_push_from_slice(first)
                    }

                    let fence_right = Interval::new(
                        second.get_unchecked(0).0.0.lower,
                        fence.upper);

                    if let PageType::IndexMut(internal_page) = right.unsafe_borrow_mut().as_page_mut() {
                        second.sort_by_key(|((.., v), ..)| *v);
                        internal_page.bulk_push_from_slice(second)
                    }

                    BlockSplit::ByKey(fence_left, left, fence_right, right)
                }
            }
        } else { // < max_units_safe. meaning: active >= 40% and active < 80%
            // VERSION SPLIT
            match is_leaf {
                true => { // LeafPage
                    if VERBOSE {
                        println!("Version Split: Leaf");
                    }
                    let new_leaf = self.block_manager
                        .new_empty_leaf(&self.ctx);

                    let block_records = block.as_records();

                    let active_records = block_records
                        .iter()
                        .filter(|record| self.record_survives_gc(record.version()))
                        .collect_vec();

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

                    if let PageType::LeafMut(leaf_page) = new_leaf.unsafe_borrow_mut().as_page_mut() {
                        leaf_page.bulk_push(active_records);
                    }

                    if TRACE_KEY_DEBUG {
                        push_trace(format!("TRACE split::ByVersion(leaf) thread={:#x} old={:p} fence={} old_live={} -> new={:p} new_live={}",
                            diag_thread_hash(),
                            block, fence,
                            block_records.iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(","),
                            new_leaf.unsafe_borrow(),
                            new_leaf.unsafe_borrow().as_records().iter().filter(|r| r.version().is_live()).map(|r| r.key.to_string()).collect_vec().join(",")));
                    }

                    BlockSplit::ByVersion(new_leaf)
                }
                false => { // VERSION SPLIT InternalPage
                    if VERBOSE {
                        println!("Version Split: Internal");
                    }
                    let new_internal_page = self.block_manager
                        .new_empty_index_block(&self.ctx);

                    let (key_intervals, versions, pointers) = block
                        .keys_versions_pointers();

                    let live
                        = block.as_internal_page_ref().live_mask();

                    let active_entries = key_intervals
                        .iter()
                        .zip(versions.iter().copied())
                        .zip(pointers.iter())
                        .enumerate()
                        .filter(|(index, ..)| live[*index])
                        .map(|(_, rest)| rest)
                        .collect_vec();

                    if VERBOSE {
                        let key_intervals = active_entries
                            .iter()
                            .map(|((k, ..), ..)| (k.lower, k.upper))
                            .sorted_by_key(|i| i.0)
                            .collect_vec();

                        if !key_intervals.iter().zip(key_intervals.iter().skip(1))
                            .all(|((k0, k1), (k2, k3))|
                                (self.dec_key)(*k2) == *k1) {
                            let s = "sdasdasdasdasln".to_string();
                        }
                    }

                    // RootSplit calls this too! Root may run under conditioned 2d
                    // debug_assert!(active_entries.len() >= block.two_d_filling(),
                    //               "Active entries = {}, required >= {}", active_entries.len(), block.two_d_filling());
                    if let PageType::IndexMut(internal_page) = new_internal_page.unsafe_borrow_mut().as_page_mut() {
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

        let child_ref = root_guard
            .as_internal_page_ref()
            .last_child();

        let child_guard = child_ref
            .borrow_read();

        if VERBOSE {
            println!("Old root height = {}, new height = {}", height, height - 1);
        }

        let guard = self.split_root(master_guard, child_guard, height - 1)?;

        if VERBOSE {
            let guard_deref
                = guard.deref_mut();

            let (active, dead)
                = guard_deref.active_dead_count();

            println!("active dead count: ({} / {})", active, dead);
        }

        Ok(guard)
    }

    #[inline]
    pub(crate) fn split_root<'a>(
        &self,
        _master_guard: RootIndexGuard<FAN_OUT, NUM_RECORDS, Key, Payload>,
        root_guard: BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>,
        height: Height) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
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

        match self.split(root_cell.deref(), &Interval::new(self.min_key, self.max_key)) {
            BlockSplit::ByKey(left_fence,
                              left,
                              right_fence,
                              right
            ) => {
                let new_root_block = self
                    .block_manager
                    .new_empty_index_block(&self.ctx);

                let root_internal_page = new_root_block
                    .unsafe_borrow_mut()
                    .as_mut()
                    .as_internal_page();

                let version
                    = self.start_tx_commit();

                root_internal_page
                    .push_uncommitted(left_fence, version, left, 0);

                root_internal_page
                    .push_uncommitted(right_fence, version, right, 1);

                root_internal_page.commit_delta(2, 0);

                let new_root_latch
                    = new_root_block.borrow_read();

                self.root.append_root(
                    Root::new(new_root_block, version, height + 1));

                // Registers the *new* root's birth version (`version`) as
                // the old root's death, not `_master_guard.version()` (the
                // old root's own, much older birth version) — see
                // `on_overflow_node`'s matching comment; the same
                // premature-reclaim bug applied here too, since any active
                // reader whose snapshot predates `version` still needs to
                // resolve through this now-superseded root. `root_cell` is
                // already retired (`try_retire()` above) — used as-is.
                self.block_manager.register_dead(
                    self.worker_id(), version, root_cell);

                Ok(new_root_latch)
            }
            BlockSplit::ByVersion(new_root_block) => {
                let version
                    = self.start_tx_commit();

                let new_root_latch
                    = new_root_block.borrow_read();

                self.root.append_root(
                    Root::new(new_root_block, version, height));

                self.block_manager.register_dead(
                    self.worker_id(), version, root_cell);

                Ok(new_root_latch)
            }
        }
    }
}