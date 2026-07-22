use crate::mv_block::block::{Block, BlockGuard};
use crate::mv_block::block_handle::BlockAllocManager;
use crate::mv_page_model::node::PageType;
use crate::mv_page_model::time_matcher::TimeMatcher;
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
        // will correctly detect, since reaching this capacity limit
        // implies `sum_len >= overflow_units_count()` already — so bail out
        // and force a restart rather than corrupt it. Root and non-root
        // `mufasa` share this same fixed-size-array capacity constraint
        // identically; `split_root`/`merge_root` don't need the same guard
        // because they only ever push into a *freshly allocated* page,
        // never one that could have already absorbed an earlier round.
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
        // entries" check (`sum_len`, not the full `unsafe_degree()`) —
        // `unsafe_degree().is_unsafe()` also trips on plain `ActiveUnderflow`
        // (`active <= one_d`, ~20% of capacity), which is the *normal*,
        // expected state for almost every internal page in a small/young
        // tree (plenty of physical room, just not many children yet) — using
        // the broad check here permanently blocks every split attempt on
        // such a `mufasa` (its "too few children" never resolves on its own,
        // and nothing above it can fix it either, since the ancestor hits
        // the identical false trip), a total deadlock confirmed via
        // `generate` stalling at ~234 keys, essentially immediately.
        if mufasa.is_overflow() { // is_overflow in re-repeated corrections
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
            eprintln!("DIAG on_overflow_node ENTER thread={:#x} page={:p} child_index={child_index} current_len={current_len} fence=[{},{}] sum_len_before={} already_obsolete={}",
                diag_thread_hash(), internal_page as *const _, fence.lower, fence.upper, internal_page.sum_len(), !internal_page.get_version(child_index).is_active());
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
                internal_page.mark_version_obsolete(child_index);
                if DIAG && format!("{}", right_fence.upper) == "18446744073709551615" {
                    eprintln!("DIAG on_overflow_node ByKey thread={:#x} page={:p} child_index={child_index} obsoleted, pushed left=[{},{}]@{current_len} right=[{},{}]@{}",
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
                internal_page.mark_version_obsolete(child_index);
                if DIAG && format!("{}", fence.upper) == "18446744073709551615" {
                    eprintln!("DIAG on_overflow_node ByVersion thread={:#x} page={:p} child_index={child_index} obsoleted, pushed fence=[{},{}]@{current_len}",
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
        index_simba: usize)
        -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
        if VERBOSE {
            println!("on_underflow_node");
        }

        // See `on_overflow_node`'s matching comment (including why this was
        // written but left disabled, why it must be the narrow `sum_len`
        // capacity check and not the broad `unsafe_degree().is_unsafe()`,
        // and how `generate` reproduces each failure mode in turn): a merge
        // can need up to 2 fresh entries (`MergeResult::KeySplit`), and
        // `mufasa` gets the same "already a Writer from an earlier round in
        // this traversal, no re-validated capacity" exposure. Checked
        // conservatively for both outcomes before doing any of the
        // (otherwise wasted) work below, since which one `merge()` produces
        // isn't known yet.
        if mufasa.is_overflow() {
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
            Err(..) => return Err(()),
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

                mufasa_internal_page
                    .mark_version_obsolete(index_sibling);

                mufasa_internal_page
                    .mark_version_obsolete(index_simba);

                if DIAG && format!("{}", merged_fence.upper) == "18446744073709551615" {
                    eprintln!("DIAG on_underflow_node Merged thread={:#x} page={:p} index_simba={index_simba} index_sibling={index_sibling} obsoleted, pushed merged=[{},{}]@{mufasa_len}",
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

                mufasa_internal_page
                    .mark_version_obsolete(index_sibling);

                mufasa_internal_page
                    .mark_version_obsolete(index_simba);

                if DIAG && format!("{}", right_interval.upper) == "18446744073709551615" {
                    eprintln!("DIAG on_underflow_node KeySplit thread={:#x} page={:p} index_simba={index_simba} index_sibling={index_sibling} obsoleted, pushed left=[{},{}]@{mufasa_len} right=[{},{}]@{}",
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

        let mut all_candidates = mufasa_children
            .iter()
            .enumerate()
            .zip(mufasa_internal_page.versions())
            .zip(mufasa_internal_page.keys())
            .filter(|(((index, ..), ..), ..)|
                *index != simba_index)
            .filter(|((.., version), ..)| version.is_active())
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

        if candidate_active_count + simba_active_count <= ((4 * simba_max_units) / 5) { // <= 80% ok merge
            let combined_block = match is_simba_leaf {
                false => {
                    let combined_block = self.block_manager
                        .new_empty_index_block(&self.ctx);

                    let (keys, versions, pointers)
                        = simba.as_internal_page_ref().keys_versions_pointers();

                    let (c_keys, c_versions, c_pointers) = candidate_cell
                        .deref()
                        .as_internal_page_ref()
                        .keys_versions_pointers();

                    let shadow_copy = keys
                        .iter()
                        .zip(versions.iter())
                        .zip(pointers.iter())
                        .filter(|((.., version), ..)| version.is_active())
                        .merge_by(c_keys.iter()
                                      .zip(c_versions.iter())
                                      .zip(c_pointers.iter())
                                      .filter(|((.., version), ..)| version.is_active()),
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
                            .filter(|r| r.version().is_live())
                            .merge_by(candidate_cell
                                          .deref()
                                          .as_records()
                                          .iter()
                                          .filter(|r| r.version().is_live()),
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
                        .filter(|r| r.version().is_live())
                        .sorted_by_key(|r| r.key)
                        .merge_by(simba_records
                                      .iter()
                                      .filter(|r| r.version().is_live())
                                      .sorted_by_key(|r| r.key),
                                  |f, s|
                                      f.key() <= s.key())
                        .collect_vec();

                    let joined_len = joined.len();
                    let (first, second)
                        = joined.split_at_mut(joined_len / 2);

                    let left_interval = Interval::new(
                        candidate_fence.lower.min(simba_fence.lower),
                        (self.dec_key)(second.get_unchecked(0).key()));

                    let right_interval = Interval::new(
                        second.get_unchecked(0).key(),
                        candidate_fence.upper.max(simba_fence.upper));

                    first.sort_by_key(|r|
                        r.version().insertion_stamp().ts_start());

                    second.sort_by_key(|r|
                        r.version().insertion_stamp().ts_start());

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

                    let (s_keys, s_version, s_children)
                        = simba.keys_versions_pointers();

                    let mut joined = c_keys
                        .iter()
                        .zip(c_versions.iter())
                        .zip(c_children.iter())
                        .filter(|((.., v), ..)| v.is_active())
                        .sorted_by_key(|((k, ..), ..)| k.lower)
                        .merge_by(s_keys.iter()
                                      .zip(s_version.iter())
                                      .zip(s_children.iter())
                                      .filter(|((.., v), ..)| v.is_active())
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

                    first.sort_by_key(|((.., v), ..)| **v);
                    second.sort_by_key(|((.., v), ..)| **v);

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

        if active_block as usize >= block.filling_80_percent() {
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
                        .filter(|r| r.version().is_live())
                        .sorted_by_key(|r| r.key())
                        .collect_vec();

                    let middle = sorted_block.len() / 2;
                    let (first, second) = sorted_block
                        .split_at_mut(middle);

                    let fence_left = Interval::new(
                        fence.lower,
                        (self.dec_key)(second.get_unchecked(0).key));

                    if let PageType::LeafMut(leaf_page) = left.unsafe_borrow_mut().as_page_mut() {
                        first.sort_by_key(|r| r.version().insertion_stamp().ts_start());
                        leaf_page.bulk_push_from_slice_ref(first);
                    }

                    let fence_right = Interval::new(
                        second.get_unchecked(0).key,
                        fence.upper);

                    if let PageType::LeafMut(leaf_page) = right.unsafe_borrow_mut().as_page_mut() {
                        second.sort_by_key(|r| r.version().insertion_stamp().ts_start());
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

                    let mut filtered = key_intervals
                        .iter()
                        .zip(versions.iter())
                        .zip(pointers.iter())
                        .filter(|((.., v), ..)| v.is_active())
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
                        first.sort_by_key(|((.., v), ..)| **v);
                        internal_page.bulk_push_from_slice(first)
                    }

                    let fence_right = Interval::new(
                        second.get_unchecked(0).0.0.lower,
                        fence.upper);

                    if let PageType::IndexMut(internal_page) = right.unsafe_borrow_mut().as_page_mut() {
                        second.sort_by_key(|((.., v), ..)| **v);
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
                        .filter(|record| record.version().is_live())
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

                    let active_entries = key_intervals
                        .iter()
                        .zip(versions.iter())
                        .zip(pointers.iter())
                        .filter(|((.., v), ..)| v.is_active())
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
    ) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
        if VERBOSE {
            println!("merge root");
        }

        // `merge_root` was reached because `unsafe_degree_root()` observed
        // exactly 1 active child — but that check happened before
        // `master_guard`'s own upgrade, and `root_guard` itself is still
        // just a `Reader` at this point. If a *different* thread
        // concurrently treats this same root as mufasa for one of its
        // children (splitting or merging it — root's own write lock is
        // entirely free for that until now), root's content can change
        // between that original check and the `last_child()` read below.
        // See `on_overflow_node`'s matching comment for why a version check
        // can't substitute for actually excluding writers here.
        // if !root_guard.upgrade_write_lock() {
        //     return Err(());
        // }

        let child_ref = root_guard
            .as_internal_page_ref()
            .last_child();

        // Left as a plain `Reader` — `split_root` now excludes it itself
        // via `try_retire()` (see that function's doc), rather than this
        // caller pre-upgrading it to a `Writer` that `split_root` would
        // then just retire anyway.
        let child_guard = child_ref
            .borrow_read();

        if VERBOSE {
            println!("Old root height = {}, new height = {}", height, height - 1);
        }

        let guard
            = self.split_root(master_guard, child_guard, height - 1)?;

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
        height: Height,
    ) -> Result<BlockGuard<'a, FAN_OUT, NUM_RECORDS, Key, Payload>, ()>
    {
        // `try_retire()`, not `upgrade_write_lock()`: `root_guard` is only
        // ever read below (`split()` takes `&Block`, never mutates it in
        // place), and `split()` has no failure path — once this call is
        // reached, the old root is unconditionally superseded. Same
        // reasoning as `on_overflow_node`'s `simba`; see
        // `SmartGuard::try_retire`'s doc. Both callers (the root-overflow
        // arm in `retrieve_root_write_internal_olc`, and `merge_root`) pass
        // `root_guard`/`child_guard` in as a still-unexcluded `Reader`.
        let root_cell = root_guard.try_retire().map_err(|_| ())?;

        Ok(match self.split(root_cell.deref(), &Interval::new(self.min_key, self.max_key)) {
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

                new_root_latch
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

                new_root_latch
            }
        })
    }
}