use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;

use crossbeam_skiplist::SkipMap;
use crate::mv_page_model::BlockRef;
use crate::mv_record_model::tx_stamp::WorkerId;
use crate::mv_record_model::version_info::Version;

pub(crate) type DeadPageValue<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload>
= BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>;

type BlockTracerIndex<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload>
= SkipMap<DeadPageKey, DeadPageValue<FAN_OUT, NUM_RECORDS, Key, Payload>>;

/// `(death version, block's own address)`. The address only exists to make
/// this unique — reclaim-eligibility ordering only ever looks at the first
/// element. Needed because a merge's two retiring blocks (`simba`/
/// `candidate`) deliberately share the *same* death version (the new
/// replacement's birth version — see `MVBTSt::merge`'s callers in
/// `mv_tree::smo`), and `SkipMap::insert` on a duplicate key removes the
/// existing entry before inserting the new one (its own doc says so): a
/// bare `Version` key here silently displaced one of the two from
/// `dead_blocks` on every such merge — retired (so never write-lockable
/// again) but no longer tracked for reclaim, i.e. permanently leaked from
/// the reuse pool. Confirmed via `mv_tree::smo`'s two `register_dead_col`
/// call sites, which pass the identical `version` for both entries of the
/// pair.
pub(crate) type DeadPageKey = (Version, usize);

/// Sharded by worker: death versions come from one global clock
/// (`MVBTSt::start_tx_commit`), so concurrent SMOs across every worker insert
/// into what used to be a *single* skip list, all clustering near its current
/// max key — its "always-contended tail" (see `TransactionTrace`'s doc for
/// the same shape of problem, measured 6-9x/8-9x under sustained concurrent
/// load elsewhere in this GC, and `TxContext::in_flight_bound`'s doc for
/// another instance of it — a single shared `registrations_in_flight`
/// counter every worker contended on, replaced by the same per-worker-slot
/// idea used here). One shard per worker (mirrors `mv_wal`'s per-worker
/// WAL shards) means each worker's own `register_died_page` calls only ever
/// contend with themselves, not with every other worker's.
///
/// `free_block` doesn't need the *global* minimum-death-version block, just
/// *a* reclaimable one — it already tolerates popping a not-yet-eligible
/// entry and reinserting it (see that method) — so scanning shard-by-shard
/// instead of one global order is a correctness-preserving trade of a little
/// pop-order precision for much less insert contention.
pub(crate) struct BlockTrace<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static>
{
    shards: Vec<BlockTracerIndex<P_F, P_N, Key, Payload>>,
    // Round-robins the *stealing* scan's start across the other shards, once
    // a caller's own shard has already been tried and come up empty (see
    // `try_reclaim`), so no single remote shard is favored steal after steal
    // (which would otherwise silently recreate the same hot-shard contention
    // this sharding exists to avoid).
    next_scan: AtomicUsize,
}

impl<const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default> BlockTrace<P_F, P_N, Key, Payload>
{
    pub(crate) fn new() -> Self {
        let shard_count = num_cpus::get().max(1);
        Self {
            shards: (0..shard_count).map(|_| SkipMap::new()).collect(),
            next_scan: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    fn shard_for(&self, worker_id: WorkerId) -> usize {
        worker_id as usize % self.shards.len()
    }

    #[inline(always)]
    pub(crate) fn register_died_page(&self, worker_id: WorkerId, page_version: Version, page: DeadPageValue<P_F, P_N, Key, Payload>) {
        let shard = self.shard_for(worker_id);
        let key: DeadPageKey = (page_version, page.0 as usize);
        let _ = self.shards[shard].insert(key, page);
    }

    #[inline(always)]
    pub(crate) fn register_died_page_col(&self, worker_id: WorkerId, dead_pages: [(Version, BlockRef<P_F, P_N, Key, Payload>); 2]) {
        dead_pages
            .into_iter()
            .for_each(|(d_v, d_p)| self.register_died_page(worker_id, d_v, d_p))
    }

    /// Reinserts an entry `free_block` popped but found not yet eligible,
    /// back into the exact shard it came from (not re-sharded by worker —
    /// there's no reader here to attribute it to). Reuses the exact `key`
    /// `pop_min_at` returned rather than rebuilding one, so this can never
    /// collide with whatever else has since been inserted at this shard.
    #[inline(always)]
    fn reinsert_at(&self, shard: usize, key: DeadPageKey, page: DeadPageValue<P_F, P_N, Key, Payload>) {
        let _ = self.shards[shard].insert(key, page);
    }

    #[inline(always)]
    fn pop_min_at(&self, shard: usize) -> Option<(DeadPageKey, DeadPageValue<P_F, P_N, Key, Payload>)> {
        self.shards[shard]
            .pop_front()
            .map(|entry| (*entry.key(), entry.value().clone()))
    }

    /// Tries the calling worker's own shard first — reusing a worker's own
    /// dead pages needs no cross-shard traffic at all, so this is the cheap,
    /// common case. Only if `worker_id`'s shard is empty or its one dead page
    /// isn't old enough yet does this fall back to "stealing": scanning
    /// every other shard once, starting from a round-robin cursor, handing
    /// `try_reclaim` each shard's current minimum in turn. `try_reclaim`
    /// returns `Ok(block)` to reclaim it, or `Err(())` to reject it (it'll be
    /// reinserted into the same shard it came from) and move on to the next
    /// shard. Stops at the first reclaimed block; `None` if no shard yields
    /// one.
    #[inline]
    pub(crate) fn try_reclaim(
        &self,
        worker_id: WorkerId,
        mut try_reclaim: impl FnMut(DeadPageKey) -> bool,
    ) -> Option<DeadPageValue<P_F, P_N, Key, Payload>> {
        let own_shard = self.shard_for(worker_id);

        if let Some((dead_v, dead_block)) = self.pop_min_at(own_shard) {
            if try_reclaim(dead_v) {
                return Some(dead_block);
            }
            self.reinsert_at(own_shard, dead_v, dead_block);
        }

        // Own shard had nothing usable — steal from the rest.
        let shard_count = self.shards.len();
        let start = self.next_scan.fetch_add(1, Relaxed) % shard_count;

        for i in 0..shard_count {
            let shard = (start + i) % shard_count;
            if shard == own_shard {
                continue;
            }
            if let Some((dead_v, dead_block)) = self.pop_min_at(shard) {
                if try_reclaim(dead_v) {
                    return Some(dead_block);
                }
                self.reinsert_at(shard, dead_v, dead_block);
            }
        }

        None
    }
}