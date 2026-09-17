use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use crossbeam_utils::CachePadded;
use parking_lot::Mutex;

/// Number of blocks obtained on an allocator miss, including the requested block.
pub const ALLOC_BATCH_SIZE: usize = 16;
/// Maximum fraction of worker queues probed on a reclaim miss.
pub const SCAN_PERCENT: usize = 25;

use crate::bat_page_model::BlockRef;
use crate::bat_record_model::tx_stamp::WorkerId;
use crate::bat_record_model::version_info::Version;

pub(crate) type DeadPageValue<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload> =
    BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>;
pub(crate) type DeadPageKey = (Version, usize);

type DeadPageEntry<const F: usize, const N: usize, Key, Payload> =
    (DeadPageKey, DeadPageValue<F, N, Key, Payload>);

/// Per-worker-sharded retired-page queues. Registration is append-only and
/// normally touches only the worker's own queue. A mutex is still required
/// because an allocator may steal from another worker, but this removes the
/// allocation and pointer-chasing cost of one concurrent skip-list node per
/// retired page.
///
/// Workers can share a CPU-count shard and a thread may be delayed between
/// drawing a version and registering its page, so append order alone is not
/// sufficient. Registration keeps each deque ordered by `(death version,
/// address)`; the overwhelmingly common monotonic case remains `push_back`,
/// while a delayed entry takes one binary search and insertion.
pub(crate) struct BlockTrace<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static,
> {
    shards: Vec<CachePadded<Mutex<VecDeque<DeadPageEntry<P_F, P_N, Key, Payload>>>>>,
}

impl<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default,
> BlockTrace<P_F, P_N, Key, Payload>
{
    pub(crate) fn new() -> Self {
        let shard_count = num_cpus::get().max(1);
        Self {
            shards: (0..shard_count)
                .map(|_| CachePadded::new(Mutex::new(VecDeque::new())))
                .collect(),
        }
    }

    #[inline(always)]
    fn shard_for(&self, worker_id: WorkerId) -> usize {
        worker_id as usize % self.shards.len()
    }

    #[inline]
    fn push_ordered(
        shard: &mut VecDeque<DeadPageEntry<P_F, P_N, Key, Payload>>,
        entry: DeadPageEntry<P_F, P_N, Key, Payload>,
    ) {
        if shard.back().is_none_or(|back| back.0 <= entry.0) {
            shard.push_back(entry);
            return;
        }
        let position = shard
            .make_contiguous()
            .partition_point(|existing| existing.0 < entry.0);
        shard.insert(position, entry);
    }

    #[inline(always)]
    pub(crate) fn register_died_page(
        &self,
        worker_id: WorkerId,
        page_version: Version,
        page: DeadPageValue<P_F, P_N, Key, Payload>,
    ) {
        let key = (page_version, page.0 as usize);
        Self::push_ordered(
            &mut self.shards[self.shard_for(worker_id)].lock(),
            (key, page),
        );
    }

    #[inline(always)]
    pub(crate) fn register_died_page_col(
        &self,
        worker_id: WorkerId,
        dead_pages: [(Version, BlockRef<P_F, P_N, Key, Payload>); 2],
    ) {
        let mut shard = self.shards[self.shard_for(worker_id)].lock();
        for (version, page) in dead_pages {
            Self::push_ordered(&mut shard, ((version, page.0 as usize), page));
        }
    }

    /// Unused new blocks have death version zero and sit at the front of the
    /// owner's queue. They are always eligible, including with live snapshots.
    pub(crate) fn register_fresh_batch(&self, worker_id: WorkerId, pages: impl IntoIterator<Item = DeadPageValue<P_F, P_N, Key, Payload>>) {
        let mut shard = self.shards[self.shard_for(worker_id)].lock();
        for page in pages {
            shard.push_front(((0, page.0 as usize), page));
        }
    }

    #[inline]
    fn drain_eligible_from(
        &self,
        shard_index: usize,
        remaining: &mut usize,
        eligible: &mut impl FnMut(DeadPageKey) -> bool,
        out: &mut Vec<DeadPageEntry<P_F, P_N, Key, Payload>>,
    ) {
        if *remaining == 0 {
            return;
        }
        let Some(mut shard) = self.shards[shard_index].try_lock() else { return; };
        while *remaining != 0 {
            match shard.front() {
                Some((key, _)) if eligible(*key) => {
                    out.push(shard.pop_front().expect("front was just observed"));
                    *remaining -= 1;
                }
                _ => break,
            }
        }
    }

    /// Reclaims up to `limit` pages using one eligibility bound computed by
    /// the caller. The owning shard is checked first; if empty, remote shards
    /// are visited linearly from a random start until a usable page is found
    /// or the configured scan fraction is exhausted.
    ///
    /// Returns `(pages, local_count, stolen_count)` — `local_count` is how
    /// many of `pages` came from `worker_id`'s own shard, `stolen_count` how
    /// many came from another worker's shard (a "steal"). Callers that don't
    /// care about the H6-style local-vs-steal breakdown can just use
    /// `pages.len() == local_count + stolen_count`.
    #[inline]
    pub(crate) fn reclaim_batch(
        &self,
        worker_id: WorkerId,
        limit: usize,
        mut eligible: impl FnMut(DeadPageKey) -> bool,
    ) -> (Vec<DeadPageEntry<P_F, P_N, Key, Payload>>, usize, usize, usize) {
        let mut out = Vec::with_capacity(limit);
        let mut remaining = limit;
        let own = self.shard_for(worker_id);
        self.drain_eligible_from(own, &mut remaining, &mut eligible, &mut out);
        let local_count = out.len();
        let mut checked = 1;
        if out.is_empty() && self.shards.len() > 1 {
            let max_checked = self.shards.len().saturating_mul(SCAN_PERCENT).div_ceil(100).max(1);
            let start = fastrand::usize(..self.shards.len());
            for offset in 0..self.shards.len() {
                if checked >= max_checked { break; }
                let shard = (start + offset) % self.shards.len();
                if shard != own {
                    checked += 1;
                    self.drain_eligible_from(shard, &mut remaining, &mut eligible, &mut out);
                    if !out.is_empty() {
                        break;
                    }
                }
            }
        }
        let stolen_count = out.len() - local_count;
        (out, local_count, stolen_count, checked)
    }
}
