use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

use parking_lot::Mutex;

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
    shards: Vec<Mutex<VecDeque<DeadPageEntry<P_F, P_N, Key, Payload>>>>,
    next_scan: AtomicUsize,
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
                .map(|_| Mutex::new(VecDeque::new()))
                .collect(),
            next_scan: AtomicUsize::new(0),
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

    #[inline]
    fn drain_eligible_from(
        &self,
        shard_index: usize,
        remaining: &mut usize,
        eligible: &mut impl FnMut(DeadPageKey) -> bool,
        out: &mut Vec<DeadPageValue<P_F, P_N, Key, Payload>>,
    ) {
        if *remaining == 0 {
            return;
        }
        let mut shard = self.shards[shard_index].lock();
        while *remaining != 0 {
            match shard.front() {
                Some((key, _)) if eligible(*key) => {
                    let (_, page) = shard.pop_front().expect("front was just observed");
                    out.push(page);
                    *remaining -= 1;
                }
                _ => break,
            }
            }
        }

    /// Reclaims up to `limit` pages using one eligibility bound computed by
    /// the caller. The owning shard is drained first; only then are remote
    /// shards visited from a rotating start position.
    #[inline]
    pub(crate) fn reclaim_batch(
        &self,
        worker_id: WorkerId,
        limit: usize,
        mut eligible: impl FnMut(DeadPageKey) -> bool,
    ) -> Vec<DeadPageValue<P_F, P_N, Key, Payload>> {
        let mut out = Vec::with_capacity(limit);
        let mut remaining = limit;
        let own = self.shard_for(worker_id);
        self.drain_eligible_from(own, &mut remaining, &mut eligible, &mut out);

        if remaining != 0 && self.shards.len() > 1 {
            let start = self.next_scan.fetch_add(1, Relaxed) % self.shards.len();
            for offset in 0..self.shards.len() {
                let shard = (start + offset) % self.shards.len();
                if shard != own {
                    self.drain_eligible_from(shard, &mut remaining, &mut eligible, &mut out);
                    if remaining == 0 {
                        break;
            }
                }
            }
        }
        out
    }
}