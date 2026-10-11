use crossbeam_channel::{Receiver, Sender};
use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::fmt::Display;
use std::hash::Hash;

/// Number of blocks obtained on an allocator miss, including the requested block.
pub const ALLOC_BATCH_SIZE: usize = 4;
/// Maximum fraction of worker queues probed on a reclaim miss.
pub const SCAN_PERCENT: usize = 25;

fn setting(name: &str, default: usize, max: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => {
            let parsed = value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("{name} must be an integer from 1 to {max}"));
            assert!(
                (1..=max).contains(&parsed),
                "{name} must be an integer from 1 to {max}"
            );
            parsed
        }
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("{name}: {error}"),
    }
}

fn optional_setting(name: &str, max: usize) -> Option<usize> {
    match std::env::var(name) {
        Ok(value) => {
            let parsed = value
                .parse::<usize>()
                .unwrap_or_else(|_| panic!("{name} must be an integer from 0 to {max}"));
            assert!(parsed <= max, "{name} must be an integer from 0 to {max}");
            Some(parsed)
        }
        Err(std::env::VarError::NotPresent) => None,
        Err(error) => panic!("{name}: {error}"),
    }
}

use crate::bat_page_model::BlockRef;
use crate::bat_record_model::tx_stamp::WorkerId;
use crate::bat_record_model::version_info::Version;

pub(crate) type DeadPageValue<const FAN_OUT: usize, const NUM_RECORDS: usize, Key, Payload> =
    BlockRef<FAN_OUT, NUM_RECORDS, Key, Payload>;
pub(crate) type DeadPageKey = (Version, usize);

type DeadPageEntry<const F: usize, const N: usize, Key, Payload> =
    (DeadPageKey, DeadPageValue<F, N, Key, Payload>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PageSource {
    Fresh,
    LocalRetired,
    StolenRetired,
}

pub(crate) struct ReclaimedPage<const F: usize, const N: usize, Key, Payload>
where
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default,
{
    pub(crate) page: DeadPageValue<F, N, Key, Payload>,
    pub(crate) source: PageSource,
}

struct BlockTraceShard<
    const F: usize,
    const N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default,
> {
    /// Freshly allocated spare pages are always eligible for reuse, so they
    /// need neither death-version ordering nor the retired-page mutex.
    fresh_tx: Sender<DeadPageValue<F, N, Key, Payload>>,
    fresh_rx: Receiver<DeadPageValue<F, N, Key, Payload>>,
    retired: Mutex<VecDeque<DeadPageEntry<F, N, Key, Payload>>>,
}

impl<
    const F: usize,
    const N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default,
> BlockTraceShard<F, N, Key, Payload>
{
    fn new() -> Self {
        let (fresh_tx, fresh_rx) = crossbeam_channel::unbounded();
        Self {
            fresh_tx,
            fresh_rx,
            retired: Mutex::new(VecDeque::new()),
        }
    }
}

/// Fixed page-reuse shards. Always-eligible fresh pages use a
/// concurrent queue, while versioned retired pages remain ordered behind a
/// mutex. Registration normally touches only the worker's assigned shard. An
/// allocator may steal from another shard, so the ordered queue still needs
/// synchronization, but fresh-page traffic never takes that lock.
///
/// A thread may be delayed between drawing a version and registering its page,
/// so append order alone is not sufficient. Registration keeps each deque
/// ordered by `(death version,
/// address)`; the overwhelmingly common monotonic case remains `push_back`,
/// while a delayed entry takes one binary search and insertion.
pub(crate) struct BlockTrace<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display + 'static,
    Payload: Clone + Default + 'static,
> {
    shards: Box<[CachePadded<BlockTraceShard<P_F, P_N, Key, Payload>>]>,
    batch_size: usize,
    scan_percent: usize,
    /// Exact number of non-local shards to probe. When absent, retain the
    /// legacy percentage-based policy for backwards compatibility.
    max_neighbors: Option<usize>,
}

impl<
    const P_F: usize,
    const P_N: usize,
    Key: Copy + Default + Hash + Ord + Display,
    Payload: Clone + Default,
> BlockTrace<P_F, P_N, Key, Payload>
{
    pub(crate) fn new_with_shard_count(shard_count: usize) -> Self {
        Self::with_config(
            shard_count,
            setting("BATSTORE_GC_BATCH_SIZE", ALLOC_BATCH_SIZE, 1024),
            setting("BATSTORE_GC_SCAN_PERCENT", SCAN_PERCENT, 100),
            optional_setting("BATSTORE_GC_MAX_NEIGHBORS", shard_count.saturating_sub(1)),
        )
    }

    fn with_config(
        shard_count: usize,
        batch_size: usize,
        scan_percent: usize,
        max_neighbors: Option<usize>,
    ) -> Self {
        assert!(shard_count > 0);
        Self {
            shards: (0..shard_count)
                .map(|_| CachePadded::new(BlockTraceShard::new()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            batch_size,
            scan_percent,
            max_neighbors,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(shard_count: usize, batch_size: usize) -> Self {
        Self::with_config(
            shard_count,
            batch_size,
            100,
            Some(shard_count.saturating_sub(1)),
        )
    }

    #[inline(always)]
    pub(crate) fn batch_size(&self) -> usize {
        self.batch_size
    }

    #[inline(always)]
    pub(crate) fn take_local_fresh(
        &self,
        worker_id: WorkerId,
    ) -> Option<DeadPageValue<P_F, P_N, Key, Payload>> {
        self.shards[self.shard_for(worker_id)]
            .fresh_rx
            .try_recv()
            .ok()
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
            &mut self.shards[self.shard_for(worker_id)].retired.lock(),
            (key, page),
        );
    }

    #[inline(always)]
    pub(crate) fn register_died_page_col(
        &self,
        worker_id: WorkerId,
        dead_pages: [(Version, BlockRef<P_F, P_N, Key, Payload>); 2],
    ) {
        let mut shard = self.shards[self.shard_for(worker_id)].retired.lock();
        for (version, page) in dead_pages {
            Self::push_ordered(&mut shard, ((version, page.0 as usize), page));
        }
    }

    /// Unused new blocks are always eligible, including with live snapshots,
    /// so they bypass the ordered retired-page queue and its mutex entirely.
    pub(crate) fn register_fresh_batch(
        &self,
        worker_id: WorkerId,
        pages: impl IntoIterator<Item = DeadPageValue<P_F, P_N, Key, Payload>>,
    ) {
        let shard = &self.shards[self.shard_for(worker_id)];
        for page in pages {
            // The receiver lives in the same shard for the lifetime of this
            // sender, so disconnection is impossible here.
            shard
                .fresh_tx
                .send(page)
                .expect("fresh-page queue receiver must remain connected");
        }
    }

    #[inline]
    fn drain_local(
        &self,
        shard_index: usize,
        remaining: &mut usize,
        eligible: &mut impl FnMut(DeadPageKey) -> bool,
        out: &mut Vec<ReclaimedPage<P_F, P_N, Key, Payload>>,
    ) {
        if *remaining == 0 {
            return;
        }
        let shard = &self.shards[shard_index];

        // Fresh pages are unconditionally eligible. Drain them before taking
        // the ordered retired-page lock, both to prioritize already-reserved
        // capacity and to keep this common path lock-free.
        while *remaining != 0 {
            let Ok(page) = shard.fresh_rx.try_recv() else {
                break;
            };
            out.push(ReclaimedPage {
                page,
                source: PageSource::Fresh,
            });
            *remaining -= 1;
        }
        if *remaining == 0 {
            return;
        }

        let Some(mut retired) = shard.retired.try_lock() else {
            return;
        };
        while *remaining != 0 {
            match retired.front() {
                Some((key, _)) if eligible(*key) => {
                    let (_, page) = retired.pop_front().expect("front was just observed");
                    out.push(ReclaimedPage {
                        page,
                        source: PageSource::LocalRetired,
                    });
                    *remaining -= 1;
                }
                _ => break,
            }
        }
    }

    #[inline]
    fn drain_retired_from(
        &self,
        shard_index: usize,
        remaining: &mut usize,
        eligible: &mut impl FnMut(DeadPageKey) -> bool,
        source: PageSource,
        out: &mut Vec<ReclaimedPage<P_F, P_N, Key, Payload>>,
    ) {
        if *remaining == 0 {
            return;
        }
        let Some(mut retired) = self.shards[shard_index].retired.try_lock() else {
            return;
        };
        while *remaining != 0 {
            match retired.front() {
                Some((key, _)) if eligible(*key) => {
                    let (_, page) = retired.pop_front().expect("front was just observed");
                    out.push(ReclaimedPage { page, source });
                    *remaining -= 1;
                }
                _ => break,
            }
        }
    }

    #[inline]
    pub(crate) fn reclaim_batch(
        &self,
        worker_id: WorkerId,
        limit: usize,
        allow_steal: bool,
        mut eligible: impl FnMut(DeadPageKey) -> bool,
    ) -> (Vec<ReclaimedPage<P_F, P_N, Key, Payload>>, usize) {
        let mut out = Vec::with_capacity(limit);
        let mut remaining = limit;
        let own = self.shard_for(worker_id);
        self.drain_local(own, &mut remaining, &mut eligible, &mut out);
        let mut checked = 1;
        if allow_steal && out.is_empty() && self.shards.len() > 1 {
            // `checked` includes the local shard. The explicit setting counts
            // only neighbours, which makes zero a useful local-only baseline.
            // If it is not set, preserve BATSTORE_GC_SCAN_PERCENT semantics.
            let max_checked = self.max_neighbors.map_or_else(
                || {
                    self.shards
                        .len()
                        .saturating_mul(self.scan_percent)
                        .div_ceil(100)
                        .max(1)
                },
                |neighbors| neighbors.saturating_add(1),
            );
            let start = fastrand::usize(..self.shards.len());
            for offset in 0..self.shards.len() {
                if checked >= max_checked {
                    break;
                }
                let shard = (start + offset) % self.shards.len();
                if shard != own {
                    checked += 1;
                    // Fresh batch reserves belong to their allocating worker.
                    // Stealing is only from another worker's retired-page list.
                    self.drain_retired_from(
                        shard,
                        &mut remaining,
                        &mut eligible,
                        PageSource::StolenRetired,
                        &mut out,
                    );
                    if !out.is_empty() {
                        break;
                    }
                }
            }
        }
        (out, checked)
    }

    /// Removes eligible retired pages from every shard. Fresh batch reserves
    /// are deliberately excluded: OS mode may use those for subsequent local
    /// allocations, but never reuses a retired page.
    pub(crate) fn reclaim_retired_for_os(
        &self,
        worker_id: WorkerId,
        limit: usize,
        mut eligible: impl FnMut(DeadPageKey) -> bool,
    ) -> Vec<ReclaimedPage<P_F, P_N, Key, Payload>> {
        let mut out = Vec::with_capacity(limit);
        let mut remaining = limit;
        let own = self.shard_for(worker_id);
        self.drain_retired_from(
            own,
            &mut remaining,
            &mut eligible,
            PageSource::LocalRetired,
            &mut out,
        );
        for shard in 0..self.shards.len() {
            if remaining == 0 {
                break;
            }
            if shard != own {
                self.drain_retired_from(
                    shard,
                    &mut remaining,
                    &mut eligible,
                    PageSource::StolenRetired,
                    &mut out,
                );
            }
        }
        out
    }
}
