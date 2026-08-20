//! Idle/proactive compaction: finds leaves whose dead/live ratio has
//! crossed a threshold and compacts them independent of any write ever
//! physically overflowing them.
//!
//! Why this needs to exist at all: `bat_tree::smo::split`'s `ByVersion`
//! branch already drops every non-surviving record when it runs — that
//! *is* compaction — but it's only ever reached from `on_overflow_node`,
//! itself only reached mid-write-traversal when a leaf's raw physical slot
//! count overflows (`MVBTSt::traversal_write_olc`/`traversal_write_olc_registered`).
//! A read-heavy table with few, infrequent writes to any given leaf (TPC-C's
//! `Warehouse`/`District` are the canonical example) can sit at a heavily
//! garbage-inflated dead/live ratio indefinitely between those rare
//! overflows — every OLAP scan over it pays to visit and discard that
//! garbage on every single pass in the meantime (see `bat_test::SCAN_TRACE`'s
//! per-table visited/matched breakdown).
//!
//! The mechanism below has two independent halves, each doing one job:
//!  - the read-only candidate scan (`RangeQueryIter::for_each_leaf_ratio`,
//!    `bat_query::iter_query`) — cheap, one packed-length read per leaf,
//!    no record-level work and no visibility check at all;
//!  - the forced compaction itself (`MVBTSt::compact_leaf_olc`,
//!    `bat_query::olc_query`) — the exact same latch/split/commit protocol
//!    a real physical overflow already gets, just triggered by dead ratio
//!    instead of raw slot count.

use std::fmt::Display;
use std::hash::Hash;

use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_tree::mvbt::MVBTSt;

impl<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
> MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>
{
    /// One idle-compaction sweep: scans every leaf currently reachable from
    /// this tree's latest root, then forces a compaction
    /// (`compact_leaf_olc`) on each whose dead/(active+dead) ratio is at or
    /// above `dead_ratio_threshold`. Returns how many leaves were actually
    /// compacted — a candidate that turned out already-fine by the time the
    /// forced descent reached it (see `compact_leaf_olc`'s doc for why that
    /// can happen) doesn't count.
    ///
    /// Candidates are collected into a `Vec` before any compaction starts,
    /// rather than compacting inline from the scan's own visitor:
    /// `compact_leaf_olc` takes write locks and can restructure the very
    /// node the read-only scan is mid-descent through, and the scan already
    /// holds its own reader-snapshot registration for its whole duration —
    /// recursing into a write from inside its visitor would self-deadlock
    /// against that registration the moment GC ever has to wait on it.
    pub fn compact_idle_pass(&self, dead_ratio_threshold: f64) -> usize {
        let version = self.current_version();
        let worker_id = self.worker_id();
        let full_range = Interval::new(self.cold.min_key, self.cold.max_key);

        let mut candidates: Vec<Key> = Vec::new();
        RangeQueryIter::new(self, version, full_range, true, worker_id).for_each_leaf_ratio(
            |fence, active, dead| {
                let total = active as u64 + dead as u64;
                if total > 0 && dead as f64 / total as f64 >= dead_ratio_threshold {
                    candidates.push(fence.lower);
                }
            },
        );

        candidates
            .into_iter()
            .filter(|&key| self.compact_leaf_olc(key, dead_ratio_threshold))
            .count()
    }
}
