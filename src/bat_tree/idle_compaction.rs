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
use std::sync::atomic::{AtomicBool, Ordering::Relaxed};
use std::thread;
use std::time::Duration;

use crate::bat_query::interval::Interval;
use crate::bat_query::iter_query::RangeQueryIter;
use crate::bat_tree::mvbt::MVBTSt;

/// The dead-ratio threshold workloads default to when GC is on and no
/// caller-supplied value overrides it: a leaf qualifies for a forced
/// compaction once at least half its raw slots are dead. Low enough that a
/// realistic write pattern actually crosses it (see this module's own doc
/// on why a read-heavy leaf can otherwise sit at a garbage-inflated ratio
/// indefinitely), high enough that a sparsely-updated leaf isn't churned by
/// every sweep for a handful of dead entries.
pub const DEFAULT_VACUUM_DEAD_RATIO: f64 = 0.5;

/// The sweep interval workloads default to alongside
/// `DEFAULT_VACUUM_DEAD_RATIO`: frequent enough that a leaf crossing the
/// threshold doesn't sit ratio-inflated for long, infrequent enough that
/// the sweep itself (already lowest-OS-priority, see
/// `bat_db::database::lower_current_thread_priority`) stays a small
/// fraction of overall background CPU.
pub const DEFAULT_VACUUM_SWEEP_INTERVAL: Duration = Duration::from_secs(5);

/// Spawns a background vacuum thread for a single, bare (not
/// `bat_db::Database`-owned) tree — for a driver like `ycsb_driver`/
/// `s_ycsb_driver` that runs one standalone `MVBTSt` rather than a
/// multi-table `Database`, so `bat_db::database::Database::set_vacuum`'s
/// own multi-table sweep doesn't apply. Same mechanism otherwise: repeatedly
/// calls `compact_idle_pass(dead_ratio_threshold)`, sleeping
/// `sweep_interval` between sweeps, until `stop` is set, at the same
/// lowest-OS-priority `bat_db::database::lower_current_thread_priority`
/// gives every other vacuum thread in this codebase. Callers own `stop` and
/// are responsible for setting it (and, if they care about a clean
/// shutdown, joining the returned handle) — this doesn't wait for anything
/// on its own. Takes `triomphe::Arc` specifically, the one `Arc` every
/// workload driver's tree handle uses (see `tpcc_driver`/`ycsb_driver`/
/// `s_ycsb_driver`, all `use triomphe::Arc`), same as `bat_db::Database`'s
/// own table storage.
pub fn spawn_vacuum_thread<
    const FAN_OUT: usize,
    const NUM_RECORDS: usize,
    Key: Default + Ord + Copy + Hash + Display + Sync + 'static,
    Payload: Display + Clone + Default + Sync + 'static,
>(
    tree: triomphe::Arc<MVBTSt<FAN_OUT, NUM_RECORDS, Key, Payload>>,
    dead_ratio_threshold: f64,
    sweep_interval: Duration,
    stop: std::sync::Arc<AtomicBool>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        crate::bat_db::database::lower_current_thread_priority();
        while !stop.load(Relaxed) {
            tree.compact_idle_pass(dead_ratio_threshold);
            if stop.load(Relaxed) {
                break;
            }
            thread::sleep(sweep_interval);
        }
    })
}

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
