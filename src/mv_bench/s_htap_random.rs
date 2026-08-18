//! Key-generation logic for the "S-HTAP" (streaming HTAP) workload: a
//! near-sorted arrival stream plus a narrow, recency-biased hot-update
//! window, modeled after real time-series/telemetry ingestion (Cooper et
//! al.'s YCSB request distributions cover neither of these — see this
//! module's docs on why each piece is new, and what's reused from
//! `ycsb_random` as-is).
//!
//! Real streaming systems (Flink/Beam) use a "watermark" to mark the point
//! in the key/event-time space past which data is considered settled: keys
//! behind the watermark rarely change again, keys at or ahead of it are
//! still being actively written. This workload models exactly that split so
//! it stresses BatStore's coldpages path (`mv_tree::smo`'s `VERSION_SPLIT`):
//! a narrow hot window absorbs repeated updates (concentrating surviving
//! versions on a handful of leaves) while concurrent OLAP scans hold open
//! snapshots that prevent GC from reclaiming those versions in between.

use std::cell::RefCell;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use rand::prelude::*;

use crate::mv_bench::ycsb_random::{KeySampler, RequestDistribution};
use crate::mv_bench::ycsb_schema::YcsbKey;

thread_local! {
    // Same rationale as `ycsb_random::FAST_RNG`: a non-cryptographic PRNG,
    // seeded once per thread, for load-generator draws that are never on
    // the benchmark's correctness-sensitive path. Not reused from
    // `ycsb_random` because that module's thread-local is private to it.
    static FAST_RNG: RefCell<SmallRng> = RefCell::new(rand::make_rng());
}

#[inline]
fn with_fast_rng<R>(f: impl FnOnce(&mut SmallRng) -> R) -> R {
    FAST_RNG.with(|rng| f(&mut rng.borrow_mut()))
}

/// Mints the next key for the arrival stream: a monotonic ticket counter
/// (guaranteeing every minted key is unique, so concurrent arrival threads
/// never mint the same key twice) with an optional bounded backward jitter
/// applied on top, modeling a stream's *allowed lateness* — real events
/// mostly arrive in timestamp order, but a bounded fraction arrive slightly
/// out of order relative to strictly increasing sequence numbers.
///
/// A jittered key can collide with one already emitted by an earlier
/// ticket; the caller (`s_htap_txn::arrival_upsert`) treats that as a
/// legitimate late-arriving upsert of an already-materialized row, not an
/// error — exactly how real stream processors handle out-of-order/
/// duplicate events under an idempotent upsert model.
pub fn mint_arrival_key(next_seq: &AtomicU64, max_lateness: u64) -> YcsbKey {
    let ticket = next_seq.fetch_add(1, Relaxed) + 1;
    if max_lateness == 0 {
        return ticket;
    }
    let lateness = with_fast_rng(|rng| rng.random_range(0..=max_lateness));
    ticket.saturating_sub(lateness).max(1)
}

/// Recency-biased sampler for the "hot tail": which of the `window` most
/// recently minted keys gets the next update. Reuses `ycsb_random`'s
/// `Latest` distribution machinery verbatim (an Alias-table Zipf sample
/// interpreted as "how many keys back from the newest", left unscrambled on
/// purpose — see that type's doc) but anchors it to a small, fixed-size
/// `window` instead of the whole loaded key range, so the hot set stays a
/// narrow handful of leaves even as the keyspace keeps growing — the
/// opposite of `Latest`, whose bias widens along with `record_count`.
pub struct HotTailSampler {
    sampler: KeySampler,
    window: u64,
}

impl HotTailSampler {
    pub fn new(theta: f64, window: u64) -> Self {
        Self {
            sampler: KeySampler::new(RequestDistribution::Latest { theta }, window.max(1)),
            window: window.max(1),
        }
    }

    /// `current_max_key` is the highest key minted so far. Returns a key in
    /// `[current_max_key.saturating_sub(window - 1), current_max_key]`,
    /// skewed towards the very newest key.
    pub fn sample(&self, current_max_key: u64) -> YcsbKey {
        self.sampler.sample(self.window, current_max_key)
    }
}

/// Write-side op mix: what fraction of write-thread iterations are brand
/// new arrivals (append near the tail) vs. hot-tail updates (revise an
/// already-arrived recent row) — the two write behaviors this workload is
/// specifically about, as opposed to YCSB's five general-purpose ops.
#[derive(Clone, Copy, Debug)]
pub struct SHtapMix {
    pub arrival: f64,
    pub hot_update: f64,
}

impl Default for SHtapMix {
    /// Mostly hot updates over a trickle of new arrivals — a dashboard-style
    /// "many small revisions to the last few minutes of data, occasionally
    /// appending a genuinely new row" pattern, rather than a pure insert
    /// firehose.
    fn default() -> Self {
        Self {
            arrival: 0.2,
            hot_update: 0.8,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SHtapWriteOp {
    Arrival,
    HotUpdate,
}

pub fn pick_write_op(mix: &SHtapMix) -> SHtapWriteOp {
    let total = mix.arrival + mix.hot_update;
    if total <= 0.0 {
        return SHtapWriteOp::HotUpdate;
    }
    let x = with_fast_rng(|rng| rng.random_range(0.0..total));
    if x < mix.arrival {
        SHtapWriteOp::Arrival
    } else {
        SHtapWriteOp::HotUpdate
    }
}

/// Computes an OLAP scan's `[lo, hi]` key interval relative to the current
/// tail, letting the same two knobs express all three region shapes this
/// workload cares about:
/// - Pure hot-tail scan: `lag = 0`, `span <= hot_window`.
/// - Pure cold-historical scan: `lag >= hot_window` (scan never reaches the
///   still-mutating tail).
/// - Straddling scan (the interesting default): `lag = 0`,
///   `span > hot_window` — starts at the current tail and reads backward
///   through the hot window into settled cold history in one snapshot,
///   exactly the "dashboard query over the last N rows" shape that forces a
///   single scan to cross the cold/hot boundary.
pub fn olap_scan_bounds(current_max_key: u64, lag: u64, span: u64) -> (YcsbKey, u64) {
    let hi = current_max_key.saturating_sub(lag).max(1);
    let lo = hi.saturating_sub(span.saturating_sub(1)).max(1);
    (lo, hi - lo + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrival_keys_are_unique_without_lateness() {
        let seq = AtomicU64::new(0);
        let mut keys = std::collections::HashSet::new();
        for _ in 0..10_000 {
            assert!(keys.insert(mint_arrival_key(&seq, 0)));
        }
    }

    #[test]
    fn arrival_keys_stay_close_to_the_ticket_with_lateness() {
        let seq = AtomicU64::new(1_000_000);
        for _ in 0..10_000 {
            let before = seq.load(Relaxed);
            let key = mint_arrival_key(&seq, 50);
            assert!(key <= before + 1);
            assert!(key >= before + 1 - 50);
        }
    }

    #[test]
    fn hot_tail_sampler_stays_within_window() {
        let sampler = HotTailSampler::new(0.99, 1_000);
        let current_max_key = 50_000u64;
        for _ in 0..10_000 {
            let key = sampler.sample(current_max_key);
            assert!(key <= current_max_key);
            assert!(key > current_max_key - 1_000);
        }
    }

    #[test]
    fn olap_bounds_hot_tail() {
        let (lo, len) = olap_scan_bounds(100_000, 0, 1_000);
        assert_eq!(len, 1_000);
        assert_eq!(lo + len - 1, 100_000);
    }

    #[test]
    fn olap_bounds_cold_history() {
        let (lo, len) = olap_scan_bounds(100_000, 10_000, 2_000);
        assert_eq!(lo + len - 1, 90_000);
    }

    #[test]
    fn olap_bounds_straddle() {
        let (lo, len) = olap_scan_bounds(100_000, 0, 3_000);
        assert_eq!(lo + len - 1, 100_000);
        assert_eq!(lo, 97_001);
    }
}
