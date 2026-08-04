//! YCSB's request-distribution and operation-mix definitions (Cooper et al.,
//! SoCC 2010, §3): which key an operation targets, and the standard "Core
//! Workloads" A-F op-type proportions.

use rand::distr::Alphanumeric;
use rand::prelude::*;
use rand_distr::Zipf;
use std::cell::RefCell;

use crate::mv_bench::ycsb_schema::{YcsbConfig, YcsbKey, YcsbRow};

thread_local! {
    // `rand::rng()` (used throughout this file previously) is a cryptographically
    // secure generator (ChaCha-backed) - overkill for load-generator data that
    // never needs to be unpredictable, and expensive enough that it dominated
    // whole-benchmark `perf` profiles (a plain YCSB-C run spent ~14% of all
    // cycles just picking which key to read). `SmallRng` (Xoshiro256++) is
    // seeded once per thread from the real thread-local RNG via `make_rng` -
    // paying the crypto-RNG cost exactly once, not once per op - then every
    // draw after that is a cheap, non-cryptographic PRNG step.
    static FAST_RNG: RefCell<SmallRng> = RefCell::new(rand::make_rng());
}

#[inline]
fn with_fast_rng<R>(f: impl FnOnce(&mut SmallRng) -> R) -> R {
    FAST_RNG.with(|rng| f(&mut rng.borrow_mut()))
}

/// YCSB `requestdistribution`: which key an op targets, relative to the
/// current loaded key range.
#[derive(Clone, Copy, Debug)]
pub enum RequestDistribution {
    /// Every key in `1..=n` equally likely.
    Uniform,
    /// Skewed towards low keys (YCSB `zipfian`); `theta` is the skew
    /// constant (YCSB default 0.99 — higher is more skewed).
    Zipfian { theta: f64 },
    /// Skewed towards the *most recently inserted* keys (YCSB `latest`),
    /// used by Workload D ("read latest"); same `theta` meaning as
    /// `Zipfian`, just anchored at the current max key instead of at 1.
    Latest { theta: f64 },
}

/// Precomputes the `Zipf` distribution once (construction has real setup
/// cost — see the loop in `mv_test::olap_tests`, which does the same) and
/// reuses it for every sampled key, rather than rebuilding it per op.
pub struct KeySampler {
    dist: RequestDistribution,
    zipf: Option<Zipf<f64>>,
}

impl KeySampler {
    pub fn new(dist: RequestDistribution, record_count: u64) -> Self {
        let zipf = match dist {
            RequestDistribution::Uniform => None,
            RequestDistribution::Zipfian { theta } | RequestDistribution::Latest { theta } =>
                Some(Zipf::new(record_count.max(1) as f64, theta).expect("invalid zipfian theta")),
        };
        Self { dist, zipf }
    }

    /// Samples a key. `record_count` is the originally loaded key range
    /// (`1..=record_count`); `current_max_key` is the highest key inserted
    /// so far (`>= record_count` once inserts start happening) — only used
    /// by `Latest`, to bias towards the newest rows.
    ///
    /// `Zipfian` scrambles the raw Zipf rank through `fnv_hash64` before
    /// treating it as a key (matching real YCSB's `ScrambledZipfianGenerator`,
    /// which every `requestdistribution=zipfian` run actually uses, and the
    /// `ScrambledZipfGenerator` the LeanStore/WiredTiger comparison engines
    /// already apply — see `scripts/engines/leanstore_build.py`'s doc).
    /// Without this, rank 1 (most popular) *is* key 1, rank 2 is key 2, etc.,
    /// so the whole hot set sits in the lowest few thousand keys - physically
    /// the same handful of leftmost leaf pages in this B-tree, for the whole
    /// run. That gives this engine free cache/latch locality the comparison
    /// engines don't get (their buffer pool has to actually keep a hot set
    /// scattered across the full key range warm under a fixed `dram_gib`
    /// budget), making cross-engine Zipfian throughput not comparable.
    /// Scrambling keeps the same skew (rank 1 is still hashed to one fixed
    /// key for the whole run, and is still drawn most often) but spreads
    /// which physical keys are hot across the whole range instead.
    /// Deliberately not applied to `Latest`: that mode's whole point is
    /// bias towards the physically-newest keys, which real YCSB's own
    /// `SkewedLatestGenerator` also leaves unscrambled.
    pub fn sample(&self, record_count: u64, current_max_key: u64) -> YcsbKey {
        with_fast_rng(|rng| match self.dist {
            RequestDistribution::Uniform =>
                rng.random_range(1..=record_count.max(1)),
            RequestDistribution::Zipfian { .. } => {
                let rank = self.zipf.as_ref().unwrap().sample(rng) as u64;
                let rank = rank.clamp(1, record_count.max(1));
                1 + fnv_hash64(rank) % record_count.max(1)
            }
            RequestDistribution::Latest { .. } => {
                // Zipf sample in [1, record_count]; treated as a 0-based
                // "how many keys back from the newest" offset, so an offset
                // of 1 (the most likely draw) lands exactly on the newest key.
                let offset = self.zipf.as_ref().unwrap().sample(rng) as u64;
                current_max_key.saturating_sub(offset - 1).max(1)
            }
        })
    }
}

/// FNV-1 (64-bit) — same algorithm/constants as YCSB's own `Utils.fnvhash64`,
/// reused here so `KeySampler::sample`'s `Zipfian` scrambling matches real
/// YCSB's `ScrambledZipfianGenerator` bit-for-bit in spirit (not literally,
/// since Java's version folds a `Math.abs` over a signed `long` - pointless
/// here, `u64` has no sign to fix up). Deliberately not `std`'s
/// `DefaultHasher`/`SipHash`: this only needs a cheap, well-mixed permutation
/// of `[1, record_count]`, not collision resistance, and this runs on every
/// single `Zipfian` key draw.
#[inline]
fn fnv_hash64(mut val: u64) -> u64 {
    const OFFSET_BASIS: u64 = 0xCBF29CE484222325;
    const PRIME: u64 = 0x0000_0100_0000_01B3;
    let mut hash = OFFSET_BASIS;
    for _ in 0..8 {
        let octet = val & 0xFF;
        val >>= 8;
        hash ^= octet;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// YCSB op-type proportions (must sum to ~1.0); a driver worker picks one
/// per iteration by drawing `0.0..1.0` and walking these cumulative bounds.
#[derive(Clone, Copy, Debug)]
pub struct YcsbMix {
    pub read: f64,
    pub update: f64,
    pub insert: f64,
    pub scan: f64,
    pub read_modify_write: f64,
}

impl YcsbMix {
    /// The six standard YCSB "Core Workloads" (spec §3 / `workloads/workload{a..f}`):
    /// - A: Update heavy — 50/50 read/update, zipfian.
    /// - B: Read mostly — 95/5 read/update, zipfian.
    /// - C: Read only — 100% read, zipfian.
    /// - D: Read latest — 95% read, 5% insert, `latest` distribution.
    /// - E: Short ranges — 95% scan, 5% insert, zipfian.
    /// - F: Read-modify-write — 50% read, 50% read-modify-write, zipfian.
    pub fn workload(name: &str) -> Option<Self> {
        Some(match name {
            "a" => Self { read: 0.5, update: 0.5, insert: 0.0, scan: 0.0, read_modify_write: 0.0 },
            "b" => Self { read: 0.95, update: 0.05, insert: 0.0, scan: 0.0, read_modify_write: 0.0 },
            "c" => Self { read: 1.0, update: 0.0, insert: 0.0, scan: 0.0, read_modify_write: 0.0 },
            "d" => Self { read: 0.95, update: 0.0, insert: 0.05, scan: 0.0, read_modify_write: 0.0 },
            "e" => Self { read: 0.0, update: 0.0, insert: 0.05, scan: 0.95, read_modify_write: 0.0 },
            "f" => Self { read: 0.5, update: 0.0, insert: 0.0, scan: 0.0, read_modify_write: 0.5 },
            _ => return None,
        })
    }

    /// The distribution the corresponding standard workload uses by default
    /// (only meaningful for the presets above; a custom mix picks its own).
    pub fn default_distribution(name: &str) -> RequestDistribution {
        match name {
            "d" => RequestDistribution::Latest { theta: 0.99 },
            _ => RequestDistribution::Zipfian { theta: 0.99 },
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum YcsbOpType {
    Read,
    Update,
    Insert,
    Scan,
    ReadModifyWrite,
}

/// Picks one op type by drawing `0.0..(sum of proportions)` and walking the
/// cumulative bounds; falls back to `Read` if the mix is degenerate (all
/// zero), so a misconfigured mix never panics mid-run.
pub fn pick_op(mix: &YcsbMix) -> YcsbOpType {
    let total = mix.read + mix.update + mix.insert + mix.scan + mix.read_modify_write;
    if total <= 0.0 {
        return YcsbOpType::Read;
    }
    let mut x = with_fast_rng(|rng| rng.random_range(0.0..total));

    if x < mix.read { return YcsbOpType::Read; }
    x -= mix.read;
    if x < mix.update { return YcsbOpType::Update; }
    x -= mix.update;
    if x < mix.insert { return YcsbOpType::Insert; }
    x -= mix.insert;
    if x < mix.scan { return YcsbOpType::Scan; }
    YcsbOpType::ReadModifyWrite
}

pub fn random_row(cfg: &YcsbConfig) -> YcsbRow {
    // Samples `Alphanumeric` bytes directly into the flat buffer - every
    // field is exactly `field_length` bytes, so there's no need to go via
    // `tpcc_random::rnd_astring`'s `String` (which would mean allocating and
    // UTF8-encoding one throwaway `String` per field, then copying its bytes
    // out again) when a `u8` can be pushed straight in.
    let total_len = cfg.field_count * cfg.field_length;
    let mut data = Vec::with_capacity(total_len);
    with_fast_rng(|rng| {
        for _ in 0..total_len {
            data.push(rng.sample(Alphanumeric));
        }
    });
    YcsbRow::from_bytes(&data)
}

/// Scan length, uniform in `[1, max_scan_length]` (YCSB `maxscanlength`).
pub fn random_scan_length(max_scan_length: u64) -> u64 {
    with_fast_rng(|rng| rng.random_range(1..=max_scan_length.max(1)))
}
