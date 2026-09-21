//! YCSB's request-distribution and operation-mix definitions (Cooper et al.,
//! SoCC 2010, §3): which key an operation targets, and the standard "Core
//! Workloads" A-F op-type proportions.

use rand::prelude::*;
use std::cell::RefCell;

use crate::bat_bench::ycsb_schema::{YcsbConfig, YcsbKey, YcsbRow};

thread_local! {
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

/// Builds a Walker alias table once and reuses it for every sampled key.
/// The O(n) setup performs Zipf's floating-point powers outside the timed
/// request loop; each request then needs only two random draws and a lookup.
pub struct KeySampler {
    dist: RequestDistribution,
    zipf: Option<AliasZipf>,
}

/// Walker alias table for an exact finite Zipf distribution. Construction
/// performs the O(n) floating-point powers once; sampling is O(1) and uses
/// no `pow` in the benchmark's timed request path.
struct AliasZipf {
    probability: Vec<f32>,
    alias: Vec<u32>,
}

impl AliasZipf {
    fn new(n: usize, theta: f64) -> Self {
        assert!(n > 0 && n <= u32::MAX as usize, "Zipf domain must fit u32");
        assert!(theta.is_finite() && theta >= 0.0, "invalid Zipf theta");
        let weights: Vec<f64> = (1..=n).map(|rank| (rank as f64).powf(-theta)).collect();
        let sum: f64 = weights.iter().sum();
        let mut scaled: Vec<f64> = weights
            .into_iter()
            .map(|weight| weight * n as f64 / sum)
            .collect();
        let mut small = Vec::with_capacity(n);
        let mut large = Vec::with_capacity(n);
        for (index, &value) in scaled.iter().enumerate() {
            if value < 1.0 {
                small.push(index);
            } else {
                large.push(index);
            }
        }
        let mut probability = vec![1.0f32; n];
        let mut alias: Vec<u32> = (0..n as u32).collect();
        while !small.is_empty() && !large.is_empty() {
            let low = small.pop().unwrap();
            let high = large.pop().unwrap();
            probability[low] = scaled[low] as f32;
            alias[low] = high as u32;
            scaled[high] = (scaled[high] + scaled[low]) - 1.0;
            if scaled[high] < 1.0 {
                small.push(high);
            } else {
                large.push(high);
            }
        }
        Self { probability, alias }
    }

    #[inline]
    fn sample(&self, rng: &mut SmallRng) -> u64 {
        let column = rng.random_range(0..self.probability.len());
        let index = if rng.random::<f32>() < self.probability[column] {
            column
        } else {
            self.alias[column] as usize
        };
        index as u64 + 1
    }
}

#[cfg(test)]
mod alias_tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn alias_zipf_samples_the_domain_with_expected_skew() {
        let zipf = AliasZipf::new(10, 0.99);
        let mut rng = SmallRng::seed_from_u64(0xC0FFEE);
        let mut counts = [0usize; 10];
        for _ in 0..200_000 {
            let rank = zipf.sample(&mut rng);
            assert!((1..=10).contains(&rank));
            counts[rank as usize - 1] += 1;
        }
        assert!(counts[0] > counts[1]);
        assert!(counts[1] > counts[4]);
        assert!(counts[4] > counts[9]);
    }

    #[test]
    fn zero_theta_is_approximately_uniform() {
        let zipf = AliasZipf::new(8, 0.0);
        let mut rng = SmallRng::seed_from_u64(7);
        let mut counts = [0usize; 8];
        for _ in 0..160_000 {
            counts[zipf.sample(&mut rng) as usize - 1] += 1;
        }
        assert!(
            counts
                .iter()
                .all(|&count| (18_500..=21_500).contains(&count)),
            "{counts:?}"
        );
    }
}

impl KeySampler {
    pub fn new(dist: RequestDistribution, record_count: u64) -> Self {
        let zipf = match dist {
            RequestDistribution::Uniform => None,
            RequestDistribution::Zipfian { theta } | RequestDistribution::Latest { theta } => {
                Some(AliasZipf::new(record_count.max(1) as usize, theta))
            }
        };
        Self { dist, zipf }
    }

    pub fn sample(&self, record_count: u64, current_max_key: u64) -> YcsbKey {
        with_fast_rng(|rng| match self.dist {
            RequestDistribution::Uniform => rng.random_range(1..=record_count.max(1)),
            RequestDistribution::Zipfian { .. } => {
                let rank = self.zipf.as_ref().unwrap().sample(rng);
                let rank = rank.clamp(1, record_count.max(1));
                1 + fnv_hash64(rank) % record_count.max(1)
            }
            RequestDistribution::Latest { .. } => {
                // Zipf sample in [1, record_count]; treated as a 0-based
                // "how many keys back from the newest" offset, so an offset
                // of 1 (the most likely draw) lands exactly on the newest key.
                let offset = self.zipf.as_ref().unwrap().sample(rng);
                current_max_key.saturating_sub(offset - 1).max(1)
            }
        })
    }
}

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
    pub fn workload(name: &str) -> Option<Self> {
        Some(match name {
            "a" => Self {
                read: 0.5,
                update: 0.5,
                insert: 0.0,
                scan: 0.0,
                read_modify_write: 0.0,
            },
            "b" => Self {
                read: 0.95,
                update: 0.05,
                insert: 0.0,
                scan: 0.0,
                read_modify_write: 0.0,
            },
            "c" => Self {
                read: 1.0,
                update: 0.0,
                insert: 0.0,
                scan: 0.0,
                read_modify_write: 0.0,
            },
            "d" => Self {
                read: 0.95,
                update: 0.0,
                insert: 0.05,
                scan: 0.0,
                read_modify_write: 0.0,
            },
            "e" => Self {
                read: 0.0,
                update: 0.0,
                insert: 0.05,
                scan: 0.95,
                read_modify_write: 0.0,
            },
            "f" => Self {
                read: 0.5,
                update: 0.0,
                insert: 0.0,
                scan: 0.0,
                read_modify_write: 0.5,
            },
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

    if x < mix.read {
        return YcsbOpType::Read;
    }
    x -= mix.read;
    if x < mix.update {
        return YcsbOpType::Update;
    }
    x -= mix.update;
    if x < mix.insert {
        return YcsbOpType::Insert;
    }
    x -= mix.insert;
    if x < mix.scan {
        return YcsbOpType::Scan;
    }
    YcsbOpType::ReadModifyWrite
}

pub fn random_row(cfg: &YcsbConfig) -> YcsbRow {
    let total_len = cfg.field_count * cfg.field_length;
    YcsbRow::from_len_with(total_len, |bytes| {
        with_fast_rng(|rng| fill_alphanumeric(rng, bytes))
    })
}

thread_local! {
    static FIELD_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

pub fn with_random_field_patch<R>(cfg: &YcsbConfig, f: impl FnOnce(usize, &[u8]) -> R) -> R {
    if cfg.field_count == 0 || cfg.field_length == 0 {
        return f(0, &[]);
    }
    FIELD_SCRATCH.with(|scratch| {
        let mut bytes = scratch.borrow_mut();
        with_fast_rng(|rng| {
            let field = rng.random_range(0..cfg.field_count);
            bytes.resize(cfg.field_length, 0);
            fill_alphanumeric(rng, &mut bytes);
            f(field, &bytes)
        })
    })
}

fn fill_alphanumeric(rng: &mut SmallRng, out: &mut [u8]) {
    const ALPHABET: &[u8; 62] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let mut scratch = [0u8; 128];
    let mut written = 0;
    while written < out.len() {
        rng.fill_bytes(&mut scratch);
        for &byte in &scratch {
            if byte < 248 {
                out[written] = ALPHABET[(byte % 62) as usize];
                written += 1;
                if written == out.len() {
                    break;
                }
            }
        }
    }
}

/// Scan length, uniform in `[1, max_scan_length]` (YCSB `maxscanlength`).
pub fn random_scan_length(max_scan_length: u64) -> u64 {
    with_fast_rng(|rng| rng.random_range(1..=max_scan_length.max(1)))
}
