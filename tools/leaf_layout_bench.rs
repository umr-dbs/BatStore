//! Standalone leaf-layout microbenchmark.
//!
//! Not wired into `cargo test` or `cargo bench` - it's a standalone tool (std only, its own
//! `fn main`) kept out of the tests/ directory so the normal test suite doesn't build or run it.
//! Compile and run directly:
//!   rustc --edition=2024 -C opt-level=3 -C target-cpu=native tools/leaf_layout_bench.rs \
//!     -o /tmp/leaf_layout_bench
//!   /tmp/leaf_layout_bench

use std::hint::black_box;
use std::mem::{align_of, size_of};
use std::time::{Duration, Instant};

const N: usize = 125; // The production 4 KiB leaf's normal record capacity.
const VERSIONS_PER_KEY: usize = 5;
const KEY_COUNT: usize = N / VERSIONS_PER_KEY;
const SAMPLE_TIME: Duration = Duration::from_millis(750);
const MULTI_LEAVES: usize = 16_384;
const OCCUPANCIES: [usize; 5] = [25, 50, 75, 100, 125];

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct Record {
    key: u64,
    begin: u64,
    end: u64,
    payload: u64,
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
struct RecordData {
    begin: u64,
    end: u64,
    payload: u64,
}

#[repr(C)]
struct AosLeaf {
    records: [Record; N],
}

#[repr(C)]
struct SplitLeaf {
    keys: [u64; N],
    data: [RecordData; N],
}

#[inline(always)]
fn visible(begin: u64, end: u64, snapshot: u64) -> bool {
    begin <= snapshot && snapshot < end
}

impl AosLeaf {
    #[inline(never)]
    fn point(&self, len: usize, key: u64, snapshot: u64) -> Option<u64> {
        for r in self.records[..len].iter().rev() {
            if r.key == key && visible(r.begin, r.end, snapshot) {
                return Some(r.payload);
            }
        }
        None
    }

    #[inline(never)]
    fn scan(&self, len: usize, lo: u64, hi: u64, snapshot: u64) -> u64 {
        let mut sum = 0u64;
        for r in &self.records[..len] {
            if lo <= r.key && r.key < hi && visible(r.begin, r.end, snapshot) {
                sum = sum.wrapping_add(r.payload);
            }
        }
        sum
    }
}

impl SplitLeaf {
    #[inline(never)]
    fn point(&self, len: usize, key: u64, snapshot: u64) -> Option<u64> {
        for i in (0..len).rev() {
            if self.keys[i] == key {
                let d = &self.data[i];
                if visible(d.begin, d.end, snapshot) {
                    return Some(d.payload);
                }
            }
        }
        None
    }

    #[inline(never)]
    fn scan(&self, len: usize, lo: u64, hi: u64, snapshot: u64) -> u64 {
        let mut sum = 0u64;
        for i in 0..len {
            let key = self.keys[i];
            if lo <= key && key < hi {
                let d = &self.data[i];
                if visible(d.begin, d.end, snapshot) {
                    sum = sum.wrapping_add(d.payload);
                }
            }
        }
        sum
    }
}

fn make_pages() -> (AosLeaf, SplitLeaf) {
    let mut aos = AosLeaf {
        records: [Record::default(); N],
    };
    let mut split = SplitLeaf {
        keys: [0; N],
        data: [RecordData::default(); N],
    };

    // Append order models five update waves. Versions of one logical key are separated
    // by all other keys, as happens when concurrent updates append to an unsorted leaf.
    for version in 0..VERSIONS_PER_KEY {
        for key in 0..KEY_COUNT {
            let i = version * KEY_COUNT + key;
            let begin = (version as u64 + 1) * 10;
            let end = if version + 1 == VERSIONS_PER_KEY {
                u64::MAX
            } else {
                begin + 10
            };
            let payload = (key as u64 + 1) * 1_000 + version as u64;
            let record = Record {
                key: key as u64,
                begin,
                end,
                payload,
            };
            aos.records[i] = record;
            split.keys[i] = record.key;
            split.data[i] = RecordData {
                begin,
                end,
                payload,
            };
        }
    }
    (aos, split)
}

struct MultiPages {
    aos: Vec<Box<AosLeaf>>,
    split: Vec<Box<SplitLeaf>>,
    lengths: Vec<usize>,
    point_keys: Vec<u64>,
    order: Vec<usize>,
    physical_records: usize,
}

fn make_multi_pages() -> MultiPages {
    let mut aos = Vec::with_capacity(MULTI_LEAVES);
    let mut split = Vec::with_capacity(MULTI_LEAVES);
    let mut lengths = Vec::with_capacity(MULTI_LEAVES);
    let mut point_keys = Vec::with_capacity(MULTI_LEAVES);
    let mut order: Vec<usize> = (0..MULTI_LEAVES).collect();
    let mut physical_records = 0;

    for page_id in 0..MULTI_LEAVES {
        let len = OCCUPANCIES[page_id % OCCUPANCIES.len()];
        let key_count = len / VERSIONS_PER_KEY;
        let key_base = page_id as u64 * 1_000;
        let mut aos_page = Box::new(AosLeaf {
            records: [Record::default(); N],
        });
        let mut split_page = Box::new(SplitLeaf {
            keys: [0; N],
            data: [RecordData::default(); N],
        });
        for version in 0..VERSIONS_PER_KEY {
            for local_key in 0..key_count {
                let i = version * key_count + local_key;
                let begin = (version as u64 + 1) * 10;
                let end = if version + 1 == VERSIONS_PER_KEY {
                    u64::MAX
                } else {
                    begin + 10
                };
                let key = key_base + local_key as u64;
                let payload = key.wrapping_mul(1_000).wrapping_add(version as u64);
                let record = Record {
                    key,
                    begin,
                    end,
                    payload,
                };
                aos_page.records[i] = record;
                split_page.keys[i] = key;
                split_page.data[i] = RecordData {
                    begin,
                    end,
                    payload,
                };
            }
        }
        point_keys.push(key_base + (page_id % key_count) as u64);
        lengths.push(len);
        physical_records += len;
        aos.push(aos_page);
        split.push(split_page);
    }

    // Fisher-Yates permutation, prepared outside measurement. Both layouts visit leaves
    // in the identical pseudo-random order, defeating sequential allocation locality.
    let mut state = 0xa076_1d64_78bd_642f;
    for i in (1..order.len()).rev() {
        let j = next(&mut state) as usize % (i + 1);
        order.swap(i, j);
    }
    MultiPages {
        aos,
        split,
        lengths,
        point_keys,
        order,
        physical_records,
    }
}

// Tiny deterministic generator: randomizes probes without adding RNG work to either
// measured layout differently.
#[inline(always)]
fn next(x: &mut u64) -> u64 {
    *x ^= *x << 13;
    *x ^= *x >> 7;
    *x ^= *x << 17;
    *x
}

fn measure(mut operation: impl FnMut(u64) -> u64) -> (f64, u64) {
    // Warm instruction/data caches and force the result to remain observable.
    let mut checksum = 0u64;
    for i in 0..20_000 {
        checksum ^= black_box(operation(i));
    }
    let start = Instant::now();
    let mut iterations = 0u64;
    while start.elapsed() < SAMPLE_TIME {
        for _ in 0..256 {
            checksum ^= black_box(operation(iterations));
            iterations += 1;
        }
    }
    black_box(checksum);
    (
        start.elapsed().as_secs_f64() * 1e9 / iterations as f64,
        checksum,
    )
}

fn row(name: &str, aos_ns: f64, split_ns: f64) {
    let speedup = aos_ns / split_ns;
    println!("{name:<31} {aos_ns:>10.2} {split_ns:>12.2} {speedup:>11.3}x");
}

fn measure_sweeps(mut sweep: impl FnMut(u64) -> u64) -> f64 {
    black_box(sweep(0));
    let start = Instant::now();
    let mut sweeps = 0u64;
    let mut checksum = 0u64;
    while start.elapsed() < SAMPLE_TIME {
        checksum ^= black_box(sweep(sweeps));
        sweeps += 1;
    }
    black_box(checksum);
    start.elapsed().as_secs_f64() * 1_000.0 / sweeps as f64
}

fn multi_row(name: &str, aos_ms: f64, split_ms: f64, operations: usize) {
    let aos_mops = operations as f64 / aos_ms / 1_000.0;
    let split_mops = operations as f64 / split_ms / 1_000.0;
    println!(
        "{name:<26} {aos_ms:>9.3} {split_ms:>11.3} {aos_mops:>11.2} {split_mops:>13.2} {:>9.3}x",
        aos_ms / split_ms
    );
}

fn main() {
    let (aos, split) = make_pages();
    assert_eq!(size_of::<AosLeaf>(), size_of::<SplitLeaf>());
    println!("leaf entries={N}, logical keys={KEY_COUNT}, versions/key={VERSIONS_PER_KEY}");
    println!(
        "Record={}B, metadata={}B, both pages={}B, alignments={}/{}",
        size_of::<Record>(),
        size_of::<RecordData>(),
        size_of::<AosLeaf>(),
        align_of::<AosLeaf>(),
        align_of::<SplitLeaf>()
    );
    println!("\noperation                         AoS ns/op  split ns/op  split speedup");
    println!("-----------------------------------------------------------------------");

    for (label, snapshot) in [
        ("point: latest visible", 55),
        ("point: 1 version fallback", 45),
        ("point: 3 version fallback", 25),
    ] {
        let mut queries = Vec::with_capacity(1 << 16);
        let mut state = 0x9e37_79b9_7f4a_7c15;
        for _ in 0..queries.capacity() {
            queries.push(next(&mut state) % KEY_COUNT as u64);
        }
        for &key in queries.iter().take(1_000) {
            assert_eq!(aos.point(N, key, snapshot), split.point(N, key, snapshot));
        }
        let (a, _) = measure(|i| {
            aos.point(N, queries[i as usize & (queries.len() - 1)], snapshot)
                .unwrap_or(0)
        });
        let (s, _) = measure(|i| {
            split
                .point(N, queries[i as usize & (queries.len() - 1)], snapshot)
                .unwrap_or(0)
        });
        row(label, a, s);
    }

    let mut missing = Vec::with_capacity(1 << 16);
    let mut state = 0xd1b5_4a32_d192_ed03;
    for _ in 0..missing.capacity() {
        missing.push(KEY_COUNT as u64 + next(&mut state) % KEY_COUNT as u64);
    }
    for &key in missing.iter().take(1_000) {
        assert_eq!(aos.point(N, key, 55), split.point(N, key, 55));
    }
    let (a, _) = measure(|i| {
        aos.point(N, missing[i as usize & (missing.len() - 1)], 55)
            .unwrap_or(0)
    });
    let (s, _) = measure(|i| {
        split
            .point(N, missing[i as usize & (missing.len() - 1)], 55)
            .unwrap_or(0)
    });
    row("point: absent key", a, s);

    for (label, width) in [
        ("scan: full page", KEY_COUNT as u64),
        ("scan: 50% key range", 13),
        ("scan: 12% key range", 3),
    ] {
        let lo = 0;
        let hi = width;
        assert_eq!(aos.scan(N, lo, hi, 55), split.scan(N, lo, hi, 55));
        // Vary both the visible version and (except for a full scan) range position so
        // LLVM cannot hoist an invariant scan out of the measurement loop.
        let (a, _) = measure(|i| {
            let lo = if width == KEY_COUNT as u64 {
                0
            } else {
                i % (KEY_COUNT as u64 - width + 1)
            };
            aos.scan(N, lo, lo + width, 15 + (i % VERSIONS_PER_KEY as u64) * 10)
        });
        let (s, _) = measure(|i| {
            let lo = if width == KEY_COUNT as u64 {
                0
            } else {
                i % (KEY_COUNT as u64 - width + 1)
            };
            split.scan(N, lo, lo + width, 15 + (i % VERSIONS_PER_KEY as u64) * 10)
        });
        row(label, a, s);
    }

    println!("\nBuilding {MULTI_LEAVES} separately boxed leaves per layout...");
    let pages = make_multi_pages();
    let bytes_per_layout = MULTI_LEAVES * size_of::<AosLeaf>();
    println!(
        "occupancies={OCCUPANCIES:?} records (20/40/60/80/100%), physical records={}, working set/layout={:.1} MiB",
        pages.physical_records,
        bytes_per_layout as f64 / (1024.0 * 1024.0)
    );

    for &i in pages.order.iter().take(1_000) {
        assert_eq!(
            pages.aos[i].point(pages.lengths[i], pages.point_keys[i], 55),
            pages.split[i].point(pages.lengths[i], pages.point_keys[i], 55)
        );
    }

    println!(
        "\noperation                    AoS ms/sweep split ms/sweep AoS Mops/s split Mops/s speedup"
    );
    println!(
        "----------------------------------------------------------------------------------------"
    );
    for (label, snapshot) in [
        ("point: latest", 55),
        ("point: 1 fallback", 45),
        ("point: 3 fallbacks", 25),
    ] {
        let aos_ms = measure_sweeps(|_| {
            let mut sum = 0;
            for &i in &pages.order {
                sum ^= pages.aos[i]
                    .point(pages.lengths[i], pages.point_keys[i], snapshot)
                    .unwrap_or(0);
            }
            sum
        });
        let split_ms = measure_sweeps(|_| {
            let mut sum = 0;
            for &i in &pages.order {
                sum ^= pages.split[i]
                    .point(pages.lengths[i], pages.point_keys[i], snapshot)
                    .unwrap_or(0);
            }
            sum
        });
        multi_row(label, aos_ms, split_ms, MULTI_LEAVES);
    }

    for (label, fraction) in [
        ("scan: full leaves", 100usize),
        ("scan: 50% ranges", 50),
        ("scan: 20% ranges", 20),
    ] {
        let aos_ms = measure_sweeps(|sweep| {
            let snapshot = 15 + (sweep % VERSIONS_PER_KEY as u64) * 10;
            let mut sum = 0;
            for &i in &pages.order {
                let keys = pages.lengths[i] / VERSIONS_PER_KEY;
                let width = (keys * fraction / 100).max(1);
                let base = (i as u64) * 1_000;
                sum ^= pages.aos[i].scan(pages.lengths[i], base, base + width as u64, snapshot);
            }
            sum
        });
        let split_ms = measure_sweeps(|sweep| {
            let snapshot = 15 + (sweep % VERSIONS_PER_KEY as u64) * 10;
            let mut sum = 0;
            for &i in &pages.order {
                let keys = pages.lengths[i] / VERSIONS_PER_KEY;
                let width = (keys * fraction / 100).max(1);
                let base = (i as u64) * 1_000;
                sum ^= pages.split[i].scan(pages.lengths[i], base, base + width as u64, snapshot);
            }
            sum
        });
        multi_row(label, aos_ms, split_ms, pages.physical_records);
    }
}
