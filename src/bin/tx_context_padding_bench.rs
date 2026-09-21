//! Small, isolated A/B benchmark for `TxContext` cache padding.
//!
//! Run with:
//!   cargo run --release --bin tx_context_padding_bench
//!
//! `clock` models the actual `TxContext` access pattern: every worker reads
//! the two effectively read-only mode flags and advances the one genuinely
//! shared clock. `slots` is a positive control for the per-worker arrays:
//! every worker repeatedly writes only its own slot.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_utils::CachePadded;

const CLOCK_OPS_PER_THREAD: u64 = 2_000_000;
const SLOT_OPS_PER_THREAD: u64 = 10_000_000;
const SAMPLES: usize = 5;

#[cfg(target_os = "linux")]
fn pin_to_cpu(cpu: usize) {
    // Stable placement matters for a cache-coherence benchmark. On the
    // benchmark host CPUs 0..physical_cores are separate physical cores,
    // followed by their SMT siblings.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set,),
            0,
            "failed to pin benchmark worker to CPU {cpu}",
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_to_cpu(_cpu: usize) {}

#[repr(C)]
struct PackedClockContext {
    clock: AtomicU64,
    block_reclaim_enabled: AtomicBool,
    truncate_commit_log: AtomicBool,
}

impl PackedClockContext {
    fn new() -> Self {
        Self {
            clock: AtomicU64::new(1),
            block_reclaim_enabled: AtomicBool::new(false),
            truncate_commit_log: AtomicBool::new(true),
        }
    }
}

struct PaddedClockContext {
    clock: CachePadded<AtomicU64>,
    block_reclaim_enabled: AtomicBool,
    truncate_commit_log: AtomicBool,
}

impl PaddedClockContext {
    fn new() -> Self {
        Self {
            clock: CachePadded::new(AtomicU64::new(1)),
            block_reclaim_enabled: AtomicBool::new(false),
            truncate_commit_log: AtomicBool::new(true),
        }
    }
}

trait ClockWorkload: Send + Sync + 'static {
    fn tick(&self);
    fn clock(&self) -> u64;
}

impl ClockWorkload for PackedClockContext {
    #[inline(always)]
    fn tick(&self) {
        black_box(self.block_reclaim_enabled.load(Relaxed));
        black_box(self.truncate_commit_log.load(Relaxed));
        black_box(self.clock.fetch_add(1, Relaxed));
    }

    fn clock(&self) -> u64 {
        self.clock.load(Relaxed)
    }
}

impl ClockWorkload for PaddedClockContext {
    #[inline(always)]
    fn tick(&self) {
        black_box(self.block_reclaim_enabled.load(Relaxed));
        black_box(self.truncate_commit_log.load(Relaxed));
        black_box(self.clock.fetch_add(1, Relaxed));
    }

    fn clock(&self) -> u64 {
        self.clock.load(Relaxed)
    }
}

fn run_clock<C: ClockWorkload>(context: C, workers: usize) -> Duration {
    let context = Arc::new(context);
    let start = Arc::new(Barrier::new(workers + 1));
    let handles: Vec<_> = (0..workers)
        .map(|worker| {
            let context = Arc::clone(&context);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                pin_to_cpu(worker);
                start.wait();
                for _ in 0..CLOCK_OPS_PER_THREAD {
                    context.tick();
                }
            })
        })
        .collect();

    let before = Instant::now();
    start.wait();
    for handle in handles {
        handle.join().unwrap();
    }
    let elapsed = before.elapsed();
    assert_eq!(context.clock(), 1 + workers as u64 * CLOCK_OPS_PER_THREAD);
    elapsed
}

trait WorkerSlots: Send + Sync + 'static {
    fn new(workers: usize) -> Self;
    fn increment(&self, worker: usize);
    fn total(&self) -> u64;
}

struct PackedSlots(Box<[AtomicU64]>);

impl WorkerSlots for PackedSlots {
    fn new(workers: usize) -> Self {
        Self((0..workers).map(|_| AtomicU64::new(0)).collect())
    }

    #[inline(always)]
    fn increment(&self, worker: usize) {
        black_box(self.0[worker].fetch_add(1, Relaxed));
    }

    fn total(&self) -> u64 {
        self.0.iter().map(|slot| slot.load(Relaxed)).sum()
    }
}

struct PaddedSlots(Box<[CachePadded<AtomicU64>]>);

impl WorkerSlots for PaddedSlots {
    fn new(workers: usize) -> Self {
        Self(
            (0..workers)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect(),
        )
    }

    #[inline(always)]
    fn increment(&self, worker: usize) {
        black_box(self.0[worker].fetch_add(1, Relaxed));
    }

    fn total(&self) -> u64 {
        self.0.iter().map(|slot| slot.load(Relaxed)).sum()
    }
}

fn run_slots<S: WorkerSlots>(workers: usize) -> Duration {
    let slots = Arc::new(S::new(workers));
    let start = Arc::new(Barrier::new(workers + 1));
    let handles: Vec<_> = (0..workers)
        .map(|worker| {
            let slots = Arc::clone(&slots);
            let start = Arc::clone(&start);
            thread::spawn(move || {
                pin_to_cpu(worker);
                start.wait();
                for _ in 0..SLOT_OPS_PER_THREAD {
                    slots.increment(worker);
                }
            })
        })
        .collect();

    let before = Instant::now();
    start.wait();
    for handle in handles {
        handle.join().unwrap();
    }
    let elapsed = before.elapsed();
    assert_eq!(slots.total(), workers as u64 * SLOT_OPS_PER_THREAD);
    elapsed
}

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn rate(operations: u64, elapsed: Duration) -> f64 {
    operations as f64 / elapsed.as_secs_f64() / 1_000_000.0
}

fn print_pair(label: &str, workers: usize, operations: u64, packed: Duration, padded: Duration) {
    let packed_rate = rate(operations, packed);
    let padded_rate = rate(operations, padded);
    println!(
        "{label:<7} workers={workers:>2}  packed={packed_rate:>8.2} Mops/s  \
         padded={padded_rate:>8.2} Mops/s  padded/packed={:>6.3}x",
        padded_rate / packed_rate,
    );
}

fn main() {
    let available = thread::available_parallelism().map_or(1, usize::from);
    let mut worker_counts = vec![1, 2, 4, 8, 12, available];
    worker_counts.retain(|&workers| workers <= available);
    worker_counts.sort_unstable();
    worker_counts.dedup();

    println!("TxContext padding microbenchmark ({available} logical CPUs, median of {SAMPLES})");
    println!(
        "CachePadded<AtomicU64>: {} bytes",
        std::mem::size_of::<CachePadded<AtomicU64>>()
    );
    println!();

    for workers in worker_counts {
        // Alternate order per sample so thermal/frequency drift does not
        // systematically favor one layout.
        let mut packed_clock = Vec::with_capacity(SAMPLES);
        let mut padded_clock = Vec::with_capacity(SAMPLES);
        let mut packed_slots = Vec::with_capacity(SAMPLES);
        let mut padded_slots = Vec::with_capacity(SAMPLES);
        for sample in 0..SAMPLES {
            if sample % 2 == 0 {
                packed_clock.push(run_clock(PackedClockContext::new(), workers));
                padded_clock.push(run_clock(PaddedClockContext::new(), workers));
                packed_slots.push(run_slots::<PackedSlots>(workers));
                padded_slots.push(run_slots::<PaddedSlots>(workers));
            } else {
                padded_clock.push(run_clock(PaddedClockContext::new(), workers));
                packed_clock.push(run_clock(PackedClockContext::new(), workers));
                padded_slots.push(run_slots::<PaddedSlots>(workers));
                packed_slots.push(run_slots::<PackedSlots>(workers));
            }
        }

        print_pair(
            "clock",
            workers,
            workers as u64 * CLOCK_OPS_PER_THREAD,
            median(packed_clock),
            median(padded_clock),
        );
        print_pair(
            "slots",
            workers,
            workers as u64 * SLOT_OPS_PER_THREAD,
            median(packed_slots),
            median(padded_slots),
        );
        println!();
    }
}
