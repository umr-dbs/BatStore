//! One-command experiment suite: `benchmark` runs the full TPC-C, CH-benCHmark
//! ("TPC-H"), HTAP (TPC-C + concurrent OLAP, following the mixed-workload
//! methodology cited in `tpcc_driver`'s module docs), and YCSB (workloads A-F)
//! benchmarks already implemented elsewhere in `mv_bench`, each once with GC
//! enabled and once with GC disabled, logging throughput and memory usage
//! (`mv_bench::mem_stats`) for every run into one timestamped results
//! directory. See `scripts/plot_suite.py` for the matching plotting code.
//!
//! `cargo run --release -- benchmark [output_root=benchmark_results] [quick]`
//! — zero args runs the full suite (`Scale::Full`, sized for a 64-core/
//! 128-thread, ~500GB RAM server at a moderate ~30-45 min total wall-clock
//! budget). Pass `quick` as the 2nd arg for a toy-sized (~1-2 min) smoke-test
//! scale that exercises the exact same code path — useful for local
//! verification before committing to a real run on the target server.
//!
//! Each experiment variant runs in its own child process (re-executing this
//! same binary with the hidden `_bench_one` subcommand), not in-process one
//! after another: sequential in-process runs measured `mem_stats.csv`'s RSS/
//! jemalloc-resident numbers were confounded by whatever memory jemalloc
//! hadn't yet returned to the OS from the *previous* experiment's now-dropped
//! tree (verified empirically — later experiments in a smoke-test run showed
//! multi-GB "peak RSS" for a workload whose own data was kilobytes). A fresh
//! process per experiment gives each one a clean address space, so its
//! `mem_stats.csv` reflects only that experiment's own memory use.

use std::fs;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use chrono::Local;

use crate::mv_bench::mem_stats;
use crate::mv_bench::olap_scan::OlapMode;
use crate::mv_bench::tpcc_driver;
use crate::mv_bench::tpcc_schema::TpccConfig;
use crate::mv_bench::ycsb_driver;
use crate::mv_bench::ycsb_random::YcsbMix;
use crate::mv_bench::ycsb_schema::YcsbConfig;
use crate::mv_root::index_root::RootIndexType;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scale {
    Full,
    Quick,
}

impl Scale {
    fn label(self) -> &'static str {
        match self {
            Scale::Full => "full",
            Scale::Quick => "quick",
        }
    }

    fn parse(s: &str) -> Self {
        if s == "quick" { Scale::Quick } else { Scale::Full }
    }

    fn tpcc_oltp_warehouses(self) -> u32 {
        match self { Scale::Full => 96, Scale::Quick => 4 }
    }
    fn tpcc_oltp_duration(self) -> Duration {
        match self { Scale::Full => Duration::from_secs(120), Scale::Quick => Duration::from_secs(5) }
    }

    fn ch_htap_warehouses(self) -> u32 {
        match self { Scale::Full => 64, Scale::Quick => 4 }
    }
    fn ch_htap_duration(self) -> Duration {
        match self { Scale::Full => Duration::from_secs(180), Scale::Quick => Duration::from_secs(5) }
    }
    fn ch_htap_olap_threads(self) -> usize {
        match self { Scale::Full => 4, Scale::Quick => 1 }
    }
    fn htap_baseline(self) -> Duration {
        match self { Scale::Full => Duration::from_secs(30), Scale::Quick => Duration::from_secs(3) }
    }

    fn ycsb_record_count(self) -> u64 {
        match self { Scale::Full => 20_000_000, Scale::Quick => 10_000 }
    }
    fn ycsb_threads(self) -> usize {
        match self { Scale::Full => 127, Scale::Quick => 4 }
    }
    fn ycsb_duration(self) -> Duration {
        match self { Scale::Full => Duration::from_secs(60), Scale::Quick => Duration::from_secs(5) }
    }
}

/// Every experiment key `main_benchmark` schedules, in run order. YCSB's 6
/// workloads are represented as `"ycsb_a"`..`"ycsb_f"` — matched by prefix in
/// `run_one`/`main_bench_one` rather than enumerated as separate variants.
const EXPERIMENTS: [&str; 9] = [
    "tpcc_oltp_only", "ch_benchmark", "htap",
    "ycsb_a", "ycsb_b", "ycsb_c", "ycsb_d", "ycsb_e", "ycsb_f",
];

fn duration_for(experiment: &str, scale: Scale) -> Duration {
    match experiment {
        "tpcc_oltp_only" => scale.tpcc_oltp_duration(),
        "ch_benchmark" | "htap" => scale.ch_htap_duration(),
        s if s.starts_with("ycsb_") => scale.ycsb_duration(),
        other => panic!("benchmark: unknown experiment '{other}'"),
    }
}

/// Runs one experiment variant directly (in the *current* process — used by
/// the `_bench_one` child) and returns `(metric_name, metric_value,
/// baseline_metric_value)`. `baseline_metric_value` is only ever populated
/// for `htap` (its OLTP-only sub-phase tpmC, already computed by
/// `tpcc_driver::run_tpcc` via `DriverConfig::htap_baseline` — see
/// `TpccRunSummary::baseline_tpm_c` — but previously only printed to stdout,
/// never persisted): it's what makes an accurate "OLTP interference from
/// concurrent OLAP" plot possible without re-deriving it from two
/// differently-configured runs.
fn run_one(experiment: &str, scale: Scale, gc: bool, out_dir: PathBuf) -> (&'static str, f64, Option<f64>) {
    match experiment {
        "tpcc_oltp_only" => {
            let warehouses = scale.tpcc_oltp_warehouses();
            let summary = tpcc_driver::run_tpcc(tpcc_driver::DriverConfig {
                tpcc: TpccConfig { num_warehouses: warehouses, ..TpccConfig::default() },
                num_terminals: warehouses as usize,
                duration: scale.tpcc_oltp_duration(),
                affinity: true,
                gc,
                update_in_place: false,
                root_star_index: RootIndexType::FrugalList,
                olap_mode: OlapMode::RepeatedFreshFullScan,
                num_olap_threads: 0,
                wal: None,
                htap_baseline: None,
                output_dir: out_dir,
            });
            ("tpmC", summary.tpm_c, None)
        }
        "ch_benchmark" | "htap" => {
            let warehouses = scale.ch_htap_warehouses();
            let htap_baseline = (experiment == "htap").then(|| scale.htap_baseline());
            let summary = tpcc_driver::run_tpcc(tpcc_driver::DriverConfig {
                tpcc: TpccConfig { num_warehouses: warehouses, ..TpccConfig::default() },
                num_terminals: warehouses as usize,
                duration: scale.ch_htap_duration(),
                affinity: true,
                gc,
                update_in_place: false,
                root_star_index: RootIndexType::FrugalList,
                olap_mode: OlapMode::ChBenchmark { region_name: "EUROPE".to_string(), date_lo: i64::MIN, date_hi: i64::MAX },
                num_olap_threads: scale.ch_htap_olap_threads(),
                wal: None,
                htap_baseline,
                output_dir: out_dir,
            });
            ("tpmC", summary.tpm_c, summary.baseline_tpm_c)
        }
        s if s.starts_with("ycsb_") => {
            let workload = &s[5..];
            let mix = YcsbMix::workload(workload)
                .unwrap_or_else(|| panic!("benchmark: unknown YCSB workload '{workload}'"));
            let distribution = YcsbMix::default_distribution(workload);
            let summary = ycsb_driver::run_ycsb(ycsb_driver::DriverConfig {
                ycsb: YcsbConfig { record_count: scale.ycsb_record_count(), field_count: 10, field_length: 100 },
                num_threads: scale.ycsb_threads(),
                duration: scale.ycsb_duration(),
                mix,
                distribution,
                max_scan_length: 100,
                gc,
                update_in_place: false,
                root_star_index: RootIndexType::FrugalList,
                wal: None,
                output_dir: out_dir,
            });
            ("ops_per_sec", summary.throughput_ops_sec, None)
        }
        other => panic!("benchmark: unknown experiment '{other}'"),
    }
}

/// Hidden subcommand the `benchmark` orchestrator re-execs itself with, once
/// per experiment variant, so each runs in its own fresh process (see module
/// docs). Args: `_bench_one <experiment> <gc:true|false> <scale:full|quick>
/// <output_dir>`. Writes `<output_dir>/result.csv` (`metric_name,metric_value`
/// header + one row) for the parent to read back.
pub fn main_bench_one(parms: Vec<String>) {
    let experiment = parms.get(2).cloned().unwrap_or_else(|| panic!("_bench_one: missing <experiment> arg"));
    let gc: bool = parms.get(3).and_then(|s| s.parse().ok()).unwrap_or_else(|| panic!("_bench_one: missing/invalid <gc> arg"));
    let scale = Scale::parse(parms.get(4).map(|s| s.as_str()).unwrap_or("full"));
    let out_dir = PathBuf::from(parms.get(5).cloned().unwrap_or_else(|| panic!("_bench_one: missing <output_dir> arg")));

    let (metric_name, metric_value, baseline_metric_value) = run_one(&experiment, scale, gc, out_dir.clone());
    let baseline_str = baseline_metric_value.map(|v| v.to_string()).unwrap_or_default();

    fs::write(out_dir.join("result.csv"), format!("metric_name,metric_value,baseline_metric_value\n{metric_name},{metric_value},{baseline_str}\n"))
        .unwrap_or_else(|e| panic!("_bench_one: failed to write result.csv: {e}"));
}

/// Appends one row per experiment run to `<run_dir>/manifest.csv` — the
/// suite-wide summary `scripts/plot_suite.py` reads for its cross-experiment
/// GC-on-vs-off comparison plots, without having to re-derive throughput
/// from every raw per-experiment CSV itself. Written incrementally (not
/// buffered until the suite finishes) so a long run that's interrupted still
/// leaves a usable partial manifest.
struct Manifest {
    path: PathBuf,
}

impl Manifest {
    fn create(run_dir: &Path) -> Self {
        let path = run_dir.join("manifest.csv");
        let mut f = OpenOptions::new().create(true).append(true).open(&path)
            .unwrap_or_else(|e| panic!("benchmark: failed to create {}: {e}", path.display()));
        f.write_all(b"experiment,gc_enabled,duration_secs,primary_metric_name,primary_metric_value,baseline_metric_value,peak_rss_mb,avg_rss_mb,peak_jemalloc_resident_mb\n").unwrap();
        Self { path }
    }

    fn append(&self, experiment: &str, gc_enabled: bool, duration: Duration, out_dir: &Path) {
        let result = fs::read_to_string(out_dir.join("result.csv")).ok().and_then(|s| {
            let row = s.lines().nth(1)?;
            let mut cols = row.split(',');
            let name = cols.next()?.to_string();
            let value: f64 = cols.next()?.parse().ok()?;
            let baseline: Option<f64> = cols.next().and_then(|s| s.parse().ok());
            Some((name, value, baseline))
        });
        let Some((metric_name, metric_value, baseline_metric_value)) = result else {
            println!("[manifest] {experiment} gc={gc_enabled}: SKIPPED (no result.csv — the child process likely failed, see its output above)");
            return;
        };

        let mem = mem_stats::summarize(&out_dir.join("mem_stats.csv"));
        let (peak_rss, avg_rss, peak_resident) = mem
            .map(|m| (m.peak_rss_mb, m.avg_rss_mb, m.peak_jemalloc_resident_mb))
            .unwrap_or((0.0, 0.0, 0.0));
        let baseline_str = baseline_metric_value.map(|v| format!("{v:.3}")).unwrap_or_default();

        let mut f = OpenOptions::new().append(true).open(&self.path)
            .unwrap_or_else(|e| panic!("benchmark: failed to append to {}: {e}", self.path.display()));
        f.write_all(format!(
            "{experiment},{gc_enabled},{:.1},{metric_name},{metric_value:.3},{baseline_str},{peak_rss:.2},{avg_rss:.2},{peak_resident:.2}\n",
            duration.as_secs_f64(),
        ).as_bytes()).unwrap();

        println!(
            "[manifest] {experiment:<20} gc={gc_enabled:<5} {metric_name}={metric_value:.2}  peak_rss={peak_rss:.1}MB  avg_rss={avg_rss:.1}MB  peak_jemalloc_resident={peak_resident:.1}MB"
        );
    }
}

fn write_system_info(run_dir: &Path, scale: Scale) {
    let mem_total_line = fs::read_to_string("/proc/meminfo").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemTotal:")).map(|l| l.to_string()))
        .unwrap_or_else(|| "MemTotal: unknown".to_string());

    let info = format!(
        "cMVBT benchmark suite\n\
         package version   = {}\n\
         scale             = {}\n\
         logical CPUs      = {}\n\
         physical cores    = {}\n\
         {mem_total_line}\n\
         started (local)   = {}\n",
        env!("CARGO_PKG_VERSION"),
        scale.label(),
        num_cpus::get(),
        num_cpus::get_physical(),
        Local::now().format("%Y-%m-%d %H:%M:%S"),
    );
    fs::write(run_dir.join("system_info.txt"), info)
        .unwrap_or_else(|e| panic!("benchmark: failed to write system_info.txt: {e}"));
}

/// Spawns one child process running `_bench_one` for a single (experiment,
/// gc) variant, waits for it to exit, and folds its result into `manifest`.
/// Stdout/stderr are inherited so the child's own driver output (population
/// progress, per-run summary, ...) still streams live to the terminal.
fn run_variant(exe: &Path, run_dir: &Path, manifest: &Manifest, scale: Scale, experiment: &str, gc: bool) {
    let out_dir = run_dir.join(format!("{experiment}_gc_{}", if gc { "on" } else { "off" }));
    let duration = duration_for(experiment, scale);

    println!("\n=== {experiment} (gc={gc}) ===");
    let status = Command::new(exe)
        .arg("_bench_one")
        .arg(experiment)
        .arg(gc.to_string())
        .arg(scale.label())
        .arg(&out_dir)
        .status();

    match status {
        Ok(s) if s.success() => manifest.append(experiment, gc, duration, &out_dir),
        Ok(s) => println!("!! {experiment} (gc={gc}) child process exited with {s} — skipping this variant's manifest row."),
        Err(e) => println!("!! {experiment} (gc={gc}) failed to launch child process: {e} — skipping this variant's manifest row."),
    }
}

pub fn main_benchmark(parms: Vec<String>) {
    let output_root = parms.get(2).cloned().unwrap_or_else(|| "benchmark_results".to_string());
    let scale = Scale::parse(parms.get(3).map(|s| s.as_str()).unwrap_or("full"));

    let exe = std::env::current_exe()
        .unwrap_or_else(|e| panic!("benchmark: failed to resolve current executable path: {e}"));

    let run_dir = PathBuf::from(&output_root).join(format!("run_{}", Local::now().format("%Y%m%d_%H%M%S")));
    fs::create_dir_all(&run_dir)
        .unwrap_or_else(|e| panic!("benchmark: failed to create run directory {}: {e}", run_dir.display()));

    write_system_info(&run_dir, scale);
    let manifest = Manifest::create(&run_dir);

    println!("\n########## cMVBT benchmark suite ##########");
    println!("run directory : {}", run_dir.display());
    println!("scale         : {}", scale.label());
    println!("#############################################");

    for &experiment in &EXPERIMENTS {
        for gc in [true, false] {
            run_variant(&exe, &run_dir, &manifest, scale, experiment, gc);
        }
    }

    println!("\n########## benchmark suite complete ##########");
    println!("results  : {}", run_dir.display());
    println!("manifest : {}", run_dir.join("manifest.csv").display());
    println!("plot with: python3 scripts/plot_suite.py --run-dir {}", run_dir.display());
    println!("################################################\n");
}
