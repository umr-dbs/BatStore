# BatStore

BatStore is a storage engine built around the concurrent multiversion B-tree
(cMVBT) with Ordered Snapshot Instant Commit. The main reproducibility entry points are the Python setup and
cross-engine comparison scripts described below. The Rust binary can also run
BatStore-only benchmarks directly.

## Quick start: cross-engine tests

The comparison workflow has three steps: prepare the engines, run a comparison
matrix, and plot its normalized results. Run all commands from the repository
root.

### 1. Prepare Python and the benchmark engines

For plotting only, create a small Python environment manually:

```bash
python3 -m venv scripts/.venv
source scripts/.venv/bin/activate
python3 -m pip install -r scripts/requirements.txt
```

For cross-engine tests, the setup script performs that Python setup and also
prepares BatStore, LeanStore, WiredTiger, PostgreSQL/BenchBase, and their system
dependencies:

```bash
python3 scripts/setup_environment.py
source scripts/.venv/bin/activate
```

By default, setup-managed checkouts are freshly cloned below `tx_tests/` (or
`$WORKSPACE_ROOT`), external engines are patched and built, PostgreSQL is
configured, benchmark storage is placed on tmpfs, and the plotting virtual
environment is created at `scripts/.venv`. The script invokes `sudo` for system
package and PostgreSQL steps and may stop or reconfigure the local PostgreSQL
service. Read [the comparison manual](manual.txt) before running it on a machine
with an existing PostgreSQL installation or benchmark data you need to retain.

Useful setup variants are:

```bash
# Keep and incrementally reuse existing engine checkouts.
python3 scripts/setup_environment.py --reuse-checkouts

# Also build both experimental vWeaver/ERMIA variants and configure hugepages.
python3 scripts/setup_environment.py --full

# Prepare a subset; all setup stages have corresponding --skip-* options.
python3 scripts/setup_environment.py --skip-postgres --skip-benchbase

# Place setup-managed engine checkouts somewhere else.
WORKSPACE_ROOT=/data/tx_tests python3 scripts/setup_environment.py
```

Run `python3 scripts/setup_environment.py --help` for the complete interface.

### 2. Run the comparison harness

Start with a small smoke test:

```bash
python3 scripts/compare_engines.py \
  --tiny \
  --engines batstore,leanstore \
  --workloads tpcc,ycsb_a \
  --threads 2,4
```

The default invocation runs the full configured matrix:

```bash
python3 scripts/compare_engines.py
```

`compare_engines.py` provides a common driver for these engines:

- `batstore`
- `leanstore`
- `wiredtiger` through LeanStore's adapter
- `postgres` through BenchBase
- `libmdbx`
- `vweaver_ermia` and `vweaver_ermia_frugal` when their optional setup succeeds

It can run TPC-C, YCSB A-F, two portable HTAP workloads that mix TPC-C with
one CH-benCHmark query (`htap_q1` or `htap_q6`), and the synthetic streaming
`s_htap` workload. Q1 and Q6 are the common analytical subset implemented by
every comparison engine. BatStore's native full CH-benCHmark mode additionally
runs Q4 and Q5.

The implemented queries are:

| Query | Name | Compact description | Comparison scope |
| --- | --- | --- | --- |
| Q1 | Pricing Summary Report | Scan delivered order lines and aggregate count, quantity, and amount by order-line position. | Cross-engine `htap_q1`; native BatStore full mode |
| Q6 | Forecasting Revenue Change | Sum order-line revenue in a delivery-date range for quantities below a threshold. | Cross-engine `htap_q6`; native BatStore full mode |
| Q4 | Order Priority Checking | Count orders with a late or undelivered order line, grouped by the order's line count. | Native BatStore full mode |
| Q5 | Local Supplier Volume | Join orders, order lines, stock, suppliers, nations, and regions; aggregate revenue by supplier nation. | Native BatStore full mode |

Every engine is launched through its adapter in `scripts/engines/`, pinned to
one NUMA node, and measured through the same matrix of workload, thread count,
and supported GC modes. Engines without a working GC toggle run once with
`gc_enabled=n/a`.

Each invocation creates a timestamped directory below `comparison_results/`.
Raw engine output is retained there and normalized into one `manifest.csv`, so
throughput, memory consumption, scan latency, and HTAP interference can be
plotted consistently. Individual engine failures are recorded in the manifest
without discarding the rest of the matrix.

LeanStore and PostgreSQL/BenchBase emit large or numerous internal result files,
including profiling tables such as `log_bm.csv`, raw latency samples, and
per-transaction BenchBase exports. All workload runner scripts remove unneeded
artifacts automatically after their run matrix finishes, while retaining the
manifest and compact measurement CSVs needed to audit or reproduce plots. To
clean older run directories manually, preview and then apply the same cleanup:

```bash
python3 scripts/clean_leanstore_runs.py comparison_results
python3 scripts/clean_leanstore_runs.py comparison_results --delete
```

The cleaner only examines recognized run directories and only changes files
below their `leanstore/` and `postgres/` subdirectories. Its default mode does
not delete anything; add `--verbose` to list every candidate.

Common focused invocations include:

```bash
# Selected engines and workloads.
python3 scripts/compare_engines.py \
  --engines batstore,leanstore,libmdbx \
  --workloads tpcc,ycsb_e,s_htap

# Explicit concurrency and GC sweep.
python3 scripts/compare_engines.py --threads 1,4,16,64 --gc on,off

# Reuse binaries already built by setup.
python3 scripts/compare_engines.py --skip-build --tiny
```

Run `python3 scripts/compare_engines.py --help` for scale, payload, allocator,
workload, and output options. The methodology and engine-specific constraints
are documented in detail in [manual.txt](manual.txt).

### 3. Plot a comparison run

The harness prints the exact run directory when it finishes. Plot it with:

```bash
python3 scripts/plot_compare.py \
  --run-dir comparison_results/run_YYYYMMDD_HHMMSS
```

For a run containing only one engine, the plotter automatically creates a
single `single_engine_overview` figure instead of cross-engine charts. Every
workload present gets throughput, scan/query latency, and peak-memory panels
across the measured thread counts and GC modes. This keeps partial runs such as
BatStore with only `htap_q1`, `htap_q6`, and `ycsb_e` meaningful.

Select the same single-engine view from a run containing several engines with:

```bash
python3 scripts/plot_compare.py \
  --run-dir comparison_results/run_YYYYMMDD_HHMMSS \
  --engine batstore
```

This writes `single_engine_overview_batstore.{svg,pdf}`, so selecting another
engine later does not overwrite the BatStore figure.

For BatStore HTAP, `compare_engines.py` keeps one analytical query active and
allocates its parallel scan pool from the CPU capacity left after OLTP. The
default budget is the largest `--threads` value; override it explicitly with
`--htap-cpu-budget`. For example, this gives scan-pool sizes 14, 12, 8, and 0:

```bash
python3 scripts/compare_engines.py \
  --engines batstore --workloads tpcc,htap_q1,htap_q6 \
  --threads 2,4,8,16 --htap-cpu-budget 16
```

`--scan-pool-workers N` still forces a fixed pool size and takes precedence
over the dynamic policy. A computed size below two disables the pool and lets
the OLAP query scan sequentially on its own thread.

## Running BatStore benchmarks directly

Build and run benchmarks with the release profile. Arguments are positional;
the examples below are complete commands that can be copied as-is.

```bash
cargo build --release
```

### Full benchmark suite

Run the complete experiment suite directly through the benchmark CLI:

```bash
./target/release/batstore benchmark
```

This runs TPC-C OLTP-only, two measurements of the CH-benCHmark HTAP workload,
and YCSB workloads A-F, each once with garbage collection enabled and once with
it disabled. The suite's `ch_benchmark` entry is the mixed phase alone: TPC-C
transactions and CH-benCHmark analytical queries run concurrently. Its `htap`
entry runs an OLTP-only baseline first and then the same mixed phase, allowing
the report to calculate OLTP interference. `ch_benchmark` is therefore not an
analytical-only workload separate from HTAP.

Every variant runs in a fresh child process so its memory measurements are not
contaminated by allocations retained from an earlier experiment. The full
scale is intended for a 64-core/128-thread server with roughly 500 GB of RAM
and takes approximately 30-45 minutes.

Results are placed in a timestamped directory below `benchmark_results/`. To
choose another output root, pass it as the first positional argument:

```bash
./target/release/batstore benchmark my_benchmark_results
```

For a local smoke test using the same experiments at a much smaller scale,
pass `quick` as the second positional argument:

```bash
./target/release/batstore benchmark benchmark_results quick
```

The suite prints the exact result directory when it finishes. Plot a completed
run with:

```bash
python3 scripts/plot_suite.py --run-dir benchmark_results/run_YYYYMMDD_HHMMSS
```

### YCSB

Run YCSB workload A with 100,000 preloaded records, 8 worker threads, and a
30-second measured phase:

```bash
cargo run --release -- ycsb a 100000 8 30
```

The workload may be `a` through `f`. The basic positional form is:

```text
ycsb [workload=a] [records=100000] [threads=CPU count] [seconds=30]
```

Workload A is update-heavy, B is read-heavy, C is read-only, D reads the
latest records and inserts new ones, E is scan-heavy, and F performs
read-modify-write operations. If omitted, the remaining options use the
workload's standard request distribution and the engine defaults. Results are
written to `ycsb_timeseries.csv`, `ycsb_scan_latency_summary.csv`, and the
memory-statistics CSV files in the current directory.

### TPC-C (OLTP only)

Run four warehouses with four terminals for 30 seconds:

```bash
cargo run --release -- tpcc 4 4 30 true true false fg none 0
```

The basic positional form is:

```text
tpcc [warehouses] [terminals] [seconds] [warehouse_affinity]
     [gc] [update_in_place] [root_index] [olap_mode] [olap_threads]
```

Use `olap_mode=none` and `olap_threads=0` for a plain TPC-C OLTP run as in the
example. `root_index` accepts `fg` (frugal list, default), `sk`, `ll`, or `bt`.
The summary reports committed/conflicting/user-aborted transactions and tpmC.

#### OSIC long-snapshot test: TPC-C plus sleeping OLAP

This is the particularly important robustness experiment from the LeanStore
OSIC paper (the "open a transaction and sleep" experiment described around
its Figures 1 and 9). An analytical worker opens a snapshot and holds it
without doing useful work while TPC-C continues. It isolates the cost of an
old, long-lived snapshot: retained versions, garbage-collection pressure, and
the effect on OLTP throughput.

The following runs four warehouses/four terminals for 60 seconds while one
OLAP thread repeatedly holds a snapshot for 10 seconds:

```bash
cargo run --release -- tpcc 4 4 60 true true false fg sleep 1 10
```

For `olap_mode=sleep`, positional argument 10 is the number of OLAP threads
and argument 11 is the snapshot hold time in seconds. Compare its tpmC and
memory behavior with the otherwise-identical OLTP-only command:

```bash
cargo run --release -- tpcc 4 4 60 true true false fg none 0
```

This is intentionally different from `htap`: `htap` executes real
CH-benCHmark queries, whereas `sleep` removes query computation and focuses
on OSIC/MVCC behavior under a pinned historical snapshot.

### CH-benCHmark HTAP (TPC-C plus analytical queries)

CH-benCHmark is an HTAP benchmark: it runs a TPC-C transactional workload and
TPC-H-derived analytical queries concurrently against the same live schema. It
is not a standalone TPC-H/OLAP workload in this project.

The historical `tpch` CLI name runs only that mixed phase. For example, run
four warehouses and one analytical thread for 60 seconds with:

```bash
cargo run --release -- tpch 4 60 1 EUROPE
```

```text
tpch [warehouses=4] [seconds=60] [olap_threads=1] [region=EUROPE]
```

The `htap` command runs an OLTP-only baseline immediately before the same
CH-benCHmark mixed phase. This is the preferred command when measuring how
much concurrent analytics reduces transactional throughput. Run four TPC-C
warehouses and one analytical thread for 60 seconds, preceded by a 15-second
OLTP-only baseline:

```bash
cargo run --release -- htap 4 60 1 15 EUROPE
```

```text
htap [warehouses=4] [seconds=60] [olap_threads=1]
     [baseline_seconds=15] [region=EUROPE]
```

In both native BatStore commands, analytical workers rotate through Q1 (Pricing
Summary Report), Q6 (Forecasting Revenue Change), Q4 (Order Priority
Checking), and Q5 (Local Supplier Volume) while OLTP terminals continue
processing transactions. Only `htap` adds the baseline needed to quantify
OLTP interference; both commands report analytical throughput and snapshot
freshness/staleness for their mixed phase.

This query set is intentionally broader than the cross-engine comparison
harness: `compare_engines.py` uses the isolated `htap_q1` and `htap_q6`
workloads because Q1 and Q6 are implemented across every engine, while the
native BatStore CH-benCHmark runner also exercises Q4 and Q5. Here, “HTAP” names
the concurrent OLTP+OLAP execution pattern; it does not imply one fixed query
set.

### S-HTAP (streaming HTAP)

For a short representative run, load 100,000 historical rows, use four writer
threads and one analytical thread, and measure for 30 seconds:

```bash
cargo run --release -- s_htap 100000 4 1 30 10000 0.99 0.20 50 0 30000
```

S-HTAP is a synthetic streaming-ingest-plus-dashboard workload. Unlike the
TPC-C-based HTAP command, it does not model business transactions or
CH-benCHmark queries. It starts with a cold historical YCSB-shaped corpus and
then runs two pools against the same tree:

- Writers append mostly increasing arrival keys and repeatedly revise a
  recency-biased hot tail. A configurable fraction of arrivals may be late,
  producing upserts behind the current maximum key.
- OLAP workers continuously scan a wide key interval around the cold/hot
  boundary. Writers keep changing that region while a scan is open, stressing
  MVCC version retention, cold pages, garbage collection, and snapshot
  isolation.

The useful positional form is:

```text
s_htap [historical_records=1000000] [write_threads=CPU count - 1]
       [olap_threads=1] [seconds=30] [hot_window=10000]
       [hot_zipf_theta=0.99] [arrival_ratio=0.20]
       [max_lateness=50] [olap_lag=0] [olap_span=3 * hot_window]
```

The parameters control the workload as follows:

- `hot_window`: number of newest keys eligible for hot updates.
- `hot_zipf_theta`: concentration of updates near the newest key; larger
  values make the hot set more skewed.
- `arrival_ratio`: fraction of write operations that are arrivals. The rest
  are updates to the hot window.
- `max_lateness`: maximum number of keys a late arrival may fall behind its
  arrival ticket. Set it to `0` for strictly ordered ingestion.
- `olap_lag`: distance between the current maximum key and the newest edge of
  each scan. `0` makes scans reach the live tail.
- `olap_span`: number of keys covered by each analytical scan. A span larger
  than `hot_window` deliberately crosses from stable history into the
  frequently revised tail.

S-HTAP reports arrival, late-upsert, and hot-update counts, aggregate write
throughput, completed scans, and scanned tuples. It also writes:

- `s_htap_timeseries.csv`: write operations completed per second;
- `s_htap_scan_latency_summary.csv`: p50/p95/p99 scan latency;
- `s_htap_staleness_summary.csv`: how many committed versions the scan's
  snapshot was behind when the scan completed.

For all commands, run the binary from a dedicated output directory if you
want to keep CSV files from different runs separate.

## Plotting standalone BatStore results

Activate the Python environment created in the quick start before running the
plotting scripts:

```bash
source scripts/.venv/bin/activate
```

Plot all recognized CSV files produced by a benchmark in the current
directory:

```bash
python3 scripts/plot_results.py auto --dir . --out-dir plots
```

Useful focused plots include:

```bash
# TPC-C scan-delay/snapshot-age sweep
python3 scripts/plot_results.py scan-sweep tpcc_scan.csv -o scan_sweep.png

# CH-benCHmark query latency and staleness
python3 scripts/plot_results.py ch tpcc_scan.csv -o htap.png

# OLTP-only versus mixed HTAP throughput
python3 scripts/plot_results.py interference \
  oltp_baseline.csv oltp_mixed.csv -o interference.png
```

Every script exposes its full interface through
`python3 scripts/<name>.py --help`.

## Papers and citation

Original MVBT and cMVBT papers:

```bibtex
@article{becker1996asymptotically,
  title={An asymptotically optimal multiversion B-tree},
  author={Becker, Bruno and Gschwind, Stephan and Ohler, Thomas and Seeger, Bernhard and Widmayer, Peter},
  journal={The VLDB Journal},
  volume={5},
  number={4},
  pages={264--275},
  year={1996},
  publisher={Springer}
}
@article{tonta2026multiversion,
  title={Multiversion Concurrency Control for Multiversion B-Trees},
  author={Tonta, Amir and Seeger, Bernhard and Soisalon-Soininen, Eljas},
  journal={arXiv preprint arXiv:2606.09133},
  year={2026}
}
```

Ordered Snapshot Instant Commit paper (LeanStore):

```bibtex
@article{alhomssi2023scalable,
  title     = {Scalable and Robust Snapshot Isolation for High-Performance Storage Engines},
  author    = {Alhomssi, Adnan and Leis, Viktor},
  journal   = {Proceedings of the VLDB Endowment},
  volume    = {16},
  number    = {6},
  pages     = {1426--1438},
  year      = {2023},
  publisher = {VLDB Endowment},
  doi       = {10.14778/3583140.3583157}
}
```

## Engineering notes

- **[Transactional Support via OSIC on BatStore: System Design, Datastructure Changes, and Optimizations](docs/transactional_osic_comprehensive.tex)** ([PDF](docs/transactional_osic_comprehensive.pdf)) --
  comprehensive synthesis of system design, all datastructure modifications, and every optimization tested or applied, with measured performance numbers and adoption decisions.

- [Unified optimization report](docs/optimization_report.tex) ([PDF](docs/optimization_report.pdf)) --
  the implementation's optimizations organized bottom-up by architectural dependency, with fresh measurements and known open issues.

- [Index optimization guide](docs/index_optimizations.md) -- compact guide to the cMVBT index used by BatStore

- Supporting documentation:
  - [Range-scan iteration: ordered routing and zero-copy streaming](docs/range_scan_iteration.md)
  - [Range-scan visibility-check optimization](docs/range_scan_visibility_check.md)
  - [Big-tree leaf-size benchmark](docs/bigtree_size_benchmark.md)
  - [OLTP/WAL optimization](docs/oltp_wal_optimization.md)

## Contact

Name: Amir Tonta

Email: amir.tonta@mathematik.uni-marburg.de
