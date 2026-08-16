# cMVBT-OSIC
> Original MVBT and cMVBT Papers:
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
> Ordered Snapshot Instant Commit from Paper (LeanStore):
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
---------------------------------------

## Running the benchmarks

Build and run benchmarks with the release profile. Arguments are positional;
the examples below are complete commands that can be copied as-is.

```bash
cargo build --release
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

### HTAP (TPC-C plus CH-benCHmark queries)

Run four TPC-C warehouses and one analytical thread for 60 seconds, preceded
by a 15-second OLTP-only baseline:

```bash
cargo run --release -- htap 4 60 1 15 EUROPE
```

```text
htap [warehouses=4] [seconds=60] [olap_threads=1]
     [baseline_seconds=15] [region=EUROPE]
```

The analytical workers rotate through the implemented CH-benCHmark queries on
the live TPC-C schema while OLTP terminals continue processing transactions.
The baseline phase lets the report quantify OLTP interference caused by the
concurrent analytical workload, in addition to OLAP throughput and snapshot
freshness/staleness.

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

## Python scripts

The scripts require Python 3. Plotting additionally needs the packages in
`scripts/requirements.txt`:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r scripts/requirements.txt
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

For a quick cross-engine smoke test, use the comparison harness's tiny scale:

```bash
python3 scripts/compare_engines.py \
  --tiny --engines cmvbt,leanstore --workloads tpcc,ycsb_a --threads 2,4
python3 scripts/plot_compare.py
```

The comparison harness writes timestamped runs below `comparison_results/`
and normalizes their measurements into `manifest.csv`. It assumes the
selected external engines are already installed. `scripts/setup_environment.py`
can prepare the full comparison environment, but it installs system packages,
clones/builds engines, configures tmpfs, and may reconfigure PostgreSQL; read
[`manual.txt`](manual.txt) before running it. Every script exposes its full
interface through `python3 scripts/<name>.py --help`.

CROSS-ENGINE BENCHMARK HARNESS - MANUAL
========================================
    Read manual.txt

## Engineering notes

- **[Transactional Support via OSIC on cMVBT: System Design, Datastructure Changes, and Optimizations](docs/transactional_osic_comprehensive.tex)** ([PDF](docs/transactional_osic_comprehensive.pdf)) --
  comprehensive synthesis of system design, all datastructure modifications, and every optimization tested or applied, with measured performance numbers and adoption decisions.

- [Unified optimization report](docs/optimization_report.tex) ([PDF](docs/optimization_report.pdf)) --
  the implementation's optimizations organized bottom-up by architectural dependency, with fresh measurements and known open issues.

- [Index optimization guide](docs/index_optimizations.md) -- compact guide to the optimizations used by the current cMVBT index

- Supporting documentation:
  - [Range-scan iteration: ordered routing and zero-copy streaming](docs/range_scan_iteration.md)
  - [Range-scan visibility-check optimization](docs/range_scan_visibility_check.md)
  - [Big-tree leaf-size benchmark](docs/bigtree_size_benchmark.md)
  - [OLTP/WAL optimization](docs/oltp_wal_optimization.md)

# Contact
    Name:               Amir Tonta
    E-Mail:             amir.tonta@mathematik.uni-marburg.de
