# BatStore

BatStore is a storage engine built around a concurrent multiversion B-tree (cMVBT) with Ordered Snapshot Instant Commit.

## Transaction isolation

BatStore supports **Snapshot Isolation (SI)** and **Read Committed** for multi-operation database and TPC-C transactions. SI is the default: every read in a transaction uses the snapshot taken at `begin`. Read Committed takes a fresh snapshot before each public read/write operation, so later operations can see transactions that committed in the meantime.

Select Read Committed with `DbTransaction::begin_with_isolation(&db, IsolationLevel::ReadCommitted)` (or `TpccTxn::begin_with_isolation` for TPC-C). Each transaction operation refreshes the read snapshot automatically. The transaction's write stamp remains fixed across operations for atomic commit, rollback, and WAL recovery. Existing `begin(&db)` calls continue to use SI.

Run commands from the repository root.

## Setup

```bash
python3 scripts/setup_environment.py
source scripts/.venv/bin/activate
```

Setup may request `sudo` for system packages and PostgreSQL. Use `--reuse-checkouts` to keep existing engine checkouts; see `--help` for other options.

## Benchmarks

Smoke test:

```bash
python3 scripts/compare_engines.py --tiny --engines batstore,leanstore --workloads tpcc,ycsb_a,htap_q1,s_htap --threads 2,4
```

Run the full cross-engine matrix with `python3 scripts/compare_engines.py`, or select comma-separated `--engines`, `--workloads`, `--threads`, and `--gc on,off`. Results go to `comparison_results/`.

| Workload key | Description |
| --- | --- |
| `tpcc` | TPC-C OLTP; committed New-Order transactions per second |
| `ycsb_a`–`ycsb_f` | YCSB: update-heavy, read-mostly, read-only, read-latest, short scans, and read-modify-write |
| `htap_q1`, `htap_q6` | TPC-C with concurrent CH-benCHmark analytical queries |
| `htap_q1_variant`, `htap_q6_variant` | Legacy engine-specific predicates; unsuitable for cross-engine comparison |
| `s_htap` | S-YCSB: cold corpus, near-sorted arrivals, hot-tail updates, and long scans |

YCSB uses Zipfian access by default, except D's latest-key pattern. The comparison runner sweeps requested thread counts and GC modes; engines without a GC toggle report `gc=n/a`.

Specialized runners:

| Purpose | Command | Results |
| --- | --- | --- |
| Fixed OLTP load, varied analytical threads | `python3 scripts/run_htap_analytical_sweep.py --oltp-terminals 4 --olap-threads 1,2,4,8,16` | `htap_analytical_results/` |
| YCSB Zipfian skew | `python3 scripts/run_skew_sweep.py --skews uniform,0.4,0.8,0.99,1.4` | `skew_sweep_results/` |
| S-YCSB hot-update skew | `python3 scripts/run_s_ycsb_sweep.py --skews uniform,0.1,0.4,0.8,0.99,1.4 --threads 2,4,8,16,32,64,128` | `s_ycsb_sweep_results/` |
| YCSB A GC parameters | `python3 scripts/run_gc_sweep.py` | Timed-phase measurements and plots |

Each runner supports `--help`; the comparison and skew runners also support `--tiny` for a quick run. S-YCSB uses `--s-ycsb-theta` for one skew value (`--s-htap-theta` remains an alias) and `--workspace-root` for external dependency checkouts.

For GC allocation metrics, build with `--features gc-stats`. YCSB writes `gc_stats_after_load.csv` and `gc_stats.csv`; subtract the former from the latter for timed-phase totals. Tune `ALLOC_BATCH_SIZE` and `SCAN_PERCENT` in `src/bat_gc/block_tracer.rs`, or set `BATSTORE_GC_BATCH_SIZE` and `BATSTORE_GC_SCAN_PERCENT` for a run.

## Plot results

```bash
python3 scripts/plot.py comparison_results/run_YYYYMMDD_HHMMSS
```

The plotter also accepts run directories from the specialized sweeps and writes figures under that run's `plots/` directory. Use `--compact` for paper figures, or `--help` for all options.

## Papers

- B. Becker et al., *An Asymptotically Optimal Multiversion B-Tree*, The VLDB Journal 5(4), 1996.
- A. Tonta et al., *Multiversion Concurrency Control for Multiversion B-Trees*, [arXiv:2606.09133](https://arxiv.org/abs/2606.09133), 2026.
- A. Alhomssi and V. Leis, *Scalable and Robust Snapshot Isolation for High-Performance Storage Engines*, [PVLDB 16(6)](https://doi.org/10.14778/3583140.3583157), 2023.
