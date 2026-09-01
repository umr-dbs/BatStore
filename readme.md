# BatStore

BatStore is a storage engine built around a concurrent multiversion B-tree
(cMVBT) with Ordered Snapshot Instant Commit.

Run the following commands from the repository root.

## Setup

Prepare BatStore and the comparison engines, then activate the plotting
environment:

```bash
python3 scripts/setup_environment.py
source scripts/.venv/bin/activate
```

The setup may request `sudo` access to install system packages and configure
PostgreSQL. To reuse existing engine checkouts instead of cloning them again:

```bash
python3 scripts/setup_environment.py --reuse-checkouts
```

See all setup options with:

```bash
python3 scripts/setup_environment.py --help
```

## Workloads

The Python harness uses the following workload keys:

| Key | Workload |
| --- | --- |
| `tpcc` | TPC-C OLTP: New-Order, Payment, Order-Status, Delivery, and Stock-Level transactions. The primary metric is committed New-Order transactions per second. |
| `ycsb_a` | YCSB A, update-heavy: 50% reads and 50% updates. |
| `ycsb_b` | YCSB B, read-mostly: 95% reads and 5% updates. |
| `ycsb_c` | YCSB C, read-only: 100% reads. |
| `ycsb_d` | YCSB D, read-latest: 95% reads and 5% inserts, biased toward recent keys. |
| `ycsb_e` | YCSB E, short ranges: 95% scans and 5% inserts. Scan latency is also recorded. |
| `ycsb_f` | YCSB F: 50% reads and 50% read-modify-write operations. |
| `htap_q1` | TPC-C OLTP plus concurrent canonical CH-benCHmark Q1 pricing-summary queries. Measures OLTP interference and analytical throughput/latency. |
| `htap_q6` | TPC-C OLTP plus concurrent canonical CH-benCHmark Q6 revenue-change queries. Measures the same HTAP trade-off with a selective query. |
| `htap_q1_variant` | Q1 using each engine's former custom predicate. Kept for compatibility; not intended for cross-engine comparison. |
| `htap_q6_variant` | Q6 using each engine's former custom predicate. Kept for compatibility; not intended for cross-engine comparison. |
| `s_htap` | **S-YCSB**: a cold YCSB corpus with concurrent near-sorted arrivals, hot-tail updates, and long OLAP scans across the cold/hot boundary. The Python harness key and flags retain `s_htap`/`--s-htap-*`; the Rust subcommands are `s_ycsb` and `mdbx_s_ycsb`. |

YCSB uses Zipfian access by default, except workload D's latest-key pattern;
engine adapters without a latest-key generator approximate D with Zipfian
sampling. The comparison harness sweeps the requested thread counts and GC
modes. Engines without a working GC toggle run once and report `gc=n/a`.

## Run benchmarks

All commands below are run from the repository root. Start with a small smoke
test covering every benchmark family:

```bash
python3 scripts/compare_engines.py \
  --tiny \
  --engines batstore,leanstore \
  --workloads tpcc,ycsb_a,htap_q1,s_htap \
  --threads 2,4
```

### Cross-engine comparison

`compare_engines.py` is the general runner. With no arguments it runs TPC-C,
YCSB A-F, canonical HTAP Q1/Q6, and S-YCSB across all configured engines,
thread counts, and GC modes:

```bash
python3 scripts/compare_engines.py
```

Select a smaller matrix with comma-separated lists:

```bash
python3 scripts/compare_engines.py \
  --engines batstore,leanstore,libmdbx \
  --workloads tpcc,ycsb_a,ycsb_e,htap_q1,s_htap \
  --threads 2,8,32 \
  --gc on,off
```

Useful workload-specific controls include `--warehouses`, `--tpcc-duration`,
`--ycsb-records`, `--ycsb-duration`, `--theta`, `--htap-olap-threads`, and
the `--s-htap-*` options. Results are written to a timestamped directory under
`comparison_results/`.

### HTAP analytical-thread sweep

Use the dedicated HTAP runner when the OLTP population should remain fixed
while the number of analytical threads changes:

```bash
python3 scripts/run_htap_analytical_sweep.py \
  --engines batstore,leanstore,libmdbx \
  --workloads htap_q1,htap_q6 \
  --oltp-terminals 4 \
  --olap-threads 1,2,4,8,16 \
  --warehouses 8
```

This produces OLTP throughput, aggregate OLAP throughput, and query-latency
curves under `htap_analytical_results/`.

### YCSB skew sweep

Use the skew runner to vary Zipfian theta across YCSB workloads and thread
counts:

```bash
python3 scripts/run_skew_sweep.py \
  --engines batstore,leanstore,libmdbx \
  --workloads ycsb_a,ycsb_e \
  --threads 4,16,64 \
  --skews uniform,0.4,0.8,0.99,1.4
```

Results are written under `skew_sweep_results/`. For a quick validation run,
add `--tiny`; for all available options, use the runner's `--help`:

```bash
python3 scripts/compare_engines.py --help
python3 scripts/run_htap_analytical_sweep.py --help
python3 scripts/run_skew_sweep.py --help
```

## Plot results

Pass the completed run directory to the unified plotter:

```bash
python3 scripts/plot.py comparison_results/run_YYYYMMDD_HHMMSS
python3 scripts/plot.py htap_analytical_results/run_YYYYMMDD_HHMMSS
python3 scripts/plot.py skew_sweep_results/run_YYYYMMDD_HHMMSS
```

The plotter detects the run type automatically. For compact, paper-oriented
figures without overall titles and with a shared top legend:

```bash
python3 scripts/plot.py --compact comparison_results/run_YYYYMMDD_HHMMSS
```

Use `--engine batstore` for a single-engine comparison overview and
`--ref-threads N` to choose the cross-engine reference point in skew plots.
Figures are written below `plots/` in the run directory, with format
subdirectories where applicable.

See plotting options with:

```bash
python3 scripts/plot.py --help
```

## Papers

- B. Becker et al., *An Asymptotically Optimal Multiversion B-Tree*, The
  VLDB Journal 5(4), 1996.
- A. Tonta, B. Seeger, and E. Soisalon-Soininen, *Multiversion Concurrency
  Control for Multiversion B-Trees*, [arXiv:2606.09133](https://arxiv.org/abs/2606.09133),
  2026.
- A. Alhomssi and V. Leis, *Scalable and Robust Snapshot Isolation for
  High-Performance Storage Engines*, PVLDB 16(6), 2023,
  [doi:10.14778/3583140.3583157](https://doi.org/10.14778/3583140.3583157).
