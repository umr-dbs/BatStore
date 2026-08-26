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

## Run benchmarks

The benchmark script supports TPC-C, YCSB A-F, HTAP Q1/Q6, and S-HTAP. A
small run covering all four benchmark families is:

```bash
python3 scripts/compare_engines.py \
  --tiny \
  --engines batstore,leanstore \
  --workloads tpcc,ycsb_a,htap_q1,s_htap \
  --threads 2,4
```

Run the complete default benchmark matrix with:

```bash
python3 scripts/compare_engines.py
```

Select workloads with `--workloads`:

```bash
# TPC-C
python3 scripts/compare_engines.py --workloads tpcc

# YCSB A-F
python3 scripts/compare_engines.py \
  --workloads ycsb_a,ycsb_b,ycsb_c,ycsb_d,ycsb_e,ycsb_f

# HTAP with CH-benCHmark Q1 and Q6
python3 scripts/compare_engines.py --workloads htap_q1,htap_q6

# Streaming HTAP
python3 scripts/compare_engines.py --workloads s_htap
```

Each run creates a timestamped directory under `comparison_results/`. Use
`--engines`, `--threads`, and `--gc` to select the engines and sweep settings.
See every option with:

```bash
python3 scripts/compare_engines.py --help
```

## Plot results

Pass the completed run directory to the unified plotter:

```bash
python3 scripts/plot.py comparison_results/run_YYYYMMDD_HHMMSS
```

PDF files are written to `plots/` and SVG files to `plots/svg/` inside the run
directory.

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
