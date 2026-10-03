# BatStore

BatStore is based on siMVBT, an extension of the [concurrent multiversion B-tree (cMVBT)](https://github.com/umr-dbs/cMVBT), and adds Ordered Snapshot Instant Commit.

## Reproducing the experiments

Use a dedicated Debian/Ubuntu machine with `sudo` access. The setup script installs the required system packages, builds the benchmark engines, configures PostgreSQL, and creates a Python environment. Run all commands from the repository root.

### 1. Set up the environment

```bash
python3 scripts/setup_environment.py
source scripts/.venv/bin/activate
```

The default setup prepares BatStore, LeanStore, WiredTiger, PostgreSQL, and BatStore's libmdbx backend. It may prompt for the `sudo` password and package-installation confirmation. Re-running with `--reuse-checkouts` keeps the existing dependency checkouts instead of cloning them again.

### 2. Run a quick check

```bash
python3 scripts/compare_engines.py \
  --tiny \
  --engines batstore,leanstore \
  --workloads tpcc,ycsb_a \
  --threads 2 \
  --gc on \
  --affinity off
```

This small run verifies that the environment works before starting the full experiments.

### 3. Run the main comparison

```bash
python3 scripts/compare_engines.py \
  --engines batstore,leanstore,wiredtiger,postgres,libmdbx
```

The runner executes TPC-C, YCSB A-F, CH-benCHmark Q1/Q6 with concurrent TPC-C, and S-YCSB. By default it sweeps 2, 4, 8, 16, 32, 64, and 128 threads and compares garbage collection where the engine supports it.

Results are written to a timestamped directory under `comparison_results/`. Each run contains the normalized `manifest.csv`, its configuration, and the per-engine output.

### 4. Plot the results

Use the run directory printed by the benchmark command:

```bash
python3 scripts/plot.py comparison_results/run_YYYYMMDD_HHMMSS
```

Plots are written to the run's `plots/` directory.

## Additional experiment suites

```bash
# Fixed OLTP load while varying analytical threads
python3 scripts/run_htap_analytical_sweep.py --oltp-terminals 4 --olap-threads 1,2,4,8,16

# YCSB skew sweep
python3 scripts/run_skew_sweep.py --skews uniform,0.4,0.8,0.99,1.4

# S-YCSB hot-update skew sweep
python3 scripts/run_s_ycsb_sweep.py --skews uniform,0.1,0.4,0.8,0.99,1.4

# Garbage-collection parameter sweep
python3 scripts/run_gc_sweep.py
```

Every runner supports `--help`. The comparison and skew runners also support `--tiny` for short validation runs.

## References

- B. Becker et al., *An Asymptotically Optimal Multiversion B-Tree*, The VLDB Journal 5(4), 1996.
- A. Tonta et al., *Multiversion Concurrency Control for Multiversion B-Trees*, [arXiv:2606.09133](https://arxiv.org/abs/2606.09133), 2026.
- A. Alhomssi and V. Leis, *Scalable and Robust Snapshot Isolation for High-Performance Storage Engines*, [PVLDB 16(6)](https://doi.org/10.14778/3583140.3583157), 2023.
