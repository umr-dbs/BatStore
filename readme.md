# BatStore

<p align="center">
  <img src="assets/batstore-logo-clean.png" alt="BatStore — Marburg, Germany" width="600">
</p>

BatStore is based on siMVBT, an extension of the [concurrent multiversion B-tree (cMVBT)](https://github.com/umr-dbs/cMVBT), and adds Ordered Snapshot Instant Commit.

## Prebuilt BTW artifact binary

A prebuilt Linux x86-64 executable is available under
[`artifacts/btw/`](artifacts/btw/README.md) for artifact-review smoke tests.
Its platform requirements, checksum, build provenance, and the distinction
between functional validation and performance measurements are documented
alongside the binary.

## Reproduce the complete paper

Use a dedicated Debian/Ubuntu machine with `sudo` access and run all commands from the repository root. First prepare the benchmark environment:

```console
python3 scripts/setup_environment.py
source scripts/.venv/bin/activate
```

Then run every paper experiment (H1-H6). The script runs the suites in order, stops on the first failure, and regenerates all individual figures plus the combined paper overview only after the measurements finish:

```console
python3 scripts/run_paper_experiments.py
```

Results are stored together under `paper_results/run_YYYYMMDD_HHMMSS/`. Each H1-H6 subdirectory contains its raw measurements, configuration, and plots. The collection's `paper_run.json` records the exact commands and completion status; the combined figures are in `plots/`.

## Optional validation and additional capabilities

The following commands are not required for the complete paper run.

### Optional: short end-to-end validation

Use the quick profile to verify every runner and plotting path before committing to the full run. These reduced settings are a smoke test and must not be reported as paper measurements.

```console
python3 scripts/run_paper_experiments.py --quick
```

Use `--only h1,h3,h6` to run selected suites, `--skip-build` to reuse binaries that were already built, or `--dry-run` to print the exact commands without creating results.

### Additional: broad cross-engine comparison

This broader matrix is useful for exploratory comparisons beyond the focused paper hypotheses:

```console
python3 scripts/compare_engines.py
python3 scripts/plot.py comparison_results/run_YYYYMMDD_HHMMSS
```

It covers TPC-C, YCSB A-F, CH-benCHmark Q1/Q6 with concurrent TPC-C, and S-YCSB across the configured engines, thread counts, and GC modes.

### Additional: specialized sweeps

```console
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

### Additional background

- B. Becker et al., *An Asymptotically Optimal Multiversion B-Tree*, The VLDB Journal 5(4), 1996.
- A. Tonta et al., *Multiversion Concurrency Control for Multiversion B-Trees*, [arXiv:2606.09133](https://arxiv.org/abs/2606.09133), 2026.
- A. Alhomssi and V. Leis, *Scalable and Robust Snapshot Isolation for High-Performance Storage Engines*, [PVLDB 16(6)](https://doi.org/10.14778/3583140.3583157), 2023.
