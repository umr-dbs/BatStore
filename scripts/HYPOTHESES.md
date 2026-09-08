# H1–H6 experiments

Run the complete suite from the repository root:

```bash
bash scripts/run_hypotheses.sh
```

Each script builds its required binary, writes a timestamped result directory,
and generates PDF/PNG plots. The suite stops on failure. Run scripts sequentially:
H6 rebuilds the shared binary with instrumentation. Do not use `--skip-build`
unless the binary has the correct features for that experiment.

| Script | Hypothesis and default experiment |
| --- | --- |
| `h1.py` | Isolation overhead: AutoCommit vs SI, YCSB A plus read-only C control; 1,2,4,8,16,32,48,64,80,96,112,128 threads, 2M records, 20s per point. |
| `h2.py` | Access skew: scrambled Zipfian theta uniform,0.1,0.4,0.8,0.99,1.4 at 1,8,16,32,48,64,80,96,112,128 threads; 2M records, 20s per point. Separate plots per thread count. |
| `h3.py` | Historical scan performance: one fixed snapshot repeatedly scanned over 600s, grouped into 12 snapshot-age windows; 8 warehouses, 2 OLTP terminals, 1 scan thread. Historic retention overrides GC and prevents commit-log truncation. |
| `h4.py` | Increasing analytical concurrency: 0,1,2,4,8,16,32,48,64,80,96,112,128 OLAP threads, 4 fixed OLTP terminals, 8 warehouses, 60s per point. Always includes a zero-scan baseline. |
| `h5.py` | Increasing transactional concurrency: 1,2,4,8,16,32,48,64,80,96,112,128 OLTP terminals, 2 fixed OLAP threads, 8 warehouses, 60s per point. Reports scan p50/p95/p99. |
| `h6.py` | Memory reuse: 1,2,4,8,16,32,48,64,80,96,112,128 threads, YCSB A, 200k records, 60s per point. Subtracts post-load counters from final counters. |

H2 and H3 used to have the opposite numbering. H4 no longer also runs H5.
Existing result files are unchanged; do not mix their old labels with new runs.

The expanded defaults target NUMA node 0 with 64 physical cores / 128 hardware
threads. Points at 80, 96 and 112 extend coverage above 64 workers; these points
measure scaling into the SMT range, though placement is left to the scheduler.
The harness pins runs to NUMA node 0. All concurrency sweeps include 128.
YCSB and TPC-C size their worker registries for the requested concurrency plus
loader and background-worker slots. The loader finishes population before the
measured phase but retains its registration slot; that slot does not consume
a CPU during the workload. Background workers can add scheduling overhead.
H3 keeps concurrency fixed because its independent variable is snapshot age.

H4/H5 also retain the driver's automatic parallel scan pool: its size can grow
with analytical concurrency. The requested OLTP/OLAP counts are not a limit on
total OS threads; high points can oversubscribe this node. Interpret them as
whole-engine concurrency experiments, including that pool's scheduling costs.
Allow capacity for loading and background work as well. The scripts reject driver clamping
instead of plotting misleading thread counts. On smaller machines use explicit
smaller sweeps, for example:

```bash
python3 scripts/h1.py --threads 1,2,4,8
python3 scripts/h2.py --threads 1,4,8 --duration 60
python3 scripts/h3.py --duration 900 --buckets 18
python3 scripts/h4.py --olap-threads 0,1,2,4,8 --fixed-oltp-terminals 4
python3 scripts/h5.py --oltp-terminals 1,2,4,8 --fixed-olap-threads 1
python3 scripts/h6.py --threads 1,2,4,8
```

H4/H5 support `--workload htap_q1` (default) or `htap_q6`. These measure
CH-benCHmark analytical queries, while H3 repeatedly scans all tables at one fixed historical snapshot.
The snapshot stays registered in one read transaction while OLTP advances; this
is not an arbitrary AS OF timestamp API. `historic` mode forces GC, in-place
updates and idle compaction off and retains commit logs. Disabling GC alone
is insufficient for general historical reads. Each CSV row records the fixed
snapshot version, actual age at scan start, latency and cardinality. H3 rejects
changing snapshot IDs or cardinalities. The 12 default reporting windows are
50 seconds wide; they do not create new snapshots. History retention increases
memory usage over the run. Rebuild Rust before using the new mode; old H3
fresh-snapshot results do not test this hypothesis.
H2 hashes hot Zipf ranks across the key space; this disperses hot keys but does
not guarantee identical access frequency on every physical page.
H1 compares single-operation write commit paths; read-only C is a control and
should not be interpreted as evidence that AutoCommit must always be faster.

H6 measures source frequencies, **not time spent allocating**. Fresh allocation
means a request to the global allocator, which may reuse memory without an OS
call. `local_reuse_share` is local reuse divided by all events; `steal_share`
retains its original denominator (local reuse plus steals). The other event
counts and shares are in `h6_gc_stats.csv`. These data test the reuse-frequency
part of H6; allocation timing/OS tracing would be needed for its cost claim.

Scripts default to the checkout containing them. Set `BATSTORE_REPO` explicitly
to use another checkout. Update the whole server checkout, including Cargo.toml
and Rust sources: H6 requires `gc-stats` and the new post-load counter snapshot.
Successful execution does not mean a hypothesis is supported; inspect measured
trends. Repeat full runs for variability before drawing research conclusions.
