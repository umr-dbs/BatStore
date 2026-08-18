# YCSB and HTAP Q1/Q6 optimization batch

This batch removes benchmark-harness work from timed paths and adds storage
primitives for the operations the workloads actually request. Measurements
used a release build, 200,000 preloaded rows, four workers, a three-second
timed phase, Zipf theta 0.99, GC enabled, no WAL, and the frugal root index.
They measure the combined batch, not isolated attribution to one row.

| Change | Reason | Implementation and gain | Potential risk / qualification |
|---|---|---|---|
| Isolate HTAP Q1 and Q6 | The old `ch` mode rotated Q1, Q6, Q4, and Q5, so an `htap_q1` or `htap_q6` point included unrelated joins. | `OlapMode::ChQ1` and `ChQ6` execute only the selected query; the cross-engine wrapper selects them explicitly. This fixes workload attribution; no isolated gain is claimed yet. | New results are intentionally not directly comparable with old `ch` results, which measured a different query mix. |
| Bulk-generate YCSB values | Intermediate strings/vectors followed by a copy into a row consumed update/insert time. | Rows allocate their final payload once and fill it in place using buffered random bytes. Full-row YCSB-A improved from 2.52M to 3.11M ops/s as part of this batch (+23.5%). | Payloads remain unbiased alphanumeric bytes, but exact seeded byte sequences differ. |
| Implement `writeallfields` | Standard YCSB defaults to updating one field, while the old harness regenerated all ten. | CLI positional argument 17 defaults to `false`. A leaf-latched `update_with` atomically copies the old row and patches one field; `true` retains full-row replacement. Standard single-field YCSB-A reached 4.81M ops/s (+90.8% versus the old full-row baseline). | The +90.8% includes different, standard YCSB semantics. Use `true` for historical comparisons. The closure executes under the leaf write latch and must stay small. |
| Add `point_exists_si` | YCSB reads only report hit/miss but previously allocated a result vector and cloned a payload handle. | The gap-free SI primitive tests visibility in place and returns `bool`. Together with the alias sampler, YCSB-C improved from 7.16M to 14.82M ops/s (+106.8%). | It cannot replace callers that consume payload data. It retains normal reader registration and visibility rules. |
| Avoid empty read-only commits | Read-only TPC-C/HTAP transactions advanced the global clock and appended an empty commit. | `DbTransaction` and `TpccTxn` now only unregister their snapshot when the write set is empty, removing a timestamp increment and commit-log operation. | Successful transactions no longer necessarily produce a commit timestamp; the existing `Option<Version>` APIs return `None`. |
| O(1) Zipf sampling | The prior distribution performed floating-point power work in each timed request. | A Walker alias table performs powers once during setup; sampling uses two random draws and lookups. `f32` probabilities plus `u32` aliases cost 8 bytes/key (about 160 MB at 20M keys). | Setup and memory are O(n), sample sequences change, and `f32` quantizes probabilities slightly. Statistical tests cover domain, skew, and theta-zero uniformity. |
| Sample YCSB-E measurement work | Timing every scan and reading the clock every operation distorted the measured workload. | One in 1,024 scans receives a latency timestamp; all scans and tuples are counted. The timeseries clock refreshes every 256 operations. YCSB-E improved from 4.59M to 6.22M ops/s (+35.4%) for the combined batch. | Latency CSV `count` is samples, not total scans. Percentiles have sampling error; bucket boundaries can lag by at most 255 operations per worker. |
| Specialize full-table scanning | Q1/Q6 scan the complete key space, making per-record range comparisons redundant. | `try_for_each_ref` detects `Key::MIN..=Key::MAX` once and uses a visibility-only leaf loop. Narrow scans retain range-first filtering. | The fast path applies only to the exact full domain; other intervals stay checked. Existing iterator tests cover both. |

## Command-line examples

The YCSB flag is the final positional value; use `true` for historical
full-row update semantics:

```text
batstore ycsb a 200000 4 3 default 0.99 10 100 100 fg true false false ycsb_wal.log 5 false
batstore tpcc 1 4 10 false true false fg ch_q1 1
batstore tpcc 1 4 10 false true false fg ch_q6 1
```

## Correctness coverage

Tests verify that `writeallfields=false` changes exactly one field, `true`
creates a fresh complete row, missing point reads remain misses, alias samples
stay in range with the expected skew/uniform behavior, and an empty read-only
commit does not advance the global clock. Existing iterator tests cover full
and bounded scans under versioning.
