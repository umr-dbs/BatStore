"""Harness regression tests; no long-running benchmark processes."""
import csv
import importlib
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from engines import common


class HypothesesTests(unittest.TestCase):
    def exercise(self, number, options):
        module = importlib.import_module(f"h{number}")
        calls = []

        def run(workload, scale, out_dir, **kwargs):
            calls.append((workload, scale))
            out_dir.mkdir(parents=True, exist_ok=True)
            if number in (1, 2):
                (out_dir / "ycsb_operation_latency_summary.csv").write_text(
                    "operation,p50_us,p95_us,p99_us,avg_us,count\n"
                    "read,1,2,3,1.5,10\n"
                    "update,2,3,4,2.5,10\n"
                )
            if number == 6:
                for name, counts in [("gc_stats_after_load.csv", "10,2,100"),
                                     ("gc_stats.csv", "100,3,105")]:
                    (out_dir / name).write_text(
                        "schema_version,shard,local_reuse,steal,fresh_alloc\n2,0," + counts + "\n"
                    )
            return common.NormalizedResult("batstore", workload, scale.label, 1,
                                           "ops_sec", 100.0, 1.0,
                                           scan_p50_us=1.0, scan_p95_us=2.0,
                                           scan_p99_us=3.0, scan_count=10)

        with tempfile.TemporaryDirectory() as tmp:
            argv = [f"h{number}.py", "--skip-build", "--output-root", tmp, *options]
            with patch.object(sys, "argv", argv), patch.object(module.batstore, "run", run), patch.dict(os.environ):
                module.main()
            self.assertTrue(list(Path(tmp).rglob("*.png")))
            if number == 5:
                with next(Path(tmp).rglob("h5_summary.csv")).open() as f:
                    rows = list(csv.DictReader(f))
                self.assertEqual([int(row["oltp_terminals"]) for row in rows], [1, 4])
                self.assertEqual({int(row["fixed_olap_threads"]) for row in rows}, {2})
                self.assertEqual({float(row["new_order_per_sec"]) for row in rows}, {100.0})
            if number == 6:
                with next(Path(tmp).rglob("h6_gc_stats.csv")).open() as f:
                    row = next(csv.DictReader(f))
                self.assertEqual(int(row["fresh_alloc"]), 5)
                self.assertEqual(int(row["local_reuse"]), 90)
        return calls

    def test_h1_modes_and_threads(self):
        calls = self.exercise(1, ["--threads", "1,4", "--workloads", "ycsb_a"])
        # H1 pairs both modes at each thread count and reverses their order at alternate
        # points to avoid a systematic run-order bias.
        self.assertEqual([s.ycsb_threads for _, s in calls], [1,1,4,4])

    def test_h2_skew_cross_thread_matrix(self):
        calls = self.exercise(2, ["--threads", "1,4", "--skews", "uniform,0.99"])
        self.assertEqual([(s.ycsb_threads, s.ycsb_theta) for _, s in calls],
                         [(1,0), (1,0.99), (4,0), (4,0.99)])

    def test_h4_only_analytical_sweep_adds_baseline(self):
        calls = self.exercise(4, ["--olap-threads", "1,4", "--fixed-oltp-terminals", "3"])
        self.assertEqual([(s.tpcc_terminals, s.htap_olap_threads) for _, s in calls],
                         [(3,0), (3,1), (3,4)])

    def test_h5_only_transactional_sweep(self):
        calls = self.exercise(5, ["--oltp-terminals", "1,4", "--fixed-olap-threads", "2"])
        self.assertEqual([(s.tpcc_terminals, s.htap_olap_threads) for _, s in calls],
                         [(1,2), (4,2)])

    def test_h6_excludes_load(self):
        self.exercise(6, ["--threads", "1,4"])

    def test_h3_buckets_by_recorded_snapshot_age(self):
        from h3 import read_historic_scan_rows, bucket_rows, build_args
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "tpcc_scan.csv"
            path.write_text("mode,elapsed_secs,delay_secs,snapshot,scanned_tuples,latency_ns,tuples_per_sec\n"
                            "historic_full_scan,100,9,42,100,2000000000,50\n"
                            "historic_full_scan,101,10,42:99:,100,1000000000,100\n")
            rows = read_historic_scan_rows(path)
        args = build_args(1, 2, 600, 1, Path("wal.log"))
        self.assertEqual(args[9], "historic")
        self.assertEqual(args[6], "false")
        self.assertEqual(rows[0]["snapshot"], 42)
        self.assertEqual(rows[1]["snapshot"], "42:99:")
        self.assertEqual(rows[0]["elapsed_secs"], 9)
        self.assertEqual(bucket_rows(rows, 20, 2)[0]["window_start"], 0)

    def test_clamped_run_rejected(self):
        from hypothesis_common import check_worker_log
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)
            (path / "stdout.log").write_text("workers > max_workers; clamping.")
            with self.assertRaises(SystemExit):
                check_worker_log(path)


if __name__ == "__main__":
    unittest.main()
