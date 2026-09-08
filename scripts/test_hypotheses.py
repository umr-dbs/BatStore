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
            if number == 6:
                for name, counts in [("gc_stats_after_load.csv", "10,2,100"),
                                     ("gc_stats.csv", "100,3,105")]:
                    (out_dir / name).write_text("shard,local_reuse,steal,fresh_alloc\n0," + counts + "\n")
            return common.NormalizedResult("batstore", workload, scale.label, 1,
                                           "ops_sec", 100.0, 1.0)

        with tempfile.TemporaryDirectory() as tmp:
            argv = [f"h{number}.py", "--skip-build", "--output-root", tmp, *options]
            with patch.object(sys, "argv", argv), patch.object(module.batstore, "run", run), patch.dict(os.environ):
                module.main()
            self.assertTrue(list(Path(tmp).rglob("*.png")))
            if number == 6:
                with next(Path(tmp).rglob("h6_gc_stats.csv")).open() as f:
                    row = next(csv.DictReader(f))
                self.assertEqual(int(row["fresh_alloc"]), 5)
                self.assertEqual(int(row["local_reuse"]), 90)
        return calls

    def test_h1_modes_and_threads(self):
        calls = self.exercise(1, ["--threads", "1,4", "--workloads", "ycsb_a"])
        self.assertEqual([s.ycsb_threads for _, s in calls], [1,4,1,4])

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

    def test_h3_buckets_by_scan_start(self):
        from h3 import read_fresh_scan_rows, bucket_rows
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "tpcc_scan.csv"
            path.write_text("mode,elapsed_secs,scanned_tuples,latency_ns,tuples_per_sec\n"
                            "fresh_full_scan,11,100,2000000000,50\n")
            rows = read_fresh_scan_rows(path)
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
