"""Regression checks for comparison plots with warehouse-affinity sweeps."""
import unittest
from pathlib import Path
from unittest.mock import patch

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

import plot_compare


class AffinityPlotTests(unittest.TestCase):
    def test_htap_sweep_uses_clean_single_metric_figures(self):
        rows = []
        for engine, scale in (("batstore", 100.0), ("postgres", 1.0)):
            for threads in (1, 4):
                rows.append(dict(
                    engine=engine, workload="htap_q1", threads=threads,
                    affinity="off", gc_enabled="on", failed=False,
                    primary_metric_name="new_order_per_sec",
                    primary_metric_value=scale * threads,
                    duration_secs=60.0, scan_count=120,
                    scan_p50_us=scale * 1_000,
                    scan_p95_us=scale * 2_000,
                    scan_p99_us=scale * 3_000,
                ))
        manifest = pd.DataFrame(rows)
        with patch.object(plot_compare, "_save") as save:
            plot_compare.plot_throughput_vs_threads_htap(manifest, Path("unused"))
            self.assertEqual(save.call_count, 5)
            self.assertEqual(
                [call.args[2] for call in save.call_args_list],
                [
                    "threads_sweep_htap_q1_gc_on",
                    "olap_rate_sweep_htap_q1_gc_on",
                    "latency_sweep_htap_q1_p50_gc_on",
                    "latency_sweep_htap_q1_p95_gc_on",
                    "latency_sweep_htap_q1_p99_gc_on",
                ],
            )
            for call in save.call_args_list:
                fig = call.args[0]
                self.assertEqual(len(fig.axes), 1)
                self.assertEqual(fig.axes[0].get_xlabel(), "OLTP terminals")
                self.assertEqual(fig.axes[0].get_yscale(), "log")
                plt.close(fig)

    def test_affinity_sweep_keeps_distinct_curves_and_shared_baseline(self):
        rows = []
        for threads in (2, 4):
            for affinity, offset in (("on", 100), ("off", 200)):
                rows.append(dict(engine="batstore", workload="tpcc", threads=threads,
                                 affinity=affinity, gc_enabled="on", failed=False,
                                 primary_metric_name="new_order_per_sec",
                                 primary_metric_value=offset + threads))
            rows.append(dict(engine="postgres", workload="tpcc", threads=threads,
                             affinity=None, gc_enabled="n/a", failed=False,
                             primary_metric_name="new_order_per_sec",
                             primary_metric_value=threads))
        manifest = pd.DataFrame(rows)
        manifest["scan_count"] = 1
        manifest["scan_p99_us"] = manifest["primary_metric_value"]
        manifest["peak_rss_mb"] = manifest["primary_metric_value"]
        with patch.object(plot_compare, "_save") as save:
            plot_compare.plot_all_engines_workload_overview(manifest, Path("unused"))
            fig = save.call_args.args[0]
            self.assertEqual(len(fig.axes), 6)
            for column, (affinity, offset) in enumerate((("on", 100), ("off", 200))):
                for row in range(3):
                    ax = fig.axes[row * 2 + column]
                    self.assertEqual(f"TPC-C - Affinity {affinity.title()}", ax.get_title())
                    self.assertEqual(len(ax.lines), 2)
                    self.assertEqual(list(ax.lines[0].get_ydata()), [offset + 2, offset + 4])
                    self.assertEqual(list(ax.lines[1].get_ydata()), [2, 4])
                    for line in ax.lines:
                        self.assertEqual(len(set(line.get_xdata())), len(line.get_xdata()))
            plt.close(fig)
        slices = dict(plot_compare.affinity_slices(manifest))
        self.assertEqual(set(slices), {"on", "off"})
        for affinity, offset in (("on", 100), ("off", 200)):
            subset = slices[affinity]
            with patch.object(plot_compare, "_save") as save:
                plot_compare.plot_throughput_vs_threads_tpcc(subset, Path("unused"))
                fig = save.call_args.args[0]
                lines = fig.axes[0].lines
                self.assertEqual(len(lines), 2)
                self.assertEqual(list(lines[0].get_ydata()), [offset + 2, offset + 4])
                self.assertEqual(list(lines[1].get_ydata()), [2, 4])
                for line in lines:
                    self.assertEqual(len(set(line.get_xdata())), len(line.get_xdata()))
                plt.close(fig)
            reference, threads = plot_compare.pick_reference_slice(subset, "on")
            self.assertEqual(threads, 4)
            self.assertFalse(reference.duplicated(["engine", "workload"]).any())

    def test_legacy_and_non_toggle_manifests_keep_original_layout(self):
        for frame in (pd.DataFrame({"engine": ["batstore"]}),
                      pd.DataFrame({"engine": ["postgres"], "affinity": [None]})):
            slices = list(plot_compare.affinity_slices(frame))
            self.assertEqual(len(slices), 1)
            self.assertIsNone(slices[0][0])
            pd.testing.assert_frame_equal(slices[0][1], frame)


if __name__ == "__main__":
    unittest.main()
