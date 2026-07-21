"""PostgreSQL engine wrapper - drives BenchBase (github.com/cmu-db/benchbase),
the same JDBC-based tool the paper itself used for its PostgreSQL comparison
(LeanStore's own repo has no working Postgres integration - see the plan doc).

Requires: local PostgreSQL server, an `admin`/`password` superuser role, and a
`benchbase` database (see the plan's setup notes) - `--create=true` recreates
the benchmark's own tables against that database on every run.

Note: peak_rss_mb is intentionally left at 0 here. Unlike the other three
engines (where the wrapped process *is* the storage engine), BenchBase is
only the JDBC client; the actual engine is Postgres's multi-process server
cluster (postmaster + per-connection backends), and there is no single PID
whose RSS is a fair analogue to the other engines' single-process peak RSS.
Tracking the Java client's own memory would be a different, misleading
number, so it's left unmeasured rather than reported inaccurately.

Not NUMA-pinned: the actual engine here is the PostgreSQL *server* process
(postmaster + backends), which is a pre-existing system service this harness
doesn't spawn - only the JDBC client below runs under numactl (via
common.run_and_track_rss). Binding the server itself to one NUMA node would
need a systemd override, outside this script's scope.
"""
from __future__ import annotations

import csv
import os
import subprocess
from pathlib import Path

from . import common

BENCHBASE_HOME = common.BENCHBASE_HOME
BENCHBASE_JAR = BENCHBASE_HOME / "benchbase.jar"

# Postgres has no literal "GC" flag; autovacuum (which cleans up dead/old row versions) is
# the closest real, standard analog, toggled without a restart via ALTER SYSTEM + reload.
SUPPORTS_GC_TOGGLE = True

# BenchBase's own transaction-type order for YCSB (config/postgres/sample_ycsb_config.xml):
# ReadRecord, InsertRecord, ScanRecord, UpdateRecord, DeleteRecord, ReadModifyWriteRecord.
# D approximates YCSB's "latest" distribution with BenchBase's zipfian skewFactor (no separate
# recency-biased generator available) - the same documented approximation as the LeanStore side.
YCSB_WEIGHTS = {
    "a": "50,0,0,50,0,0",
    "b": "95,0,0,5,0,0",
    "c": "100,0,0,0,0,0",
    "d": "95,5,0,0,0,0",
    "e": "0,5,95,0,0,0",
    "f": "50,0,0,0,0,50",
}

TPCC_CONFIG_TEMPLATE = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:5432/benchbase?sslmode=disable&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true</url>
    <username>admin</username>
    <password>password</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_REPEATABLE_READ</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <rate>10000</rate>
            <weights>45,43,4,4,4</weights>
        </work>
    </works>
    <transactiontypes>
        <transactiontype><name>NewOrder</name></transactiontype>
        <transactiontype><name>Payment</name></transactiontype>
        <transactiontype><name>OrderStatus</name></transactiontype>
        <transactiontype><name>Delivery</name></transactiontype>
        <transactiontype><name>StockLevel</name></transactiontype>
    </transactiontypes>
</parameters>
"""

# BenchBase's chbenchmark plugin mixes a TPC-C work phase with the standard CH-benCHmark
# Q1-Q22 analytical queries (real SQL - see src/main/java/.../chbenchmark/queries/Q*.java);
# here only Q1/Q6 ever get nonzero weight, matching HTAP_WORKLOADS (common.py) and the
# LeanStore/WiredTiger/cMVBT side of this same restriction.
CHBENCHMARK_WEIGHTS = {
    "htap_q1": "100," + ",".join(["0"] * 21),
    "htap_q6": ",".join(["0"] * 5) + ",100," + ",".join(["0"] * 16),
}

CHBENCHMARK_CONFIG_TEMPLATE = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:5432/benchbase?sslmode=disable&amp;ApplicationName=chbenchmark&amp;reWriteBatchedInserts=true</url>
    <username>admin</username>
    <password>password</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_SERIALIZABLE</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <!-- Default (no @bench) applies to tpcc; chbenchmark gets its own fixed 1 dedicated
         analytics terminal, matching the "N OLTP threads + 1 OLAP thread" convention used
         for cMVBT/LeanStore/WiredTiger's htap_q1/htap_q6 (see their engines/*.py). -->
    <terminals>{terminals}</terminals>
    <terminals bench="chbenchmark">1</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <rate>10000</rate>
            <weights bench="tpcc">45,43,4,4,4</weights>
            <weights bench="chbenchmark">{ch_weights}</weights>
        </work>
    </works>
    <transactiontypes bench="chbenchmark">
        <transactiontype><name>Q1</name></transactiontype>
        <transactiontype><name>Q2</name></transactiontype>
        <transactiontype><name>Q3</name></transactiontype>
        <transactiontype><name>Q4</name></transactiontype>
        <transactiontype><name>Q5</name></transactiontype>
        <transactiontype><name>Q6</name></transactiontype>
        <transactiontype><name>Q7</name></transactiontype>
        <transactiontype><name>Q8</name></transactiontype>
        <transactiontype><name>Q9</name></transactiontype>
        <transactiontype><name>Q10</name></transactiontype>
        <transactiontype><name>Q11</name></transactiontype>
        <transactiontype><name>Q12</name></transactiontype>
        <transactiontype><name>Q13</name></transactiontype>
        <transactiontype><name>Q14</name></transactiontype>
        <transactiontype><name>Q15</name></transactiontype>
        <transactiontype><name>Q16</name></transactiontype>
        <transactiontype><name>Q17</name></transactiontype>
        <transactiontype><name>Q18</name></transactiontype>
        <transactiontype><name>Q19</name></transactiontype>
        <transactiontype><name>Q20</name></transactiontype>
        <transactiontype><name>Q21</name></transactiontype>
        <transactiontype><name>Q22</name></transactiontype>
    </transactiontypes>
    <transactiontypes bench="tpcc">
        <transactiontype><name>NewOrder</name></transactiontype>
        <transactiontype><name>Payment</name></transactiontype>
        <transactiontype><name>OrderStatus</name></transactiontype>
        <transactiontype><name>Delivery</name></transactiontype>
        <transactiontype><name>StockLevel</name></transactiontype>
    </transactiontypes>
</parameters>
"""

YCSB_CONFIG_TEMPLATE = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:5432/benchbase?sslmode=disable&amp;ApplicationName=ycsb&amp;reWriteBatchedInserts=true</url>
    <username>admin</username>
    <password>password</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_REPEATABLE_READ</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{scalefactor}</scalefactor>
    <skewFactor>{theta}</skewFactor>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <rate>10000</rate>
            <weights>{weights}</weights>
        </work>
    </works>
    <transactiontypes>
        <transactiontype><name>ReadRecord</name></transactiontype>
        <transactiontype><name>InsertRecord</name></transactiontype>
        <transactiontype><name>ScanRecord</name></transactiontype>
        <transactiontype><name>UpdateRecord</name></transactiontype>
        <transactiontype><name>DeleteRecord</name></transactiontype>
        <transactiontype><name>ReadModifyWriteRecord</name></transactiontype>
    </transactiontypes>
</parameters>
"""


def _set_autovacuum(enabled: bool) -> None:
    """ALTER SYSTEM + reload takes effect immediately, no server restart needed. Runs
    directly (not through common.run_and_track_rss) - this is a tiny admin statement, not
    part of the measured workload, and shouldn't be numactl-wrapped or RSS-sampled."""
    env = os.environ.copy()
    env["PGPASSWORD"] = common.PG_PASSWORD
    value = "on" if enabled else "off"
    subprocess.run(
        ["psql", "-U", common.PG_ROLE, "-h", "localhost", "-d", common.PG_DATABASE,
         "-c", f"ALTER SYSTEM SET autovacuum = {value};", "-c", "SELECT pg_reload_conf();"],
        env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )


def _latency_from_results(results_dir: Path, tx_type_name: str) -> dict:
    """Reads BenchBase's per-transaction-type results CSV (same file pattern already used
    below for TPC-C's NewOrder-only metric - `tx_type_name` is e.g. "ScanRecord", "Q1",
    "Q6") for its periodic-window latency percentile columns (milliseconds - BenchBase
    doesn't expose raw per-op samples, only these windowed summaries), converts to
    microseconds, and averages across windows.
    """
    path = next(results_dir.glob(f"*.results.{tx_type_name}.csv"), None)
    if not path:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}

    def avg_ms(col: str) -> float:
        vals = [float(r[col]) for r in rows if r.get(col)]
        return sum(vals) / len(vals) if vals else 0.0

    times = [float(r["Time (seconds)"]) for r in rows if r.get("Time (seconds)")]
    window_secs = (times[1] - times[0]) if len(times) >= 2 else (times[0] if times else 0.0)
    total_count = sum(
        float(r["Throughput (requests/second)"]) * window_secs
        for r in rows if r.get("Throughput (requests/second)")
    )
    return {
        "p50": avg_ms("Median Latency (millisecond)") * 1000.0,
        "p95": avg_ms("95th Percentile Latency (millisecond)") * 1000.0,
        "p99": avg_ms("99th Percentile Latency (millisecond)") * 1000.0,
        "count": round(total_count),
    }


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "on", reload: bool = True) -> common.NormalizedResult:
    """`reload=False` skips BenchBase's (expensive) --create/--load steps and reuses
    whatever data a prior call already loaded into the `benchbase` database for this
    workload - safe as long as `scale`'s data-volume fields (warehouses/records) are
    unchanged from that prior call, which compare_engines.py's thread/gc sweep guarantees
    (only terminals/autovacuum vary within one workload's sweep).
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    results_dir = output_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / "config.xml"

    if workload == "tpcc":
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        config_path.write_text(TPCC_CONFIG_TEMPLATE.format(
            warehouses=scale.tpcc_warehouses, terminals=threads, duration=duration,
        ))
        bench_type = "tpcc"
        metric_name = "new_order_per_sec"
    elif workload in ("htap_q1", "htap_q6"):
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        config_path.write_text(CHBENCHMARK_CONFIG_TEMPLATE.format(
            warehouses=scale.tpcc_warehouses, terminals=threads, duration=duration,
            ch_weights=CHBENCHMARK_WEIGHTS[workload],
        ))
        bench_type = "tpcc,chbenchmark"
        metric_name = "new_order_per_sec"
    else:
        letter = workload.split("_", 1)[1]
        duration = scale.ycsb_duration
        threads = scale.ycsb_threads
        config_path.write_text(YCSB_CONFIG_TEMPLATE.format(
            scalefactor=scale.ycsb_records / 1000.0, theta=scale.ycsb_theta,
            terminals=threads, duration=duration, weights=YCSB_WEIGHTS[letter],
        ))
        bench_type = "ycsb"
        metric_name = "ops_per_sec"

    _set_autovacuum(gc != "off")

    create_load = ["--create=true", "--load=true"] if reload else ["--create=false", "--load=false"]
    args = [
        "java", "-Duser.language=en", "-Duser.country=US", "-jar", str(BENCHBASE_JAR),
        "-b", bench_type, "-c", str(config_path),
        *create_load, "--execute=true",
        "-d", str(results_dir),
    ]
    returncode, _client_rss_unused = common.run_and_track_rss(
        args, cwd=BENCHBASE_HOME, stdout_path=output_dir / "stdout.log",
    )
    if returncode != 0:
        return common.NormalizedResult(
            "postgres", workload, scale.label, duration, metric_name, 0.0, 0.0,
            threads=threads, gc_enabled=gc,
            notes=f"FAILED exit={returncode}, see stdout.log",
        )

    if workload in ("tpcc", "htap_q1", "htap_q6"):
        # New-Order-only, for tpmC parity with the other 3 engines (see leanstore.py/wiredtiger.py) -
        # also the OLTP-side metric for htap_q1/htap_q6, directly comparable to plain "tpcc"
        # at the same threads/gc for an interference% computation (see plot_compare.py).
        results_csv = next(results_dir.glob("*.results.NewOrder.csv"), None)
    else:
        # The aggregate file (all transaction types) - "*.results.csv" doesn't match the
        # per-type "*.results.ReadRecord.csv" etc. siblings BenchBase also writes.
        results_csv = next(results_dir.glob("*.results.csv"), None)

    value = common.avg_csv_column(results_csv, "Throughput (requests/second)") if results_csv else 0.0

    latency = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "count": 0}
    if workload == "ycsb_e":
        latency = _latency_from_results(results_dir, "ScanRecord")
    elif workload in ("htap_q1", "htap_q6"):
        latency = _latency_from_results(results_dir, "Q1" if workload == "htap_q1" else "Q6")

    return common.NormalizedResult(
        "postgres", workload, scale.label, duration, metric_name, value, 0.0,
        threads=threads, gc_enabled=gc,
        scan_p50_us=latency["p50"], scan_p95_us=latency["p95"], scan_p99_us=latency["p99"],
        scan_count=latency["count"],
        notes="peak_rss not tracked (multi-process Postgres server, see module docstring)",
    )
