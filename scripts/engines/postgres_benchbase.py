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
"""
from __future__ import annotations

from pathlib import Path

from . import common

BENCHBASE_HOME = Path("/home/amir/CLionProjects/benchbase/target/benchbase-postgres")
BENCHBASE_JAR = BENCHBASE_HOME / "benchbase.jar"

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


def run(workload: str, scale: common.Scale, output_dir: Path) -> common.NormalizedResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    results_dir = output_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / "config.xml"

    if workload == "tpcc":
        duration = scale.tpcc_duration
        config_path.write_text(TPCC_CONFIG_TEMPLATE.format(
            warehouses=scale.tpcc_warehouses, terminals=scale.tpcc_terminals, duration=duration,
        ))
        bench_type = "tpcc"
        metric_name = "new_order_per_sec"
    else:
        letter = workload.split("_", 1)[1]
        duration = scale.ycsb_duration
        config_path.write_text(YCSB_CONFIG_TEMPLATE.format(
            scalefactor=scale.ycsb_records / 1000.0, theta=scale.ycsb_theta,
            terminals=scale.ycsb_threads, duration=duration, weights=YCSB_WEIGHTS[letter],
        ))
        bench_type = "ycsb"
        metric_name = "ops_per_sec"

    args = [
        "java", "-Duser.language=en", "-Duser.country=US", "-jar", str(BENCHBASE_JAR),
        "-b", bench_type, "-c", str(config_path),
        "--create=true", "--load=true", "--execute=true",
        "-d", str(results_dir),
    ]
    returncode, _client_rss_unused = common.run_and_track_rss(
        args, cwd=BENCHBASE_HOME, stdout_path=output_dir / "stdout.log",
    )
    if returncode != 0:
        return common.NormalizedResult(
            "postgres", workload, scale.label, duration, metric_name, 0.0, 0.0,
            notes=f"FAILED exit={returncode}, see stdout.log",
        )

    if workload == "tpcc":
        # New-Order-only, for tpmC parity with the other 3 engines (see leanstore.py/wiredtiger.py).
        results_csv = next(results_dir.glob("*.results.NewOrder.csv"), None)
    else:
        # The aggregate file (all transaction types) - "*.results.csv" doesn't match the
        # per-type "*.results.ReadRecord.csv" etc. siblings BenchBase also writes.
        results_csv = next(results_dir.glob("*.results.csv"), None)

    value = common.avg_csv_column(results_csv, "Throughput (requests/second)") if results_csv else 0.0

    return common.NormalizedResult(
        "postgres", workload, scale.label, duration, metric_name, value, 0.0,
        notes="peak_rss not tracked (multi-process Postgres server, see module docstring)",
    )
