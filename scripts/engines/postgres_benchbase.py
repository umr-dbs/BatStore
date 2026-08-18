"""PostgreSQL engine wrapper - drives BenchBase (github.com/cmu-db/benchbase),
the same JDBC-based tool the paper itself used for its PostgreSQL comparison
(LeanStore's own repo has no working Postgres integration - see the plan doc).

Requires: local PostgreSQL server, an `admin`/`password` superuser role, and a
`benchbase` database (see the plan's setup notes) - `--create=true` recreates
the benchmark's own tables against that database on every run.

Note: peak_rss_mb reports the PostgreSQL SERVER's memory, not the JDBC client's (those
would be a different, misleading number - the client is just driving requests, not
storing anything). Unlike the other three engines (a single process IS the storage
engine), Postgres's engine is a whole process tree (postmaster + checkpointer + bgwriter +
walwriter + one backend per connection) - see common.py's start_process_tree_sampler,
which sums current RSS across that whole tree and tracks its peak, the cross-process
analogue of the other engines' single-PID VmHWM sampling.

NUMA-pinned: setup_environment.py constrains the actual PostgreSQL cluster service's
cgroup to node 0's CPUs and memory nodes. This wrapper verifies the live postmaster's
effective masks before every run; the JDBC client is independently pinned by
common.run_and_track_rss.
"""
from __future__ import annotations

import csv
import os
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Optional
from urllib.parse import quote
from xml.sax.saxutils import escape

from . import common

BENCHBASE_HOME = common.BENCHBASE_HOME
BENCHBASE_JAR = BENCHBASE_HOME / "benchbase.jar"

def ensure_built() -> None:
    repo = BENCHBASE_HOME.parent.parent
    subprocess.run(["mvn", "package", "-P", "postgres", "-DskipTests",
                    "-Dmaven.compiler.release=21"], cwd=repo, check=True)
    tgz = repo / "target" / "benchbase-postgres.tgz"
    subprocess.run(["tar", "xzf", str(tgz), "-C", str(repo / "target")], check=True)

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
    <url>jdbc:postgresql://localhost:5432/{database}?sslmode=disable&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true</url>
    <username>{username}</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_REPEATABLE_READ</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <!-- Measure saturation throughput; a numeric rate is a global BenchBase
                 client-side request throttle and would flatten every thread sweep. -->
            <rate>unlimited</rate>
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
# LeanStore/WiredTiger/BatStore side of this same restriction.
CHBENCHMARK_WEIGHTS = {
    "htap_q1": "100," + ",".join(["0"] * 21),
    "htap_q6": ",".join(["0"] * 5) + ",100," + ",".join(["0"] * 16),
}

CHBENCHMARK_CONFIG_TEMPLATE = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:5432/{database}?sslmode=disable&amp;ApplicationName=chbenchmark&amp;reWriteBatchedInserts=true</url>
    <username>{username}</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_SERIALIZABLE</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <!-- Default (no @bench) applies to tpcc; chbenchmark gets its own fixed 1 dedicated
         analytics terminal, matching the "N OLTP threads + 1 OLAP thread" convention used
         for BatStore/LeanStore/WiredTiger's htap_q1/htap_q6 (see their engines/*.py). -->
    <terminals>{terminals}</terminals>
    <terminals bench="chbenchmark">1</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <!-- Neither the OLTP nor analytical client should impose a throughput cap. -->
            <rate>unlimited</rate>
            <rate bench="chbenchmark">unlimited</rate>
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
    <url>jdbc:postgresql://localhost:5432/{database}?sslmode=disable&amp;ApplicationName=ycsb&amp;reWriteBatchedInserts=true</url>
    <username>{username}</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_REPEATABLE_READ</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{scalefactor}</scalefactor>
    <skewFactor>{theta}</skewFactor>
    <fieldSize>{field_size}</fieldSize>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <!-- Measure saturation throughput rather than BenchBase's sample-config
                 default ceiling of 10,000 requests/second. -->
            <rate>unlimited</rate>
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


SHTAP_CONFIG_TEMPLATE = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://localhost:5432/{database}?sslmode=disable&amp;ApplicationName=s_htap&amp;reWriteBatchedInserts=true</url>
    <username>{username}</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_REPEATABLE_READ</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{scalefactor}</scalefactor>
    <hotWindow>{hot_window}</hotWindow>
    <hotTheta>{hot_theta}</hotTheta>
    <arrivalRatio>{arrival_ratio}</arrivalRatio>
    <maxLateness>{max_lateness}</maxLateness>
    <olapThreads>{olap_threads}</olapThreads>
    <olapLag>{olap_lag}</olapLag>
    <olapSpan>{olap_span}</olapSpan>
    <terminals>{terminals}</terminals>
    <works>
        <work>
            <time>{duration}</time>
            <!-- Measure saturation throughput; a numeric rate is a global BenchBase
                 client-side request throttle and would flatten every thread sweep. -->
            <rate>unlimited</rate>
            <!-- These weights only matter for BenchBase's own bookkeeping (its per-
                 transaction-type latency CSVs bucket by whichever type its global weighted
                 dispatch assigns) - which SQL actually runs is decided by SHTAPWorker itself
                 from its fixed write/OLAP terminal-id role (see that class's doc), not by
                 this distribution. Proportioned to the write/OLAP terminal split (and,
                 within the write share, to arrivalRatio) purely to keep that bookkeeping
                 close to reality. -->
            <weights>{weights}</weights>
        </work>
    </works>
    <transactiontypes>
        <transactiontype><name>ArrivalUpsert</name></transactiontype>
        <transactiontype><name>HotTailUpdate</name></transactiontype>
        <transactiontype><name>OlapScan</name></transactiontype>
    </transactiontypes>
</parameters>
"""


def _find_postmaster_pid() -> Optional[int]:
    """The oldest process matching the postmaster's own invocation - `-o` asks pgrep for
    the single oldest match, which is the postmaster itself (every backend/checkpointer/
    etc. process is younger and forked from it, so this is stable even with active
    connections)."""
    result = subprocess.run(
        ["pgrep", "-o", "-f", "postgres -D"], capture_output=True, text=True,
    )
    try:
        return int(result.stdout.strip().splitlines()[0])
    except (ValueError, IndexError):
        return None


def _expand_cpu_list(value: str) -> set[int]:
    cpus: set[int] = set()
    for part in value.strip().split(","):
        if not part:
            continue
        bounds = part.split("-", 1)
        start = int(bounds[0])
        end = int(bounds[1]) if len(bounds) == 2 else start
        cpus.update(range(start, end + 1))
    return cpus


def _verify_postmaster_numa_binding() -> int:
    """Return the postmaster PID, refusing a comparison unless its live effective CPU
    and memory-node masks are exactly the node used for every embedded engine."""
    pid = _find_postmaster_pid()
    if pid is None:
        sys.exit("cannot locate the PostgreSQL postmaster; is the cluster running?")
    status: dict[str, str] = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            status[key] = value.strip()
    expected_cpus = _expand_cpu_list(common.numa_node_cpu_list())
    actual_cpus = _expand_cpu_list(status.get("Cpus_allowed_list", ""))
    actual_nodes = _expand_cpu_list(status.get("Mems_allowed_list", ""))
    if actual_cpus != expected_cpus or actual_nodes != {common.NUMA_NODE}:
        sys.exit(
            f"PostgreSQL postmaster PID {pid} is not pinned to NUMA node {common.NUMA_NODE}: "
            f"Cpus_allowed_list={status.get('Cpus_allowed_list')!r}, "
            f"Mems_allowed_list={status.get('Mems_allowed_list')!r}; expected CPUs "
            f"{common.numa_node_cpu_list()!r}, memory node {common.NUMA_NODE}. Run "
            f"`python3 scripts/setup_environment.py --reuse-checkouts` to configure the "
            f"cluster service cgroup."
        )
    return pid


def _template_connection_values() -> dict[str, str]:
    """BenchBase XML values matching common's environment-overridable connection.

    The database is a JDBC URL path component; credentials are XML text. Keeping these
    transformations here avoids both malformed generated configs and silently connecting
    as the old hardcoded admin/password user when PG_* overrides are set.
    """
    return {
        "database": quote(common.PG_DATABASE, safe=""),
        "username": escape(common.PG_ROLE),
        "password": escape(common.PG_PASSWORD),
    }


def _assert_unlimited_config(config_path: Path) -> None:
    """Refuse to execute a stale/generated BenchBase config with a numeric rate cap."""
    rates = [((node.text or "").strip(), node.attrib.get("bench", "default"))
             for node in ET.parse(config_path).getroot().findall(".//rate")]
    capped = [(rate, bench) for rate, bench in rates if rate != "unlimited"]
    if not rates or capped:
        raise RuntimeError(
            f"generated BenchBase config is not unlimited-rate: {capped or rates}; "
            f"config={config_path}. Ensure the server is running this updated "
            f"{Path(__file__).resolve()} rather than an older checkout."
        )


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


def _psql_scalar(sql: str) -> str:
    env = os.environ.copy()
    env["PGPASSWORD"] = common.PG_PASSWORD
    result = subprocess.run(
        ["psql", "-U", common.PG_ROLE, "-h", "localhost", "-d", common.PG_DATABASE, "-tAc", sql],
        env=env, capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def _verify_tmpfs_datadir() -> None:
    """Every other engine's on-disk data lives under common.fresh_scratch_dir, which fails
    loudly if SCRATCH_ROOT isn't tmpfs. PostgreSQL is a pre-existing system service this
    harness doesn't spawn, so it can't go through fresh_scratch_dir - its in-memory-only
    guarantee instead depends on setup_environment.py's default PostgreSQL tmpfs step
    having been run (and tmpfs surviving since, which a reboot would undo). Without an
    equivalent check here, a stale/never-run tmpfs setup would silently benchmark against
    real disk while every other engine's numbers are RAM-only - so ask the live server for
    its actual data_directory and check its mount, same "fail loudly, not silently" rule as
    common.fresh_scratch_dir.
    """
    data_dir = Path(_psql_scalar("SHOW data_directory;"))
    fstype = common._mount_fstype(data_dir)
    if fstype != "tmpfs":
        sys.exit(
            f"PostgreSQL's data_directory ({data_dir}) is not tmpfs-backed (fstype={fstype!r}) - "
            f"refusing to run, since every other engine in this harness is guaranteed "
            f"in-memory-only (see common.fresh_scratch_dir). Run "
            f"`python scripts/setup_environment.py --reuse-checkouts` first (tmpfs doesn't "
            f"survive a reboot, so this can go stale)."
        )


def _set_unsafe_durability() -> None:
    """Matches every other engine's benchmark-only durability posture (see the libmdbx
    UtterlyNoSync/WriteMap fix and LeanStore's wal_pwrite=false/wal_fsync=false): none of
    these settings should be reintroduced accidentally by whatever the OS package's default
    postgresql.conf happens to ship. `synchronous_commit=off` stops commits blocking on WAL
    flush, `fsync=off` stops WAL/data file writes from calling fsync at all, and
    `full_page_writes=off` skips the extra page image written on first modification after a
    checkpoint - all three are PGC_SIGHUP (take effect on pg_reload_conf(), no restart
    needed), same mechanism as the existing autovacuum toggle below. Safe here because
    PGDATA is tmpfs-backed (_verify_tmpfs_datadir already refused to run otherwise), and
    compare_engines recreates the benchmark tables for every point - never do this against
    a real database.
    """
    env = os.environ.copy()
    env["PGPASSWORD"] = common.PG_PASSWORD
    subprocess.run(
        ["psql", "-U", common.PG_ROLE, "-h", "localhost", "-d", common.PG_DATABASE,
         "-c", "ALTER SYSTEM SET synchronous_commit = off;",
         "-c", "ALTER SYSTEM SET fsync = off;",
         "-c", "ALTER SYSTEM SET full_page_writes = off;",
         "-c", "SELECT pg_reload_conf();"],
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
    empty = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "count": 0}
    if not path:
        return empty
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return empty

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
        "avg": avg_ms("Average Latency (millisecond)") * 1000.0,
        "count": round(total_count),
    }


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "on", reload: bool = True,
        ycsb_payload: str = "standard", read_payload: bool = True) -> common.NormalizedResult:
    """`reload=False` can skip BenchBase's expensive --create/--load steps for direct
    callers. compare_engines.py deliberately passes reload=True for every PostgreSQL point
    so every measurement starts from freshly created and loaded benchmark tables.
    """
    postmaster_pid = _verify_postmaster_numa_binding()
    _verify_tmpfs_datadir()
    _set_unsafe_durability()

    output_dir.mkdir(parents=True, exist_ok=True)
    results_dir = output_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / "config.xml"

    if workload == "tpcc":
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        config_path.write_text(TPCC_CONFIG_TEMPLATE.format(
            warehouses=scale.tpcc_warehouses, terminals=threads, duration=duration,
            **_template_connection_values(),
        ))
        bench_type = "tpcc"
        metric_name = "new_order_per_sec"
    elif workload in ("htap_q1", "htap_q6"):
        duration = scale.tpcc_duration
        threads = scale.tpcc_terminals
        config_path.write_text(CHBENCHMARK_CONFIG_TEMPLATE.format(
            warehouses=scale.tpcc_warehouses, terminals=threads, duration=duration,
            ch_weights=CHBENCHMARK_WEIGHTS[workload],
            **_template_connection_values(),
        ))
        bench_type = "tpcc,chbenchmark"
        metric_name = "new_order_per_sec"
    elif workload == "s_htap":
        duration = scale.s_htap_duration
        # scale.ycsb_threads holds the swept --threads value for every workload (see
        # compare_engines.py's scale_variant construction) - split it into a fixed OLAP-scanner
        # pool plus the remainder as write threads, matching batstore.py's "s_htap" branch exactly
        # (SHTAPBenchmark.makeWorkersImpl on the Java side re-derives the same split from
        # olapThreads/terminals independently, so this is only needed here for the <weights>
        # bookkeeping below, not to tell the JVM anything it couldn't figure out itself).
        threads = scale.ycsb_threads
        olap_threads = min(scale.s_htap_olap_threads, max(1, threads - 1))
        write_threads = max(1, threads - olap_threads)
        write_share = write_threads / threads * 100.0
        olap_share = olap_threads / threads * 100.0
        weights = (
            f"{write_share * scale.s_htap_arrival_ratio:.3f},"
            f"{write_share * (1.0 - scale.s_htap_arrival_ratio):.3f},"
            f"{olap_share:.3f}"
        )
        config_path.write_text(SHTAP_CONFIG_TEMPLATE.format(
            scalefactor=scale.s_htap_record_count / 1000.0,
            terminals=threads, duration=duration, weights=weights,
            hot_window=scale.s_htap_hot_window, hot_theta=scale.s_htap_theta,
            arrival_ratio=scale.s_htap_arrival_ratio, max_lateness=scale.s_htap_max_lateness,
            olap_threads=scale.s_htap_olap_threads, olap_lag=scale.s_htap_olap_lag,
            olap_span=scale.s_htap_olap_span,
            **_template_connection_values(),
        ))
        bench_type = "s_htap"
        metric_name = "write_ops_per_sec"
    else:
        letter = workload.split("_", 1)[1]
        duration = scale.ycsb_duration
        threads = scale.ycsb_threads
        config_path.write_text(YCSB_CONFIG_TEMPLATE.format(
            scalefactor=scale.ycsb_records / 1000.0, theta=scale.ycsb_theta,
            terminals=threads, duration=duration, weights=YCSB_WEIGHTS[letter],
            field_size=8 if ycsb_payload == "u64" else 100,
            **_template_connection_values(),
        ))
        bench_type = "ycsb"
        metric_name = "ops_per_sec"

    _assert_unlimited_config(config_path)
    print(f"PostgreSQL BenchBase config: {config_path} (rate=unlimited, terminals={threads})")
    _set_autovacuum(gc != "off")

    tree_sampler = common.start_process_tree_sampler(postmaster_pid)

    create_load = ["--create=true", "--load=true"] if reload else ["--create=false", "--load=false"]
    args = [
        "java", "-Duser.language=en", "-Duser.country=US", "-jar", str(BENCHBASE_JAR),
        "-b", bench_type, "-c", str(config_path),
        *create_load, "--execute=true",
        "-d", str(results_dir),
    ]
    timeout = common.default_subprocess_timeout(duration)
    bench_env = os.environ.copy()
    bench_env["YCSB_READ_PAYLOAD"] = "true" if read_payload else "false"
    bench_env["YCSB_U64_PAYLOAD"] = "true" if ycsb_payload == "u64" else "false"
    returncode, _client_rss_unused = common.run_and_track_rss(
        args, cwd=BENCHBASE_HOME, env=bench_env, stdout_path=output_dir / "stdout.log", timeout=timeout,
    )

    server_peak_rss_mb = 0.0
    if tree_sampler:
        stop, thread, peak_box = tree_sampler
        stop.set()
        thread.join()
        server_peak_rss_mb = peak_box["mb"]

    if returncode != 0:
        notes = f"TIMEOUT after {timeout:.0f}s, see stdout.log" if returncode is None else \
            f"FAILED exit={returncode}, see stdout.log"
        return common.NormalizedResult(
            "postgres", workload, scale.label, duration, metric_name, 0.0, server_peak_rss_mb,
            threads=threads, gc_enabled=gc,
            notes=notes,
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

    latency = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "count": 0}
    if workload == "ycsb_e":
        latency = _latency_from_results(results_dir, "ScanRecord")
    elif workload in ("htap_q1", "htap_q6"):
        latency = _latency_from_results(results_dir, "Q1" if workload == "htap_q1" else "Q6")
    elif workload == "s_htap":
        latency = _latency_from_results(results_dir, "OlapScan")

    return common.NormalizedResult(
        "postgres", workload, scale.label, duration, metric_name, value, server_peak_rss_mb,
        threads=threads, gc_enabled=gc,
        scan_p50_us=latency["p50"], scan_p95_us=latency["p95"], scan_p99_us=latency["p99"],
        scan_avg_us=latency["avg"], scan_count=latency["count"],
        notes="" if postmaster_pid else "peak_rss unavailable (couldn't locate the postmaster PID)",
    )
