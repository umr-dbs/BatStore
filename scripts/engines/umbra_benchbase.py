"""Umbra engine wrapper - drives the SAME BenchBase (github.com/cmu-db/benchbase) JDBC
harness as engines/postgres_benchbase.py, against umbradb/umbra
(https://umbra-db.com, https://hub.docker.com/r/umbradb/umbra) instead of a real
PostgreSQL server. Umbra (TUM's db.in.tum.de research engine) speaks the PostgreSQL wire
protocol closely enough that BenchBase's existing <type>POSTGRES</type> JDBC target works
against it unmodified - confirmed directly (DDL/DML/FK/prepared-statement/batch-insert
smoke test against a live umbra-server) before writing this wrapper; only the connection
target (a different port, so the real PostgreSQL cluster service can keep 5432) differs
from postgres_benchbase.py's templates.

Unlike PostgreSQL (an always-running system service - see postgres_benchbase.py's own
doc) or BatStore/LeanStore/WiredTiger (a single binary this harness launches directly),
Umbra ships ONLY as a Docker image (no native package/tarball - see
setup_environment.py::step_umbra) - this wrapper starts/stops one throwaway container per
run(), the same "fresh every run" lifecycle every other engine's own scratch data gets via
common.fresh_scratch_dir, just at the container level instead of a single process.

Durability: this build of Umbra (v0.2, "internal evaluation" license) exposes NO working
way to relax fsync/synchronous_commit/full_page_writes/autovacuum in software - confirmed
directly: `ALTER SYSTEM SET ...` returns "ALTER SYSTEM not implemented yet", and plain
`SET fsync = ...` etc. return "cannot change configuration parameter" even against a live
server; there is no equivalent command-line flag either (umbra-server --help lists only
-address/-port/-createdb/-readonly/-certFile/-keyFile/-pgSocketDir). So, unlike PostgreSQL
(_set_unsafe_durability below has no Umbra analog):
  - "Memory-only" here means ONLY the tmpfs-backed scratch directory
    (common.fresh_scratch_dir) that every non-PostgreSQL engine already uses - Umbra's own
    fsync calls still happen, they just land on tmpfs (RAM), never real disk.
  - compare_engines_new.py's NO_DURABILITY mode (real disk, relying on each engine's own
    fsync-off) has no safe equivalent for Umbra - see SUPPORTS_GC_TOGGLE's sibling guard
    in run() below, which reports a SKIPPED result instead of silently paying real-disk
    fsync cost under a flag whose whole point is "no durability cost".
  - No working GC/autovacuum toggle either (same "cannot change configuration parameter"
    restriction) - SUPPORTS_GC_TOGGLE = False, matching leanstore.py/wiredtiger.py.

NUMA-pinned via the container's own cgroup (`docker run --cpuset-cpus/--cpuset-mems`,
verified against the live container's /proc/<pid>/status after start - the same "trust
but verify" the PostgreSQL wrapper applies to the postmaster), not via numactl in front of
`docker run` itself: numactl would only pin the short-lived `docker` CLI invocation, not
the actual server process the daemon spawns. The BenchBase JDBC client is independently
pinned to the next NUMA node when available, so its JVM does not consume the database
server's benchmark CPUs.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

from . import common, postgres_benchbase

BENCHBASE_HOME = common.BENCHBASE_HOME
BENCHBASE_JAR = BENCHBASE_HOME / "benchbase.jar"

# Same BenchBase jar/profile as PostgreSQL - Umbra is driven through the identical
# <type>POSTGRES</type> JDBC target (see module doc), so there is no separate Maven build.
ensure_built = postgres_benchbase.ensure_built

# See module doc: confirmed directly against a live umbra-server that neither `ALTER
# SYSTEM` nor plain `SET` can change autovacuum (or any other GUC) in this build.
SUPPORTS_GC_TOGGLE = False

YCSB_WEIGHTS = postgres_benchbase.YCSB_WEIGHTS
CHBENCHMARK_WEIGHTS = postgres_benchbase.CHBENCHMARK_WEIGHTS

TPCC_CONFIG_TEMPLATE = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://{host}:{port}/{database}?sslmode=disable&amp;ApplicationName=tpcc&amp;reWriteBatchedInserts=true&amp;currentSchema=public&amp;preferQueryMode=simple</url>
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

CHBENCHMARK_CONFIG_TEMPLATE = """<?xml version="1.0"?>
<parameters>
    <type>POSTGRES</type>
    <driver>org.postgresql.Driver</driver>
    <url>jdbc:postgresql://{host}:{port}/{database}?sslmode=disable&amp;ApplicationName=chbenchmark&amp;reWriteBatchedInserts=true&amp;currentSchema=public&amp;preferQueryMode=simple</url>
    <username>{username}</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <isolation>TRANSACTION_SERIALIZABLE</isolation>
    <batchsize>128</batchsize>
    <scalefactor>{warehouses}</scalefactor>
    <terminals>{terminals}</terminals>
    <terminals bench="chbenchmark">{olap_threads}</terminals>
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
    <url>jdbc:postgresql://{host}:{port}/{database}?sslmode=disable&amp;ApplicationName=ycsb&amp;reWriteBatchedInserts=true&amp;currentSchema=public&amp;preferQueryMode=simple</url>
    <username>{username}</username>
    <password>{password}</password>
    <reconnectOnConnectionFailure>true</reconnectOnConnectionFailure>
    <!-- Umbra has no "FOR UPDATE" ("ERROR: locking specifier not implemented yet",
         confirmed live) - workload f's ReadModifyWriteRecord would otherwise fail every
         single execution. See patches/umbra_ycsb_rmw_no_lock_benchbase.patch's own
         comment: this falls back to a plain read then a separate unlocked write, trading
         away lost-update protection under concurrent writers to the same key for a
         throughput number that isn't simply zero. Every other engine leaves this at
         BenchBase's own default (true, the real locking "workload F"). -->
    <selectForUpdate>false</selectForUpdate>
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
    <url>jdbc:postgresql://{host}:{port}/{database}?sslmode=disable&amp;ApplicationName=s_htap&amp;reWriteBatchedInserts=true&amp;currentSchema=public&amp;preferQueryMode=simple</url>
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
            <rate>unlimited</rate>
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


def _docker(*args: str, check: bool = True, capture: bool = False) -> subprocess.CompletedProcess:
    kwargs = dict(capture_output=True, text=True) if capture else {}
    return subprocess.run(["docker", *args], check=check, **kwargs)


def _remove_stale_container() -> None:
    """Idempotent - matches common.fresh_scratch_dir's own "wipe whatever's there first"
    convention, but at the container level: a container from a crashed prior run (or one
    left over from manual debugging) must not collide with `docker run --name`."""
    _docker("rm", "-f", common.UMBRA_CONTAINER_NAME, check=False, capture=True)


def _save_container_logs(stdout_path: Path) -> None:
    """Snapshot of the container's own stdout/stderr (createdb/SSL-cert-generation/
    server-start log lines, and anything umbra-server itself printed) for post-hoc
    debugging - same role as every other engine's stdout.log. Called right before the
    container is removed (logs vanish with it), both on success and on failure."""
    with open(stdout_path, "wb") as f:
        subprocess.run(["docker", "logs", common.UMBRA_CONTAINER_NAME], stdout=f, stderr=subprocess.STDOUT, check=False)


def _start_container(scratch_dir: Path) -> None:
    """Starts umbra-server in a detached container bind-mounted onto `scratch_dir`
    (tmpfs - see common.fresh_scratch_dir), NUMA-constrained via the container's own
    cgroup (cpuset), and publishes it on 127.0.0.1:common.UMBRA_PORT only (not 5432 -
    the real PostgreSQL cluster service already owns that port on the same machine).

    `--user {uid}:{gid}` (this process's own): the image's default `umbra:umbra` user may
    not have write access to scratch_dir's host-side ownership (it's created by whichever
    user/root runs this harness, not by the image's own UID). `--user root` (the original
    approach here) also writes the bind mount fine, but leaves every file it creates
    root-owned on the host - confirmed live this then makes the NEXT run's
    common.fresh_scratch_dir() (shutil.rmtree, running as this same non-root user) raise
    PermissionError on its very own leftovers, poisoning every run after the first one in
    a sweep. Matching this process's real UID/GID instead sidesteps the UID-mismatch
    problem AND keeps every file already correctly host-user-owned - confirmed live no
    root is actually needed for the image's own bootstrap/createdb/serve steps.

    The `--ulimit` values match the image's own documented `docker run` example
    (https://hub.docker.com/r/umbradb/umbra).
    """
    _docker(
        "run", "-d", "--name", common.UMBRA_CONTAINER_NAME,
        "--user", f"{os.getuid()}:{os.getgid()}",
        f"--cpuset-cpus={common.numa_node_cpu_list()}",
        f"--cpuset-mems={common.NUMA_NODE}",
        "--ulimit", "nofile=1048576:1048576",
        "--ulimit", "memlock=8388608:8388608",
        "-p", f"127.0.0.1:{common.UMBRA_PORT}:5432",
        "-v", f"{scratch_dir}:/var/db",
        common.UMBRA_IMAGE,
    )


def _wait_for_server(timeout: float = 30.0) -> bool:
    """Polls a real `psql` query (not a bare TCP connect - confirmed live: Docker's own
    `-p` port-forwarding accepts the TCP handshake instantly, well before umbra-server is
    actually ready, so a bare `socket.create_connection` reports "ready" ~5-6s too early
    and the very next real query gets "server closed the connection unexpectedly") against
    the published port until it succeeds or `timeout` elapses. Needed because
    docker-entrypoint.sh's own createdb + self-signed-SSL-cert generation takes a moment
    before umbra-server actually starts listening for real."""
    env = os.environ.copy()
    env["PGPASSWORD"] = common.UMBRA_PASSWORD
    env["PGSSLMODE"] = "disable"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["psql", "-U", common.UMBRA_ROLE, "-h", "127.0.0.1", "-p", str(common.UMBRA_PORT),
             "-d", "postgres", "-c", "SELECT 1;"],
            env=env, capture_output=True, text=True,
        )
        if result.returncode == 0:
            return True
        time.sleep(0.5)
    return False


def _container_pid() -> int:
    result = _docker("inspect", "--format", "{{.State.Pid}}", common.UMBRA_CONTAINER_NAME, capture=True)
    return int(result.stdout.strip())


def _verify_container_numa_binding(pid: int) -> None:
    """Trust but verify - same reasoning as postgres_benchbase.py's
    _verify_postmaster_numa_binding: confirm docker's --cpuset-cpus/--cpuset-mems (passed
    in _start_container) actually took effect in the container's cgroup, rather than
    silently benchmarking an unpinned process."""
    status: dict = {}
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            status[key] = value.strip()
    expected_cpus = common.expand_cpu_list(common.numa_node_cpu_list())
    actual_cpus = common.expand_cpu_list(status.get("Cpus_allowed_list", ""))
    actual_nodes = common.expand_cpu_list(status.get("Mems_allowed_list", ""))
    if actual_cpus != expected_cpus or actual_nodes != {common.NUMA_NODE}:
        sys.exit(
            f"Umbra container {common.UMBRA_CONTAINER_NAME!r} (PID {pid}) is not pinned to "
            f"NUMA node {common.NUMA_NODE}: Cpus_allowed_list={status.get('Cpus_allowed_list')!r}, "
            f"Mems_allowed_list={status.get('Mems_allowed_list')!r}; expected CPUs "
            f"{common.numa_node_cpu_list()!r}, memory node {common.NUMA_NODE}. Check that "
            f"`docker info`'s cgroup driver actually supports cpuset (cgroup v2 with the "
            f"cpuset controller enabled)."
        )


def _create_benchbase_database() -> None:
    """The image's docker-entrypoint.sh only ever creates its own default `postgres`
    database - BenchBase needs its own logical database to create/load tables into (same
    role `benchbase` plays for the real PostgreSQL server, created once in
    setup_environment.py::step_postgres there; here it's created fresh every run() since
    the whole container is throwaway). Confirmed directly that Umbra's wire-protocol
    surface accepts a plain `CREATE DATABASE` here, same syntax as PostgreSQL. Uses `psql`
    - already a hard dependency of this harness via postgres_benchbase.py.
    """
    env = os.environ.copy()
    env["PGPASSWORD"] = common.UMBRA_PASSWORD
    # The entrypoint always generates a self-signed cert (-createSSLFiles) - disable SSL
    # explicitly rather than relying on libpq's "prefer" negotiation default, matching the
    # BenchBase JDBC URL's own explicit sslmode=disable.
    env["PGSSLMODE"] = "disable"
    subprocess.run(
        ["psql", "-U", common.UMBRA_ROLE, "-h", "127.0.0.1", "-p", str(common.UMBRA_PORT),
         "-d", "postgres", "-c", f"CREATE DATABASE {common.UMBRA_DATABASE};"],
        env=env, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )


def _stop_container() -> None:
    _docker("rm", "-f", common.UMBRA_CONTAINER_NAME, check=False, capture=True)


def _template_connection_values() -> dict:
    return {
        "host": "127.0.0.1", "port": common.UMBRA_PORT,
        "database": common.UMBRA_DATABASE, "username": common.UMBRA_ROLE, "password": common.UMBRA_PASSWORD,
    }


def run(workload: str, scale: common.Scale, output_dir: Path, gc: str = "on", reload: bool = True,
        ycsb_payload: str = "standard", read_payload: bool = True) -> common.NormalizedResult:
    """`gc` accepted for interface parity with the other engine wrappers but unused - see
    SUPPORTS_GC_TOGGLE above. `reload` accepted for parity too; unlike PostgreSQL's
    always-running service, every Umbra run() already starts from an empty, freshly
    created database (a new container + fresh tmpfs scratch dir every time), so there is
    no cheaper "skip --create/--load" path to offer here.
    """
    del reload
    if workload == "s_htap":
        return common.NormalizedResult(
            "umbra", workload, scale.label, scale.s_htap_duration,
            "write_ops_per_sec", 0.0, 0.0, threads=scale.ycsb_threads,
            gc_enabled=gc,
            notes="SKIPPED: the pinned BenchBase build has no s_htap plugin",
            memory_source="not_measured",
        )
    if common.NO_DURABILITY:
        # See module doc: Umbra has no software fsync/durability toggle to turn off, so
        # compare_engines_new.py's whole premise (real disk, relying on the engine's own
        # fsync-off) has no safe equivalent here - a plain real-disk run would silently
        # benchmark real fsync cost while being labeled a "no durability cost" comparison
        # point. SKIPPED (not sys.exit) so a combined `--engines ...,umbra` sweep under
        # compare_engines_new.py still completes for every other engine - same pattern as
        # the YCSB skewFactor==1 case below.
        return common.NormalizedResult(
            "umbra", workload, scale.label, 0.0, "ops_per_sec", 0.0, 0.0,
            threads=scale.ycsb_threads, gc_enabled=gc,
            notes="SKIPPED: Umbra has no software fsync/durability toggle (confirmed: "
                  "ALTER SYSTEM/SET both refuse to change it in this build) - only "
                  "compare_engines.py's tmpfs-backed mode is safe for this engine.",
        )

    if workload == "tpcc" or workload in common.HTAP_WORKLOADS:
        # NOT SUPPORTED - do not re-wire this without re-reading the history below.
        #
        # TPC-C's NewOrder transaction (the dominant transaction in both the plain "tpcc"
        # mix and every HTAP workload's OLTP side) issues `SELECT ... FOR UPDATE` on the
        # district/stock rows it's about to modify - a hardcoded, spec-required part of
        # BenchBase's TPCCLoader/NewOrder.java, not something this wrapper's config
        # controls. Confirmed live this build of Umbra rejects that clause outright
        # ("ERROR: locking specifier not implemented yet"), so every NewOrder execution
        # fails, and - unlike YCSB's ReadModifyWriteRecord, which fails the same way but
        # still finishes cleanly (see patches/umbra_ycsb_rmw_no_lock_benchbase.patch,
        # which drops the same clause there instead) - the whole BenchBase run then hangs
        # indefinitely at "TERMINATE :: Waiting for all terminals to finish" instead of
        # completing within the configured duration. That second failure mode's root
        # cause was not identified, so unlike the YCSB case there is no equivalent
        # unlocked-read workaround here: NewOrder's FOR UPDATE cannot simply be dropped
        # without patching BenchBase's own TPCC transaction logic (a materially different,
        # correctness-affecting change to what TPC-C measures, not attempted).
        #
        # Getting to this point took real, kept fixes along the way - all still in effect
        # for every other Umbra workload: the _wait_for_server() startup-race fix above,
        # the --user UID/GID fix in _start_container() (was `--user root`, which left
        # root-owned files poisoning every later run's scratch-dir cleanup), and three
        # BenchBase patches (patches/umbra_search_path_benchbase.patch,
        # umbra_catalog_direct_benchbase.patch, umbra_isolation_level_benchbase.patch) that
        # fix Umbra's JDBC catalog/schema-introspection gaps in general, not just for TPC-C
        # - YCSB needs every one of them too. A per-workload custom DDL (stripping "ON
        # DELETE CASCADE", which Umbra also does not implement) got schema creation and
        # data loading working before the FOR UPDATE hang was found; that DDL is no longer
        # wired in (see git history for scripts/engines/umbra_ddl/ if picking this back up).
        return common.NormalizedResult(
            "umbra", workload, scale.label, 0.0, "new_order_per_sec", 0.0, 0.0,
            threads=scale.tpcc_terminals, gc_enabled=gc,
            notes="SKIPPED: TPC-C's NewOrder requires SELECT ... FOR UPDATE, which this "
                  "build of Umbra does not implement ('locking specifier not implemented "
                  "yet') - every NewOrder execution fails and the run then hangs "
                  "indefinitely at termination instead of completing (root cause not "
                  "identified). See this function's own comment for what was tried.",
        )

    ycsb_theta = scale.ycsb_theta
    if workload in common.YCSB_WORKLOADS and ycsb_theta == 1.0:
        # Same BenchBase jar/patch as postgres_benchbase.py - see that module's own
        # comment on patches/ycsb_skew_factor_benchbase.patch.
        return common.NormalizedResult(
            "umbra", workload, scale.label, scale.ycsb_duration, "ops_per_sec", 0.0, 0.0,
            threads=scale.ycsb_threads, gc_enabled=gc,
            notes="SKIPPED: BenchBase's YCSB module rejects skewFactor==1 (the "
                  "ZipfianGenerator singularity - no reasonable substitute)",
        )

    output_dir.mkdir(parents=True, exist_ok=True)
    results_dir = output_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / "config.xml"

    # Always tmpfs (never NO_DURABILITY_SCRATCH_ROOT - the guard above already returned
    # before reaching here whenever NO_DURABILITY is set).
    scratch_dir = common.fresh_scratch_dir("umbra_data")

    _remove_stale_container()
    _start_container(scratch_dir)
    try:
        if not _wait_for_server():
            # `finally` below saves container.log and removes the container either way.
            return common.NormalizedResult(
                "umbra", workload, scale.label, 0.0, "ops_per_sec", 0.0, 0.0,
                threads=scale.ycsb_threads, gc_enabled=gc,
                notes="FAILED: umbra-server never started listening within 30s - see container.log",
            )
        pid = _container_pid()
        _verify_container_numa_binding(pid)
        _create_benchbase_database()

        if workload == "tpcc":
            duration = scale.tpcc_duration
            threads = scale.tpcc_terminals
            config_path.write_text(TPCC_CONFIG_TEMPLATE.format(
                warehouses=scale.tpcc_warehouses, terminals=threads, duration=duration,
                **_template_connection_values(),
            ))
            bench_type = "tpcc"
            metric_name = "new_order_per_sec"
        elif workload in common.HTAP_WORKLOADS:
            duration = scale.tpcc_duration
            threads = scale.tpcc_terminals
            config_path.write_text(CHBENCHMARK_CONFIG_TEMPLATE.format(
                warehouses=scale.tpcc_warehouses, terminals=threads, duration=duration,
                ch_weights=CHBENCHMARK_WEIGHTS[workload], olap_threads=scale.htap_olap_threads,
                **_template_connection_values(),
            ))
            bench_type = "tpcc,chbenchmark"
            metric_name = "new_order_per_sec"
        elif workload == "s_htap":
            duration = scale.s_htap_duration
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
                scalefactor=scale.ycsb_records / 1000.0, theta=ycsb_theta,
                terminals=threads, duration=duration, weights=YCSB_WEIGHTS[letter],
                field_size=8 if ycsb_payload == "u64" else 100,
                **_template_connection_values(),
            ))
            bench_type = "ycsb"
            metric_name = "ops_per_sec"

        postgres_benchbase._assert_unlimited_config(config_path)
        client_numa_node = common.external_client_numa_node()
        print(f"Umbra BenchBase config: {config_path} (rate=unlimited, terminals={threads}, "
              f"port={common.UMBRA_PORT}, client_numa_node={client_numa_node})")

        tree_sampler = common.start_process_tree_sampler(pid)

        args = [
            "java", "-Duser.language=en", "-Duser.country=US", "-jar", str(BENCHBASE_JAR),
            "-b", bench_type, "-c", str(config_path),
            "--create=true", "--load=true", "--execute=true",
            "-d", str(results_dir),
        ]
        timeout = common.default_subprocess_timeout(duration)
        bench_env = os.environ.copy()
        bench_env["YCSB_READ_PAYLOAD"] = "true" if read_payload else "false"
        bench_env["YCSB_U64_PAYLOAD"] = "true" if ycsb_payload == "u64" else "false"
        returncode, _client_rss_unused = common.run_and_track_rss(
            args, cwd=BENCHBASE_HOME, env=bench_env, stdout_path=output_dir / "stdout.log", timeout=timeout,
            numa_node=client_numa_node,
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
                "umbra", workload, scale.label, duration, metric_name, 0.0, server_peak_rss_mb,
                threads=threads, gc_enabled=gc,
                notes=notes,
            )

        if workload in (["tpcc"] + common.HTAP_WORKLOADS):
            results_csv = next(results_dir.glob("*.results.NewOrder.csv"), None)
        else:
            results_csv = next(results_dir.glob("*.results.csv"), None)

        value = common.avg_csv_column(results_csv, "Throughput (requests/second)") if results_csv else 0.0

        latency = {"p50": 0.0, "p95": 0.0, "p99": 0.0, "avg": 0.0, "count": 0}
        if workload == "ycsb_e":
            latency = common.latency_from_results(results_dir, "ScanRecord")
        elif workload in common.HTAP_WORKLOADS:
            latency = common.latency_from_results(results_dir, "Q1" if "q1" in workload else "Q6")
        elif workload == "s_htap":
            latency = common.latency_from_results(results_dir, "OlapScan")

        return common.NormalizedResult(
            "umbra", workload, scale.label, duration, metric_name, value, server_peak_rss_mb,
            threads=threads, gc_enabled=gc,
            scan_p50_us=latency["p50"], scan_p95_us=latency["p95"], scan_p99_us=latency["p99"],
            scan_avg_us=latency["avg"], scan_count=latency["count"],
        )
    finally:
        _save_container_logs(output_dir / "container.log")
        _stop_container()
