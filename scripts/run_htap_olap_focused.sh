#!/usr/bin/env bash
# OLAP-focused BatStore HTAP experiment tuned for one 64-core/128-thread NUMA
# node: keep OLTP small and fixed while sweeping concurrent Q1/Q6 analytical
# threads up to the node's useful limit. Extra arguments override these defaults.
#
# Why these defaults:
# - 16 warehouses give a low-concurrency query up to 16 non-empty scan ranges
#   without doubling the work again merely to manufacture more partitions.
# - 2 OLTP terminals retain genuine HTAP interference but leave CPU and memory
#   bandwidth focused on analytical work.
# - 128 scan workers match the node's hardware threads. For <=64 OLAP callers,
#   fair_query_fanout divides that fixed pool among them; above 64, its share is
#   below two and each OLAP caller scans directly instead. That avoids the auto
#   pool's 4 * OLAP-thread expansion (up to 480 workers at 120 callers).
# - 120 OLAP callers plus 2 terminals and the loader fit BatStore's 128-slot
#   WorkerId budget without being clamped.
#
# Usage:
#   scripts/run_htap_olap_focused.sh
#   scripts/run_htap_olap_focused.sh --scan-pool-workers 96 --tpcc-duration 300
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec python3 "$SCRIPT_DIR/run_htap_analytical_sweep.py" \
    --engines batstore,postgres,libmdbx,wiredtiger \
    --workloads htap_q1,htap_q6 \
    --oltp-terminals 2 \
    --warehouses 16 \
    --olap-threads 1,2,4,8,16,32,48,64,96,120 \
    --scan-pool-workers 128 \
    --tpcc-duration 180 \
    --gc on,off \
    "$@"
