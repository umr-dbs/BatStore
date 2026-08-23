#!/usr/bin/env bash
# OLAP-focused BatStore HTAP experiment: keep OLTP small and fixed while sweeping
# concurrent Q1/Q6 analytical threads. Extra arguments override these defaults.
#
# Usage:
#   scripts/run_htap_olap_focused.sh
#   scripts/run_htap_olap_focused.sh --scan-pool-workers 64 --tpcc-duration 300
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

exec python3 "$SCRIPT_DIR/run_htap_analytical_sweep.py" \
    --engines batstore \
    --workloads htap_q1,htap_q6 \
    --oltp-terminals 4 \
    --warehouses 16 \
    --olap-threads 1,2,4,8,16,32 \
    --scan-pool-workers 96 \
    --tpcc-duration 180 \
    --gc on,off \
    "$@"
