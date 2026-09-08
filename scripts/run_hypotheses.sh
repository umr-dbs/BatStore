#!/usr/bin/env bash
# Run each hypothesis once, stopping immediately on failure.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for hypothesis in 1 2 3 4 5 6; do
    python3 "$SCRIPT_DIR/h${hypothesis}.py"
done
