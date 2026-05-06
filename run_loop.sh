#!/bin/bash
# Run reproducer in a loop until failure
#
# Usage:
#   ./run_loop.sh /path/to/parquet/files [max_runs] [gpu_id]
#
# Example:
#   ./run_loop.sh /data/tpch_sf10/lineitem
#   ./run_loop.sh /data/tpch_sf10/lineitem 50
#   ./run_loop.sh /data/tpch_sf10/lineitem 50 2

set -e

if [ -z "$1" ]; then
    echo "Usage: $0 <parquet_dir> [max_runs] [gpu_id]"
    echo ""
    echo "Example:"
    echo "  $0 /data/tpch_sf10/lineitem"
    echo "  $0 /data/tpch_sf10/lineitem 50"
    echo "  $0 /data/tpch_sf10/lineitem 50 2   # Use GPU 2"
    exit 1
fi

PARQUET_DIR="$1"
MAX_RUNS="${2:-100}"
GPU_ID="${3:-0}"
ITERATIONS="${ITERATIONS:-10}"
THREADS="${THREADS:-5}"

export CUDA_VISIBLE_DEVICES="$GPU_ID"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPRODUCER="$SCRIPT_DIR/build/reproducer"

if [ ! -x "$REPRODUCER" ]; then
    echo "ERROR: Reproducer not found at $REPRODUCER"
    echo "Run ./build.sh first"
    exit 1
fi

echo "Running reproducer in loop"
echo "  Parquet dir: $PARQUET_DIR"
echo "  Max runs: $MAX_RUNS"
echo "  GPU: $GPU_ID"
echo "  Iterations per run: $ITERATIONS"
echo "  Threads: $THREADS"
echo ""

for i in $(seq 1 $MAX_RUNS); do
    echo "=== Run $i ==="
    if ! "$REPRODUCER" "$PARQUET_DIR" --iterations "$ITERATIONS" --threads "$THREADS"; then
        echo ""
        echo "FAILED on run $i"
        exit 1
    fi
done

echo ""
echo "All $MAX_RUNS runs completed successfully"
