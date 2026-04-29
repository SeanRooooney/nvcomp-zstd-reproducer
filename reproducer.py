#!/usr/bin/env python3
"""
nvCOMP/cuDF ZSTD Decompression Reproducer

This script attempts to reproduce intermittent ZSTD decompression failures
observed when reading small Parquet files via cuDF/nvCOMP.

Usage:
    python reproducer.py /path/to/parquet/files [iterations]

Environment:
    - cuDF 26.06
    - nvCOMP 5.2.0
    - Requires NVIDIA GPU with CUDA
"""

import argparse
import glob
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


def main():
    parser = argparse.ArgumentParser(
        description="Reproduce nvCOMP ZSTD decompression failures"
    )
    parser.add_argument(
        "parquet_path",
        help="Path to directory containing Parquet files or a single Parquet file",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=100,
        help="Number of iterations to run (default: 100)",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel readers (default: 1, use higher to stress GPU)",
    )
    parser.add_argument(
        "--shuffle",
        action="store_true",
        help="Shuffle file order each iteration",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print details for each file read",
    )
    args = parser.parse_args()

    # Import cudf here to catch import errors early
    try:
        import cudf
        print(f"cuDF version: {cudf.__version__}")
    except ImportError as e:
        print(f"ERROR: Failed to import cudf: {e}")
        print("Install with: conda install -c rapidsai -c conda-forge -c nvidia cudf=26.06 cuda-version=12.0")
        sys.exit(1)

    # Get list of parquet files
    if os.path.isfile(args.parquet_path):
        files = [args.parquet_path]
    elif os.path.isdir(args.parquet_path):
        files = glob.glob(os.path.join(args.parquet_path, "**/*.parquet"), recursive=True)
        if not files:
            # Try without recursion
            files = glob.glob(os.path.join(args.parquet_path, "*.parquet"))
    else:
        print(f"ERROR: Path does not exist: {args.parquet_path}")
        sys.exit(1)

    if not files:
        print(f"ERROR: No Parquet files found in {args.parquet_path}")
        sys.exit(1)

    print(f"Found {len(files)} Parquet file(s)")

    # Print file sizes
    total_size = 0
    for f in files:
        size = os.path.getsize(f)
        total_size += size
        if args.verbose:
            print(f"  {f}: {size:,} bytes")
    print(f"Total size: {total_size:,} bytes ({total_size / 1024 / 1024:.2f} MB)")
    print()

    # Helper function to read a single file
    def read_file(filepath, iteration, verbose):
        """Read a single parquet file and return result."""
        try:
            df = cudf.read_parquet(filepath)
            # Force materialization of data
            row_count = len(df)
            if verbose:
                print(f"  Iteration {iteration}, {os.path.basename(filepath)}: {row_count} rows")
            return {"success": True, "file": filepath, "rows": row_count}
        except Exception as e:
            return {"success": False, "file": filepath, "error": str(e)}

    # Run iterations
    failures = []
    start_time = time.time()

    for iteration in range(1, args.iterations + 1):
        iteration_start = time.time()

        # Optionally shuffle files
        iteration_files = files.copy()
        if args.shuffle:
            random.shuffle(iteration_files)

        if args.parallel > 1:
            # Parallel execution
            with ThreadPoolExecutor(max_workers=args.parallel) as executor:
                futures = {
                    executor.submit(read_file, f, iteration, args.verbose): f
                    for f in iteration_files
                }
                for future in as_completed(futures):
                    result = future.result()
                    if not result["success"]:
                        elapsed = time.time() - start_time
                        failure_info = {
                            "iteration": iteration,
                            "file": result["file"],
                            "error": result["error"],
                            "elapsed_seconds": elapsed,
                        }
                        failures.append(failure_info)

                        print(f"\n{'='*60}")
                        print(f"FAILURE at iteration {iteration}")
                        print(f"File: {result['file']}")
                        print(f"Error: {result['error']}")
                        print(f"Elapsed time: {elapsed:.2f} seconds")
                        print(f"{'='*60}\n")
        else:
            # Sequential execution
            for f in iteration_files:
                result = read_file(f, iteration, args.verbose)
                if not result["success"]:
                    elapsed = time.time() - start_time
                    failure_info = {
                        "iteration": iteration,
                        "file": result["file"],
                        "error": result["error"],
                        "elapsed_seconds": elapsed,
                    }
                    failures.append(failure_info)

                    print(f"\n{'='*60}")
                    print(f"FAILURE at iteration {iteration}")
                    print(f"File: {result['file']}")
                    print(f"Error: {result['error']}")
                    print(f"Elapsed time: {elapsed:.2f} seconds")
                    print(f"{'='*60}\n")

        iteration_elapsed = time.time() - iteration_start
        print(f"Iteration {iteration}/{args.iterations} completed in {iteration_elapsed:.2f}s")

    # Summary
    total_elapsed = time.time() - start_time
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total iterations: {args.iterations}")
    print(f"Total files per iteration: {len(files)}")
    print(f"Total reads attempted: {args.iterations * len(files)}")
    print(f"Total failures: {len(failures)}")
    print(f"Total time: {total_elapsed:.2f} seconds")

    if failures:
        print()
        print("Failures:")
        for f in failures:
            print(f"  - Iteration {f['iteration']}: {f['file']}")
            print(f"    Error: {f['error']}")
        sys.exit(1)
    else:
        print()
        print("All iterations passed successfully")
        sys.exit(0)


if __name__ == "__main__":
    main()
