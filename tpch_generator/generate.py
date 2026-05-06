#!/usr/bin/env python3
"""
Generate TPC-H data as compressed Parquet files.

Uses DuckDB to generate TPC-H data, then writes it as many small
compressed Parquet files to reproduce the cuDF/nvCOMP bug.

Usage:
    # Generate SF1 with 100 files per table, ZSTD compression
    python generate.py ./tpch_data --scale-factor 1 --files-per-table 100

    # Generate SF10 with 500 files, SNAPPY compression
    python generate.py ./tpch_data -s 10 -f 500 --compression snappy

    # Generate only lineitem table with 1000 small files
    python generate.py ./tpch_data -s 1 -f 1000 --tables lineitem
"""

import argparse
import os
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed. Run: pip install -r requirements.txt")
    sys.exit(1)

try:
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow not installed. Run: pip install -r requirements.txt")
    sys.exit(1)


TPCH_TABLES = [
    'lineitem',    # Largest table (~6M rows at SF1)
    'orders',      # ~1.5M rows at SF1
    'partsupp',    # ~800K rows at SF1
    'part',        # ~200K rows at SF1
    'customer',    # ~150K rows at SF1
    'supplier',    # ~10K rows at SF1
    'nation',      # 25 rows (fixed)
    'region',      # 5 rows (fixed)
]

# Supported compression algorithms
COMPRESSION_ALGORITHMS = ['zstd', 'snappy', 'gzip', 'lz4', 'none']


def generate_tpch_data(
    output_dir: str,
    scale_factor: float = 1.0,
    files_per_table: int = 100,
    compression: str = 'zstd',
    compression_level: int = None,
    tables: list = None,
):
    """Generate TPC-H data as compressed Parquet files."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if tables is None:
        tables = TPCH_TABLES

    # Normalize compression
    if compression == 'none':
        compression = None

    # Create DuckDB connection and generate TPC-H data
    print(f"Generating TPC-H data at scale factor {scale_factor}...")
    con = duckdb.connect(':memory:')
    con.execute(f"CALL dbgen(sf={scale_factor})")

    total_files = 0
    total_size = 0
    total_rows = 0

    for table_name in tables:
        if table_name not in TPCH_TABLES:
            print(f"Warning: Unknown table {table_name}, skipping")
            continue

        # Get row count
        row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"\n{table_name}: {row_count:,} rows")

        if row_count == 0:
            print(f"  Skipping empty table")
            continue

        # Create table directory
        table_dir = output_path / table_name
        table_dir.mkdir(exist_ok=True)

        # Calculate rows per file
        # For small tables (nation, region), just create one file
        if row_count < files_per_table:
            actual_files = 1
            rows_per_file = row_count
        else:
            actual_files = files_per_table
            rows_per_file = row_count // files_per_table

        print(f"  Creating {actual_files} files (~{rows_per_file:,} rows each)")

        # Fetch all data as Arrow table
        arrow_table = con.execute(f"SELECT * FROM {table_name}").fetch_arrow_table()

        # Split into chunks and write files
        table_size = 0
        offset = 0

        for i in range(actual_files):
            # Calculate chunk boundaries
            if i == actual_files - 1:
                # Last file gets remaining rows
                chunk = arrow_table.slice(offset)
            else:
                chunk = arrow_table.slice(offset, rows_per_file)

            offset += rows_per_file

            # Generate filename
            file_path = table_dir / f"{i:05d}-{table_name}.parquet"

            # Build write options
            write_kwargs = {'compression': compression}
            if compression_level is not None and compression in ('zstd', 'gzip'):
                write_kwargs['compression_level'] = compression_level

            pq.write_table(chunk, file_path, **write_kwargs)

            file_size = os.path.getsize(file_path)
            table_size += file_size
            total_files += 1
            total_rows += chunk.num_rows

            if (i + 1) % 50 == 0 or i == actual_files - 1:
                print(f"    Created {i + 1}/{actual_files} files")

        total_size += table_size
        print(f"  Done: {actual_files} files, {table_size / 1024 / 1024:.1f} MB")

    con.close()

    print(f"\n{'=' * 60}")
    print(f"SUMMARY")
    print(f"{'=' * 60}")
    print(f"Output directory: {output_path.absolute()}")
    print(f"Scale factor: {scale_factor}")
    print(f"Total tables: {len(tables)}")
    print(f"Total files: {total_files}")
    print(f"Total rows: {total_rows:,}")
    print(f"Total size: {total_size / 1024 / 1024:.1f} MB ({total_size / 1024 / 1024 / 1024:.2f} GB)")
    comp_str = compression if compression else 'none'
    if compression_level and compression in ('zstd', 'gzip'):
        comp_str += f" (level {compression_level})"
    print(f"Compression: {comp_str}")

    return total_files, total_size


def main():
    parser = argparse.ArgumentParser(
        description='Generate TPC-H data as compressed Parquet files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # SF1 with ZSTD (default)
    python generate.py ./tpch_data -s 1 -f 100

    # SF10 with 500 files, higher ZSTD compression
    python generate.py ./tpch_data -s 10 -f 500 --compression-level 9

    # SNAPPY compression for comparison
    python generate.py ./tpch_snappy -s 1 -f 100 --compression snappy

    # Only lineitem with many small files
    python generate.py ./tpch_data -s 1 -f 1000 --tables lineitem
""")

    parser.add_argument('output_dir', help='Output directory for generated files')
    parser.add_argument('--scale-factor', '-s', type=float, default=1.0,
                        help='TPC-H scale factor (default: 1.0)')
    parser.add_argument('--files-per-table', '-f', type=int, default=100,
                        help='Number of files per table (default: 100)')
    parser.add_argument('--compression', '-c', choices=COMPRESSION_ALGORITHMS, default='zstd',
                        help='Compression algorithm (default: zstd)')
    parser.add_argument('--compression-level', '-l', type=int, default=None,
                        help='Compression level (zstd: 1-22, gzip: 1-9)')
    parser.add_argument('--tables', '-t', nargs='+', choices=TPCH_TABLES,
                        help=f'Tables to generate (default: all)')

    args = parser.parse_args()

    # Validate compression level
    if args.compression_level is not None:
        if args.compression == 'zstd' and not (1 <= args.compression_level <= 22):
            print("ERROR: ZSTD compression level must be 1-22")
            sys.exit(1)
        if args.compression == 'gzip' and not (1 <= args.compression_level <= 9):
            print("ERROR: GZIP compression level must be 1-9")
            sys.exit(1)

    print(f"TPC-H Data Generator")
    print(f"{'=' * 60}")
    print(f"Output: {args.output_dir}")
    print(f"Scale factor: {args.scale_factor}")
    print(f"Files per table: {args.files_per_table}")
    print(f"Compression: {args.compression}", end="")
    if args.compression_level:
        print(f" (level {args.compression_level})")
    else:
        print()
    if args.tables:
        print(f"Tables: {', '.join(args.tables)}")
    print()

    generate_tpch_data(
        args.output_dir,
        scale_factor=args.scale_factor,
        files_per_table=args.files_per_table,
        compression=args.compression,
        compression_level=args.compression_level,
        tables=args.tables,
    )


if __name__ == '__main__':
    main()
