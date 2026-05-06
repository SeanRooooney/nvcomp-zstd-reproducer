# nvCOMP/cuDF ZSTD Decompression Bug Reproducer

Standalone reproducer for intermittent ZSTD decompression failures in cuDF when multiple `chunked_parquet_reader` instances read concurrently.

## Bug Summary

**Symptom:** Intermittent failures when reading ZSTD-compressed Parquet files concurrently via cuDF.

**Four failure modes (same underlying bug):**
1. `CUDF failure at: .../reader_impl_chunking_utils.cu:622: Error during decompression`
2. `Page offsets mismatch` - Corrupted metadata/reader state
3. `cudaErrorIllegalAddress: an illegal memory access was encountered`
4. Process hangs at 100% GPU utilization (infinite loop/deadlock)

**Failure rate:** ~60-70% of runs fail within 50 iterations

**Key finding:** Individual files are NOT corrupt - the same file reads successfully 1000+ times single-threaded. Bug only manifests under concurrent multi-threaded access.

## Quick Start

### Prerequisites

- Docker with NVIDIA GPU support
- A Prestissimo build with cuDF
- Python 3.8+ with pip (for generating test data)

### Step 1: Generate TPC-H Test Data

On your host machine (outside the container):

```bash
cd tpch_generator
pip install -r requirements.txt

# Generate SF10 with 1000 files per table (recommended for reliable reproduction)
python generate.py /path/to/output/tpch_sf10 \
    --scale-factor 10 \
    --files-per-table 1000 \
    --compression zstd

# Or smaller dataset for quick testing
python generate.py /path/to/output/tpch_sf1 \
    --scale-factor 1 \
    --files-per-table 100 \
    --compression zstd
```

### Step 2: Start the Build Container

Start a container with GPU access and mount the required directories:

```bash
docker run -it --rm --gpus all \
    -v /path/to/prestissimo:/prestissimo \
    -v /path/to/nvcomp-zstd-reproducer:/nvcomp-zstd-reproducer \
    -v /path/to/tpch_data:/data \
    <your-build-image> \
    /bin/bash
```

Example with a Presto native dependency image:

```bash
docker run -it --rm --gpus all \
    -v ${HOME}/git/presto/presto-native-execution:/prestissimo \
    -v ${HOME}/git/nvcomp-zstd-reproducer:/nvcomp-zstd-reproducer \
    -v /gpfs/zc2/data:/gpfs/zc2/data \
    de.icr.io/perfleap/centos-native-dependency:master-cuda13.0 \
    /bin/bash
```

### Step 3: Build the Reproducer

Inside the container:

```bash
cd /nvcomp-zstd-reproducer
./build.sh /prestissimo
```

This creates `./build/reproducer`.

### Step 4: Run the Reproducer

```bash
# Basic usage - reads all parquet files in directory tree
./build/reproducer /data/tpch_sf10 --iterations 10 --threads 5

# Run on a specific GPU
CUDA_VISIBLE_DEVICES=0 ./build/reproducer /data/tpch_sf10 --iterations 10 --threads 5

# Just the lineitem table (largest, most likely to trigger)
./build/reproducer /data/tpch_sf10/lineitem --iterations 10 --threads 5
```

### Expected Output (Failure)

```
=== Velox Pattern Reproducer ===
GPU device: 0
...
=== Starting concurrent reads ===

[T4] iter 1/10 | 200 files | 11997252 rows | 398.2 MB | 8408 ms | 47.4 MB/s | elapsed 8s
[T3] iter 1/10 | 200 files | 11997200 rows | 398.2 MB | 8745 ms | 45.5 MB/s | elapsed 8s
CUDA Error detected. cudaErrorIllegalAddress an illegal memory access was encountered
```

## Environment

Tested with:
- **cuDF:** 26.06 (built from source via Prestissimo)
- **nvCOMP:** 5.2.0.10 (dev) / 5.2.0.13 (runtime)
- **CUDA:** 13.0
- **Driver:** 580.126.16
- **GPU:** NVIDIA A100-SXM4-80GB

## Reproducer Options

```
./build/reproducer <parquet_dir> [options]

Options:
  --iterations N    Number of iterations per thread (default: 100)
  --threads N       Number of concurrent threads (default: 5)
  --chunk-limit N   Chunk read limit in GB (default: 4)
  --pass-limit N    Pass read limit in GB (default: 16)
```

## TPC-H Generator Options

```
python tpch_generator/generate.py <output_dir> [options]

Options:
  --scale-factor, -s    TPC-H scale factor (default: 1.0)
  --files-per-table, -f Number of files per table (default: 100)
  --compression, -c     Compression: zstd, snappy, gzip, lz4, none (default: zstd)
  --compression-level, -l  Compression level (zstd: 1-22, gzip: 1-9)
  --tables, -t          Specific tables to generate (default: all)
```

## What the Reproducer Does

Mimics how Velox/Prestissimo reads multiple tables concurrently in a JOIN query:

1. Creates N concurrent threads (simulating multi-table scans)
2. Each thread iterates through its assigned files
3. For each file, creates a new `chunked_parquet_reader`
4. Each reader gets a stream from `cudf::detail::global_cuda_stream_pool()`
5. All readers share the same `cuda_async_memory_resource`

This pattern exposes a race condition in cuDF/nvCOMP's ZSTD decompression.

## Conditions That Trigger the Bug

**Required:**
1. Multiple concurrent `chunked_parquet_reader` instances
2. Streams from `cudf::detail::global_cuda_stream_pool()`
3. ZSTD-compressed Parquet files
4. Shared async memory resource

**Increases failure probability:**
- Many small files (more reader creation/destruction)
- Higher concurrency (5+ threads)
- Larger total data volume

**What does NOT trigger the bug:**
- Single-threaded sequential reads
- One reader per thread, reused across files
- SNAPPY compression (needs confirmation)

## Root Cause Hypothesis

The bug appears to be in cuDF's Parquet reader or nvCOMP when:
- Multiple decompression operations run on different CUDA streams concurrently
- All streams come from `cudf::detail::global_cuda_stream_pool()`
- Possible shared scratch space, decompression state, or nvCOMP resources that aren't thread-safe

## Files

```
.
├── reproducer.cpp          # Main reproducer source
├── CMakeLists.txt          # CMake build configuration
├── build.sh                # Build script for Prestissimo environment
├── tpch_generator/         # TPC-H data generator
│   ├── generate.py         # Generator script
│   └── requirements.txt    # Python dependencies (duckdb, pyarrow)
├── SESSION_STATE.md        # Detailed investigation notes
└── README.md               # This file
```

## See Also

- [cuDF GitHub](https://github.com/rapidsai/cudf)
- [nvCOMP GitHub](https://github.com/NVIDIA/nvcomp)
