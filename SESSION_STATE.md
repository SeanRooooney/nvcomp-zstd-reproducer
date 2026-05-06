# Session State: nvCOMP ZSTD Decompression Bug Investigation

## Status: REPRODUCED WITH TPC-H DATA

We have successfully reproduced the intermittent ZSTD decompression failure using:
1. **Production telemetry data** - Original reproduction
2. **TPC-H generated data** - Confirms bug is not data-specific

This confirms the bug is in cuDF/nvCOMP, not in Velox's integration layer or specific to our production data.

## Latest Testing (2026-05-06)

### TPC-H Reproduction

**Data generation:**
```bash
cd tpch_generator
pip install -r requirements.txt
python generate.py ./tpch_data -s 10 -f 1000 --tables lineitem
```

**Test Configuration:** SF10 lineitem, 1000 files, 5 threads

**Result:** Bug reproduced - `cudaErrorIllegalAddress` after first iteration

```
[T4] iter 1/10 | 200 files | 11997252 rows | 398.2 MB | 8408 ms | 47.4 MB/s | elapsed 8s
[T3] iter 1/10 | 200 files | 11997200 rows | 398.2 MB | 8745 ms | 45.5 MB/s | elapsed 8s
CUDA Error detected. cudaErrorIllegalAddress an illegal memory access was encountered
```

### Intermittent Nature Confirmed

The bug is highly intermittent:
- Failed several times in a row initially
- Then stopped failing on the same dataset
- Factors that may affect reproduction:
  - File cache state (warm vs cold)
  - GPU memory state
  - Timing/race condition window
  - I/O latency variability

## Previous Results (2026-05-05)

**Test Configuration:** 50 iterations, 5 threads, 6046 production telemetry files (~15GB total)

### Failure Summary

| Run | GPU | Result | Failed at | Error Type |
|-----|-----|--------|-----------|------------|
| 1 | 0 | FAIL | iter 17 | Decompression error |
| 2 | 0 | FAIL | iter 30+ | Hang (infinite loop) |
| 3 | 0 | PASS | - | - |
| 4 | 1 | FAIL | iter 30+ | cudaErrorIllegalAddress |
| 5 | 0 | PASS | - | - |
| 6 | 0 | FAIL | iter 30+ | cudaErrorIllegalAddress |

**Failure rate:** ~60-70% of runs fail

### Four Failure Modes (Same Underlying Bug)

1. **"Error during decompression"** - cuDF catches the error
   ```
   CUDF failure at: .../reader_impl_chunking_utils.cu:622: Error during decompression
   ```

2. **"Page offsets mismatch"** - Corrupted metadata/reader state
   ```
   CUDF failure at: .../reader_impl_preprocess.cu:559: Encountered page_offsets / num_columns mismatch
   ```

3. **cudaErrorIllegalAddress** - Memory corruption crashes the process
   ```
   CUDA Error detected. cudaErrorIllegalAddress an illegal memory access was encountered
   ```

4. **Hang** - Process stuck at 100% GPU utilization, no progress (infinite loop or deadlock)

### Confirmation that Files are NOT Corrupt

- Failing file read 1400 times single-threaded with zero failures
- Bug only manifests under concurrent multi-threaded access
- Different files fail on different runs (random based on shuffle order)

## The Bug

**Summary:** Intermittent failures when multiple `chunked_parquet_reader` instances read ZSTD-compressed Parquet files concurrently using streams from the same global stream pool.

## Environment

- **cuDF:** Built from source (Prestissimo dependency, version 26.06)
- **nvCOMP:** 5.2.0.10 (dev) / 5.2.0.13 (runtime)
- **CUDA:** 13.0
- **Driver:** 580.126.16
- **GPU:** NVIDIA A100-SXM4-80GB
- **OS:** Linux (GPFS filesystem)
- **Container:** Presto native dependency image

## Reproducer

**Build:**
```bash
./build.sh /prestissimo
```

**Run:**
```bash
# Single run
./build/reproducer /path/to/zstd-parquet-files --iterations 10 --threads 5

# Loop until failure
./run_loop.sh /path/to/zstd-parquet-files 100 0
```

**What it does:**
- Creates N concurrent "table scan" threads (default 5)
- Each thread creates a new `chunked_parquet_reader` **per file**
- Each reader gets a stream from `cudf::detail::global_cuda_stream_pool()`
- All readers share the same `cuda_async_memory_resource`
- Files are shuffled randomly each run

## Conditions That Trigger the Bug

**Required:**
1. Multiple concurrent `chunked_parquet_reader` instances
2. Streams from `cudf::detail::global_cuda_stream_pool()`
3. ZSTD-compressed Parquet files
4. Shared async memory resource

**Increases failure probability:**
- Many small Parquet files (more reader creation/destruction)
- Higher concurrency (5+ threads)
- Variable I/O latency (cold cache, network filesystem)
- More total data volume

**What does NOT trigger the bug:**
- Single-threaded sequential reads
- Warm file cache (sometimes)
- SNAPPY compression (needs confirmation)

## Files in This Repo

```
.
├── reproducer.cpp          # Main reproducer source
├── CMakeLists.txt          # CMake build configuration
├── build.sh                # Build script for Prestissimo environment
├── run_loop.sh             # Run reproducer in loop until failure
├── tpch_generator/         # TPC-H data generator
│   ├── generate.py         # Generator script
│   └── requirements.txt    # Python dependencies (duckdb, pyarrow)
├── SESSION_STATE.md        # This file
└── README.md               # Quick start guide
```

## Root Cause Hypothesis

The bug is likely in cuDF's Parquet reader or nvCOMP when:
- Multiple decompression operations run on different CUDA streams concurrently
- All streams come from `cudf::detail::global_cuda_stream_pool()`
- Possibly shared scratch space, decompression state, or nvCOMP resources that aren't thread-safe
- Or stream pool reuse patterns that cause resource conflicts before previous operations complete

## Next Steps

1. **File bug report with cuDF/RAPIDS team** with TPC-H reproducer
2. **Test with SNAPPY compression** to confirm ZSTD-specific
3. **Test with cold cache** (drop caches between runs)
4. **Investigate nvCOMP** concurrent decompression behavior
5. **Check cuDF stream pool** implementation for thread safety issues

## Related Links

- [cuDF GitHub](https://github.com/rapidsai/cudf)
- [nvCOMP GitHub](https://github.com/NVIDIA/nvcomp)
