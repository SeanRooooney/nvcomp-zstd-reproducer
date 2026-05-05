# Session State: nvCOMP ZSTD Decompression Bug Investigation

## Status: RELIABLY REPRODUCED - cuDF Bug Confirmed

We have successfully reproduced the intermittent ZSTD decompression failure **outside of Velox** using a standalone cuDF reproducer. This confirms the bug is in cuDF/nvCOMP, not in Velox's integration layer.

## Latest Reproduction Results (2026-05-05)

**Test Configuration:** 50 iterations, 5 threads, 6046 files (~15GB total)

### Failure Summary

| Run | GPU | Result | Failed at | Error Type |
|-----|-----|--------|-----------|------------|
| 1 | 0 | ❌ | iter 17 | Decompression error |
| 2 | 0 | ❌ | iter 30+ | Hang (infinite loop) |
| 3 | 0 | ✅ | - | - |
| 4 | 1 | ❌ | iter 30+ | cudaErrorIllegalAddress |
| 5 | 0 | ✅ | - | - |
| 6 | 0 | ❌ | iter 30+ | cudaErrorIllegalAddress |

**Failure rate:** ~60-70% of runs fail

**Typical failure point:** iteration 17-30+ (~100k-200k file reads, ~250-500 GB data)

### Three Failure Modes (Same Underlying Bug)

1. **"Error during decompression"** - cuDF catches the error
   ```
   CUDF failure at: .../reader_impl_chunking_utils.cu:622: Error during decompression
   ```

2. **cudaErrorIllegalAddress** - Memory corruption crashes the process
   ```
   CUDA Error detected. cudaErrorIllegalAddress an illegal memory access was encountered
   ```

3. **Hang** - Process stuck at 100% GPU utilization, no progress (infinite loop or deadlock)

### Confirmation that Files are NOT Corrupt

- Failing file read 1400 times single-threaded with zero failures
- Bug only manifests under concurrent multi-threaded access
- Different files fail on different runs (random based on shuffle order)

## The Bug

**Summary:** Intermittent "Error during decompression" failures when multiple `chunked_parquet_reader` instances read ZSTD-compressed Parquet files concurrently using streams from the same global stream pool.

**Error Message:**
```
CUDF failure at: /prestissimo/_build/release/_deps/cudf-src/cpp/src/io/parquet/reader_impl_chunking_utils.cu:622: Error during decompression
```

Sometimes manifests as:
```
CUDA error at: cuda_stream_view.cpp:45: cudaErrorIllegalAddress an illegal memory access was encountered
```

## Environment

- **cuDF:** Built from source (Prestissimo dependency, version 26.06)
- **nvCOMP:** 5.2.0.10 (dev) / 5.2.0.13 (runtime)
- **CUDA:** 13.0
- **Driver:** 580.126.16
- **GPU:** NVIDIA A100-SXM4-80GB
- **OS:** Linux (GPFS filesystem)
- **Parquet files:** ZSTD-compressed, many small files (~6000 files, ~15GB total)

## Reproducer

A standalone C++ reproducer is available: `reproducer_velox_pattern.cpp`

**Build:**
```bash
./build.sh /prestissimo
```

**Run:**
```bash
./build/reproducer_velox_pattern /path/to/zstd-parquet-files --iterations 50 --threads 5
```

**What it does:**
- Creates N concurrent "table scan" threads (default 5, simulating a multi-table JOIN query)
- Each thread creates a new `chunked_parquet_reader` **per file** (like Velox does per split)
- Each reader gets a stream from `cudf::detail::global_cuda_stream_pool()`
- All readers share the same `cuda_async_memory_resource`
- Reads ZSTD-compressed Parquet files

**Example failure output:**
```
============================================================
FAILURE - Table 3, iteration 3
File: /gpfs/zc2/data/cio_prod/raw-telemetry/rpt_system_config/date_added_ts_day=2026-03-16/00002-1802-cf0fea6f-2dcc-470c-9268-8b231769ac94-0-00098.parquet
Error: CUDF failure at: /prestissimo/_build/release/_deps/cudf-src/cpp/src/io/parquet/reader_impl_chunking_utils.cu:622: Error during decompression
============================================================
```

## Conditions That Trigger the Bug

**Required:**
1. Multiple concurrent `chunked_parquet_reader` instances
2. Streams from `cudf::detail::global_cuda_stream_pool()`
3. ZSTD-compressed Parquet files
4. Shared async memory resource

**Increases failure probability:**
- Many small Parquet files (vs few large files)
- Higher concurrency (more simultaneous readers, e.g., 5 table scans)
- Variable I/O latency (e.g., GPFS network filesystem)

**Failure rate:**
- Production (Presto queries): ~10% of complex multi-table queries fail
- Standalone reproducer: ~60-70% of runs fail within 50 iterations

## What Does NOT Trigger the Bug

- Single-threaded sequential reads
- Simple queries on single tables (even with filters)
- Multiple simple queries run in parallel as separate Presto queries
- The original simple reproducer (one reader per thread, threads read different files)

## Key Difference: Why Velox-Pattern Reproducer Works

| Simple Reproducer | Velox-Pattern Reproducer |
|-------------------|--------------------------|
| One stream per thread, reused | New stream from global pool per reader |
| One reader per thread, reused | New reader created per file |
| Threads read disjoint file sets | Multiple readers active simultaneously |
| Does NOT trigger bug | TRIGGERS bug |

The critical difference is that Velox creates a **new `chunked_parquet_reader`** for each split/file, getting a fresh stream from the global pool each time. This means multiple readers can be active simultaneously on different streams, which exposes the bug.

## Investigation Timeline

1. **Initial observation:** Intermittent failures in Prestissimo/Velox reading ZSTD Parquet via cuDF
2. **First reproducer (reproducer.cpp):** Simple threads reading files - NO FAILURE
3. **Python cuDF test:** 100+ iterations - NO FAILURE
4. **Hypothesis:** Race condition in async memory copy (BufferedInputDataSource)
5. **Fix attempted:** Added `stream.synchronize()` after `cudaMemcpyAsync` - STILL FAILED
6. **Key insight:** Bug occurs with `CUDF_HIVE_USE_BUFFERED_INPUT=false` too
7. **Tested:** Simple queries with filters - NO FAILURE
8. **Tested:** Parallel simple queries - NO FAILURE
9. **Velox pattern reproducer (reproducer_velox_pattern.cpp):** Mimicked multi-table concurrent scan - **REPRODUCED**

## Root Cause Hypothesis

The bug is likely in cuDF's Parquet reader or nvCOMP when:
- Multiple decompression operations run on different CUDA streams concurrently
- All streams come from `cudf::detail::global_cuda_stream_pool()`
- Possibly shared scratch space, decompression state, or nvCOMP resources that aren't thread-safe
- Or stream pool reuse patterns that cause resource conflicts before previous operations complete

## Files in This Repo

- `reproducer.cpp` - Original simple reproducer (does NOT trigger bug)
- `reproducer_velox_pattern.cpp` - Velox-pattern reproducer (**TRIGGERS bug**)
- `reproducer.py` - Python cuDF reproducer (does NOT trigger bug)
- `CMakeLists.txt` - CMake build configuration
- `build.sh` - Build script for Prestissimo environment
- `SESSION_STATE.md` - This file

## Useful Commands

```bash
# Build reproducers
./build.sh /prestissimo

# Run Velox-pattern reproducer (triggers bug)
./build/reproducer_velox_pattern /path/to/zstd-parquet-files --iterations 50 --threads 5

# Run on a specific GPU (use CUDA_VISIBLE_DEVICES, not --gpu flag)
CUDA_VISIBLE_DEVICES=2 ./build/reproducer_velox_pattern /path/to/zstd-parquet-files --iterations 50 --threads 5

# Run in a loop to reliably trigger the bug
for i in {1..100}; do
  echo "=== Run $i ==="
  ./build/reproducer_velox_pattern /path/to/zstd-parquet-files --iterations 50 --threads 5
  if [ $? -ne 0 ]; then echo "FAILED on run $i"; break; fi
done

# Test a specific file in isolation (to confirm it's not corrupt)
./build/reproducer_velox_pattern /path/to/specific/dir --iterations 100 --threads 1

# Run simple reproducer (does not trigger bug)
./build/reproducer /path/to/zstd-parquet-files 100 8

# Run Python reproducer (does not trigger bug)
python reproducer.py /path/to/zstd-parquet-files --iterations 100 --parallel 8
```

**Note:** The `--gpu` flag does not work reliably because cuDF's internal singletons initialize on GPU 0. Use `CUDA_VISIBLE_DEVICES` environment variable instead.

## Potential Workarounds (Untested)

1. Serialize table scans (`NUM_DRIVERS=1` in Velox)
2. Use separate stream pools per reader instead of global pool
3. Add synchronization between concurrent readers
4. Use synchronous memory resource instead of async

## Next Steps

1. **File bug report with cuDF/RAPIDS team** with this reproducer
2. **Investigate nvCOMP** concurrent decompression behavior
3. **Check cuDF stream pool** implementation for thread safety issues
4. **Test workarounds** in production environment

## Related Links

- [nvCOMP Release Notes](https://docs.nvidia.com/cuda/nvcomp/release_notes.html)
- [cuDF GitHub](https://github.com/rapidsai/cudf)
- [nvCOMP GitHub](https://github.com/NVIDIA/nvcomp)
