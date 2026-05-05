# nvCOMP/cuDF ZSTD Decompression Bug Reproducer

Standalone reproducer for intermittent ZSTD decompression failures in cuDF when multiple `chunked_parquet_reader` instances read concurrently.

## Bug Summary

**Symptom:** Intermittent failures when reading ZSTD-compressed Parquet files concurrently via cuDF.

**Three failure modes (same underlying bug):**
1. `CUDF failure at: .../reader_impl_chunking_utils.cu:622: Error during decompression`
2. `cudaErrorIllegalAddress: an illegal memory access was encountered`
3. Process hangs at 100% GPU utilization (infinite loop/deadlock)

**Failure rate:** ~60-70% of runs fail within 50 iterations (5 threads, 6000 files)

**Key finding:** Individual files are NOT corrupt - the same file reads successfully 1000+ times single-threaded. Bug only manifests under concurrent multi-threaded access.

## Environment

- cuDF 26.06 (built from source)
- nvCOMP 5.2.0
- CUDA 13.0
- NVIDIA A100-SXM4-80GB

## Building

Requires a Prestissimo build with cuDF:

```bash
./build.sh /path/to/prestissimo
```

## Usage

```bash
# Basic usage
./build/reproducer /path/to/zstd-parquet-files --iterations 50 --threads 5

# Run on a specific GPU
CUDA_VISIBLE_DEVICES=2 ./build/reproducer /path/to/files --iterations 50 --threads 5

# Run in a loop to reliably trigger the bug
for i in {1..100}; do
  echo "=== Run $i ==="
  ./build/reproducer /path/to/files --iterations 50 --threads 5
  if [ $? -ne 0 ]; then echo "FAILED on run $i"; break; fi
done
```

## What the Reproducer Does

Mimics how Velox reads multiple tables concurrently:
- Creates N concurrent threads (simulating multi-table JOINs)
- Each thread creates a new `chunked_parquet_reader` per file
- Each reader gets a stream from `cudf::detail::global_cuda_stream_pool()`
- All readers share the same `cuda_async_memory_resource`

## Conditions That Trigger the Bug

**Required:**
1. Multiple concurrent `chunked_parquet_reader` instances
2. Streams from `cudf::detail::global_cuda_stream_pool()`
3. ZSTD-compressed Parquet files
4. Shared async memory resource

**What does NOT trigger the bug:**
- Single-threaded sequential reads
- Reading the same files with one reader at a time

## See Also

- `SESSION_STATE.md` - Detailed investigation notes and test results
