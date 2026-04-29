# Session State: nvCOMP ZSTD Decompression Investigation

**Last Updated**: 2026-04-29

## Problem Summary

Intermittent "Error during decompression" when reading ZSTD-compressed Parquet files via Prestissimo/Velox using cuDF/nvCOMP.

### Key Characteristics
- **Intermittent**: Same query on same files sometimes works, sometimes fails
- **ZSTD only**: Snappy compression works fine
- **GPU only**: CPU decompression path works
- **Small files**: Parquet files from Iceberg tables

### Error Message
```
VeloxRuntimeError: CUDF failure at: /prestissimo/_build/_deps/cudf-src/cpp/src/io/parquet/reader_impl_chunking_utils.cu:622: Error during decompression
```

## Environment

- **cuDF**: 26.06 (26.04 in some tests)
- **nvCOMP**: 5.2.0.10 (dev) / 5.2.0.13 (runtime)
- **Source data**: IBM watsonx.data / Presto with Iceberg table format
- **Compression**: ZSTD (level unknown, likely default 3)
- **Velox Chunk Limits**:
  - `PARQUET_READER_CHUNK_READ_LIMIT`: 4GB (4294967296)
  - `PARQUET_READER_PASS_READ_LIMIT`: 16GB (17179869184)

## Root Cause Investigation

### cuDF Error Location
File: `cpp/src/io/parquet/reader_impl_chunking_utils.cu:622`

```cpp
CUDF_EXPECTS(
  cudf::detail::all_of(comp_res.begin(),
                       comp_res.end(),
                       cuda::proclaim_return_type<bool>([] __device__(auto const& res) {
                         return res.status == codec_status::SUCCESS;
                       }),
                       stream),
  "Error during decompression");
```

This check verifies all decompression results succeeded. The actual nvCOMP error code is not logged.

### Known nvCOMP Issue
From [nvCOMP release notes](https://docs.nvidia.com/cuda/nvcomp/release_notes.html):

> "Zstd decompression fails when decompressing buffers compressed with compression level 18 and higher using the Zstd library version 1.5.6. To workaround the problem temporarily, you can provide 1.5x the scratch required by nvcompBatchedZstdDecompressGetTempSizeAsync to nvcompBatchedZstdDecompressAsync."

**Note**: This documented issue is deterministic, but our issue is intermittent, suggesting a potentially different or related root cause.

## Reproduction Attempts

### 1. Python cuDF (reproducer.py)
- Ran 100+ iterations with --parallel 16
- 6046 files, ~15.5 GB total
- **Result**: No failures reproduced
- **Conclusion**: Python path may have different memory/threading behavior

### 2. C++ libcudf (reproducer.cpp)
- Mimics Velox's approach:
  - `cudf::io::chunked_parquet_reader`
  - `rmm::mr::cuda_async_memory_resource`
  - `rmm::cuda_stream_pool` for per-thread streams
  - Multi-threaded parallel reads
  - Now uses Velox's chunk/pass read limits (4GB/16GB)
- **Status**: Extensive testing with no failures

### 3. Stress Testing Results
Ran 4 concurrent C++ reproducer processes:
- Total GPU memory usage: ~55GB (18GB + 16GB + 15GB + 5GB)
- GPU utilization: 100% (800%+ compute)
- **Result**: No failures reproduced even under heavy load

### Key Finding
**The standalone reproducers (both Python and C++) cannot trigger the failure**, even under extreme GPU memory pressure and high concurrency. This strongly suggests the issue is specific to Velox's integration layer, not cuDF/nvCOMP itself.

## Hypotheses

### Ruled Out (by reproducer testing)
1. ~~**Race condition** in nvCOMP zstd decompression kernel~~ - Would have appeared in C++ reproducer
2. ~~**GPU memory pressure** - scratch space allocation varies~~ - Tested at 55GB+ usage, no failures
3. ~~**cuDF chunked_parquet_reader bug**~~ - Works fine in isolation

### Still Possible (Velox-specific)
4. **Velox's BufferedInputDataSource** - async I/O layer wrapping cuDF data sources
5. **Velox's memory pool wrapper** - may interact differently with rmm
6. **Velox's TableScan operator** - concurrent operators, task scheduling
7. **AST filter pushdown** - reproducer doesn't use filters
8. **Stream synchronization in Velox** - async CUDA operations between Velox and cuDF
9. **Data source lifecycle** - how Velox manages cuDF data source objects

## How the C++ Reproducer Works

### Setup
```cpp
// Create async memory allocator (same as Velox)
auto async_mr = rmm::mr::cuda_async_memory_resource();

// Create pool of CUDA streams (one per thread)
stream_pool = rmm::cuda_stream_pool(num_threads);
```

### Per Thread (in parallel)
```cpp
// Get a dedicated CUDA stream
auto stream = stream_pool->get_stream();

// Loop through iterations
for (iteration = 1 to N) {
    // Read assigned files
    for (file in my_files) {
        // Create chunked reader (same as Velox uses)
        cudf::io::chunked_parquet_reader reader(0, 0, options, stream, mr);

        // Read all chunks
        while (reader.has_next()) {
            auto chunk = reader.read_chunk();  // <-- decompression happens here
            total_rows += chunk.num_rows();
        }
    }
}
```

### What It Tests
- Multiple threads reading Parquet files concurrently
- Each thread uses its own CUDA stream
- All threads share the async memory allocator
- Creates GPU memory pressure and concurrent decompression operations

### What It Does NOT Test (compared to Velox)
- Column projection / filtering
- Velox's specific memory pool settings
- Multiple concurrent queries
- The exact chunking limits Velox uses (`maxChunkReadLimit`, `maxPassReadLimit`)
- AST filter pushdown
- Velox memory pool integration

### Where Decompression Failure Would Occur
Inside `reader.read_chunk()` when nvCOMP tries to decompress ZSTD-compressed Parquet pages.

## Files in This Repo

- `reproducer.py` - Python cuDF reproducer
- `reproducer.cpp` - C++ libcudf reproducer (closer to Velox)
- `CMakeLists.txt` - CMake build configuration
- `build.sh` - Build script for Prestissimo environment
- `SESSION_STATE.md` - This file

## Next Steps

Since the standalone reproducers cannot trigger the failure, investigation must move to Velox level:

1. **Create a Velox-level reproducer** - Use Velox's TableScan directly instead of raw cuDF
2. **Examine Velox's BufferedInputDataSource** - Check async I/O handling
3. **Add nvCOMP error code logging** - Modify cuDF or Velox to capture actual error codes
4. **Check for specific query patterns** - Does it only fail with certain filters or projections?
5. **Examine concurrent query behavior** - Does it only fail when multiple queries run?
6. **Review Velox's CUDA stream management** - Stream synchronization between components

### Files to Investigate in Velox
- `velox/experimental/cudf/connectors/hive/CudfHiveDataSource.cpp`
- `velox/experimental/cudf/connectors/hive/CudfHiveDataSourceHelpers.cpp`
- `velox/experimental/cudf/exec/GpuResources.cpp`

## Useful Commands

```bash
# Build C++ reproducer
./build.sh /prestissimo

# Run C++ reproducer
./build/reproducer /path/to/parquet/files 100 8

# Run Python reproducer
python reproducer.py /path/to/parquet/files --iterations 100 --parallel 8
```

## Related Links

- [nvCOMP Release Notes](https://docs.nvidia.com/cuda/nvcomp/release_notes.html)
- [cuDF GitHub](https://github.com/rapidsai/cudf)
- [nvCOMP GitHub](https://github.com/NVIDIA/nvcomp)
