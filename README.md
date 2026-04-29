# nvCOMP ZSTD Decompression Reproducer

Reproducer for intermittent ZSTD decompression failures when reading small Parquet files via cuDF/nvCOMP.

## Problem Description

- **Symptom**: Intermittent "Error during decompression" when reading ZSTD-compressed Parquet files
- **Environment**: cuDF 26.06, nvCOMP 5.2.0
- **Conditions**:
  - Small Parquet files
  - ZSTD compression (Snappy works fine)
  - GPU path only (CPU decompression works)
  - Non-deterministic (same files sometimes work, sometimes fail)

## Error Message

```
CUDF failure at: cpp/src/io/parquet/reader_impl_chunking_utils.cu:622: Error during decompression
```

## Related nvCOMP Known Issue

From [nvCOMP release notes](https://docs.nvidia.com/cuda/nvcomp/release_notes.html):

> "Zstd decompression fails when decompressing buffers compressed with compression level 18 and higher using the Zstd library version 1.5.6. To workaround the problem temporarily, you can provide 1.5x the scratch required by nvcompBatchedZstdDecompressGetTempSizeAsync to nvcompBatchedZstdDecompressAsync."

Note: The documented issue is deterministic, but our observed behavior is intermittent, suggesting a potentially different (or related) root cause.

## Reproducers

Two reproducers are provided:

1. **Python** (`reproducer.py`) - Uses cuDF Python bindings
2. **C++** (`reproducer.cpp`) - Uses libcudf directly, closer to Velox/Prestissimo

The C++ reproducer mimics how Velox reads Parquet files:
- Uses `cudf::io::chunked_parquet_reader`
- Uses `rmm::mr::cuda_async_memory_resource` (async memory)
- Uses streams from `cudf::detail::global_cuda_stream_pool()`
- Multi-threaded parallel reads

## Setup

### Python Setup

#### Option 1: Conda (recommended)

```bash
conda create -n cudf_test python=3.11
conda activate cudf_test
conda install -c rapidsai -c conda-forge -c nvidia cudf=26.06 python=3.11 cuda-version=12.0
```

#### Option 2: pip

```bash
pip install cudf-cu12 --extra-index-url=https://pypi.nvidia.com
```

### C++ Setup

Requires cuDF and RMM libraries installed. Build with CMake:

```bash
mkdir build && cd build
cmake .. -DCUDF_INCLUDE_DIR=/path/to/cudf/include \
         -DCUDF_LIBRARY=/path/to/cudf/lib/libcudf.so \
         -DRMM_INCLUDE_DIR=/path/to/rmm/include
make
```

Or if cudf/rmm are installed system-wide or via conda:

```bash
mkdir build && cd build
cmake ..
make
```

## Usage

### Python

```bash
# Basic usage - run 100 iterations on all parquet files in a directory
python reproducer.py /path/to/parquet/files

# Specify number of iterations
python reproducer.py /path/to/parquet/files --iterations 500

# Parallel reads (stress GPU)
python reproducer.py /path/to/parquet/files --iterations 100 --parallel 8

# Verbose output (show each file read)
python reproducer.py /path/to/parquet/files --verbose

# Single file
python reproducer.py /path/to/file.parquet --iterations 1000
```

### C++

```bash
# Basic usage
./reproducer /path/to/parquet/files

# Specify iterations and threads
./reproducer /path/to/parquet/files 100 8
```

## Expected Output

### Success
```
cuDF version: 26.06.xx
Found 10 Parquet file(s)
Total size: 1,234,567 bytes (1.18 MB)

Iteration 1/100 completed in 0.15s
Iteration 2/100 completed in 0.12s
...
============================================================
SUMMARY
============================================================
Total iterations: 100
Total files per iteration: 10
Total reads attempted: 1000
Total failures: 0
Total time: 12.34 seconds

All iterations passed successfully
```

### Failure
```
============================================================
FAILURE at iteration 42
File: /path/to/some_file.parquet
Error: CUDF failure at: cpp/src/io/parquet/reader_impl_chunking_utils.cu:622: Error during decompression
Elapsed time: 5.23 seconds
============================================================
```

## Collecting Debug Information

If you reproduce the failure, please collect:

1. cuDF version: `python -c "import cudf; print(cudf.__version__)"`
2. GPU info: `nvidia-smi`
3. File that failed (if shareable)
4. Parquet metadata: `python -c "import pyarrow.parquet as pq; print(pq.read_metadata('failing_file.parquet'))"`
