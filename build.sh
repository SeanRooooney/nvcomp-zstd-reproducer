#!/bin/bash
# Build script for nvCOMP ZSTD reproducer
#
# Usage:
#   ./build.sh /path/to/prestissimo
#
# Example:
#   ./build.sh /prestissimo

set -e

PRESTISSIMO_PATH="${1:-/prestissimo}"

if [ ! -d "$PRESTISSIMO_PATH" ]; then
    echo "ERROR: Prestissimo path not found: $PRESTISSIMO_PATH"
    echo "Usage: ./build.sh /path/to/prestissimo"
    exit 1
fi

BUILD_DIR="$PRESTISSIMO_PATH/_build/release/_deps"

# Verify dependencies exist
if [ ! -f "$BUILD_DIR/cudf-src/cpp/include/cudf/io/parquet.hpp" ]; then
    echo "ERROR: cudf headers not found at $BUILD_DIR/cudf-src/cpp/include"
    exit 1
fi

echo "Using Prestissimo build at: $PRESTISSIMO_PATH"
echo "Dependencies at: $BUILD_DIR"

mkdir -p build
cd build

cmake .. \
    -DCUDF_INCLUDE_DIR="$BUILD_DIR/cudf-src/cpp/include" \
    -DCUDF_LIBRARY="$BUILD_DIR/cudf-build/libcudf.so" \
    -DRMM_INCLUDE_DIR="$BUILD_DIR/rmm-src/cpp/include" \
    -DRMM_LIBRARY="$BUILD_DIR/rmm-build/librmm.so" \
    -DCMAKE_CXX_FLAGS="-I$BUILD_DIR/cccl-src/libcudacxx/include -I$BUILD_DIR/cccl-src/thrust -I$BUILD_DIR/cccl-src/cub"

make

echo ""
echo "Build complete. Run with:"
echo "  ./build/reproducer /path/to/parquet/files --iterations 50 --threads 5"
