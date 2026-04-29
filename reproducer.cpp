/*
 * nvCOMP/cuDF ZSTD Decompression Reproducer (C++)
 *
 * This reproducer mimics how Velox/Prestissimo reads Parquet files using
 * libcudf with async memory allocation.
 *
 * Build:
 *   nvcc -o reproducer reproducer.cpp \
 *     -I/path/to/cudf/include \
 *     -I/path/to/rmm/include \
 *     -L/path/to/cudf/lib -lcudf \
 *     -L/path/to/rmm/lib -lrmm \
 *     -std=c++17
 *
 * Or use CMake (see CMakeLists.txt)
 *
 * Usage:
 *   ./reproducer /path/to/parquet/files [iterations] [parallel_threads]
 */

#include <cudf/io/parquet.hpp>
#include <cudf/io/types.hpp>
#include <cudf/table/table.hpp>

#include <rmm/cuda_stream.hpp>
#include <rmm/cuda_stream_pool.hpp>
#include <rmm/mr/device/cuda_async_memory_resource.hpp>
#include <rmm/mr/device/per_device_resource.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;

// Global state
std::mutex cout_mutex;
std::atomic<int> failure_count{0};
std::atomic<int> success_count{0};

struct FailureInfo {
  int iteration;
  std::string file;
  std::string error;
};

std::vector<FailureInfo> failures;
std::mutex failures_mutex;

// Collect all parquet files recursively
std::vector<std::string> collect_parquet_files(const std::string& path) {
  std::vector<std::string> files;

  if (fs::is_regular_file(path)) {
    if (path.ends_with(".parquet")) {
      files.push_back(path);
    }
  } else if (fs::is_directory(path)) {
    for (const auto& entry : fs::recursive_directory_iterator(path)) {
      if (entry.is_regular_file() &&
          entry.path().extension() == ".parquet") {
        files.push_back(entry.path().string());
      }
    }
  }

  return files;
}

// Read a single parquet file using chunked reader (like Velox does)
void read_parquet_file(const std::string& filepath, int iteration,
                       rmm::cuda_stream_view stream,
                       rmm::device_async_resource_ref mr) {
  try {
    // Build parquet reader options
    auto source = cudf::io::source_info(filepath);
    auto options = cudf::io::parquet_reader_options::builder(source).build();

    // Use chunked reader like Velox does
    // chunk_read_limit = 0 means no limit (read all at once)
    // pass_read_limit = 0 means no limit
    cudf::io::chunked_parquet_reader reader(0, 0, options, stream, mr);

    int64_t total_rows = 0;
    while (reader.has_next()) {
      auto chunk = reader.read_chunk();
      total_rows += chunk.tbl->num_rows();
    }

    success_count++;

  } catch (const std::exception& e) {
    failure_count++;

    std::lock_guard<std::mutex> lock(failures_mutex);
    failures.push_back({iteration, filepath, e.what()});

    std::lock_guard<std::mutex> cout_lock(cout_mutex);
    std::cerr << "\n============================================================\n";
    std::cerr << "FAILURE at iteration " << iteration << "\n";
    std::cerr << "File: " << filepath << "\n";
    std::cerr << "Error: " << e.what() << "\n";
    std::cerr << "============================================================\n\n";
  }
}

// Global stream pool (created in main)
std::unique_ptr<rmm::cuda_stream_pool> stream_pool;

// Worker thread function
void worker_thread(const std::vector<std::string>& files,
                   int iterations,
                   int thread_id,
                   int num_threads,
                   rmm::device_async_resource_ref mr) {
  // Each thread gets its own stream from the pool
  auto stream = stream_pool->get_stream();

  for (int iter = 1; iter <= iterations; iter++) {
    // Distribute files across threads
    for (size_t i = thread_id; i < files.size(); i += num_threads) {
      read_parquet_file(files[i], iter, stream, mr);
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0]
              << " <parquet_path> [iterations] [parallel_threads]\n";
    std::cerr << "\n";
    std::cerr << "Arguments:\n";
    std::cerr << "  parquet_path      Path to parquet file or directory\n";
    std::cerr << "  iterations        Number of iterations (default: 100)\n";
    std::cerr << "  parallel_threads  Number of parallel threads (default: 4)\n";
    return 1;
  }

  std::string parquet_path = argv[1];
  int iterations = (argc > 2) ? std::stoi(argv[2]) : 100;
  int num_threads = (argc > 3) ? std::stoi(argv[3]) : 4;

  // Setup async memory resource (like Velox does)
  std::cout << "Setting up CUDA async memory resource...\n";
  auto async_mr = std::make_shared<rmm::mr::cuda_async_memory_resource>();
  rmm::mr::set_current_device_resource(async_mr.get());

  // Create stream pool (like Velox uses)
  stream_pool = std::make_unique<rmm::cuda_stream_pool>(num_threads);

  // Collect parquet files
  std::cout << "Collecting parquet files from: " << parquet_path << "\n";
  auto files = collect_parquet_files(parquet_path);

  if (files.empty()) {
    std::cerr << "ERROR: No parquet files found in " << parquet_path << "\n";
    return 1;
  }

  // Calculate total size
  uint64_t total_size = 0;
  for (const auto& f : files) {
    total_size += fs::file_size(f);
  }

  std::cout << "Found " << files.size() << " parquet file(s)\n";
  std::cout << "Total size: " << total_size << " bytes ("
            << (total_size / 1024.0 / 1024.0) << " MB)\n";
  std::cout << "Iterations: " << iterations << "\n";
  std::cout << "Parallel threads: " << num_threads << "\n";
  std::cout << "\n";

  auto start_time = std::chrono::steady_clock::now();

  // Launch worker threads
  std::vector<std::thread> threads;
  for (int t = 0; t < num_threads; t++) {
    threads.emplace_back(worker_thread, std::ref(files), iterations, t,
                         num_threads, async_mr.get());
  }

  // Wait for all threads to complete
  for (auto& t : threads) {
    t.join();
  }

  auto end_time = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                     end_time - start_time)
                     .count();

  // Summary
  std::cout << "\n";
  std::cout << "============================================================\n";
  std::cout << "SUMMARY\n";
  std::cout << "============================================================\n";
  std::cout << "Total iterations: " << iterations << "\n";
  std::cout << "Total files per iteration: " << files.size() << "\n";
  std::cout << "Parallel threads: " << num_threads << "\n";
  std::cout << "Total reads attempted: " << (iterations * files.size()) << "\n";
  std::cout << "Successful reads: " << success_count.load() << "\n";
  std::cout << "Failed reads: " << failure_count.load() << "\n";
  std::cout << "Total time: " << (elapsed / 1000.0) << " seconds\n";

  if (!failures.empty()) {
    std::cout << "\nFailures:\n";
    for (const auto& f : failures) {
      std::cout << "  - Iteration " << f.iteration << ": " << f.file << "\n";
      std::cout << "    Error: " << f.error << "\n";
    }
    return 1;
  } else {
    std::cout << "\nAll iterations passed successfully\n";
    return 0;
  }
}
