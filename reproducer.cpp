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
#include <rmm/mr/cuda_async_memory_resource.hpp>
#include <rmm/mr/per_device_resource.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <set>
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
  std::set<std::string> file_set;  // Use set to deduplicate

  if (fs::is_regular_file(path)) {
    if (path.ends_with(".parquet")) {
      file_set.insert(fs::canonical(path).string());
    }
  } else if (fs::is_directory(path)) {
    for (const auto& entry : fs::recursive_directory_iterator(path)) {
      if (entry.is_regular_file() &&
          entry.path().extension() == ".parquet") {
        file_set.insert(fs::canonical(entry.path()).string());
      }
    }
  }

  return std::vector<std::string>(file_set.begin(), file_set.end());
}

// Velox/Prestissimo chunk limits (from environment or defaults)
// PARQUET_READER_CHUNK_READ_LIMIT=4294967296 (4GB)
// PARQUET_READER_PASS_READ_LIMIT=17179869184 (16GB)
std::size_t chunk_read_limit = 4294967296UL;   // 4GB
std::size_t pass_read_limit = 17179869184UL;   // 16GB

// Read a single parquet file using chunked reader (like Velox does)
void read_parquet_file(const std::string& filepath, int iteration,
                       rmm::cuda_stream_view stream,
                       rmm::device_async_resource_ref mr) {
  try {
    // Build parquet reader options
    auto source = cudf::io::source_info(filepath);
    auto options = cudf::io::parquet_reader_options::builder(source).build();

    // Use chunked reader like Velox does with same limits
    cudf::io::chunked_parquet_reader reader(chunk_read_limit, pass_read_limit, options, stream, mr);

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

// Track iteration progress
std::atomic<int> completed_iterations{0};
int total_iterations = 0;
uint64_t total_file_size = 0;

// Synchronization for iteration timing
std::atomic<int> threads_done{0};
int current_iteration = 0;
std::chrono::steady_clock::time_point iteration_start_time;

// File info (path + size)
struct FileInfo {
  std::string path;
  uint64_t size;
};

// Worker thread function
void worker_thread(const std::vector<FileInfo>& files,
                   int iterations,
                   int thread_id,
                   int num_threads,
                   rmm::device_async_resource_ref mr) {
  // Each thread gets its own stream from the pool
  auto stream = stream_pool->get_stream();

  for (int iter = 1; iter <= iterations; iter++) {
    // Thread 0 starts the iteration timing
    if (thread_id == 0) {
      iteration_start_time = std::chrono::steady_clock::now();
      threads_done = 0;
      current_iteration = iter;
    }

    // Simple barrier - wait for thread 0 to set up
    while (current_iteration < iter) {
      std::this_thread::yield();
    }

    // Distribute files across threads
    for (size_t i = thread_id; i < files.size(); i += num_threads) {
      read_parquet_file(files[i].path, iter, stream, mr);
    }

    // Mark this thread as done
    int done = ++threads_done;

    // Last thread to finish reports progress
    if (done == num_threads) {
      auto iteration_end_time = std::chrono::steady_clock::now();
      auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                            iteration_end_time - iteration_start_time)
                            .count();
      double elapsed_sec = elapsed_ms / 1000.0;
      double bandwidth_gbps = (static_cast<double>(total_file_size) / (1024.0 * 1024.0 * 1024.0)) / elapsed_sec;

      int completed = ++completed_iterations;
      std::cout << "Iteration " << completed << "/" << total_iterations
                << " | " << std::fixed << std::setprecision(2) << elapsed_sec << "s"
                << " | " << std::setprecision(2) << bandwidth_gbps << " GB/s"
                << " (successes: " << success_count.load()
                << ", failures: " << failure_count.load() << ")\n";
    }

    // Wait for reporting to complete before next iteration
    while (threads_done < num_threads) {
      std::this_thread::yield();
    }
  }
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0]
              << " <parquet_path> [iterations] [parallel_threads] [chunk_limit_gb] [pass_limit_gb]\n";
    std::cerr << "\n";
    std::cerr << "Arguments:\n";
    std::cerr << "  parquet_path      Path to parquet file or directory\n";
    std::cerr << "  iterations        Number of iterations (default: 100)\n";
    std::cerr << "  parallel_threads  Number of parallel threads (default: 4)\n";
    std::cerr << "  chunk_limit_gb    Chunk read limit in GB (default: 4, Velox default)\n";
    std::cerr << "  pass_limit_gb     Pass read limit in GB (default: 16, Velox default)\n";
    return 1;
  }

  std::string parquet_path = argv[1];
  int iterations = (argc > 2) ? std::stoi(argv[2]) : 100;
  int num_threads = (argc > 3) ? std::stoi(argv[3]) : 4;

  // Allow overriding chunk limits via command line (in GB)
  if (argc > 4) {
    chunk_read_limit = static_cast<std::size_t>(std::stod(argv[4]) * 1024 * 1024 * 1024);
  }
  if (argc > 5) {
    pass_read_limit = static_cast<std::size_t>(std::stod(argv[5]) * 1024 * 1024 * 1024);
  }

  std::cout << "Chunk read limit: " << (chunk_read_limit / 1024.0 / 1024.0 / 1024.0) << " GB\n";
  std::cout << "Pass read limit: " << (pass_read_limit / 1024.0 / 1024.0 / 1024.0) << " GB\n";

  total_iterations = iterations;

  // Setup async memory resource (like Velox does)
  std::cout << "Setting up CUDA async memory resource...\n";
  auto async_mr = std::make_shared<rmm::mr::cuda_async_memory_resource>();
  rmm::mr::set_current_device_resource(async_mr.get());

  // Create stream pool (like Velox uses)
  stream_pool = std::make_unique<rmm::cuda_stream_pool>(num_threads);

  // Collect parquet files
  std::cout << "Collecting parquet files from: " << parquet_path << "\n";
  auto file_paths = collect_parquet_files(parquet_path);

  if (file_paths.empty()) {
    std::cerr << "ERROR: No parquet files found in " << parquet_path << "\n";
    return 1;
  }

  // Build file info with sizes
  std::vector<FileInfo> files;
  files.reserve(file_paths.size());
  uint64_t total_size = 0;
  for (const auto& path : file_paths) {
    uint64_t size = fs::file_size(path);
    files.push_back({path, size});
    total_size += size;
  }
  total_file_size = total_size;

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
  double total_bytes_read = static_cast<double>(total_file_size) * iterations;
  double total_elapsed_sec = elapsed / 1000.0;
  double avg_bandwidth_gbps = (total_bytes_read / (1024.0 * 1024.0 * 1024.0)) / total_elapsed_sec;

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
  std::cout << "Total time: " << std::fixed << std::setprecision(2) << total_elapsed_sec << " seconds\n";
  std::cout << "Total data read: " << std::setprecision(2) << (total_bytes_read / (1024.0 * 1024.0 * 1024.0)) << " GB\n";
  std::cout << "Average bandwidth: " << std::setprecision(2) << avg_bandwidth_gbps << " GB/s\n";

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
