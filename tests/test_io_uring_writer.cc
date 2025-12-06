#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <getopt.h>
#include <unistd.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <span>
#include <print>
#include <memory>
#include <bit>
#include <algorithm>
#include <random>
#include <cmath>
#include <filesystem>

#include "coro/task.h"
#include "util/util.h"
#include "util/thread_pool.h"
#include "util/metrics.h"
#include "wal/wal.h"
#include "wal/io_uring_writer.h"
#include "wal/io.h"

/* Local logger instance for this test file */
util::Logger<util::MT_logger_writer> g_logger(util::MT_logger_writer{std::cerr}, util::Log_level::Trace);

struct Test_config {
  std::size_t m_num_messages{1000000};
  std::size_t m_log_block_size{4096};
  std::size_t m_num_writers{1};
  std::size_t m_queue_depth{64};
  bool m_disable_writes{false};
  bool m_test_linked_sync{true};
  double m_sync_probability{0.01};  /* 1% of writes trigger sync */
  std::size_t m_message_size{512};  /* Size of each test message */
};

/**
 * Test basic write functionality
 */
static void test_basic_write(const Test_config& config) {
  std::println("[test_basic_write] Testing basic io_uring write operations");

  std::string path = config.m_disable_writes ? "/dev/null" : "/tmp/test_io_uring_basic.log";
  int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    std::println(stderr, "Failed to open test file: {}", std::strerror(errno));
    std::abort();
  }

  if (!config.m_disable_writes) {
    auto ret = ::posix_fallocate(fd, 0, 100 * 1024 * 1024);  /* 100MB */
    if (ret != 0) {
      std::println(stderr, "Failed to preallocate file: {}", std::strerror(errno));
    }
  }

  /* Create io_uring writer */
  /* Create thread pool for reaper coroutine */
  util::Thread_pool::Config reaper_pool_config(1, 1024);
  util::Thread_pool reaper_pool(reaper_pool_config);
  
  wal::Io_uring_writer::Config uring_config(fd, config.m_queue_depth, &reaper_pool);
  wal::Io_uring_writer writer(uring_config);

  /* Test data */
  const std::size_t num_writes = 1000;
  const std::size_t data_size = config.m_message_size;
  std::vector<std::byte> test_data(data_size);
  for (std::size_t i = 0; i < data_size; ++i) {
    test_data[i] = std::byte(static_cast<unsigned char>(i & 0xFF));
  }

  /* Prepare iovec */
  struct iovec iov;
  iov.iov_base = test_data.data();
  iov.iov_len = data_size;

  std::span<const struct iovec> iovecs(&iov, 1);

  /* Perform writes */
  auto start = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < num_writes; ++i) {
    auto task = writer.write_async(iovecs, wal::Log::Sync_type::None);
    auto result = task.get();  /* Block until completion */
    
    if (!result.has_value()) {
      std::println(stderr, "Write {} failed", i);
      std::abort();
    }
    
    if (result.value() != static_cast<wal::lsn_t>(data_size)) {
      std::println(stderr, "Write {} returned wrong size: expected {}, got {}", 
                   i, data_size, result.value());
      std::abort();
    }
  }
  auto end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  std::println("[test_basic_write] Completed {} writes of {} bytes in {} ms ({:.2f} MB/s)",
               num_writes, data_size, duration.count(),
               static_cast<double>(num_writes * data_size) / (1024.0 * 1024.0) / (static_cast<double>(duration.count()) / 1000.0));

  ::close(fd);
  if (!config.m_disable_writes) {
    std::filesystem::remove(path);
  }
}

/**
 * Test linked sync operations (write + fdatasync/fsync)
 */
static void test_linked_sync(const Test_config& config) {
  std::println("[test_linked_sync] Testing linked write+sync operations");

  std::string path = config.m_disable_writes ? "/dev/null" : "/tmp/test_io_uring_sync.log";
  int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    std::println(stderr, "Failed to open test file: {}", std::strerror(errno));
    std::abort();
  }

  if (!config.m_disable_writes) {
    auto ret = ::posix_fallocate(fd, 0, 100 * 1024 * 1024);
    if (ret != 0) {
      std::println(stderr, "Failed to preallocate file: {}", std::strerror(errno));
    }
  }

  /* Create thread pool for reaper coroutine */
  util::Thread_pool::Config reaper_pool_config(1, 1024);
  util::Thread_pool reaper_pool(reaper_pool_config);
  
  wal::Io_uring_writer::Config uring_config(fd, config.m_queue_depth, &reaper_pool);
  wal::Io_uring_writer writer(uring_config);

  const std::size_t num_writes = 100;
  const std::size_t data_size = config.m_message_size;
  std::vector<std::byte> test_data(data_size);
  for (std::size_t i = 0; i < data_size; ++i) {
    test_data[i] = std::byte(static_cast<unsigned char>(i & 0xFF));
  }

  struct iovec iov;
  iov.iov_base = test_data.data();
  iov.iov_len = data_size;
  std::span<const struct iovec> iovecs(&iov, 1);

  /* Test fdatasync */
  std::println("[test_linked_sync] Testing fdatasync...");
  auto start = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < num_writes; ++i) {
    auto task = writer.write_async(iovecs, wal::Log::Sync_type::Fdatasync);
    auto result = task.get();
    
    if (!result.has_value()) {
      std::println(stderr, "Write+sync {} failed", i);
      std::abort();
    }
  }
  auto end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::println("[test_linked_sync] Completed {} write+fdatasync operations in {} ms",
               num_writes, duration.count());

  /* Test fsync */
  std::println("[test_linked_sync] Testing fsync...");
  start = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < num_writes; ++i) {
    auto task = writer.write_async(iovecs, wal::Log::Sync_type::Fsync);
    auto result = task.get();
    
    if (!result.has_value()) {
      std::println(stderr, "Write+sync {} failed", i);
      std::abort();
    }
  }
  end = std::chrono::steady_clock::now();
  duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::println("[test_linked_sync] Completed {} write+fsync operations in {} ms",
               num_writes, duration.count());

  ::close(fd);
  if (!config.m_disable_writes) {
    std::filesystem::remove(path);
  }
}

/**
 * Test concurrent writes from multiple coroutines
 */
static Task<void> concurrent_writer(
    wal::Io_uring_writer* writer,
    util::Thread_pool* thread_pool,
    std::span<const struct iovec> iovecs,
    std::size_t num_writes,
    std::atomic<std::size_t>& completed) {
  
  /* Schedule this coroutine on the thread pool */
  co_await thread_pool->schedule();
  
  for (std::size_t i = 0; i < num_writes; ++i) {
    auto task = writer->write_async(iovecs, wal::Log::Sync_type::None);
    auto result = co_await task;
    
    if (!result.has_value()) {
      std::println(stderr, "Concurrent write failed");
      std::abort();
    }
    
    completed.fetch_add(1, std::memory_order_relaxed);
  }
  
  co_return;
}

static void test_concurrent_writes(const Test_config& config) {
  std::println("[test_concurrent_writes] Testing concurrent writes from {} coroutines",
               config.m_num_writers);

  std::string path = config.m_disable_writes ? "/dev/null" : "/tmp/test_io_uring_concurrent.log";
  int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    std::println(stderr, "Failed to open test file: {}", std::strerror(errno));
    std::abort();
  }

  if (!config.m_disable_writes) {
    auto ret = ::posix_fallocate(fd, 0, 500 * 1024 * 1024);
    if (ret != 0) {
      std::println(stderr, "Failed to preallocate file: {}", std::strerror(errno));
    }
  }

  /* Create thread pool for reaper coroutine */
  util::Thread_pool::Config reaper_pool_config(1, 1024);
  util::Thread_pool reaper_pool(reaper_pool_config);
  
  wal::Io_uring_writer::Config uring_config(fd, config.m_queue_depth, &reaper_pool);
  wal::Io_uring_writer writer(uring_config);

  const std::size_t writes_per_writer = 1000;
  const std::size_t data_size = config.m_message_size;
  std::vector<std::byte> test_data(data_size);
  for (std::size_t i = 0; i < data_size; ++i) {
    test_data[i] = std::byte(static_cast<unsigned char>(i & 0xFF));
  }

  struct iovec iov;
  iov.iov_base = test_data.data();
  iov.iov_len = data_size;
  std::span<const struct iovec> iovecs(&iov, 1);

  /* Create thread pool for coroutines - need at least 2 threads to avoid deadlock
   * when io_uring_wait_cqe() blocks (one thread can process completions while
   * another waits) */
  const std::size_t pool_threads = std::max(config.m_num_writers, std::size_t(2));
  util::Thread_pool::Config pool_config(pool_threads, 1024);
  util::Thread_pool thread_pool(pool_config);

  std::atomic<std::size_t> completed{0};
  std::vector<Task<void>> tasks;
  tasks.reserve(config.m_num_writers);

  /* Start concurrent writers */
  auto start = std::chrono::steady_clock::now();
  for (std::size_t i = 0; i < config.m_num_writers; ++i) {
    tasks.emplace_back(concurrent_writer(&writer, &thread_pool, iovecs, writes_per_writer, completed));
    tasks.back().start_on_pool(thread_pool);
  }

  /* Wait for all tasks to complete */
  while (completed.load(std::memory_order_acquire) < config.m_num_writers * writes_per_writer) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  auto end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  std::println("[test_concurrent_writes] Completed {} writes from {} writers in {} ms ({:.2f} MB/s)",
               completed.load(), config.m_num_writers, duration.count(),
               static_cast<double>(completed.load() * data_size) / (1024.0 * 1024.0) / (static_cast<double>(duration.count()) / 1000.0));

  ::close(fd);
  if (!config.m_disable_writes) {
    std::filesystem::remove(path);
  }
}

/**
 * Performance test simulating Log service workload
 */
static Task<void> write_coroutine(
    wal::Io_uring_writer* writer,
    util::Thread_pool* thread_pool,
    std::span<const struct iovec> iovecs,
    std::size_t num_messages,
    double sync_probability,
    std::atomic<std::size_t>& write_count,
    std::atomic<std::size_t>& sync_count) {
  
  co_await thread_pool->schedule();
  
  std::mt19937 rng(std::random_device{}());
  std::uniform_real_distribution<double> dist(0.0, 1.0);
  
  for (std::size_t i = 0; i < num_messages; ++i) {
    wal::Log::Sync_type sync_type = wal::Log::Sync_type::None;
    if (dist(rng) < sync_probability) {
      sync_type = wal::Log::Sync_type::Fdatasync;
      sync_count.fetch_add(1, std::memory_order_relaxed);
    }
    
    auto task = writer->write_async(iovecs, sync_type);
    auto result = co_await task;
    
    if (!result.has_value()) {
      std::println(stderr, "Write {} failed", i);
      std::abort();
    }
    
    write_count.fetch_add(1, std::memory_order_relaxed);
  }
  
  co_return;
}

static void test_performance_log_service(const Test_config& config) {
  std::println("[test_performance_log_service] Simulating Log service workload");
  std::println("  messages: {}, sync_probability: {:.2f}, message_size: {}",
               config.m_num_messages, config.m_sync_probability * 100.0, config.m_message_size);

  std::string path = config.m_disable_writes ? "/dev/null" : "/tmp/test_io_uring_perf.log";
  int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    std::println(stderr, "Failed to open test file: {}", std::strerror(errno));
    std::abort();
  }

  if (!config.m_disable_writes) {
    auto ret = ::posix_fallocate(fd, 0, 1024 * 1024 * 1024);  /* 1GB */
    if (ret != 0) {
      std::println(stderr, "Failed to preallocate file: {}", std::strerror(errno));
    }
  }

  /* Create thread pool for reaper coroutine */
  util::Thread_pool::Config reaper_pool_config(1, 1024);
  util::Thread_pool reaper_pool(reaper_pool_config);
  
  wal::Io_uring_writer::Config uring_config(fd, config.m_queue_depth, &reaper_pool);
  wal::Io_uring_writer writer(uring_config);

  /* Create thread pool */
  util::Thread_pool::Config pool_config(1, 1024);  /* Single I/O thread */
  util::Thread_pool thread_pool(pool_config);

  /* Prepare test data */
  const std::size_t data_size = config.m_message_size;
  std::vector<std::byte> test_data(data_size);
  for (std::size_t i = 0; i < data_size; ++i) {
    test_data[i] = std::byte(static_cast<unsigned char>(i & 0xFF));
  }

  struct iovec iov;
  iov.iov_base = test_data.data();
  iov.iov_len = data_size;
  std::span<const struct iovec> iovecs(&iov, 1);

  /* Metrics */
  std::atomic<std::size_t> sync_count{0};
  std::atomic<std::size_t> write_count{0};

  /* Run performance test */
  auto start = std::chrono::steady_clock::now();
  auto perf_task = write_coroutine(&writer, &thread_pool, iovecs, config.m_num_messages, 
                                   config.m_sync_probability, write_count, sync_count);
  perf_task.start_on_pool(thread_pool);
  
  /* Wait for completion */
  while (write_count.load(std::memory_order_acquire) < config.m_num_messages) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  
  auto end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  /* Print results */
  std::println("[test_performance_log_service] Results:");
  std::println("  Total writes: {}", write_count.load());
  std::println("  Sync operations: {}", sync_count.load());
  std::println("  Duration: {} ms", duration.count());
  std::println("  Throughput: {:.2f} MB/s",
               static_cast<double>(config.m_num_messages * data_size) / (1024.0 * 1024.0) / (static_cast<double>(duration.count()) / 1000.0));
  std::println("  Writes/sec: {:.0f}",
               static_cast<double>(config.m_num_messages) / (static_cast<double>(duration.count()) / 1000.0));

  ::close(fd);
  if (!config.m_disable_writes) {
    std::filesystem::remove(path);
  }
}

/**
 * Test error handling
 */
static void test_error_handling(const Test_config& /*config*/) {
  std::println("[test_error_handling] Testing error handling");

  /* Test with invalid file descriptor */
  try {
    wal::Io_uring_writer::Config uring_config(-1, 64);
    wal::Io_uring_writer writer(uring_config);
    std::println(stderr, "Should have thrown exception for invalid FD");
    std::abort();
  } catch (const std::exception& e) {
    std::println("[test_error_handling] Correctly caught exception for invalid FD: {}", e.what());
  }

  /* Test with non-power-of-2 queue depth */
  int fd = ::open("/dev/null", O_WRONLY);
  if (fd < 0) {
    std::println(stderr, "Failed to open /dev/null");
    std::abort();
  }

  try {
    wal::Io_uring_writer::Config uring_config(fd, 63);  /* Not a power of 2 */
    wal::Io_uring_writer writer(uring_config);
    std::println(stderr, "Should have thrown exception for non-power-of-2 queue depth");
    std::abort();
  } catch (const std::exception& e) {
    std::println("[test_error_handling] Correctly caught exception for invalid queue depth: {}", e.what());
  }

  ::close(fd);
}

/**
 * Test with multiple iovecs (scatter-gather)
 */
static void test_multiple_iovecs(const Test_config& config) {
  std::println("[test_multiple_iovecs] Testing multiple iovec scatter-gather writes");

  std::string path = config.m_disable_writes ? "/dev/null" : "/tmp/test_io_uring_iovecs.log";
  int fd = ::open(path.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    std::println(stderr, "Failed to open test file: {}", std::strerror(errno));
    std::abort();
  }

  /* Create thread pool for reaper coroutine */
  util::Thread_pool::Config reaper_pool_config(1, 1024);
  util::Thread_pool reaper_pool(reaper_pool_config);
  
  wal::Io_uring_writer::Config uring_config(fd, config.m_queue_depth, &reaper_pool);
  wal::Io_uring_writer writer(uring_config);

  /* Create multiple buffers for scatter-gather */
  const std::size_t num_buffers = 4;
  const std::size_t buffer_size = 1024;
  std::vector<std::vector<std::byte>> buffers(num_buffers);
  std::vector<struct iovec> iovecs(num_buffers);

  for (std::size_t i = 0; i < num_buffers; ++i) {
    buffers[i].resize(buffer_size);
    std::fill(buffers[i].begin(), buffers[i].end(), std::byte(static_cast<unsigned char>(i)));
    
    iovecs[i].iov_base = buffers[i].data();
    iovecs[i].iov_len = buffer_size;
  }

  std::span<const struct iovec> iovecs_span(iovecs.data(), num_buffers);

  /* Perform write */
  auto task = writer.write_async(iovecs_span, wal::Log::Sync_type::None);
  auto result = task.get();

  if (!result.has_value()) {
    std::println(stderr, "Scatter-gather write failed");
    std::abort();
  }

  std::size_t expected_size = num_buffers * buffer_size;
  if (result.value() != static_cast<wal::lsn_t>(expected_size)) {
    std::println(stderr, "Scatter-gather write returned wrong size: expected {}, got {}",
                 expected_size, result.value());
    std::abort();
  }

  std::println("[test_multiple_iovecs] Successfully wrote {} buffers of {} bytes each ({} total)",
               num_buffers, buffer_size, expected_size);

  ::close(fd);
  if (!config.m_disable_writes) {
    std::filesystem::remove(path);
  }
}

int main(int argc, char* argv[]) {
  Test_config config;

  /* Parse command line arguments */
  static struct option long_options[] = {
    {"num-messages", required_argument, nullptr, 'm'},
    {"block-size", required_argument, nullptr, 'b'},
    {"num-writers", required_argument, nullptr, 'w'},
    {"queue-depth", required_argument, nullptr, 'q'},
    {"disable-writes", no_argument, nullptr, 'd'},
    {"no-linked-sync", no_argument, nullptr, 'n'},
    {"sync-probability", required_argument, nullptr, 'p'},
    {"message-size", required_argument, nullptr, 's'},
    {"help", no_argument, nullptr, 'h'},
    {nullptr, 0, nullptr, 0}
  };

  int opt;
  while ((opt = getopt_long(argc, argv, "m:b:w:q:dnp:s:h", long_options, nullptr)) != -1) {
    switch (opt) {
      case 'm':
        config.m_num_messages = std::stoull(optarg);
        break;
      case 'b':
        config.m_log_block_size = std::stoull(optarg);
        break;
      case 'w':
        config.m_num_writers = std::stoull(optarg);
        break;
      case 'q':
        config.m_queue_depth = std::stoull(optarg);
        break;
      case 'd':
        config.m_disable_writes = true;
        break;
      case 'n':
        config.m_test_linked_sync = false;
        break;
      case 'p':
        config.m_sync_probability = std::stod(optarg);
        break;
      case 's':
        config.m_message_size = std::stoull(optarg);
        break;
      case 'h':
        std::println("Usage: {} [options]", argv[0]);
        std::println("Options:");
        std::println("  -m, --num-messages NUM    Number of messages to write (default: 1000000)");
        std::println("  -b, --block-size NUM      Block size in bytes (default: 4096)");
        std::println("  -w, --num-writers NUM     Number of concurrent writers (default: 1)");
        std::println("  -q, --queue-depth NUM     io_uring queue depth, must be power of 2 (default: 64)");
        std::println("  -d, --disable-writes      Disable actual disk writes (use /dev/null)");
        std::println("  -n, --no-linked-sync      Disable testing linked sync operations");
        std::println("  -p, --sync-probability NUM Sync probability 0.0-1.0 (default: 0.01)");
        std::println("  -s, --message-size NUM    Size of each message in bytes (default: 512)");
        std::println("  -h, --help                Show this help message");
        return EXIT_SUCCESS;
      default:
        return EXIT_FAILURE;
    }
  }

  /* Ensure queue depth is power of 2 */
  if (!std::has_single_bit(config.m_queue_depth)) {
    std::println(stderr, "Error: queue-depth must be a power of 2 (got {})", config.m_queue_depth);
    return EXIT_FAILURE;
  }

  std::println("=== io_uring Writer Test Suite ===");
  std::println("Configuration:");
  std::println("  num_messages: {}", config.m_num_messages);
  std::println("  block_size: {}", config.m_log_block_size);
  std::println("  num_writers: {}", config.m_num_writers);
  std::println("  queue_depth: {}", config.m_queue_depth);
  std::println("  disable_writes: {}", config.m_disable_writes);
  std::println("  test_linked_sync: {}", config.m_test_linked_sync);
  std::println("  sync_probability: {:.2f}%", config.m_sync_probability * 100.0);
  std::println("  message_size: {}", config.m_message_size);
  std::println("");

  try {
    /* Run test suite */
    test_basic_write(config);
    std::println("");

    if (config.m_test_linked_sync) {
      test_linked_sync(config);
      std::println("");
    }

    test_multiple_iovecs(config);
    std::println("");

    test_concurrent_writes(config);
    std::println("");

    test_performance_log_service(config);
    std::println("");

    test_error_handling(config);
    std::println("");

    std::println("=== All tests passed ===");
    return EXIT_SUCCESS;
  } catch (const std::exception& e) {
    std::println(stderr, "Test failed with exception: {}", e.what());
    return EXIT_FAILURE;
  }
}
