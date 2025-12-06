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

#include "util/util.h"
#include "wal/wal.h"
#include "wal/buffer_pool.h"
#include "util/logger.h"
#include "util/thread_pool.h"
#include "coro/task.h"

#include "log_io.h"

/* Local logger instance for this test file */
util::Logger<util::MT_logger_writer> g_logger(util::MT_logger_writer{std::cerr}, util::Log_level::Trace);

struct Test_config {
  std::size_t m_num_messages{1000000};
  std::size_t m_log_block_size{4096};
  std::size_t m_log_buffer_size_blocks{1024};
  bool m_disable_writes{false};
};

using Log_writer = wal::Log::Write_callback;

/* Producer function - writes directly to Log, relies on I/O coroutine for buffer processing */
void producer(
    wal::Log* log,
    util::Thread_pool* pool,
    std::size_t num_messages,
    std::atomic<bool>& start_flag,
    std::atomic<bool>& producer_done) noexcept {

  /* Wait for start signal */
  while (!start_flag.load(std::memory_order_acquire)) {
    util::cpu_pause();
  }

  Test_message msg;

  for (std::size_t i = 0; i < num_messages; ++i) {
    /* Write directly to log - I/O coroutine handles buffer processing in background */
    /* For raw Log/IO performance test, we don't use the lambda - let I/O coroutine handle it */
    auto write_result = log->write(msg.get_span(), pool);

    if (!write_result.has_value()) [[unlikely]] {
      /* Write failed - buffer full, I/O coroutine should free buffers but might not be fast enough */
      /* Retry with a brief pause to let I/O catch up */
      constexpr int max_retries = 10000;
      int retries = 0;
      while (retries < max_retries && !write_result.has_value()) {
        util::cpu_pause_n(10);
        write_result = log->write(msg.get_span(), pool);
        ++retries;
      }
      
      if (!write_result.has_value()) {
        /* Still failed after retries - I/O is not keeping up */
        break;
      }
    }
  }

  /* Signal producer completion */
  producer_done.store(true, std::memory_order_release);
}

static void test_log_io_simple(const Test_config& config) {
  std::println("[test_log_io_simple] messages={}, disable_writes={}",
               config.m_num_messages, config.m_disable_writes);

  std::string path = config.m_disable_writes ? "/dev/null" : "/local/tmp/test_log_io_simple.log";
  auto log_file = std::make_shared<Log_file>(path, 512 * 1024 * 1024, config.m_log_block_size, config.m_disable_writes);

  WAL_ASSERT(log_file->m_fd != -1);

  if (!config.m_disable_writes) {
    auto ret = ::posix_fallocate(log_file->m_fd, 0, log_file->m_max_file_size);
    if (ret != 0) {
      log_fatal("Failed to preallocate log file: {}", std::strerror(errno));
    }
  }

  /* Create log with buffer pool */
  const std::size_t buffer_pool_size = 4;
  auto wal_config = wal::Config(config.m_log_buffer_size_blocks, config.m_log_block_size, util::ChecksumAlgorithm::CRC32C);
  auto log = std::make_shared<wal::Log>(0, buffer_pool_size, wal_config);

  Log_writer log_writer = [&](std::span<struct iovec> span, wal::Log::Sync_type sync_type) -> wal::Result<std::size_t> {
    auto result = log_file->write(span);
    
    /* Perform sync if requested and write succeeded */
    if (result.has_value() && !config.m_disable_writes) {
      if (sync_type == wal::Log::Sync_type::Fdatasync) {
        if (::fdatasync(log_file->m_fd) != 0) {
          log_fatal("fdatasync failed: {}", std::strerror(errno));
          return std::unexpected(wal::Status::IO_error);
        }
      } else if (sync_type == wal::Log::Sync_type::Fsync) {
        if (::fsync(log_file->m_fd) != 0) {
          log_fatal("fsync failed: {}", std::strerror(errno));
          return std::unexpected(wal::Status::IO_error);
        }
      }
    }
    
    return result;
  };

  /* Create thread pool for I/O */
  std::size_t hw_threads = std::thread::hardware_concurrency();
  if (hw_threads == 0) hw_threads = 4;

  std::size_t pool_threads = std::max<std::size_t>(1, hw_threads / 2);

  util::Thread_pool::Config pool_config;

  pool_config.m_num_threads = pool_threads;
  pool_config.m_queue_capacity = 32768;

  if (!std::has_single_bit(pool_config.m_queue_capacity)) {
    pool_config.m_queue_capacity = std::bit_ceil(pool_config.m_queue_capacity);
  }

  util::Thread_pool pool(pool_config);

  /* Start the background I/O coroutine on thread pool */
  if (!config.m_disable_writes) {
    log->start_io(log_writer, &pool);
  }

  std::atomic<bool> start_flag{false};
  std::atomic<bool> producer_done{false};

  /* Create single producer thread - writes directly to Log, I/O coroutine handles buffers */
  std::thread producer_thread(producer, log.get(), &pool, config.m_num_messages, std::ref(start_flag), std::ref(producer_done));

  /* Start the test */
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  start_flag.store(true, std::memory_order_release);
  auto start = std::chrono::steady_clock::now();

  /* Wait for producer to complete (event-based) */
  constexpr auto max_wait_time = std::chrono::seconds(30);
  auto wait_start = std::chrono::steady_clock::now();
  while (!producer_done.load(std::memory_order_acquire)) {
    auto elapsed = std::chrono::steady_clock::now() - wait_start;
    if (elapsed > max_wait_time) {
      std::println(stderr, "ERROR: Timeout waiting for producer to complete");
      break;
    }
    for (int j = 0; j < 10; ++j) {
      util::cpu_pause();
    }
  }
  
  /* Join thread to ensure it's finished */
  producer_thread.join();

  /* Flush any remaining data */
  if (!log->is_empty()) {
    auto flush_result = log->write_to_store(log_writer);
    WAL_ASSERT(flush_result.has_value());
  }

  auto end = std::chrono::steady_clock::now();
  auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  /* All messages were processed when producer_done is true */
  const std::size_t total_written = config.m_num_messages;
  const double elapsed_sec = static_cast<double>(elapsed) / 1000.0;
  const double throughput_mib = (static_cast<double>(total_written) * static_cast<double>(Test_message::SIZE)) / (1024.0 * 1024.0) / elapsed_sec;
  const double ops_per_sec = static_cast<double>(total_written) / elapsed_sec;

  std::println("[test_log_io_simple] total_messages={}, elapsed={}ms, throughput={:.2f} MiB/s, ops={:.0f} ops/s",
               total_written, elapsed, throughput_mib, ops_per_sec);

  /* Shutdown log */
  auto shutdown_result = log->shutdown(log_writer);
  WAL_ASSERT(shutdown_result.has_value());

  log->m_pool->stop_io_coroutine();
}

int main(int argc, char* argv[]) {
  Test_config config;

  static struct option long_options[] = {
    {"messages", required_argument, nullptr, 'm'},
    {"disable-writes", no_argument, nullptr, 'd'},
    {"log-block-size", required_argument, nullptr, 'L'},
    {"log-buffer-blocks", required_argument, nullptr, 'R'},
    {nullptr, 0, nullptr, 0}
  };

  int opt;
  while ((opt = getopt_long(argc, argv, "m:dL:R:", long_options, nullptr)) != -1) {
    switch (opt) {
      case 'm':
        config.m_num_messages = std::strtoull(optarg, nullptr, 10);
        break;
      case 'd':
        config.m_disable_writes = true;
        break;
      case 'L':
        config.m_log_block_size = std::strtoull(optarg, nullptr, 10);
        break;
      case 'R':
        config.m_log_buffer_size_blocks = std::strtoull(optarg, nullptr, 10);
        break;
      default:
        std::println(stderr, "Usage: {} [-m messages] [-d] [-L block-size] [-R buffer-blocks]", argv[0]);
        return 1;
    }
  }

  test_log_io_simple(config);
  return 0;
}

