#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <getopt.h>

#include "wal/wal.h"

using wal::lsn_t;
using wal::Log;
using wal::Status;
using wal::Circular_buffer;

/* Null writer callback that simulates writing to storage without actually doing I/O */
static wal::Result<lsn_t> null_writer(lsn_t lsn, const wal::Log::IO_vecs &iovecs) {
  std::size_t bytes_written{};
  for (std::size_t i = 1; i < iovecs.size(); i += 3) {
    bytes_written += iovecs[i].iov_len;
  }
  return wal::Result<lsn_t>(lsn + bytes_written);
}

static void test_multiple_writers(std::size_t num_threads,
                                  std::size_t writes_per_thread,
                                  std::size_t payload_size,
                                  std::size_t n_blocks,
                                  std::size_t block_size) {
  std::fprintf(stderr,
               "[test_multiple_writers] start (threads=%zu, writes_per_thread=%zu, payload=%zu bytes, blocks=%zu, block_size=%zu)\n",
               num_threads,
               writes_per_thread,
               payload_size,
               n_blocks,
               block_size);

  Circular_buffer::Config config(n_blocks, block_size);
  Log log(0, config);
  Log::Write_callback writer = null_writer;

  std::atomic<std::size_t> threads_ready{0};
  std::atomic<bool> start_flag{false};
  std::atomic<std::size_t> total_writes{0};
  std::atomic<std::size_t> total_errors{0};

  // Pre-generate data for each thread to avoid contention on data generation
  std::vector<std::vector<std::byte>> thread_data(num_threads);
  for (std::size_t t = 0; t < num_threads; ++t) {
    thread_data[t].resize(payload_size);
    // Fill with thread-specific pattern
    for (std::size_t i = 0; i < payload_size; ++i) {
      thread_data[t][i] = std::byte{static_cast<unsigned char>((t * 256 + i) & 0xFF)};
    }
  }

  auto worker = [&](std::size_t thread_id) {
    threads_ready.fetch_add(1, std::memory_order_relaxed);
    
    // Wait for start signal
    while (!start_flag.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    std::size_t local_writes = 0;
    std::size_t local_errors = 0;

    for (std::size_t i = 0; i < writes_per_thread; ++i) {
      const auto& data = thread_data[thread_id];
      std::span<const std::byte> span(data.data(), payload_size);

      // Reserve space
      auto slot = log.m_buffer.reserve(payload_size);
      
      // Write data
      auto result = log.m_buffer.write(slot, span);
      
      if (result.has_value()) {
        ++local_writes;
      } else {
        // Handle Not_enough_space by flushing and retrying
        if (result.error() == Status::Not_enough_space) {
          auto flush_result = log.m_buffer.write_to_store(writer);
          if (flush_result.has_value()) {
            // Retry after flush
            slot = log.m_buffer.reserve(payload_size);
            result = log.m_buffer.write(slot, span);
            if (result.has_value()) {
              ++local_writes;
            } else {
              ++local_errors;
            }
          } else {
            ++local_errors;
          }
        } else {
          ++local_errors;
        }
      }
    }

    total_writes.fetch_add(local_writes, std::memory_order_relaxed);
    total_errors.fetch_add(local_errors, std::memory_order_relaxed);
  };

  // Create and start threads
  std::vector<std::thread> threads;
  threads.reserve(num_threads);
  for (std::size_t i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker, i);
  }

  // Wait for all threads to be ready
  while (threads_ready.load(std::memory_order_relaxed) < num_threads) {
    std::this_thread::yield();
  }

  // Start all threads simultaneously
  const auto start_time = std::chrono::steady_clock::now();
  start_flag.store(true, std::memory_order_release);

  // Wait for all threads to complete
  for (auto& t : threads) {
    t.join();
  }
  const auto end_time = std::chrono::steady_clock::now();

  // Flush any remaining data
  auto flush_result = log.m_buffer.write_to_store(writer);
  assert(flush_result.has_value());

  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
  const auto elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;

  const auto expected_writes = num_threads * writes_per_thread;
  const auto actual_writes = total_writes.load();
  const auto actual_errors = total_errors.load();

  std::fprintf(stderr,
               "[test_multiple_writers] done (writes=%zu/%zu, errors=%zu, elapsed=%.3fs, throughput=%.2f writes/s)\n",
               actual_writes,
               expected_writes,
               actual_errors,
               elapsed_s,
               static_cast<double>(actual_writes) / elapsed_s);

  // Verify all writes succeeded
  assert(actual_errors == 0);
  assert(actual_writes == expected_writes);

  // Verify buffer state is consistent
  assert(log.m_buffer.m_lwm <= log.m_buffer.m_hwm);
  assert(log.m_buffer.m_hwm <= log.m_buffer.m_written_lsn);
}

static void print_usage(const char* program_name) noexcept {
  std::fprintf(stderr,
               "Usage: %s [OPTIONS]\n"
               "\n"
               "Options:\n"
               "  -t, --threads NUM        Number of writer threads (default: 4)\n"
               "  -w, --writes NUM        Number of writes per thread (default: 1000)\n"
               "  -p, --payload BYTES     Payload size in bytes (default: 32)\n"
               "  -n, --blocks BLOCKS     Number of blocks in buffer (default: 16384)\n"
               "  -s, --block-size BYTES  Block size in bytes (default: 4096)\n"
               "  -h, --help              Show this help message\n"
               "\n"
               "Examples:\n"
               "  %s -t 8 -w 10000 -p 64\n"
               "  %s --threads 16 --writes 5000 --payload 128\n",
               program_name,
               program_name,
               program_name);
}

int main(int argc, char** argv) {
  std::size_t num_threads = 4;
  std::size_t writes_per_thread = 1000;
  std::size_t payload_size = 32;
  std::size_t n_blocks = 16384;
  std::size_t block_size = 4096;
  bool show_help = false;

  static const struct option long_options[] = {
    {"threads", required_argument, nullptr, 't'},
    {"writes", required_argument, nullptr, 'w'},
    {"payload", required_argument, nullptr, 'p'},
    {"blocks", required_argument, nullptr, 'n'},
    {"block-size", required_argument, nullptr, 's'},
    {"help", no_argument, nullptr, 'h'},
    {nullptr, 0, nullptr, 0}
  };

  int opt;
  int option_index = 0;
  while ((opt = getopt_long(argc, argv, "t:w:p:n:s:h", long_options, &option_index)) != -1) {
    switch (opt) {
      case 't': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          std::fprintf(stderr, "[wal_mt] invalid number of threads '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        num_threads = static_cast<std::size_t>(parsed);
        break;
      }
      case 'w': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          std::fprintf(stderr, "[wal_mt] invalid number of writes '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        writes_per_thread = static_cast<std::size_t>(parsed);
        break;
      }
      case 'p': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          std::fprintf(stderr, "[wal_mt] invalid payload size '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        if (parsed > std::numeric_limits<std::uint16_t>::max()) {
          std::fprintf(stderr,
                       "[wal_mt] payload must be <= %u bytes\n",
                       std::numeric_limits<std::uint16_t>::max());
          return EXIT_FAILURE;
        }
        payload_size = static_cast<std::size_t>(parsed);
        break;
      }
      case 'n': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          std::fprintf(stderr, "[wal_mt] invalid number of blocks '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        n_blocks = static_cast<std::size_t>(parsed);
        break;
      }
      case 's': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          std::fprintf(stderr, "[wal_mt] invalid block size '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        block_size = static_cast<std::size_t>(parsed);
        break;
      }
      case 'h': {
        show_help = true;
        break;
      }
      default: {
        print_usage(argv[0]);
        return EXIT_FAILURE;
      }
    }
  }

  if (optind < argc) {
    std::fprintf(stderr, "[wal_mt] unexpected argument: '%s'\n", argv[optind]);
    print_usage(argv[0]);
    return EXIT_FAILURE;
  }

  if (show_help) {
    print_usage(argv[0]);
    return EXIT_SUCCESS;
  }

  test_multiple_writers(num_threads, writes_per_thread, payload_size, n_blocks, block_size);

  return EXIT_SUCCESS;
}

