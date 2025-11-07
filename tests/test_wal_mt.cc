#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <getopt.h>
#include <mutex>
#include <condition_variable>
#include <span>
#include <bit>
#include <string>
#include <format>
#include <map>

#include "wal/wal.h"
#include "util/bounded_channel.h"

using wal::lsn_t;
using wal::Log;
using wal::Status;
using wal::Circular_buffer;
using util::Bounded_queue;

/* Null writer callback that simulates writing to storage without actually doing I/O */
static wal::Result<lsn_t> null_writer(lsn_t lsn, const wal::Log::IO_vecs &iovecs, std::size_t n_slots) {
  std::size_t bytes_written{};
  for (std::size_t i = 1; i < n_slots; i += 3) {
    bytes_written += iovecs[i].iov_len;
  }
  return wal::Result<lsn_t>(lsn + bytes_written);
}

/* Write request message for actor model */
struct Write_request {
  wal::Circular_buffer::Slot slot{};
  const std::byte* data_ptr{nullptr};
  std::size_t data_size{0};
  
  Write_request() = default;
  
  Write_request(wal::Circular_buffer::Slot s, std::span<const std::byte> span) 
    : slot(s), data_ptr(span.data()), data_size(span.size()) {
  }
  
  [[nodiscard]] std::span<const std::byte> data() const noexcept {
    return std::span<const std::byte>(data_ptr, data_size);
  }
};

/* Test configuration */
struct Test_config {
  std::size_t num_threads{1};
  std::size_t writes_per_thread{100000};
  double duration_seconds{0.0};
  std::size_t payload_size{32};
  std::size_t n_blocks{16384};
  std::size_t block_size{4096};
  std::size_t writer_batch_size{1};
  std::size_t log_service_batch_size{256};
  
  [[nodiscard]] bool use_duration() const noexcept {
    return duration_seconds > 0.0;
  }
  
  [[nodiscard]] std::string to_string() const noexcept {
    if (use_duration()) {
      return std::format("threads={}, duration={:.3f}s, payload={} bytes, blocks={}, block_size={}, writer_batch={}, log_batch={}",
                         num_threads, duration_seconds, payload_size, n_blocks, block_size, writer_batch_size, log_service_batch_size);
    } else {
      return std::format("threads={}, writes_per_thread={}, payload={} bytes, blocks={}, block_size={}, writer_batch={}, log_batch={}",
                         num_threads, writes_per_thread, payload_size, n_blocks, block_size, writer_batch_size, log_service_batch_size);
    }
  }
};

static void test_multiple_writers(const Test_config& config) {
  const bool use_duration = config.use_duration();
  std::fprintf(stderr, "[test_multiple_writers] start (%s)\n", config.to_string().c_str());

  Log::Write_callback writer = null_writer;
  Circular_buffer::Config buffer_config(config.n_blocks, config.block_size);
  Log log(0, buffer_config);

  /* Calculate queue size (power of 2, at least 1024) */
  std::size_t queue_size = 1024;
  std::size_t estimated_writes = use_duration ? (config.num_threads * 100000) : (config.num_threads * config.writes_per_thread);

  while (queue_size < estimated_writes / 4) {
    queue_size *= 2;
  }
  if (queue_size > 65536) {
    queue_size = 65536; /* Cap at 64K */
  }

  Bounded_queue<Write_request> request_queue(queue_size);
  
  std::atomic<bool> start_flag{false};
  std::atomic<bool> shutdown_flag{false};
  std::atomic<std::size_t> threads_ready{0};
  std::atomic<std::size_t> total_requests{0};
  std::atomic<std::chrono::steady_clock::time_point> start_time{};
  
  /* Counters only accessed by log service thread */
  std::size_t total_writes{0};
  std::size_t total_errors{0};
  std::size_t disk_flushes{0};
  
  /* Histogram for batch sizes (only accessed by log service thread) */
  std::map<std::size_t, std::size_t> batch_size_histogram;

  /* Condition variable for notifying writers when queue has space */
  std::mutex queue_mutex;
  std::condition_variable queue_not_full_cv;

  /* Pre-generate data for each thread to avoid contention on data generation */
  std::vector<std::vector<std::byte>> thread_data(config.num_threads);

  for (std::size_t t = 0; t < config.num_threads; ++t) {
    thread_data[t].resize(config.payload_size);
    /* Fill with thread-specific pattern */
    for (std::size_t i = 0; i < config.payload_size; ++i) {
      thread_data[t][i] = std::byte{static_cast<unsigned char>((t * 256 + i) & 0xFF)};
    }
  }

  /* Log service thread - reads from queue, batches writes, and writes to Circular_buffer */
  auto log_service = [&]() {
    std::vector<Write_request> batch;

     /* Batch up to log_service_batch_size requests */
    batch.reserve(config.log_service_batch_size);

    auto process_batch = [&]() {
      if (batch.empty()) {
        return;
      }
      
      /* Record batch size in histogram */
      batch_size_histogram[batch.size()]++;
      
      /* Process batch */
      for (auto& write_req : batch) {
        auto result = log.m_buffer.write(write_req.slot, write_req.data());
        
        if (result.has_value()) {
          ++total_writes;
        } else {
          /* Handle Not_enough_space by flushing and retrying */
          if (result.error() == Status::Not_enough_space) {
            ++disk_flushes;
            auto flush_result = log.m_buffer.write_to_store(writer);
            if (flush_result.has_value()) {
              /* Retry after flush */
              write_req.slot = log.m_buffer.reserve(static_cast<std::uint16_t>(write_req.data_size));
              result = log.m_buffer.write(write_req.slot, write_req.data());
              if (result.has_value()) {
                ++total_writes;
              } else {
                ++total_errors;
              }
            } else {
              ++total_errors;
            }
          } else {
            ++total_errors;
          }
        }
      }
      batch.clear();
    };

    while (!shutdown_flag.load(std::memory_order_relaxed) || !batch.empty()) {
      Write_request req;
      
      /* Try to dequeue a request */
      if (request_queue.dequeue(req)) {
        batch.push_back(std::move(req));
        
        /* Batch requests - try to fill batch up to log_service_batch_size */
        while (batch.size() < config.log_service_batch_size && request_queue.dequeue(req)) {
          batch.push_back(std::move(req));
        }
        
        /* Notify writers that queue has space (only occasionally to reduce overhead) */
        if (batch.size() % config.log_service_batch_size == 0) {
          queue_not_full_cv.notify_all();
        }
        
        /* Process batch if full */
        if (batch.size() >= config.log_service_batch_size) {
          process_batch();
        }
      } else {
        /* Process remaining batch if any */
        process_batch();
        
        /* Check shutdown after processing batch */
        if (shutdown_flag.load(std::memory_order_relaxed)) {
          break;
        }
        
        /* Queue is empty, yield briefly */
        std::this_thread::yield();
      }
    }
    
    /* Process any remaining items in batch */
    process_batch();
  };

  /* Writer threads - enqueue messages to the queue */
  auto worker = [&](std::size_t thread_id) {
    threads_ready.fetch_add(1, std::memory_order_relaxed);
    
    /* Wait for start signal */
    while (!start_flag.load(std::memory_order_relaxed)) {
      std::this_thread::yield();
    }

    std::size_t local_requests = 0;
    const auto duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::duration<double>(config.duration_seconds));
    
    /* Store slots and spans separately to avoid Write_request construction overhead */
    struct Pending_write {
      wal::Circular_buffer::Slot slot;
      std::span<const std::byte> span;
    };
    std::vector<Pending_write> pending;
    pending.reserve(config.writer_batch_size);

    auto flush_pending = [&]() {
      if (pending.empty()) {
        return;
      }
      
      /* Enqueue all pending writes - create Write_request only when needed */
      for (auto& p : pending) {
        if (shutdown_flag.load(std::memory_order_relaxed)) {
          pending.clear();
          return;
        }
        
        /* Create Write_request only when enqueueing */
        Write_request req(p.slot, p.span);
        
        /* Try enqueueing - if it fails, the queue is full */
        if (!request_queue.enqueue(std::move(req))) {
          /* Queue is full, wait for notification */
          std::unique_lock<std::mutex> lock(queue_mutex);
          queue_not_full_cv.wait(lock, [&]() {
            return !request_queue.full() || shutdown_flag.load(std::memory_order_relaxed);
          });
          
          /* Retry enqueue after notification */
          if (!shutdown_flag.load(std::memory_order_relaxed)) {
            Write_request retry_req(p.slot, p.span);
            if (!request_queue.enqueue(std::move(retry_req))) {
              /* Still full, wait again */
              queue_not_full_cv.wait(lock, [&]() {
                return !request_queue.full() || shutdown_flag.load(std::memory_order_relaxed);
              });
              Write_request final_req(p.slot, p.span);
              (void)request_queue.enqueue(std::move(final_req));
            }
          }
        }
        ++local_requests;
        total_requests.fetch_add(1, std::memory_order_relaxed);
      }
      pending.clear();
    };

    if (use_duration) {
      /* Run for specified duration */
      const auto start = start_time.load(std::memory_order_relaxed);
      while (!shutdown_flag.load(std::memory_order_relaxed)) {
        const auto now = std::chrono::steady_clock::now();
        if (now - start >= duration_ns) {
          break;
        }

        const auto& data = thread_data[thread_id];
        std::span<const std::byte> span(data.data(), config.payload_size);

        /* Reserve space */
        auto slot = log.m_buffer.reserve(static_cast<std::uint16_t>(config.payload_size));
        
        /* Store slot and span (cheap, no Write_request construction) */
        pending.push_back({slot, span});
        
        /* Flush when batch is full */
        if (pending.size() >= config.writer_batch_size) {
          flush_pending();
        }
      }
      /* Flush remaining items */
      flush_pending();
    } else {
      /* Run for fixed number of writes */
      for (std::size_t i = 0; i < config.writes_per_thread; ++i) {
        const auto& data = thread_data[thread_id];
        std::span<const std::byte> span(data.data(), config.payload_size);

        /* Reserve space */
        auto slot = log.m_buffer.reserve(static_cast<std::uint16_t>(config.payload_size));
        
        /* Store slot and span (cheap, no Write_request construction) */
        pending.push_back({slot, span});
        
        /* Flush when batch is full */
        if (pending.size() >= config.writer_batch_size) {
          flush_pending();
        }
      }
      /* Flush remaining items */
      flush_pending();
    }
  };

  /* Start log service thread */
  std::thread service_thread(log_service);

  /* Create and start writer threads */
  std::vector<std::thread> threads;
  threads.reserve(config.num_threads);

  for (std::size_t i = 0; i < config.num_threads; ++i) {
    threads.emplace_back(worker, i);
  }

  /* Wait for all threads to be ready */
  while (threads_ready.load(std::memory_order_relaxed) < config.num_threads) {
    std::this_thread::yield();
  }

  /* Start all threads simultaneously */
  const auto start_time_val = std::chrono::steady_clock::now();
  start_time.store(start_time_val, std::memory_order_release);
  start_flag.store(true, std::memory_order_release);

  /* Wait for all writer threads to complete */
  for (auto& t : threads) {
    t.join();
  }
  
  /* Signal shutdown and wait for service thread */
  shutdown_flag.store(true, std::memory_order_release);
  queue_not_full_cv.notify_all(); /* Wake up any waiting writers */
  service_thread.join();
  
  const auto end_time = std::chrono::steady_clock::now();

  /* Flush any remaining data */
  ++disk_flushes;
  auto flush_result = log.m_buffer.write_to_store(writer);
  assert(flush_result.has_value());

  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time_val);
  const auto elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;

  const auto expected_writes = use_duration ? total_requests.load() : (config.num_threads * config.writes_per_thread);
  const auto actual_writes = total_writes;
  const auto actual_errors = total_errors;
  const auto actual_requests = total_requests.load();
  
  /* Calculate throughput in MiB/s */
  const auto total_bytes = actual_writes * config.payload_size;
  const auto throughput_mib_s = (static_cast<double>(total_bytes) / elapsed_s) / (1024.0 * 1024.0);

  std::fprintf(stderr,
               "[test_multiple_writers] done (requests=%zu, writes=%zu/%zu, errors=%zu, flushes=%zu, elapsed=%.3fs, throughput=%.2f MiB/s)\n",
               actual_requests,
               actual_writes,
               expected_writes,
               actual_errors,
               disk_flushes,
               elapsed_s,
               throughput_mib_s);
  
  /* Print batch size histogram */
  if (!batch_size_histogram.empty()) {
    std::fprintf(stderr, "[test_multiple_writers] batch size histogram:\n");
    std::size_t total_batches = 0;
    for (const auto& [size, count] : batch_size_histogram) {
      total_batches += count;
    }
    for (const auto& [size, count] : batch_size_histogram) {
      const double percentage = (static_cast<double>(count) / static_cast<double>(total_batches)) * 100.0;
      std::fprintf(stderr, "  size=%3zu: count=%6zu (%.1f%%)\n", size, count, percentage);
    }
  }

  /* Verify all writes succeeded */
  assert(actual_errors == 0);
  if (!use_duration) {
    assert(actual_writes == expected_writes);
    assert(actual_requests == expected_writes);
  } else {
    /* In duration mode, some requests may still be in queue at shutdown,
     * so writes may be less than requests */
    assert(actual_writes <= actual_requests);
  }

  /* Verify buffer state is consistent */
  assert(log.m_buffer.m_lsn_counters.m_lwm <= log.m_buffer.m_reserve_counters.m_hwm);
  /* Note: m_written_lsn can exceed m_hwm when retries occur after partial writes,
   * as the original slot's reservation remains in m_hwm but we write to a new slot */
}

/* Print usage */
static void print_usage(const char* program_name) noexcept {
  std::fprintf(stderr,
               "Usage: %s [OPTIONS]\n"
               "\n"
               "Options:\n"
               "  -t, --threads NUM       Number of writer threads (default: 4)\n"
               "  -w, --writes NUM        Number of writes per thread (default: 100000)\n"
               "  -d, --duration SECONDS  Run for specified duration in seconds (overrides -w)\n"
               "  -p, --payload BYTES     Payload size in bytes (default: 32)\n"
               "  -n, --blocks BLOCKS     Number of blocks in buffer (default: 16384)\n"
               "  -s, --block-size BYTES  Block size in bytes (default: 4096)\n"
               "  -b, --writer-batch NUM  Writer local batch size (default: 256)\n"
               "  -l, --log-batch NUM     Log service batch size (default: 64)\n"
               "  -h, --help              Show this help message\n"
               "\n"
               "Examples:\n"
               "  %s -t 8 -w 10000 -p 64\n"
               "  %s --threads 16 --duration 10 --payload 128\n",
               program_name,
               program_name,
               program_name);
}

/* Main */
int main(int argc, char** argv) {
  Test_config test_config;
  bool show_help = false;

  /* Long options */
  static const struct option long_options[] = {
    {"threads", required_argument, nullptr, 't'},
    {"writes", required_argument, nullptr, 'w'},
    {"duration", required_argument, nullptr, 'd'},
    {"payload", required_argument, nullptr, 'p'},
    {"blocks", required_argument, nullptr, 'n'},
    {"block-size", required_argument, nullptr, 's'},
    {"writer-batch", required_argument, nullptr, 'b'},
    {"log-batch", required_argument, nullptr, 'l'},
    {"help", no_argument, nullptr, 'h'},
    {nullptr, 0, nullptr, 0}
  };

  /* Parse options */
  int opt;
  int option_index = 0;
  while ((opt = getopt_long(argc, argv, "t:w:d:p:n:s:b:l:h", long_options, &option_index)) != -1) {
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
        test_config.num_threads = static_cast<std::size_t>(parsed);
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
        test_config.writes_per_thread = static_cast<std::size_t>(parsed);
        break;
      }
      case 'd': {
        char* end = nullptr;
        errno = 0;
        const double seconds = std::strtod(optarg, &end);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0) || seconds <= 0.0;
        if (invalid) {
          std::fprintf(stderr, "[wal_mt] invalid duration '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        test_config.duration_seconds = seconds;
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
        test_config.payload_size = static_cast<std::size_t>(parsed);
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
        test_config.n_blocks = static_cast<std::size_t>(parsed);
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
        test_config.block_size = static_cast<std::size_t>(parsed);
        break;
      }
      case 'b': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          std::fprintf(stderr, "[wal_mt] invalid writer batch size '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        test_config.writer_batch_size = static_cast<std::size_t>(parsed);
        break;
      }
      case 'l': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          std::fprintf(stderr, "[wal_mt] invalid log batch size '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        test_config.log_service_batch_size = static_cast<std::size_t>(parsed);
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

  test_multiple_writers(test_config);

  return EXIT_SUCCESS;
}

