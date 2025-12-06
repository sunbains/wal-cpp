#ifndef _GNU_SOURCE
#ifdef HAVE_PTHREAD_SETAFFINITY_NP
#define _GNU_SOURCE
#endif
#endif

#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <stop_token>
#include <mutex>
#include <condition_variable>
#include <getopt.h>
#include <sched.h>
#include <unistd.h>
#include <print>

#if defined(HAVE_PTHREAD_SETAFFINITY_NP) || defined(HAVE_PTHREAD_SETSCHEDPARAM)
#include <pthread.h>
#endif

#include "util/bounded_channel.h"
#include "util/util.h"
#include "util/logger.h"

using util::Bounded_queue;

util::Logger<util::MT_logger_writer> g_logger(util::MT_logger_writer{std::cerr}, util::Log_level::Info);

/* Simple message type for testing */
struct Test_message {
  std::uint64_t value{0};
  std::uint64_t padding[7]{};
};

static_assert(sizeof(Test_message) == 64, "Test_message should be 64 bytes");

/* Test configuration */
struct Test_config {
  std::size_t queue_size{1024};
  std::size_t num_producers{1};
  std::size_t num_consumers{1};
  std::size_t num_messages{1000000};
  std::size_t num_iterations{1};
};

#ifdef HAVE_PTHREAD_SETAFFINITY_NP
template <typename Thread>
static void set_cpu_affinity(Thread& thread, int cpu) {
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(cpu, &cpuset);
  pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
}
#else
template <typename Thread>
static void set_cpu_affinity(Thread& , int) {
  // Function not available on this platform
}
#endif

#ifdef HAVE_PTHREAD_SETSCHEDPARAM
template <typename Thread>
static void set_high_priority(Thread& thread) {
  struct sched_param param;
  param.sched_priority = 50;  // High priority (range: 1-99 for SCHED_FIFO)
  pthread_setschedparam(thread.native_handle(), SCHED_FIFO, &param);
}
#else
template <typename Thread>
static void set_high_priority(Thread&) {
  // Function not available on this platform
}
#endif

/* Fast xorshift32 PRNG for random pause count */
inline std::uint32_t fast_rand(std::uint32_t& state) noexcept {
  state ^= state << 13;
  state ^= state >> 17;
  state ^= state << 5;
  return state;
}

/* Return a random pause count in the range [0, max) */
[[maybe_unused]] static std::size_t random_pause_count(std::size_t max) noexcept {
  if (max == 0) {
    return 0;
  }
  static std::uint32_t rng_state = static_cast<std::uint32_t>(
    std::chrono::steady_clock::now().time_since_epoch().count());
  const std::uint32_t r = fast_rand(rng_state);
  return r % max;
}

static void test_throughput(const Test_config& config) {
  log_info("[test_throughput] start (queue_size={}, producers={}, consumers={}, messages={}, iterations={})",
               config.queue_size, config.num_producers, config.num_consumers, config.num_messages, config.num_iterations);

  Bounded_queue<Test_message> queue(config.queue_size);

  std::stop_source done_src;
  std::size_t messages_sent{0};
  std::size_t messages_received{0};
  
  /* Synchronization for coordinated start */
  std::atomic<bool> start_flag{false};
  
  /* Condition variables for producer/consumer synchronization */
  std::mutex queue_mutex;
  std::condition_variable queue_not_full_cv;   /* Signaled when queue has space */
  std::condition_variable queue_not_empty_cv;  /* Signaled when queue has items */
  constexpr auto kQueueWaitDuration = std::chrono::microseconds(50);

  /* Producer thread */
  auto producer = [&]() {
    /* Wait for start signal */
    while (!start_flag.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    
    Test_message msg{};
    const std::size_t total_messages = config.num_iterations * config.num_messages;
    std::size_t retry_count = 0;
    constexpr std::size_t kYieldInterval = 10;
    
    for (std::size_t i = 0; i < total_messages; ++i) {
      msg.value = i;
      
      /* Try to enqueue - use condition variable if queue is full */
      if (queue.enqueue(msg)) [[likely]] {
        /* Successfully enqueued - notify consumers */
        queue_not_empty_cv.notify_one();
        retry_count = 0;
        continue;
      }
      
      /* Queue full - busy-wait briefly, then use condition variable */
      if (retry_count < kYieldInterval) [[likely]] {
        ++retry_count;
        util::cpu_pause_n(10);
      } else {
        retry_count = 0;
        std::unique_lock<std::mutex> lock(queue_mutex);
        /* Re-check queue state after acquiring lock */
        if (queue.enqueue(msg)) {
          queue_not_empty_cv.notify_one();
          continue;
        }
        /* Wait for space in queue */
        queue_not_full_cv.wait_for(lock, kQueueWaitDuration);
      }
    }
    messages_sent = total_messages;
  };

  /* Consumer thread */
  auto consumer = [&](std::stop_token st) {
    /* Wait for start signal */
    while (!start_flag.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    
    Test_message msg;
    const std::size_t expected_messages = config.num_iterations * config.num_messages;
    std::size_t received_count = 0;
    std::size_t consecutive_failures = 0;
    constexpr std::size_t kBusyWaitLimit = 5000;
    
    while (received_count < expected_messages) {
      if (queue.dequeue(msg)) [[likely]] {
        ++received_count;
        consecutive_failures = 0;
        /* Successfully dequeued - notify producers */
        queue_not_full_cv.notify_one();
        continue;
      }
      
      /* Queue empty - busy-wait briefly, then use condition variable */
      if (consecutive_failures < kBusyWaitLimit) [[likely]] {
        ++consecutive_failures;
        util::cpu_pause_n(10);
      } else {
        consecutive_failures = 0;
        if (st.stop_requested()) {
          break;
        }
        std::unique_lock<std::mutex> lock(queue_mutex);
        /* Re-check queue state after acquiring lock */
        if (queue.dequeue(msg)) {
          ++received_count;
          queue_not_full_cv.notify_one();
          continue;
        }
        /* Wait for items in queue */
        queue_not_empty_cv.wait_for(lock, kQueueWaitDuration);
      }
    }
    
    /* Drain remaining messages */
    while (queue.dequeue(msg)) {
      ++received_count;
      queue_not_full_cv.notify_one();
    }
    messages_received = received_count;
  };

  std::thread prod_thread(producer);
  std::jthread cons_thread(consumer, done_src.get_token());
  
  /* Set CPU affinity to reduce variance (pin to adjacent cores) */
  set_cpu_affinity(prod_thread, 0);
  set_cpu_affinity(cons_thread, 1);
  
  /* Set consumer thread to high priority */
  set_high_priority(cons_thread);
  
  /* Warm-up: run a small batch to warm caches */
  {
    Test_message warm_msg{};
    for (int i = 0; i < 1000; ++i) {
      if (queue.enqueue(warm_msg)) {
        (void)queue.dequeue(warm_msg);
      }
    }
  }
  
  /* Synchronize threads before starting timer */
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  start_flag.store(true, std::memory_order_release);
  
  const auto start = std::chrono::steady_clock::now();

  prod_thread.join();
  /* Signal consumer to stop after producer finishes */
  done_src.request_stop();
  /* Notify consumer in case it's waiting on condition variable */
  queue_not_empty_cv.notify_all();
  cons_thread.join();

  const auto end = std::chrono::steady_clock::now();
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  const auto elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;

  const auto sent = messages_sent;
  const auto received = messages_received;
  const auto total_bytes = sent * sizeof(Test_message);
  const auto throughput_mib_s = (static_cast<double>(total_bytes) / elapsed_s) / (1024.0 * 1024.0);
  const auto ops_per_sec = static_cast<double>(sent) / elapsed_s;

  log_info("[test_throughput] done (sent={}, received={}, elapsed={:.3f}s, throughput={:.2f} MiB/s, ops={:.0f} ops/s)",
               sent, received, elapsed_s, throughput_mib_s, ops_per_sec);

  assert(sent == received);
  assert(sent == config.num_iterations * config.num_messages);
}

static void print_usage(const char* program_name) noexcept {
  std::println("Usage: {} [OPTIONS]\n"
               "\n"
               "Options:\n"
               "  -q, --queue-size NUM    Queue size (must be power of 2, default: 1024)\n"
               "  -m, --messages NUM      Number of messages per iteration (default: 1000000)\n"
               "  -i, --iterations NUM    Number of iterations (default: 10)\n"
               "  -t, --threads NUM       Number of producer threads for MPSC test (default: 4)\n"
               "  -p, --producers NUM     Number of producer threads for MPMC test (default: 1)\n"
               "  -c, --consumers NUM     Number of consumer threads for MPMC test (default: 1)\n"
               "  -h, --help              Show this help message\n"
               "\n"
               "Examples:\n"
               "  {} -q 1024 -m 1000000 -i 10\n"
               "  {} -t 4 -q 2048 -m 500000\n"
               "  {} -p 4 -c 2 -q 2048 -m 500000\n",
               program_name, program_name, program_name, program_name);
}

/* Main */
int main(int argc, char** argv) {
  Test_config config;
  bool show_help = false;

  static const struct option long_options[] = {
    {"queue-size", required_argument, nullptr, 'q'},
    {"messages", required_argument, nullptr, 'm'},
    {"iterations", required_argument, nullptr, 'i'},
    {"threads", required_argument, nullptr, 't'},
    {"producers", required_argument, nullptr, 'p'},
    {"consumers", required_argument, nullptr, 'c'},
    {"help", no_argument, nullptr, 'h'},
    {nullptr, 0, nullptr, 0}
  };

  int opt;
  int option_index = 0;
  while ((opt = getopt_long(argc, argv, "q:m:i:t:p:c:h", long_options, &option_index)) != -1) {
    switch (opt) {
      case 'q': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          log_info( "[bounded_queue_perf] invalid queue size '{}'", optarg);
          return EXIT_FAILURE;
        }
        if ((parsed & (parsed - 1)) != 0) {
          log_info( "[bounded_queue_perf] queue size must be a power of 2, got '{}'", optarg);
          return EXIT_FAILURE;
        }
        config.queue_size = static_cast<std::size_t>(parsed);
        break;
      }
      case 'm': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          log_info( "[bounded_queue_perf] invalid number of messages '{}'", optarg);
          return EXIT_FAILURE;
        }
        config.num_messages = static_cast<std::size_t>(parsed);
        break;
      }
      case 'i': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          log_info( "[bounded_queue_perf] invalid number of iterations '{}'", optarg);
          return EXIT_FAILURE;
        }
        config.num_iterations = static_cast<std::size_t>(parsed);
        break;
      }
      case 't': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          log_info( "[bounded_queue_perf] invalid number of threads '{}'", optarg);
          return EXIT_FAILURE;
        }
        // num_threads not used in current implementation
        (void)parsed;
        break;
      }
      case 'p': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          log_info( "[bounded_queue_perf] invalid number of producers '{}'", optarg);
          return EXIT_FAILURE;
        }
        config.num_producers = static_cast<std::size_t>(parsed);
        break;
      }
      case 'c': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          log_info( "[bounded_queue_perf] invalid number of consumers '{}'", optarg);
          return EXIT_FAILURE;
        }
        config.num_consumers = static_cast<std::size_t>(parsed);
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
    log_info( "[bounded_queue_perf] unexpected argument: '{}'", argv[optind]);
    print_usage(argv[0]);
    return EXIT_FAILURE;
  }

  if (show_help) {
    print_usage(argv[0]);
    return EXIT_SUCCESS;
  }

  test_throughput(config);

  return EXIT_SUCCESS;
}

