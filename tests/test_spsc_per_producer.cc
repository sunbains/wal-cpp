#include <thread>
#include <atomic>
#include <chrono>
#include <vector>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <print>

#include "util/bounded_channel.h"
#include "util/logger.h"
#include "util/thread_pool.h"
#include "coro/task.h"

/**
 * SPSC-per-producer architecture (Erlang style):
 * - Each producer is a coroutine with its own SPSC queue
 * - Producers enqueue themselves into MPMC scheduling queue when they have data
 * - Consumer dequeues from MPMC to find which SPSC queues to drain
 * - All coroutines run on thread pool (no dedicated threads)
 * - Run-to-completion model: schedule once, then run tight loops
 */

using Item = std::size_t;

util::Logger<util::MT_logger_writer> g_logger(util::MT_logger_writer{std::cerr}, util::Log_level::Info);

/**
 * Benchmark configuration
 */
struct Config {
  std::size_t m_num_producers = 1024;
  std::size_t m_total_items = 10'000'000;
  std::size_t m_producer_queue_size = 256;
  std::size_t m_drain_batch_size = 512;
  bool m_run_suite = false;  /* Run full suite instead of single config */
};

/* Producer state - owns SPSC queue */
struct Producer_state {
  explicit Producer_state(std::size_t producer_id, std::size_t queue_size)
    : m_id(producer_id), m_queue(queue_size) {}

  /* Enqueue self into scheduling queue if not already scheduled */
  void schedule_self() {
    if (m_schedule_queue && !m_in_schedule_queue.exchange(true, std::memory_order_acq_rel)) {
      while (!m_schedule_queue->enqueue(this)) {
        std::this_thread::yield();
      }
    }
  }

  std::size_t m_id;
  util::Bounded_queue<Item> m_queue;
  std::atomic<std::size_t> m_items_enqueued{0};
  std::atomic<bool> m_in_schedule_queue{false};  // Avoid duplicate scheduling
  util::Bounded_queue<Producer_state*>* m_schedule_queue{nullptr};
};

/* Producer coroutine - Erlang style run-to-completion */
Task<void> producer_coro(Producer_state& state, util::Thread_pool& pool, std::size_t num_items, std::atomic<bool>& start_flag) {
  /* Schedule once onto thread pool */
  co_await pool.schedule();

  /* Wait for start */
  while (!start_flag.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  /* Tight loop: produce all items */
  for (std::size_t i = 0; i < num_items; ++i) {
    if (!state.m_queue.enqueue(i)) {
      /* Queue full - schedule self to notify consumer */
      state.schedule_self();

      /* Spin until space available */
      while (!state.m_queue.enqueue(i)) {
        std::this_thread::yield();
      }
    }
  }

  /* Schedule at completion */
  state.schedule_self();

  state.m_items_enqueued.store(num_items, std::memory_order_release);
  co_return;
}

/* Consumer context - Erlang style message passing */
struct Consumer_context {
  util::Bounded_queue<Producer_state*>* m_schedule_queue{nullptr};
  util::Thread_pool* m_pool{nullptr};
  std::size_t m_total_items{0};
  std::size_t m_drain_batch_size{0};
  std::atomic<bool>* m_start_flag{nullptr};
  std::atomic<std::size_t>* m_consumed{nullptr};
};

/* Consumer coroutine - drains producer queues via MPMC scheduling */
Task<void> consumer_coro(Consumer_context& ctx) {
  /* Schedule once onto thread pool */
  co_await ctx.m_pool->schedule();

  /* Wait for start */
  while (!ctx.m_start_flag->load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  /* Tight loop: pull from scheduling queue and drain SPSC queues */
  Item item;
  std::size_t local_consumed = 0;

  while (local_consumed < ctx.m_total_items) {
    Producer_state* producer = nullptr;

    /* Dequeue next producer from scheduling queue */
    if (ctx.m_schedule_queue->dequeue(producer)) {
      /* Mark as not in queue so it can re-schedule */
      producer->m_in_schedule_queue.store(false, std::memory_order_release);

      /* Drain this producer's SPSC queue aggressively */
      std::size_t drained = 0;
      while (drained < ctx.m_drain_batch_size && producer->m_queue.dequeue(item)) {
        ++drained;
        ++local_consumed;
        if (local_consumed >= ctx.m_total_items) break;
      }

      /* If producer still has data, re-schedule it immediately */
      if (drained == ctx.m_drain_batch_size && local_consumed < ctx.m_total_items) {
        /* Drained full batch - likely more data available */
        producer->schedule_self();
      }
    } else {
      /* No producers in queue - yield briefly to avoid spinning */
      std::this_thread::yield();
    }
  }

  ctx.m_consumed->store(local_consumed, std::memory_order_release);
  co_return;
}

/**
 * Benchmark result structure
 */
struct BenchmarkResult {
  std::size_t m_num_producers;
  double m_spsc_ops_per_sec;
  double m_spsc_time_s;
};

/* SPSC-per-producer test with coroutines and MPMC scheduling queue */
BenchmarkResult test_spsc_per_producer(const Config& config) {
  std::size_t num_producers = config.m_num_producers;
  std::size_t total_items = config.m_total_items;
  std::size_t producer_queue_size = config.m_producer_queue_size;

  /* MPMC scheduling queue size = num_producers (one slot per producer) */
  std::size_t schedule_queue_size = num_producers;
  if (!std::has_single_bit(schedule_queue_size)) {
    schedule_queue_size = std::bit_ceil(schedule_queue_size);
  }

  /* Use hardware thread count for thread pool with scaled task queue */
  std::size_t hw_threads = std::thread::hardware_concurrency();
  if (hw_threads == 0) hw_threads = 4;

  util::Thread_pool::Config pool_config;
  pool_config.m_num_threads = hw_threads;
  /* Scale queue capacity with producers, but cap at reasonable limit */
  pool_config.m_queue_capacity = std::min<std::size_t>(65536, std::max<std::size_t>(16384, num_producers * 2));
  if (!std::has_single_bit(pool_config.m_queue_capacity)) {
    pool_config.m_queue_capacity = std::bit_ceil(pool_config.m_queue_capacity);
  }

  util::Thread_pool pool(pool_config);

  /* Pre-warm memory pools to avoid allocation during benchmark */
  {
    std::size_t needed_coro_frames = num_producers + 1;  /* producers + consumer */
    std::size_t arenas_needed = (needed_coro_frames + 2047) / 2048;
    auto& coro_pool = util::get_coroutine_pool();
    for (std::size_t i = 1; i < arenas_needed; ++i) {
      coro_pool.grow();  /* Pre-allocate arenas */
    }
  }

  /* Create MPMC scheduling queue */
  util::Bounded_queue<Producer_state*> schedule_queue(schedule_queue_size);

  /* Create producer states */
  std::vector<std::unique_ptr<Producer_state>> producer_states;

  for (std::size_t i = 0; i < num_producers; ++i) {
    auto producer = std::make_unique<Producer_state>(i, producer_queue_size);
    producer->m_schedule_queue = &schedule_queue;
    producer_states.push_back(std::move(producer));
  }

  std::atomic<bool> start_flag{false};
  std::atomic<std::size_t> consumed{0};
  std::size_t items_per_producer = total_items / num_producers;

  /* Create consumer context */
  Consumer_context consumer_ctx;
  consumer_ctx.m_schedule_queue = &schedule_queue;
  consumer_ctx.m_pool = &pool;
  consumer_ctx.m_total_items = total_items;
  consumer_ctx.m_drain_batch_size = config.m_drain_batch_size;
  consumer_ctx.m_start_flag = &start_flag;
  consumer_ctx.m_consumed = &consumed;

  /* Launch consumer coroutine */
  detach(consumer_coro(consumer_ctx));

  /* Launch producer coroutines */
  for (std::size_t p = 0; p < num_producers; ++p) {
    std::size_t items = (p == num_producers - 1)
      ? total_items - (p * items_per_producer)
      : items_per_producer;
    detach(producer_coro(*producer_states[p], pool, items, start_flag));
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  start_flag.store(true, std::memory_order_release);

  auto start_time = std::chrono::steady_clock::now();

  /* Wait for completion */
  while (consumed.load(std::memory_order_acquire) < total_items) {
    std::this_thread::yield();
  }

  auto end_time = std::chrono::steady_clock::now();

  auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
  double elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;
  double ops_per_sec = static_cast<double>(total_items) / elapsed_s;

  BenchmarkResult result{};
  result.m_num_producers = num_producers;
  result.m_spsc_ops_per_sec = ops_per_sec;
  result.m_spsc_time_s = elapsed_s;
  return result;
}

void run_benchmark_suite(const Config& base_config) {
  std::vector<std::size_t> producer_counts = {
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192
  };

  log_info("=== SPSC-per-Producer + MPMC Scheduling Benchmark Suite ===");
  log_info("Items: {}, Producer Queue: {}, Drain Batch: {}\n",
               base_config.m_total_items, base_config.m_producer_queue_size, base_config.m_drain_batch_size);
  log_info("{:>5} | {:>12} {:>8}", "Prod", "Throughput", "Time(s)");
  log_info("{}", std::string(40, '-'));

  for (std::size_t num_prod : producer_counts) {
    Config config = base_config;
    config.m_num_producers = num_prod;

    auto result = test_spsc_per_producer(config);

    log_info("{:5}P | {:9.2f} M/s {:8.3f}",
                 num_prod,
                 result.m_spsc_ops_per_sec / 1'000'000.0,
                 result.m_spsc_time_s);
  }

  log_info("\n=== Done ===");
}

void print_usage(const char* prog) {
  std::println("Usage: {} [OPTIONS]", prog);
  std::println("\nOptions:");
  std::println("  -p, --producers <N>      Number of producers (default: 1024)");
  std::println("  -n, --items <N>          Total items to process (default: 10000000)");
  std::println("  -q, --queue-size <N>     Producer queue size (default: 256)");
  std::println("  -d, --drain-batch <N>    Consumer drain batch size (default: 512)");
  std::println("  -s, --suite              Run full benchmark suite");
  std::println("  -h, --help               Show this help message");
  std::println("\nExamples:");
  std::println("  {} -p 2048 -n 50000000", prog);
  std::println("  {} --suite --queue-size 512", prog);
  std::println("  perf record {} -p 4096 -n 100000000", prog);
}

int main(int argc, char* argv[]) {
  Config config;

  /* Parse command line arguments */
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      return 0;
    } else if (arg == "-s" || arg == "--suite") {
      config.m_run_suite = true;
    } else if ((arg == "-p" || arg == "--producers") && i + 1 < argc) {
      config.m_num_producers = std::stoull(argv[++i]);
    } else if ((arg == "-n" || arg == "--items") && i + 1 < argc) {
      config.m_total_items = std::stoull(argv[++i]);
    } else if ((arg == "-q" || arg == "--queue-size") && i + 1 < argc) {
      config.m_producer_queue_size = std::stoull(argv[++i]);
    } else if ((arg == "-d" || arg == "--drain-batch") && i + 1 < argc) {
      config.m_drain_batch_size = std::stoull(argv[++i]);
    } else {
      log_info( "Unknown option: {}", arg);
      print_usage(argv[0]);
      return 1;
    }
  }

  if (config.m_run_suite) {
    run_benchmark_suite(config);
  } else {
    log_info("=== SPSC-per-Producer + MPMC Scheduling Benchmark ===");
    log_info("Producers: {}, Items: {}, Producer Queue: {}, Drain Batch: {}\n",
                 config.m_num_producers, config.m_total_items,
                 config.m_producer_queue_size, config.m_drain_batch_size);

    auto result = test_spsc_per_producer(config);

    log_info("Throughput: {:.2f} M/s", result.m_spsc_ops_per_sec / 1'000'000.0);
    log_info("Time: {:.3f}s", result.m_spsc_time_s);
  }

  return 0;
}
