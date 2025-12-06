// Correctness and performance tests for util::Bounded_channel_coro

#include <cassert>
#include <atomic>
#include <thread>
#include <vector>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <cstdlib>
#include <cstdio>
#include <print>

#include "util/bounded_channel_coro.h"
#include "coro/task.h"
#include "util/thread_pool.h"
#include "util/util.h"

using util::Bounded_queue_coro;
using util::Segmented_queue;

using Queue = Segmented_queue<std::size_t>;
using Channel = Bounded_queue_coro<Queue>;

static void test_basic_single_thread() {
  log_info("[test_basic_single_thread] start");

  Queue::Config config{.m_slots_per_segment = 8, .m_num_segments = 4, .m_num_stripes = 4};
  Channel ch(config);

  assert(ch.capacity() == 32);
  assert(ch.empty());
  assert(!ch.full());

  Channel::Handle handle{};
  const auto total_items = config.m_slots_per_segment * config.m_num_segments;

  for (std::size_t i = 0; i < total_items; ++i) {
    if (!ch.try_enqueue(handle, i)) {
      log_info("try_enqueue fail at i={}", i);
      std::abort();
    }
  }

  assert(ch.full());

  std::vector<std::size_t> out;

  for (std::size_t i = 0; i < total_items; ++i) {
    std::size_t v = ~0UL;

    if (!ch.try_dequeue(handle, v)) {
      log_info("try_dequeue fail at i={}", i);
      std::abort();
    }

    assert(v != ~0UL);
    // log_info("dequeued={}", v);
    out.push_back(v);
  }
  assert(ch.empty());

  std::sort(out.begin(), out.end());

  for (std::size_t i = 0; i < total_items; ++i) {
    if (out[i] != i) {
      log_info("order mismatch got={} expect={}", out[i], i);
      std::abort();
    }
  }

  log_info("[test_basic_single_thread] done");
}

static void test_wraparound() {
  using Channel = Bounded_queue_coro<Segmented_queue<int>>;

  log_info("[test_wraparound] start");
  Channel::Handle handle{};
  Channel ch({.m_slots_per_segment = 8, .m_num_segments = 4, .m_num_stripes = 4});

  for (int i = 0; i < 8; ++i) {
    if (!ch.try_enqueue(handle, i)) {
      log_info("initial enqueue fail i={}", i);
      std::abort();
    }
  }

  for (int i = 0; i < 4; ++i) {
    int v{};
    if (!ch.try_dequeue(handle, v)) {
      log_info("dequeue(phase1) fail i={}", i);
      std::abort();
    }
    if (v != i) {
      log_info("order(phase1) mismatch got={} expect={}", v, i);
      std::abort();
    }
  }

  for (int i = 8; i < 12; ++i) {
    if (!ch.try_enqueue(handle, i)) {
      log_info("enqueue(phase2) fail i={}", i);
      std::abort();
    }
  }

  std::vector<int> out;

  int v{};

  while (ch.try_dequeue(handle, v)) {
    out.push_back(v);
  }

  std::vector<int> expect{4,5,6,7,8,9,10,11};

  if (out != expect) {
    log_info(stderr, "wrap result mismatch");
    log_info(stderr, "got: {}", out);
    for (auto x: out) {
      std::print("{} ", x);
    }
    log_info("");
    log_info("exp: {}", expect);

    for (auto x: expect) {
      std::print("{} ", x);
    }

    log_info("");
    std::abort();
  }
  assert(ch.empty());
  log_info("[test_wraparound] done");
}

[[maybe_unused]] Task<void> producer_coro(Channel& ch, util::Thread_pool& pool, std::size_t start, std::size_t count, std::atomic<std::size_t>& produced) {

  co_await pool.schedule();

  Channel::Handle handle{};

  for (std::size_t i = start; i < start + count; ++i) {
    std::size_t v = i;

    if (!ch.try_enqueue(handle, v)) {
      co_await ch.enqueue_awaitable(v);
    }
    produced.fetch_add(1, std::memory_order_relaxed);
  }

  co_return;
}

[[maybe_unused]] Task<void> consumer_coro(Channel& ch, util::Thread_pool& pool, std::size_t n, std::atomic<std::size_t>& consumed, std::vector<std::size_t>& consumed_values) noexcept {

  co_await pool.schedule();

  Channel::Handle handle{};

  do {
    std::size_t v{};

    if (!ch.try_dequeue(handle, v)) {
      co_await ch.dequeue_awaitable(v);
    }
    consumed.fetch_add(1, std::memory_order_relaxed);
    consumed_values.push_back(v);

  } while (consumed.load(std::memory_order_acquire) < n);

  co_return;
}

[[maybe_unused]] static void test_coro_mpmc_correctness() {
  log_info("[test_coro_mpmc_correctness] start");

  constexpr std::size_t num_producers = 1;
  constexpr std::size_t num_consumers = 1;
  constexpr std::size_t items_per_producer = 100;
  constexpr std::size_t total_items = num_producers * items_per_producer;

  Channel ch({.m_slots_per_segment = 8, .m_num_segments = 2});
  util::Thread_pool pool(4);

  std::atomic<std::size_t> produced{0};

  for (std::size_t i = 0; i < num_producers; ++i) {
    std::size_t start = i * items_per_producer;
    detach(producer_coro(ch, pool, start, items_per_producer, produced));
  }

  std::atomic<std::size_t> consumed{0};
  std::vector<std::vector<std::size_t>> consumed_values;

  consumed_values.resize(num_consumers);

  for (std::size_t i = 0; i < num_consumers; ++i) {
    consumed_values[i].reserve(total_items);
    detach(consumer_coro(ch, pool, total_items, consumed, consumed_values[i]));
  }

  std::size_t iterations = 0;
  while (produced.load(std::memory_order_acquire) < total_items) {
    std::this_thread::yield();
    if (++iterations % 1000000 == 0) {
      log_info("[test_coro_mpmc_correctness] waiting for produced: {}/{}", produced.load(), total_items);
    }
  }
  log_info("[test_coro_mpmc_correctness] produced complete: {}", produced.load());

  iterations = 0;
  while (consumed.load(std::memory_order_acquire) < total_items) {
    std::this_thread::yield();
    if (++iterations % 1000000 == 0) {
      log_info("[test_coro_mpmc_correctness] waiting for consumed: {}/{}", consumed.load(), total_items);
    }
  }
  log_info("[test_coro_mpmc_correctness] consumed complete: {}", consumed.load());

  consumed_values[0].reserve(consumed.load(std::memory_order_acquire));

  for (std::size_t i = 1; i < num_consumers; ++i) {
    consumed_values[0].insert(consumed_values[0].end(), consumed_values[i].begin(), consumed_values[i].end());
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  assert(consumed_values[0].size() == total_items);

  std::sort(consumed_values[0].begin(), consumed_values[0].end());

  for (std::size_t i = 0; i < total_items; ++i) {
    if (consumed_values[0][i] != i) {
      log_info("Missing or duplicate item at index {}, got {}", i, consumed_values[0][i]);
      for (auto x: consumed_values[0]) {
        std::print("{} ", x);
      }
      log_info("");
      std::abort();
    }
  }
}

[[maybe_unused]] Task<void> producer_coro_st(Channel& ch, std::size_t start, std::size_t n) noexcept {

  Channel::Handle handle{};

  for (std::size_t i = start; i < start + n; ++i) {
    std::size_t v = i;
    if (!ch.try_enqueue(handle, v)) {
      co_await ch.enqueue_awaitable(v);
    }
  }
  co_return;
}

[[maybe_unused]] Task<void> consumer_coro_st(Channel& ch, std::size_t n, std::vector<std::size_t>& consumed) noexcept {

  Channel::Handle handle{};

  for (std::size_t i = 0; i < n; ++i) {
    std::size_t v = ~0UL;
    if (!ch.try_dequeue(handle, v)) {
      co_await ch.dequeue_awaitable(v);
    }
    consumed.push_back(v);
  }
  co_return;
}

[[maybe_unused]] static void test_coro_mpmc_correctness_st() noexcept {
  log_info("[test_coro_mpmc_correctness_st] start (single-threaded)");

  constexpr std::size_t n_producers = 2;
  constexpr std::size_t n_consumers = 2;
  constexpr std::size_t items_per_producer = 100;
  constexpr std::size_t total_items = n_producers * items_per_producer;

  Channel ch({.m_slots_per_segment = 8, .m_num_segments = 2});
  std::vector<Task<void>> producer_tasks{};
  std::vector<Task<void>> consumer_tasks{};
  std::vector<std::size_t> consumed{};

  consumed.reserve(total_items);

  for (std::size_t i = 0; i < n_producers; ++i) {
    std::size_t start = i * items_per_producer;
    producer_tasks.push_back(producer_coro_st(ch, start, items_per_producer));
  }

  for (std::size_t i = 0; i < n_consumers; ++i) {
    consumer_tasks.push_back(consumer_coro_st(ch, total_items, consumed));
  }

  for (auto& task : producer_tasks) {
    task.start();
  }

  for (auto& task : consumer_tasks) {
    task.start();
  }

  assert(consumed.size() == total_items);

  std::sort(consumed.begin(), consumed.end());

  for (std::size_t i = 0; i < total_items; ++i) {
    if (consumed[i] != i) {
      log_info("Missing or duplicate item at index {}, got {}", i, consumed[i]);
      std::abort();
    }
  }

  log_info("[test_coro_mpmc_correctness_st] done");
}

struct Perf_config {
  std::size_t m_num_producers{2};
  std::size_t m_num_consumers{2};
  std::size_t m_items_per_producer{1000000};
  std::size_t m_channel_size{1024};
  std::size_t m_pool_threads{4};
};

[[maybe_unused]] Task<void> perf_producer(Channel& ch, util::Thread_pool& pool, std::size_t n, std::atomic<bool>& start_flag, std::atomic<std::size_t>& produced) {

  co_await pool.schedule();

  while (!start_flag.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  Channel::Handle handle{};

  for (std::size_t i = 0; i < n; ++i) {
    std::size_t val = i;

    /* Yield to pool if can't enqueue */
    while (!ch.try_enqueue(handle, val)) [[unlikely]] {
      co_await pool.schedule();
    }

    produced.fetch_add(1, std::memory_order_relaxed);
  }

  co_return;
}

[[maybe_unused]] Task<void> perf_consumer(Channel& ch, util::Thread_pool& pool, std::size_t n, std::atomic<bool>& start_flag, std::atomic<std::size_t>& consumed) {

  co_await pool.schedule();

  while (!start_flag.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }

  Channel::Handle handle{};

  for (std::size_t i = 0; i < n; ++i) {
    std::size_t v{};

    /* Yield to pool if can't dequeue */
    while (!ch.try_dequeue(handle, v)) [[unlikely]] {
      co_await pool.schedule();
    }

    consumed.fetch_add(1, std::memory_order_relaxed);
  }

  log_info("consumer {} : done", consumed.load(std::memory_order_acquire));

  co_return;
}

[[maybe_unused]] static void test_coro_performance(const Perf_config& config) {
  log_info("[test_coro_performance] producers={}, consumers={}, items_per_producer={}, channel_size={}, pool_threads={}",
               config.m_num_producers, config.m_num_consumers, config.m_items_per_producer, config.m_channel_size, config.m_pool_threads);

  const std::size_t total_items = config.m_num_producers * config.m_items_per_producer;

  std::size_t num_segments = 8;
  std::size_t slots_per_segment = (config.m_channel_size + num_segments - 1) / num_segments;

  slots_per_segment = std::bit_ceil(slots_per_segment);

  Channel ch({.m_slots_per_segment = slots_per_segment, .m_num_segments = num_segments});

  util::Thread_pool pool(config.m_pool_threads);

  std::atomic<bool> start_flag{false};

  std::atomic<std::size_t> produced{0};

  for (std::size_t i = 0; i < config.m_num_producers; ++i) {
    detach(perf_producer(ch, pool, config.m_items_per_producer, start_flag, produced));
  }

  std::atomic<std::size_t> consumed{0};
  const std::size_t items_per_consumer = total_items / config.m_num_consumers;

  for (std::size_t i = 0; i < config.m_num_consumers; ++i) {
    detach(perf_consumer(ch, pool, items_per_consumer, start_flag, consumed));
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  start_flag.store(true, std::memory_order_release);
  auto start = std::chrono::steady_clock::now();

  while (produced.load(std::memory_order_acquire) < total_items) {
    std::this_thread::yield();
  }

  while (consumed.load(std::memory_order_acquire) < total_items) {
    std::this_thread::yield();
  }

  auto end = std::chrono::steady_clock::now();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  double elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;
  double ops_per_sec = static_cast<double>(total_items) / elapsed_s;
  double ns_per_op = static_cast<double>(elapsed_ns.count()) / static_cast<double>(total_items);

  /* Format throughput with appropriate units (K/s, M/s, etc.) */
  const char* throughput_unit = "ops/s";
  double throughput_value = ops_per_sec;

  if (ops_per_sec >= 1'000'000'000.0) {
    throughput_value = ops_per_sec / 1'000'000'000.0;
    throughput_unit = "G ops/s";
  } else if (ops_per_sec >= 1'000'000.0) {
    throughput_value = ops_per_sec / 1'000'000.0;
    throughput_unit = "M ops/s";
  } else if (ops_per_sec >= 1'000.0) {
    throughput_value = ops_per_sec / 1'000.0;
    throughput_unit = "K ops/s";
  }

  log_info("[test_coro_performance] produced={}, consumed={}, elapsed={:.3f}s, throughput={:.2f} {}, latency={:.1f} ns/op",
               produced.load(), consumed.load(), elapsed_s, throughput_value, throughput_unit, ns_per_op);

  assert(produced.load() == total_items);
  assert(consumed.load() == total_items);
}


int main() {
  log_info("[main] Starting tests...");
  log_info("[main] test_basic_single_thread");
  test_basic_single_thread();
  log_info("[main] test_wraparound");
  test_wraparound();
  log_info("[main] test_coro_mpmc_correctness_st");
  test_coro_mpmc_correctness_st();
  log_info("[main] test_coro_mpmc_correctness");
  test_coro_mpmc_correctness();

  Perf_config config{
    .m_num_producers = 1,
    .m_num_consumers = 1,
    .m_items_per_producer = 20'000'000,
    .m_channel_size = 1024,
    .m_pool_threads = 1
  };

  log_info("[main] test_coro_performance");
  test_coro_performance(config);

  log_info("\n=== All tests passed ===");

  return 0;
}
