#include <atomic>
#include <chrono>
#include <thread>
#include <vector>
#include <cassert>
#include <mutex>
#include <algorithm>

#include "util/segmented_queue.h"
#include "util/logger.h"

using namespace util;
using namespace std::chrono_literals;

/* Global logger for this test file */
static util::Logger<util::MT_logger_writer> g_logger(util::MT_logger_writer{std::cerr}, util::Log_level::Info);

void test_basic_operations() {
  log_info("Testing basic operations...");

  Segmented_queue<int> queue({.m_slots_per_segment = 64, .m_num_segments = 4});

  {
    Segmented_queue<int>::Handle handle{};
    [[maybe_unused]] auto ok = queue.enqueue(handle, 1);
    assert(ok);
  }

  {
    Segmented_queue<int>::Handle handle{};
    [[maybe_unused]] auto ok = queue.enqueue(handle, 2);
    assert(ok);
  }

  {
    Segmented_queue<int>::Handle handle{};
    [[maybe_unused]] auto ok = queue.enqueue(handle, 3);
    assert(ok);
  }

  {
    int val = 0;
    std::size_t stripe_idx = 0;

    /* It doesn't guarantee the order of dequeue but in this case we know it will be 1, 2, 3.*/
    {
      [[maybe_unused]] auto ok = queue.dequeue(stripe_idx, val);
      assert(ok && val == 1);
    }

    {
      [[maybe_unused]] auto ok = queue.dequeue(stripe_idx, val);
      assert(ok && val == 2);
    }

    {
      [[maybe_unused]] auto ok = queue.dequeue(stripe_idx, val);
      assert(ok && val == 3);
    }

    {
      [[maybe_unused]] auto ok = queue.dequeue(stripe_idx, val);
      assert(!ok);
    }
  }

  {
    Segmented_queue<int>::Handle handle{};
    [[maybe_unused]] auto ok = queue.enqueue(handle, 42);
    assert(ok);
  }

  {
    std::size_t stripe_idx = 0;

    [[maybe_unused]] auto opt = queue.try_dequeue(stripe_idx);
    assert(opt.has_value());
    assert(opt.value() == 42);
  }

  {
    [[maybe_unused]] std::size_t stripe_idx = 0;
    assert(!queue.try_dequeue(stripe_idx).has_value());
  }

  {
    Segmented_queue<int>::Handle handle{};
    [[maybe_unused]] auto ok = queue.emplace(handle, 100);

    assert(ok);
  }

  {
    Segmented_queue<int>::Handle handle{};
    [[maybe_unused]] auto ok = queue.emplace(handle, 100);

    assert(ok);
  }

  log_info("Basic operations: PASSED");
}

void test_empty_full() {
  log_info("Testing empty/full detection...");

  Segmented_queue<int> queue({.m_slots_per_segment = 4, .m_num_segments = 2});

  assert(queue.empty());
  assert(!queue.full());

  std::vector<Segmented_queue<int>::Handle> handles(8);

  // With 8 stripes, each stripe has capacity 2 (rounded up from 1), so total is 16
  // Fill the queue to test full() - enqueue 16 items
  for (std::size_t i = 0; i < 16; ++i) {
    [[maybe_unused]] auto ok = queue.enqueue(handles[i % 8], static_cast<int>(i));
    assert(ok);
  }

  assert(!queue.empty());
  assert(queue.full());

  Segmented_queue<int>::Handle full_handle{};

  [[maybe_unused]] bool ok = queue.enqueue(full_handle, 999);
  assert(!ok);

  {
    int val = 0;
    std::size_t stripe_idx = 0;

    // Dequeue all 16 items
    for (int i = 0; i < 16; ++i) {
      [[maybe_unused]] bool ok = queue.dequeue(stripe_idx, val);
      assert(ok);
      // Note: values may not be in order due to non-FIFO nature
    }
  }

  assert(queue.empty());
  assert(!queue.full());

  log_info("Empty/full detection: PASSED");
}


void test_multi_threaded() {
  log_info("Testing multi-threaded operations...");

  constexpr int num_stripes = 8;
  constexpr int num_producers = 8;
  constexpr int num_consumers = 2;
  constexpr int items_per_producer = 10000;
  constexpr int total_items = num_producers * items_per_producer;

  Segmented_queue<int> queue({.m_slots_per_segment = 1024, .m_num_segments = num_stripes});

  std::atomic<int> enqueue_count{0};
  std::atomic<int> dequeue_count{0};
  std::atomic<bool> producers_done{false};

  std::vector<std::thread> producers;

  for (int p = 0; p < num_producers; ++p) {
    producers.emplace_back([&, p]() {
      Segmented_queue<int>::Handle handle{};

      handle.m_stripe_idx = static_cast<std::size_t>(p) % num_stripes;

      for (int i = 0; i < items_per_producer; ++i) {
        int value = p * items_per_producer + i;

        while (!queue.enqueue(handle, value)) {
          std::this_thread::yield();
        }
      }

      enqueue_count.fetch_add(items_per_producer, std::memory_order_relaxed);
    });
  }

  std::mutex consumed_mutex;
  std::vector<int> consumed_values;
  std::vector<std::thread> consumers;
  consumed_values.reserve(static_cast<std::size_t>(total_items));

  for (int c = 0; c < num_consumers; ++c) {
    consumers.emplace_back([&, c]() {
      int val;
      std::vector<int> local_values;
      std::size_t stripe_idx = static_cast<std::size_t>(c);

      while (!producers_done.load(std::memory_order_acquire) || !queue.empty()) {
        if (queue.dequeue(stripe_idx, val)) {
          local_values.push_back(val);
        } else {
          std::this_thread::yield();
        }
      }

      std::lock_guard<std::mutex> lock(consumed_mutex);
      consumed_values.insert(consumed_values.end(), local_values.begin(), local_values.end());
      dequeue_count.fetch_add(static_cast<int>(local_values.size()), std::memory_order_relaxed);
    });
  }

  for (auto& t : producers) {
    t.join();
  }

  producers_done.store(true, std::memory_order_release);

  for (auto& t : consumers) {
    t.join();
  }

  assert(enqueue_count.load() == total_items);
  assert(dequeue_count.load() == total_items);
  assert(consumed_values.size() == static_cast<std::size_t>(total_items));
  assert(queue.empty());

  std::sort(consumed_values.begin(), consumed_values.end());

  for (int i = 0; i < total_items; ++i) {
    assert(consumed_values[static_cast<std::size_t>(i)] == i);
  }

  log_info("Multi-threaded operations: PASSED ({} producers, {} consumers, {} items)",
          num_producers, num_consumers, total_items);
}

void benchmark_throughput() {
  log_info("Running throughput benchmark...");

  /* First, run single producer/consumer baseline */
  {
    constexpr int total_items = 20'000'000;
    Bounded_queue<int> queue(1024);
    std::atomic<bool> start{false};

    auto start_time = std::chrono::steady_clock::now();

    std::thread producer([&]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }

      for (int i = 0; i < total_items; ++i) {
        while (!queue.enqueue(i)) {
          std::this_thread::yield();
        }
      }
    });

    std::thread consumer([&]() {
      int val;
      int count = 0;

      while (count < total_items) {
        while (!queue.dequeue(val)) {
          std::this_thread::yield();
        }
        ++count;
      }
    });

    start.store(true, std::memory_order_release);

    producer.join();
    consumer.join();

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    auto ops_per_sec = (static_cast<double>(total_items) * 1000.0) / static_cast<double>(duration.count());

    /* Format throughput with appropriate units */
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

    log_info("Bounded_queue (1P/1C):        {} ops in {} ms = {:.2f} {} [BASELINE]",
            total_items, duration.count(), throughput_value, throughput_unit);
  }

  constexpr int num_stripes = 8;
  constexpr int num_producers = 32;
  constexpr int items_per_producer = 100000;
  constexpr int total_items = num_producers * items_per_producer;

  {
    Segmented_queue<int> queue({.m_slots_per_segment = 1024, .m_num_segments = num_stripes});
    std::atomic<bool> start{false};
    std::atomic<int> completed{0};

    auto start_time = std::chrono::steady_clock::now();
    std::vector<std::thread> producers;

    for (int p = 0; p < num_producers; ++p) {
      producers.emplace_back([&, p]() {
        while (!start.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }

        Segmented_queue<int>::Handle handle{};

        handle.m_stripe_idx = static_cast<std::size_t>(p) % num_stripes;

        for (int i = 0; i < items_per_producer; ++i) {
          while (!queue.enqueue(handle, p * items_per_producer + i)) {
            std::this_thread::yield();
          }
        }
        completed.fetch_add(1, std::memory_order_release);
      });
    }

    std::thread consumer([&]() {
      int val;
      int count = 0;
      std::size_t stripe_idx = 0;

      while (count < total_items) {
        if (queue.dequeue(stripe_idx, val)) {
          ++count;
        }
      }
    });

    start.store(true, std::memory_order_release);

    for (auto& t : producers) {
      t.join();
    }
    consumer.join();

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    auto ops_per_sec = (static_cast<double>(total_items) * 1000.0) / static_cast<double>(duration.count());

    /* Format throughput with appropriate units (K/s, M/s, G/s) */
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

    log_info("Segmented_queue (8 segments): {} ops in {} ms = {:.2f} {}",
            total_items, duration.count(), throughput_value, throughput_unit);
  }

  {
    Bounded_queue<int> queue(8192);
    std::atomic<bool> start{false};
    std::vector<std::thread> producers;
    auto start_time = std::chrono::steady_clock::now();

    for (int p = 0; p < num_producers; ++p) {
      producers.emplace_back([&, p]() {
        while (!start.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }

        for (int i = 0; i < items_per_producer; ++i) {
          while (!queue.enqueue(p * items_per_producer + i)) {
            std::this_thread::yield();
          }
        }
      });
    }

    std::thread consumer([&]() {
      int val;
      int count = 0;

      while (count < total_items) {
        while (!queue.dequeue(val)) {
          std::this_thread::yield();
        }
        ++count;
      }
    });

    start.store(true, std::memory_order_release);

    for (auto& t : producers) {
      t.join();
    }
    consumer.join();

    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
    auto ops_per_sec = (static_cast<double>(total_items) * 1000.0) / static_cast<double>(duration.count());

    /* Format throughput with appropriate units (K/s, M/s, G/s) */
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

    log_info("Bounded_queue:      {} ops in {} ms = {:.2f} {}", total_items, duration.count(), throughput_value, throughput_unit);
  }

  log_info("Throughput benchmark complete");
}
void test_segment_distribution() {
  log_info("Testing segment distribution...");

  constexpr int num_segments = 8;
  Segmented_queue<int> queue({.m_slots_per_segment = 64, .m_num_segments = num_segments});

  constexpr int num_threads = 32;
  constexpr int items_per_thread = 100;
  constexpr int total_items = num_threads * items_per_thread;

  std::atomic<int> enqueued_count{0};
  std::atomic<int> dequeued_count{0};
  std::atomic<bool> producers_done{false};

  std::vector<std::thread> threads;
  for (int t = 0; t < num_threads; ++t) {
    threads.emplace_back([&, t]() {
      Segmented_queue<int>::Handle handle{};

      handle.m_stripe_idx = static_cast<std::size_t>(t) % 8;

      for (int i = 0; i < items_per_thread; ++i) {
        while (!queue.enqueue(handle, t * items_per_thread + i)) {
          std::this_thread::yield();
        }
        enqueued_count.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }

  std::thread consumer([&]() {
    int val;
    std::size_t stripe_idx = 0;

    while (!producers_done.load(std::memory_order_acquire) || !queue.empty()) {
      if (queue.dequeue(stripe_idx, val)) {
        dequeued_count.fetch_add(1, std::memory_order_relaxed);
      } else {
        std::this_thread::yield();
      }
    }
  });

  for (auto& t : threads) {
    t.join();
  }
  producers_done.store(true, std::memory_order_release);

  consumer.join();

  assert(enqueued_count.load() == total_items);
  assert(dequeued_count.load() == total_items);
  assert(queue.empty());

  log_info("Segment distribution test: PASSED ({} items enqueued and dequeued)", total_items);
}

int main() {
  try {
    test_basic_operations();
    test_empty_full();
    test_segment_distribution();
    test_multi_threaded();
    benchmark_throughput();

    log_info("All tests PASSED!");
    return 0;

  } catch (const std::exception& e) {
    log_err("Test failed with exception: {}", e.what());
    return 1;
  }
}
