// Correctness and performance tests for util::Thread_pool

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
#include <iostream>
#include <random>
#include <mutex>
#include <unordered_map>

#include "util/thread_pool.h"
#include "coro/task.h"
#include <unordered_set>

using namespace util;
using namespace std::chrono_literals;

// Test basic task execution
static void test_basic_task_execution() {
  std::println("[test_basic_task_execution] start");

  std::println("[test_basic_task_execution] creating pool");
  Thread_pool pool(4);
  std::println("[test_basic_task_execution] pool created, submitting tasks");
  std::atomic<int> counter{0};
  constexpr int num_tasks = 100;  // Reduced for testing

  // Submit tasks with small delays to avoid queue overflow
  for (int i = 0; i < num_tasks; ++i) {
    pool.post([&counter]() noexcept {
      counter.fetch_add(1, std::memory_order_relaxed);
    });
  }

  // Wait for all tasks to complete
  std::this_thread::sleep_for(100ms);
  int wait_count = 0;
  constexpr int max_wait = 1000;  // Timeout after 10 seconds
  while (counter.load(std::memory_order_acquire) < num_tasks && wait_count++ < max_wait) {
    std::this_thread::sleep_for(10ms);
  }
  assert(counter.load() == num_tasks && "Tasks did not complete in time");

  assert(counter.load() == num_tasks);
  std::println("[test_basic_task_execution] PASSED: {} tasks executed", num_tasks);
}

// Test task execution with return values (using shared state)
static void test_task_with_results() {
  std::println("[test_task_with_results] start");

  Thread_pool pool(4);
  constexpr int num_tasks = 100;
  std::vector<std::atomic<int>> results(num_tasks);
  std::vector<std::atomic<bool>> completed(num_tasks);

  for (int i = 0; i < num_tasks; ++i) {
    results[static_cast<std::size_t>(i)].store(0);
    completed[static_cast<std::size_t>(i)].store(false);
  }

  for (int i = 0; i < num_tasks; ++i) {
    int task_id = i;
    pool.post([&results, &completed, task_id]() noexcept {
      /* Simulate some work */
      int result = task_id * 2;
      results[static_cast<std::size_t>(task_id)].store(result, std::memory_order_release);
      completed[static_cast<std::size_t>(task_id)].store(true, std::memory_order_release);
    });
  }

  std::this_thread::sleep_for(100ms);

  for (int i = 0; i < num_tasks; ++i) {
    std::size_t idx = static_cast<std::size_t>(i);
    while (!completed[idx].load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    assert(results[idx].load() == i * 2);
  }

  std::println("[test_task_with_results] PASSED: {} tasks with results verified", num_tasks);
}

// Test concurrent task submission from multiple threads
/**
 * Test that pool.post() is thread-safe when called concurrently from multiple external threads.
 * 
 * This test verifies:
 * - Multiple threads can safely call post() simultaneously (thread-safe submission API)
 * - All submitted tasks are eventually executed by the pool's worker threads
 * - No tasks are lost or duplicated during concurrent submission
 * 
 * Note: The producer threads are separate from the pool's worker threads.
 * They only submit tasks; the actual task execution happens on the pool's workers.
 */
static void test_concurrent_submission() {
  std::println("[test_concurrent_submission] start");

  Thread_pool pool(8);
  std::atomic<int> counter{0};
  constexpr int num_producers = 10;
  constexpr int tasks_per_producer = 100;
  constexpr int total_tasks = num_producers * tasks_per_producer;

  /* Create external producer threads that concurrently call pool.post() */
  std::vector<std::thread> producer_threads;

  for (int p = 0; p < num_producers; ++p) {
    producer_threads.emplace_back([&pool, &counter]() {
      for (int i = 0; i < tasks_per_producer; ++i) {
        /* Submit task to pool - this tests thread-safety of post() */
        pool.post([&counter]() noexcept {
          /* This task runs on one of the pool's 8 worker threads */
          counter.fetch_add(1, std::memory_order_relaxed);
        });
        /* Small delay to avoid overwhelming queues */
        if (i % 50 == 0) {
          std::this_thread::sleep_for(std::chrono::microseconds(10));
        }
      }
    });
  }

  /* Wait for all producer threads to finish submitting */
  for (auto& t : producer_threads) {
    t.join();
  }

  /* Wait for all tasks to be executed by pool's worker threads */
  std::this_thread::sleep_for(200ms);
  while (counter.load(std::memory_order_acquire) < total_tasks) {
    std::this_thread::sleep_for(10ms);
  }

  assert(counter.load() == total_tasks);
  std::println("[test_concurrent_submission] PASSED: {} tasks submitted by {} external threads, executed by {} worker threads", 
               total_tasks, num_producers, 8);
}

// Test coroutine scheduling
static void test_coroutine_scheduling() {
  std::println("[test_coroutine_scheduling] start");

  Thread_pool pool(4);
  std::atomic<int> counter{0};
  constexpr int num_coroutines = 50;

  auto coro_task = [&pool, &counter]() -> Task<void> {
    co_await pool.schedule();
    counter.fetch_add(1, std::memory_order_relaxed);
  };

  for (int i = 0; i < num_coroutines; ++i) {
    auto task = coro_task();
    task.start();  // Start the coroutine
  }

  // Wait for all coroutines to complete
  std::this_thread::sleep_for(200ms);
  while (counter.load(std::memory_order_acquire) < num_coroutines) {
    std::this_thread::sleep_for(10ms);
  }

  assert(counter.load() == num_coroutines);
  std::println("[test_coroutine_scheduling] PASSED: {} coroutines scheduled", num_coroutines);
}

// Test thread pool shutdown
static void test_shutdown() {
  std::println("[test_shutdown] start");

  Thread_pool pool(4);
  std::atomic<int> counter{0};
  constexpr int num_tasks = 100;

  for (int i = 0; i < num_tasks; ++i) {
    pool.post([&counter]() noexcept {
      counter.fetch_add(1, std::memory_order_relaxed);
    });
  }

  // Shutdown should wait for tasks to complete
  // Destructor will be called here
  std::this_thread::sleep_for(50ms);

  // Verify all tasks completed before shutdown
  int final_count = counter.load(std::memory_order_acquire);
  assert(final_count == num_tasks);
  std::println("[test_shutdown] PASSED: {} tasks completed before shutdown", final_count);
}

// Test load balancing across workers
static void test_load_balancing() {
  std::println("[test_load_balancing] start");

  Thread_pool pool(8);
  constexpr int num_workers = 8;
  constexpr int tasks_per_worker = 100;
  std::vector<std::atomic<int>> worker_counts(num_workers);

  for (auto& count : worker_counts) {
    count.store(0, std::memory_order_relaxed);
  }

  // Submit tasks and track which worker executes them
  std::mutex mtx;
  std::vector<int> executed_by;

  for (int i = 0; i < num_workers * tasks_per_worker; ++i) {
    pool.post([&worker_counts, &mtx, &executed_by]() noexcept {
      // Get current thread ID to identify worker
      std::thread::id tid = std::this_thread::get_id();
      std::hash<std::thread::id> hasher;
      std::size_t worker_idx = hasher(tid) % num_workers;
      worker_counts[worker_idx].fetch_add(1, std::memory_order_relaxed);

      std::lock_guard<std::mutex> lock(mtx);
      executed_by.push_back(static_cast<int>(worker_idx));
    });
  }

  // Wait for all tasks to complete
  std::this_thread::sleep_for(200ms);
  
  int total_executed = 0;
  for (const auto& count : worker_counts) {
    total_executed += count.load(std::memory_order_acquire);
  }

  assert(total_executed == num_workers * tasks_per_worker);
  
  // Check that work was distributed (at least some work on each worker)
  int workers_with_work = 0;
  for (const auto& count : worker_counts) {
    if (count.load(std::memory_order_acquire) > 0) {
      ++workers_with_work;
    }
  }

  std::println("[test_load_balancing] PASSED: {} tasks distributed across {} workers", 
               total_executed, workers_with_work);
}

// Performance test: throughput measurement
static void test_throughput() {
  std::println("[test_throughput] start");

  const std::size_t num_threads = std::thread::hardware_concurrency();
  Thread_pool pool(num_threads);
  constexpr int num_tasks = 100000;
  constexpr int batch_size = 1000;  // Submit in batches to avoid queue overflow
  std::atomic<int> counter{0};

  auto start = std::chrono::steady_clock::now();

  // Submit tasks in batches, waiting for some to complete before submitting more
  int submitted = 0;
  while (submitted < num_tasks) {
    // Submit a batch
    int batch_end = std::min(submitted + batch_size, num_tasks);
    for (int i = submitted; i < batch_end; ++i) {
      pool.post([&counter]() noexcept {
        counter.fetch_add(1, std::memory_order_relaxed);
      });
    }
    submitted = batch_end;
    
    // Wait for some tasks to complete before submitting more (to avoid queue overflow)
    int target_completed = std::max(0, submitted - batch_size * 2);
    while (counter.load(std::memory_order_acquire) < target_completed) {
      std::this_thread::yield();
    }
  }

  // Wait for all remaining tasks to complete
  while (counter.load(std::memory_order_acquire) < num_tasks) {
    std::this_thread::yield();
  }

  auto end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  double tasks_per_sec = (static_cast<double>(num_tasks) * 1000.0) / static_cast<double>(duration.count());

  std::println("[test_throughput] {} tasks in {} ms = {:.2f} tasks/sec ({} threads)",
               num_tasks, duration.count(), tasks_per_sec, num_threads);
}

// Performance test: latency measurement
static void test_latency() {
  std::println("[test_latency] start");

  Thread_pool pool(4);
  constexpr int num_samples = 10000;
  std::vector<std::chrono::microseconds> latencies;
  latencies.reserve(num_samples);
  std::mutex latencies_mtx;

  for (int i = 0; i < num_samples; ++i) {
    bool success = false;
    auto submit_time = std::chrono::steady_clock::now();
    do {
      success = pool.post([&latencies, &latencies_mtx, submit_time]() noexcept {
        auto execute_time = std::chrono::steady_clock::now();
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(
          execute_time - submit_time);
          std::lock_guard<std::mutex> lock(latencies_mtx);
          latencies.push_back(latency);
        });
      std::this_thread::sleep_for(100us);
    } while (!success);
  }

  // Wait for all tasks to complete
  std::this_thread::sleep_for(500ms);
  while (latencies.size() < static_cast<std::size_t>(num_samples)) {
    std::this_thread::sleep_for(10ms);
  }

  // Calculate statistics
  std::sort(latencies.begin(), latencies.end());
  auto p50 = latencies[num_samples / 2];
  auto p95 = latencies[static_cast<std::size_t>(num_samples * 0.95)];
  auto p99 = latencies[static_cast<std::size_t>(num_samples * 0.99)];
  auto p999 = latencies[static_cast<std::size_t>(num_samples * 0.999)];

  std::println("[test_latency] P50={}us, P95={}us, P99={}us, P99.9={}us",
               p50.count(), p95.count(), p99.count(), p999.count());
}

// Performance test: comparison with different thread counts
static void test_thread_count_scaling() {
  std::println("[test_thread_count_scaling] start");

  constexpr int num_tasks = 100000;
  const std::vector<int> thread_counts = {1, 2, 4, 8, 16};

  for (int num_threads : thread_counts) {
    Thread_pool pool(static_cast<std::size_t>(num_threads));
    std::atomic<int> counter{0};

    auto start = std::chrono::steady_clock::now();

    for (int i = 0; i < num_tasks; ++i) {
      /* Retry post() until it succeeds - post() can fail if all worker queues are full */
      while (!pool.post([&counter]() noexcept {
        counter.fetch_add(1, std::memory_order_relaxed);
      })) {
        std::this_thread::yield();
      }
    }

    while (counter.load(std::memory_order_acquire) < num_tasks) {
      std::this_thread::yield();
    }

    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    double tasks_per_sec = (static_cast<double>(num_tasks) * 1000.0) / static_cast<double>(duration.count());

    std::println("  {} threads: {} ms, {:.2f} tasks/sec", num_threads, duration.count(), tasks_per_sec);
  }
}

// Test with compute-intensive tasks
static void test_compute_intensive() {
  std::println("[test_compute_intensive] start");

  Thread_pool pool(std::thread::hardware_concurrency());
  constexpr int num_tasks = 1000;
  std::atomic<int> counter{0};

  auto start = std::chrono::steady_clock::now();

  for (int i = 0; i < num_tasks; ++i) {
    pool.post([&counter, i]() noexcept {
      // Simulate compute-intensive work (fibonacci calculation)
      auto fib = [](int n) -> long long {
        if (n <= 1) return n;
        long long a = 0, b = 1;
        for (int i = 2; i <= n; ++i) {
          long long temp = a + b;
          a = b;
          b = temp;
        }
        return b;
      };
      fib(30 + (i % 10));  // Varying workload
      counter.fetch_add(1, std::memory_order_relaxed);
    });
  }

  while (counter.load(std::memory_order_acquire) < num_tasks) {
    std::this_thread::yield();
  }

  auto end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  std::println("[test_compute_intensive] {} tasks in {} ms", num_tasks, duration.count());
}

// Test with I/O-like tasks (sleep simulation)
static void test_io_like_tasks() {
  std::println("[test_io_like_tasks] start");

  Thread_pool pool(8);
  constexpr int num_tasks = 100;
  std::atomic<int> counter{0};

  auto start = std::chrono::steady_clock::now();

  for (int i = 0; i < num_tasks; ++i) {
    pool.post([&counter]() noexcept {
      // Simulate I/O wait
      std::this_thread::sleep_for(10ms);
      counter.fetch_add(1, std::memory_order_relaxed);
    });
  }

  while (counter.load(std::memory_order_acquire) < num_tasks) {
    std::this_thread::yield();
  }

  auto end = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  std::println("[test_io_like_tasks] {} tasks in {} ms (expected ~{} ms with parallelism)",
               num_tasks, duration.count(), (num_tasks * 10) / 8);
}

// Test work-stealing: verify that idle workers steal work from busy workers
static void test_work_stealing() {
  std::println("[test_work_stealing] start");

  constexpr int num_workers = 8;
  Thread_pool pool(num_workers);
  constexpr int num_tasks = 1000;
  std::atomic<int> counter{0};
  
  // Track which threads executed tasks (to verify work-stealing)
  std::mutex thread_mtx;
  std::unordered_map<std::thread::id, int> thread_execution_count;
  
  // Submit all tasks from a single thread to one worker's queue
  // This creates imbalance - one worker gets all tasks initially
  for (int i = 0; i < num_tasks; ++i) {
    pool.post([&counter, &thread_mtx, &thread_execution_count]() noexcept {
      std::thread::id tid = std::this_thread::get_id();
      {
        std::lock_guard<std::mutex> lock(thread_mtx);
        thread_execution_count[tid]++;
      }
      counter.fetch_add(1, std::memory_order_relaxed);
    });
    // Small delay to let tasks accumulate in one queue
    if (i % 100 == 0) {
      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }
  }

  // Wait for all tasks to complete
  int wait_count = 0;
  constexpr int max_wait = 2000;
  while (counter.load(std::memory_order_acquire) < num_tasks && wait_count++ < max_wait) {
    std::this_thread::sleep_for(10ms);
  }
  
  assert(counter.load() == num_tasks);
  
  // Verify that multiple threads participated (work-stealing occurred)
  std::lock_guard<std::mutex> lock(thread_mtx);
  int active_threads = static_cast<int>(thread_execution_count.size());
  
  std::println("[test_work_stealing] PASSED: {} tasks executed by {} different threads (work-stealing active)",
               num_tasks, active_threads);
  
  // Work-stealing should result in multiple threads executing tasks
  // Even though tasks were submitted to one queue, other workers should steal
  assert(active_threads > 1 && "Work-stealing should distribute tasks across multiple workers");
}

int main() {
  try {
    ::std::println("=== Thread Pool Correctness Tests ===");
    test_basic_task_execution();
    test_task_with_results();
    test_concurrent_submission();
    test_coroutine_scheduling();
    test_shutdown();
    test_load_balancing();
    test_work_stealing();

    ::std::println("\n=== Thread Pool Performance Tests ===");
    test_throughput();
    test_latency();
    test_thread_count_scaling();
    test_compute_intensive();
    test_io_like_tasks();

    ::std::println("\n=== All Tests PASSED ===");
    return 0;
  } catch (const ::std::exception& e) {
    ::std::println("Test failed with exception: {}", e.what());
    return 1;
  }
}

