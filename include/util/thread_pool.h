#pragma once

#include <vector>
#include <thread>
#include <stop_token>
#include <functional>
#include <atomic>
#include <coroutine>
#include <utility>
#include <cstdio>
#include <algorithm>
#include <cstdint>
#include <optional>
#include <climits>
#include <memory>
#include <mutex>
#include <print>
#include <condition_variable>

#ifdef __linux__
#include <linux/futex.h>
#include <sys/syscall.h>
#include <unistd.h>
#endif

#include "util/bounded_channel.h"
#include "util/util.h"

namespace util {

// Thread-local worker ID for current thread (0 for non-worker threads)
inline thread_local std::size_t g_thread_pool_worker_id = 0;

#ifdef __linux__
/**
 * Linux_parker - multi-waiter futex-based parking lot.
 *
 * State semantics:
 * - count >= 0 : number of available permits (coalesced notify_one calls)
 * - count < 0  : negative of the number of waiters (e.g., -3 means 3 waiters)
 *
 * This uses a single counter to avoid lost wakeups: each park() decrements,
 * and if the result is < 0, we sleep on that exact value. Each notify_one()
 * increments, and if the old value was < 0, we wake one waiter.
 */
struct Linux_parker {
  static_assert(std::atomic_ref<int>::is_always_lock_free, "atomic_ref<int> must be lock-free for futex use");

  /**
   * Park the current thread until someone calls notify_one().
   *
   * Decrements the counter. If the new value is < 0, sleeps on that exact value.
   * After waking, if the counter is >= 0, we got the permit. Otherwise, sleep again.
   * This guarantees that each release() will wake exactly one waiter until
   * the count becomes non-negative.
   */
  void park() noexcept {
    std::atomic_ref<int> count(m_count);

    for (;;) {
      int current = count.load(std::memory_order_acquire);

      /* Fast path: try to consume an available permit without sleeping. */
      if (current > 0) {
        if (count.compare_exchange_weak(current, current - 1, std::memory_order_acq_rel, std::memory_order_acquire)) {
          return;
        }
        /* current reloaded by failed CAS */
        continue;
      }

      /* No permits available: decrement to become a waiter. */
      int expected = current;
      int new_value = current - 1;
      if (count.compare_exchange_weak(expected, new_value, std::memory_order_acq_rel, std::memory_order_acquire)) {
        /* We successfully registered as a waiter. If new_value < 0, sleep on that exact value. */
        if (new_value < 0) {
          /* Sleep on the exact value we decremented to. When we wake, check if we got the permit. */
          do {
            futex_wait(m_count, new_value);
            /* After waking, check if we got a permit. The counter was incremented by notify_one(),
             * so if it's now >= 0, we got the permit and can return. */
            current = count.load(std::memory_order_acquire);
            if (current >= 0) {
              return;
            }
            /* Still negative. If it changed, update new_value and sleep on the new value.
             * Otherwise, might be spurious wakeup, sleep again on same value. */
            if (current != new_value) {
              new_value = current;
            }
          } while (current < 0);
        } else {
          /* Between decrementing and the CAS, someone incremented the counter.
           * new_value >= 0 means we got a permit, so we can return. */
          return;
        }
      }
      /* CAS failed, retry */
    }
  }

  /**
   * Wake exactly one parked thread
   *
   * Increments the counter. If the old value was < 0, there's at least one
   * sleeper, so wake one. This guarantees each release wakes one waiter
   * until the count becomes non-negative.
   */
  void notify_one() noexcept {
    std::atomic_ref<int> count(m_count);
    int old_value = count.fetch_add(1, std::memory_order_acq_rel);
    
    /* If old_value < 0, there's at least one sleeper waiting on a value <= old_value.
     * Since we incremented, one of them (waiting on old_value) can now be woken. */
    if (old_value < 0) {
      futex_wake(m_count, 1);
    }
  }

  static inline int futex_wait(int& addr, int expected) noexcept {
    return static_cast<int>(syscall(SYS_futex, &addr, FUTEX_WAIT_PRIVATE, expected, nullptr, nullptr, 0));
  }

  static inline int futex_wake(int& addr, int n) noexcept {
    return static_cast<int>(syscall(SYS_futex, &addr, FUTEX_WAKE_PRIVATE, n, nullptr, nullptr, 0));
  }

  /* Testing helpers: observe internal counter. */
  int debug_state() const noexcept { return m_count; }
  int debug_waiters() const noexcept { 
    int c = m_count;
    return c < 0 ? -c : 0;
  }

private:
  /* Single counter: >= 0 means available permits, < 0 means waiters.
   * Start at 0: first park blocks. */
  int m_count{0};
};
#endif /* __linux__ */

/**
 * Portable_parker - Portable implementation for platforms using std::mutex and std::condition_variable
 */
struct Portable_parker {
  void park() noexcept {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_cv.wait(lock, [this]() { return m_armed.exchange(false, std::memory_order_acq_rel); });
  }

  void notify_one() noexcept {
    std::scoped_lock lock(m_mutex);
    if (!m_armed.load(std::memory_order_acquire)) {
      m_armed.store(true, std::memory_order_release);
      m_cv.notify_one();
    }
  }

  std::mutex m_mutex{};
  std::condition_variable m_cv{};
  std::atomic<bool> m_armed{false};
};

/**
 * Parker - Templated abstraction for thread parking/unparking
 * 
 * Delegates to a concrete implementation (e.g., Linux_parker, Portable_parker).
 * Provides a consistent interface across different platforms.
 * 
 * @tparam Impl Concrete implementation type (Linux_parker, Portable_parker, etc.)
 */
template<typename Impl>
struct Parker {
  void park() noexcept {
    m_impl.park();
  }

  void notify_one() noexcept {
    m_impl.notify_one();
  }

private:
  Impl m_impl;
};

#ifdef __linux__
  using Parker_type = Parker<Linux_parker>;
#else
  using Parker_type = Parker<Portable_parker>;
#endif

/* Task structure for thread pool */
struct Thread_pool {
  /**
   * Configuration for Thread_pool instance
   */
  struct Config {
    /** Number of worker threads (0 = hardware_concurrency) */
    std::size_t m_num_threads{0};

    /** Capacity of each worker's task queue */
    std::size_t m_queue_capacity{1024};

    /** Number of protected workers (first N workers don't allow stealing) */
    std::size_t m_num_protected_workers{0};

    Config() = default;

    Config(std::size_t n_threads, std::size_t q_capacity = 1024, std::size_t n_protected = 0)
      : m_num_threads(n_threads), m_queue_capacity(q_capacity), m_num_protected_workers(n_protected) {}
  };
  
  struct Task {
    Task(Task&& rhs)  {
      m_id = std::exchange(rhs.m_id, 0);
      m_shutdown = std::exchange(rhs.m_shutdown, false);
      m_queue_id = std::exchange(rhs.m_queue_id, 0);
      m_fn = std::exchange(rhs.m_fn, nullptr);

      m_failed_steal = std::exchange(rhs.m_failed_steal, false);
      m_stolen = std::exchange(rhs.m_stolen, false);

      m_free = rhs.m_free.load(std::memory_order_acquire);
    }

    Task& operator=(Task&& rhs)  {
      if (this != &rhs) {
        m_id = std::exchange(rhs.m_id, 0);
        m_shutdown = std::exchange(rhs.m_shutdown, false);
        m_queue_id = std::exchange(rhs.m_queue_id, 0);
        m_fn = std::exchange(rhs.m_fn, nullptr);

        m_failed_steal = std::exchange(rhs.m_failed_steal, false);
        m_stolen = std::exchange(rhs.m_stolen, false);

        m_free = rhs.m_free.load(std::memory_order_acquire);
      }

      return *this;
    }

    void operator()() noexcept {
      // std::println("{}: Executing task {}.", m_queue_id, m_id);
      m_fn();
      m_fn = nullptr;
      // std::println("{}: Executed task {}.", m_queue_id, m_id);
    
    }

    Task() = default;
    Task(const Task&) = default;
    Task& operator=(const Task&) = default;
    ~Task() = default;

    std::size_t m_id{};
    bool m_shutdown{false};
    std::size_t m_queue_id{};
    std::function<void()> m_fn;

    bool m_failed_steal{false};
    bool m_stolen{false};

    std::atomic<bool> m_free{true};
  };

  using Task_queue = Bounded_queue<Task*>;
  
  /* Select appropriate parker implementation based on platform */
  struct Worker_queue {
    explicit Worker_queue(std::size_t queue_id, std::size_t capacity)
    : m_queue_id(queue_id) {
      if (!std::has_single_bit(capacity)) {
        throw std::invalid_argument("capacity must be a power of 2");
      }

      m_storage.resize(capacity);
      
      m_free_queue = std::make_unique<Task_queue>(capacity);
      m_active_queue = std::make_unique<Task_queue>(capacity);
      
      /* Initialize all tasks and add them to free list */
      for (std::size_t i = 0; i < m_storage.size(); ++i) {
        m_storage[i] = Task{};
        m_storage[i].m_id = i;
        m_storage[i].m_queue_id = m_queue_id;
        m_storage[i].m_free.store(true, std::memory_order_relaxed);
        if (!m_free_queue->enqueue(&m_storage[i])) {
          std::terminate();
        }
      }
    }

    std::size_t m_queue_id{};
    std::vector<Task> m_storage;
    std::unique_ptr<Task_queue> m_free_queue;
    std::unique_ptr<Task_queue> m_active_queue;

    /** For parking idle worker threads */
    Parker_type m_parker;
  };
  
  /**
   * Construct Thread_pool with configuration
   */
  explicit Thread_pool(const Config& config) {
    std::size_t n_threads = config.m_num_threads;

    if (n_threads == 0) {
      n_threads = std::thread::hardware_concurrency();
    }

    if (n_threads == 0) {
      /* Fallback if hardware_concurrency() returns 0 */
      n_threads = 1;
    }

    std::size_t queue_capacity = config.m_queue_capacity;

    if (queue_capacity == 0) {
      /* Default if not specified */
      queue_capacity = 1024;
    }

    m_num_protected_workers = std::min(config.m_num_protected_workers, n_threads);

    m_worker_queues.resize(n_threads);

    for (std::size_t i = 0; i < m_worker_queues.size(); ++i) {
      // std::println("Creating worker queue {}.", i);
      m_worker_queues[i] = std::make_unique<Worker_queue>(i, queue_capacity);
    }
    
    /* Create jthreads - they will auto-join on destruction,
     * Use shared stop_token from m_stop_source for coordinated shutdown */
    auto stop_token = m_stop_source.get_token();
    for (std::size_t i = 0; i < n_threads; ++i) {
      m_workers.emplace_back([this, i, stop_token]() { 
        this->worker_loop(i, stop_token); 
      });
    }
  }

  ~Thread_pool() noexcept {
    /* Request stop - workers will check stop_token and exit */
    m_stop_source.request_stop();
    
    /* Wake up workers by posting no-op tasks to each queue */
    for (std::size_t i = 0; i < m_worker_queues.size(); ++i) {
      auto& wq = *m_worker_queues[i];
      Task* task = nullptr;
      if (!wq.m_free_queue->dequeue(task)) {
        continue;
      }

      assert(task != nullptr);
      assert(task->m_fn == nullptr);
      assert(task->m_free.load(std::memory_order_acquire));

      /* Signal for shutdown. Poison pill.  */
      task->m_shutdown = true;
      task->m_free.store(false, std::memory_order_relaxed);

      [[maybe_unused]] auto success = wq.m_active_queue->enqueue(task);
      assert(success);
      wq.m_parker.notify_one();
    }
    
    /* jthread automatically joins on destruction, no manual join needed */
  }

  Thread_pool(const Thread_pool&) = delete;
  Thread_pool& operator=(const Thread_pool&) = delete;
  Thread_pool(Thread_pool&&) = delete;
  Thread_pool& operator=(Thread_pool&&) = delete;

  template<typename Fn>
  bool post(Fn&& fn) noexcept {
    return post_impl(std::forward<Fn>(fn));
  }

private:
  template<typename Fn>
  bool post_impl(Fn&& fn) noexcept {
    std::size_t worker_id;
    /* Distribute tasks using weighted round-robin with prime stride.
     * Using a prime number coprime to worker count provides better distribution
     * than simple increment, reducing hot spots and cache conflicts. */
    constexpr std::size_t stride = 17;  // Prime number for better distribution
    worker_id = (m_worker_counter.fetch_add(1, std::memory_order_relaxed) * stride) % m_worker_queues.size();
    
    /* Enqueue to worker's queue - retry with different worker if full */
    int retries = 0;

    constexpr int max_retries = 100;

    while (retries++ < max_retries) {
      auto& wq = *m_worker_queues[worker_id];
      Task* task = nullptr;
      if (!wq.m_free_queue->dequeue(task)) {
        /* No free task slot - try next worker */
        worker_id = (worker_id + 1) % m_worker_queues.size();
        continue;
      }

      task->m_free.store(false, std::memory_order_relaxed);

      task->m_stolen = false;
      task->m_failed_steal = false;

      task->m_fn = std::forward<Fn>(fn);
        
      auto target_queue = wq.m_active_queue.get();
        
      /* Since we have a free slot, the active list must have space (they share storage).
       * enqueue may fail temporarily due to lock-free CAS races, so we retry.
       * With a single worker, the worker's dequeue() races with our enqueue(),
       * so we need to yield periodically to let the worker make progress. */
      int enqueue_retries = 0;
      constexpr int max_enqueue_retries = 1000;

      while (enqueue_retries++ < max_enqueue_retries) {
        if (target_queue->enqueue(task)) {
          wq.m_parker.notify_one();
          return true;
        }
        
        /* CAS failed due to concurrent modification - retry with backoff */
        if (enqueue_retries % 13 == 0) {
          cpu_pause();
        }
        
        /* After many retries, yield to let the worker thread make progress.
         * This is especially important with a single worker where the worker's
         * dequeue() is racing with our enqueue(). */
        if (enqueue_retries % 100 == 0) {
          std::this_thread::yield();
        }
      }
      
      /* Should not happen - if we have a free slot, enqueue should eventually succeed.
       * Return task to free list and retry outer loop. */
      task->m_fn = nullptr;
      task->m_free.store(true, std::memory_order_release);

      {
        [[maybe_unused]] auto success = wq.m_free_queue->enqueue(task);
        assert(success);
      }
    }

    return false;
  }

public:

  /* Awaitable that schedules the awaiting coroutine to resume on the pool. */
  struct Schedule_awaitable {
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const {
      /* Try to post the coroutine to the thread pool */
      /* If we can't enqueue, resume directly on current thread to avoid deadlock */
      if (!m_pool.try_post_coroutine_handle(h)) {
        /* No space in queue - resume directly on current thread to avoid deadlock */
        h.resume();
      }
    }
    void await_resume() const noexcept {}

    Thread_pool& m_pool;
  };

  [[nodiscard]] Schedule_awaitable schedule() { return Schedule_awaitable(*this); }

  /* Awaitable that schedules to a specific worker for affinity */
  struct Schedule_on_awaitable {
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) const {
      if (!m_pool.try_post_coroutine_handle_on(h, m_worker_id)) {
        h.resume();
      }
    }
    void await_resume() const noexcept {}

    Thread_pool& m_pool;
    std::size_t m_worker_id;
  };

  [[nodiscard]] Schedule_on_awaitable schedule_on(std::size_t worker_id) {
    return Schedule_on_awaitable{*this, worker_id % m_worker_queues.size()};
  }

  /* Helper method for Schedule_awaitable to try posting a coroutine handle */
  bool try_post_coroutine_handle(std::coroutine_handle<> h) noexcept {
    /* Use weighted distribution like post() for consistent load balancing */
    constexpr std::size_t stride = 17;
    std::size_t worker_id = (m_worker_counter.fetch_add(1, std::memory_order_relaxed) * stride) % m_worker_queues.size();
    return try_post_coroutine_handle_on(h, worker_id);
  }

  /* Helper to post to specific worker */
  bool try_post_coroutine_handle_on(std::coroutine_handle<> h, std::size_t worker_id) noexcept {
    auto& wq = *m_worker_queues[worker_id];
    int retries = 0;
    constexpr int max_retries = 10;
    
    while (retries++ < max_retries) {
      /* Get a free task slot */
      Task* task = nullptr;
      if (wq.m_free_queue->dequeue(task)) {
        assert(task->m_free.load(std::memory_order_relaxed));

        task->m_free.store(false, std::memory_order_relaxed);
        
        /* Capture coroutine handle by value */
        task->m_fn = [h]() noexcept {
          h.resume();
        };
        
        /* Enqueue to active queue */
        if (wq.m_active_queue->enqueue(task)) {
          wq.m_parker.notify_one();
          return true;
        }
        
        /* Failed to enqueue to active queue - clear function and put slot back to free list */
        task->m_fn = nullptr;

        task->m_free.store(true, std::memory_order_relaxed);

        {
          [[maybe_unused]] auto success = wq.m_free_queue->enqueue(task);
          assert(success);
        }
      }
      
      /* Try next worker's queue */
      worker_id = (worker_id + 1) % m_worker_queues.size();
      cpu_pause();
    }
    
    /* Failed to enqueue - return false */
    return false;
  }

private:
  void worker_loop(std::size_t worker_id, std::stop_token stop_token) noexcept {
    // Set thread-local worker ID for this thread
    g_thread_pool_worker_id = worker_id;

    auto& wq = *m_worker_queues[worker_id];
    
    /* Return task to free list */
    auto return_to_free_queue = [](Task& task, Worker_queue& queue) noexcept {
      assert(!task.m_free.load(std::memory_order_acquire) );
      task.m_free.store(true, std::memory_order_release);

      while (!queue.m_free_queue->enqueue(&task)) {
        cpu_pause();
      }
    };
    
    auto execute_task_and_return_to_free_queue = [&return_to_free_queue](Task& task, Worker_queue& queue) noexcept {
      task();
      return_to_free_queue(task, queue);
    };
    
    /** @return true if should exit, false if should continue */
    auto handle_shutdown_signal = [&return_to_free_queue, &stop_token](Task& task, Worker_queue& queue) -> bool {
      /* Returns true if should exit, false if should continue */
      return_to_free_queue(task, queue);
      return stop_token.stop_requested();
    };
    
    /** @return std::nullopt if not shutdown (proceed), true if exit loop, false if shutdown handled (continue) */
    auto check_and_handle_shutdown = [&handle_shutdown_signal](Task* task, Worker_queue& queue) -> std::optional<bool> {
      if (task != nullptr && task->m_shutdown) {
        assert(task->m_fn == nullptr);
        bool should_exit = handle_shutdown_signal(*task, queue);
        /* Task has been returned to free list, don't access it anymore */
        return should_exit ? std::optional<bool>{true} : std::optional<bool>{false};
      }
      return std::nullopt;  /* Not a shutdown task, proceed with execution */
    };
    
    /** @return true if should exit loop, false if should continue loop */
    auto process_task = [&check_and_handle_shutdown, &execute_task_and_return_to_free_queue](Task* task, Worker_queue& queue) -> bool {
      const auto shutdown_result = check_and_handle_shutdown(task, queue);

      if (shutdown_result.has_value()) {
        return *shutdown_result;
      }
      
      execute_task_and_return_to_free_queue(*task, queue);

      return false;
    };
    
    auto try_to_steal_work = [this, worker_id, &stop_token]() -> Task* {
      /* Increase steal attempts for better load rebalancing.
       * With many coroutines, we want to check more victims to find work. */
      constexpr std::size_t max_steal_attempts = 16;
      auto our_wq = m_worker_queues[worker_id].get();
      std::size_t attempts = std::min(max_steal_attempts, m_worker_queues.size() - 1);

      for (std::size_t attempt = 0; attempt < attempts; ++attempt) {
        if (stop_token.stop_requested()) {
          break;
        }

        const std::size_t victim_id = (worker_id + attempt + 1) % m_worker_queues.size();

        /* Skip protected workers - they don't allow stealing */
        if (victim_id < m_num_protected_workers) {
          continue;
        }

        auto victim_wq = m_worker_queues[victim_id].get();

        if (victim_wq == our_wq) {
          /* We can't steal from our own queue. */
          continue;
        }

        Task* task_to_steal{};

        {
          auto success = victim_wq->m_active_queue->dequeue(task_to_steal);

          if (!success) {
            continue;
          }

          assert(!task_to_steal->m_free.load(std::memory_order_acquire));

          if (task_to_steal->m_shutdown) {
            assert(task_to_steal->m_fn == nullptr);
            /* Shutdown task must be executed in the work queue that it belongs to. */
            success = victim_wq->m_active_queue->enqueue(task_to_steal);
            assert(success);
            continue;
          }

          assert(task_to_steal->m_fn != nullptr);
        }

        /* We need to copy the task we are going to steal because we can't move pointers
        from one list to to another list from different work queues. */
        Task* stolen_task{};
        if (!our_wq->m_free_queue->dequeue(stolen_task)) {
          // std::println("Strange, our {} free queue is empty.", worker_id);
          [[maybe_unused]] bool success = victim_wq->m_active_queue->enqueue(task_to_steal);
          assert(success);
          break;
        }

        assert(stolen_task->m_free.load(std::memory_order_acquire));
        stolen_task->m_free.store(false, std::memory_order_relaxed);

        assert(stolen_task->m_fn == nullptr);

        std::swap(stolen_task->m_fn, task_to_steal->m_fn);

        assert(stolen_task->m_fn != nullptr);

        [[maybe_unused]] auto victim_task_id = task_to_steal->m_id;

        task_to_steal->m_stolen = true;
        task_to_steal->m_free.store(true, std::memory_order_release);

        while (!victim_wq->m_free_queue->enqueue(task_to_steal)) {
          cpu_pause();
        }

        // std::println("{}: stole task {}:{} -> {}.", worker_id, victim_task_id, victim_id, stolen_task->m_id);

        return stolen_task;
      }
      
      return nullptr;
    };

    for (;;) {

      Task* task{};

      if (!wq.m_active_queue->dequeue(task)) {
        task = nullptr;
      }

      if (task != nullptr && task->m_fn == nullptr && !task->m_shutdown) {
        std::abort();
      }

      if (task == nullptr) [[unlikely]] {
        task = try_to_steal_work();
      }

      if (task != nullptr) [[likely]] {
        assert(!task->m_free.load(std::memory_order_acquire));
        if (process_task(task, wq)) {
          return;
        }
        continue;
      }
        
      /* Check for shutdown before parking */
      if (stop_token.stop_requested()) {
        return;
      }
        
      /* No work available - park thread until woken.
       * Double-check queues before parking to avoid lost wakeup race:
       * 1. We check queues, they're empty
       * 2. Main thread enqueues task and calls notify_one()
       * 3. We park (missed the wakeup)
       * By checking again right before parking, we catch tasks added between
       * the dequeue() and the park() call. */
      if (!wq.m_active_queue->dequeue(task)) {
        task = nullptr;
      }

      if (task != nullptr) [[likely]] {
        /* Found a task that was added after our initial check - process it */
        assert(!task->m_free.load(std::memory_order_acquire));

        if (process_task(task, wq)) {
          return;
        }
        continue;
      }
        
      /* Still no work - park thread until woken */
      wq.m_parker.park();
    }
  }

  public:
  std::stop_source m_stop_source;

  /* Atomic counter for round-robin task distribution across workers */
  alignas(64) std::atomic<std::size_t> m_worker_counter{0};

  /* Number of protected workers (first N workers don't allow stealing) */
  std::size_t m_num_protected_workers{0};

  /* Per-worker queues - MPMC (multiple producers, single consumer per queue) */
  std::vector<std::unique_ptr<Worker_queue>> m_worker_queues;

  std::vector<std::jthread> m_workers;
};

} // namespace util
