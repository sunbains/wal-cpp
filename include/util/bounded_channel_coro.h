#pragma once

#include <atomic>
#include <coroutine>
#include <memory>
#include <vector>
#include <bit>
#include <thread>
#include <algorithm>
#include <type_traits>
#include <optional>

#include "bounded_channel.h"
#include "lockfreelist.h"
#include "util.h"
#include "segmented_queue.h"
#include "backoff.h"

namespace util {

/** Type trait to detect if Queue has a Handle type */
template<typename Queue>
struct has_handle {
private:
  template<typename U>
  static auto test(int) -> decltype(std::declval<typename U::Handle>(), std::true_type{});
  template<typename>
  static std::false_type test(...);
public:
  static constexpr bool value = decltype(test<Queue>(0))::value;
};

template<typename Queue>
constexpr bool has_handle_v = has_handle<Queue>::value;

/** Helper to get Handle type only when it exists (SFINAE-friendly) */
template<typename Queue, typename = void>
struct queue_handle_type {
  using type = std::monostate;
};

template<typename Queue>
struct queue_handle_type<Queue, std::void_t<typename Queue::Handle>> {
  using type = typename Queue::Handle;
};

template<typename Queue>
using queue_handle_t = typename queue_handle_type<Queue>::type;

/**
 * Bounded_queue_coro - A coroutine-aware bounded MPMC channel
 *
 * This is a wrapper around a queue implementation that follows the Bounded_queue_value concept.
 * When the queue is full, enqueue operations suspend the coroutine instead of returning false.
 * When the queue is empty, dequeue operations suspend the coroutine instead of returning false.
 *
 * Key features:
 * - Lock-free enqueue/dequeue when space/data available
 * - Coroutine suspension when queue is full (enqueue) or empty (dequeue)
 * - Automatic wake-up when conditions change
 * - Configurable backoff policy for retry strategies
 * - Backoff policy is passed through to underlying queue (e.g., Segmented_queue)
 *
 * Design:
 * - Three lock-free lists: one for enqueue waiters (when full), one for dequeue waiters (when empty)
 * - Enqueue operations notify dequeue waiters
 * - Dequeue operations notify enqueue waiters
 * - Pool of waiters are stored in a lock-free list
 * - Backoff policy controls retry behavior with full context in awaitables
 * - Backoff policy is adapted and passed to underlying queue operations
 *
 * @tparam Queue Queue type (defaults to Segmented_queue with exponential backoff)
 * @tparam Backoff_policy Backoff policy that takes Coro_backoff_context& (used in awaitables)
 *                       and is adapted for underlying queue operations
 */
template <typename Queue, typename Backoff_policy = Exponential_coro_backoff>
struct Bounded_queue_coro {
  using Pos = std::size_t;
  using T = typename Queue::Type;
  using Type = typename Queue::Type;
  using Backoff_context = Coro_backoff_context;
  constexpr static std::size_t kMaxWaiters = 32;
  using Queue_config = typename Queue::Config;
  using Handle = typename Queue::Handle;

  Backoff_policy m_backoff_policy;
  
  /** Maximum number of coroutines to wake up simultaneously (limited by CPU cores) */
  static std::size_t max_concurrent_wakeups() noexcept {
    static const std::size_t hw_threads = std::thread::hardware_concurrency();
    return hw_threads > 0 ? hw_threads : 4; 
  }

  /**
   * Construct a bounded channel
   * @param[in] n Capacity (must be power of 2)
   * @param[in] max_waiters Maximum number of waiters to store in the free list pool
   */
  explicit Bounded_queue_coro(const Queue_config& config, std::size_t max_waiters = kMaxWaiters)
    : m_queue(config) {
    initialize_waiters(max_waiters);
  }

  Bounded_queue_coro(Bounded_queue_coro&&) = delete;
  Bounded_queue_coro(const Bounded_queue_coro&) = delete;
  Bounded_queue_coro& operator=(Bounded_queue_coro&&) = delete;
  Bounded_queue_coro& operator=(const Bounded_queue_coro&) = delete;

  ~Bounded_queue_coro() = default;

  /**
   * Try to enqueue by copy with handle (for queues that require it)
   * @param[in] handle Handle for the queue
   * @param[in] data Data to enqueue
   * @return true if enqueued, false if queue is full
   */
  [[nodiscard]] bool try_enqueue(queue_handle_t<Queue>& handle, const T& data)
    noexcept(noexcept(std::declval<Queue&>().enqueue(handle, data)))
    requires std::assignable_from<T&, const T&> && has_handle_v<Queue> {
    if (m_queue.enqueue(handle, data)) [[likely]] {
      if (m_has_dequeue_waiters.load(std::memory_order_relaxed) > 0) [[unlikely]] {
        notify_dequeue_waiters(1);
      }
      m_n_enqueued.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    return false;
  }

  /**
   * Try to enqueue by move with handle (for queues that require it)
   * @param[in] handle Handle for the queue
   * @param[in] data Data to enqueue
   * @return true if enqueued, false if queue is full
   */
  [[nodiscard]] bool try_enqueue(queue_handle_t<Queue>& handle, T&& data)
    noexcept(noexcept(std::declval<Queue&>().enqueue(handle, std::move(data))))
    requires std::assignable_from<T&, T&&> && has_handle_v<Queue> {
    if (m_queue.enqueue(handle, std::move(data))) [[likely]] {
      if (m_has_dequeue_waiters.load(std::memory_order_relaxed) > 0) [[unlikely]] {
        notify_dequeue_waiters(1);
      }
      m_n_enqueued.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    return false;
  }

  /**
   * Blocking enqueue by copy with handle - waits if queue is full (for queues with handles)
   * Use for high producer counts (1000+) to avoid CAS contention.
   * @param[in] handle Handle for the queue
   * @param[in] data Data to enqueue
   */
  void enqueue_wait(queue_handle_t<Queue>& handle, const T& data)
    noexcept(noexcept(std::declval<Queue&>().enqueue_wait(handle, data)))
    requires std::assignable_from<T&, const T&> && has_handle_v<Queue> {
    m_queue.enqueue_wait(handle, data);
    if (m_has_dequeue_waiters.load(std::memory_order_relaxed) > 0) [[unlikely]] {
      notify_dequeue_waiters(1);
    }
    m_n_enqueued.fetch_add(1, std::memory_order_relaxed);
  }

  /**
   * Blocking enqueue by move with handle - waits if queue is full (for queues with handles)
   * Use for high producer counts (1000+) to avoid CAS contention.
   * @param[in] handle Handle for the queue
   * @param[in] data Data to enqueue
   */
  void enqueue_wait(queue_handle_t<Queue>& handle, T&& data)
    noexcept(noexcept(std::declval<Queue&>().enqueue_wait(handle, std::move(data))))
    requires std::assignable_from<T&, T&&> && has_handle_v<Queue> {
    m_queue.enqueue_wait(handle, std::move(data));
    if (m_has_dequeue_waiters.load(std::memory_order_relaxed) > 0) [[unlikely]] {
      notify_dequeue_waiters(1);
    }
    m_n_enqueued.fetch_add(1, std::memory_order_relaxed);
  }

  /**
   * Try to emplace with handle (non-blocking) - for queues with handles
   * @param[in] handle Handle for the queue
   * @param[in] args Arguments to construct element
   * @return true if emplaced, false if queue is full
   */
  template <typename... Args>
  [[nodiscard]] bool try_emplace(queue_handle_t<Queue>& handle, Args&&... args)
    noexcept(std::is_nothrow_constructible_v<T, Args...>)
    requires std::constructible_from<T, Args...> && has_handle_v<Queue> {
    if (m_queue.emplace(handle, std::forward<Args>(args)...)) [[likely]] {
      if (m_has_dequeue_waiters.load(std::memory_order_relaxed) > 0) [[unlikely]] {
        notify_dequeue_waiters(1);
      }
      m_n_enqueued.fetch_add(1, std::memory_order_relaxed);
      return true;
    }
    return false;
  }

  /**
   * Try to dequeue (non-blocking) with sticky round-robin - for queues with handles
   * @param[in] handle Handle for the queue
   * @param[out] data Output parameter for dequeued data
   * @return true if dequeued, false if queue is empty
   */
   [[nodiscard]] bool try_dequeue(queue_handle_t<Queue>& handle, T& data) noexcept
   requires has_handle_v<Queue> {
   if (m_queue.dequeue(handle.m_stripe_idx, data)) [[likely]] {
     if (m_has_enqueue_waiters.load(std::memory_order_relaxed)) [[unlikely]] {
       notify_enqueue_waiters(1);
     }
     m_n_dequeued.fetch_add(1, std::memory_order_relaxed);
     return true;
   }
   return false;
 }

 /**
   * Try to dequeue with move semantics and sticky round-robin - for queues with handles
   * @param[in] handle Handle for the queue
   * @return optional with value if dequeued, nullopt if queue is empty
   */
   [[nodiscard]] std::optional<T> try_dequeue(queue_handle_t<Queue>& handle) noexcept
   requires std::move_constructible<T> && has_handle_v<Queue> {
   if (auto result = m_queue.try_dequeue(handle.m_stripe_idx)) [[likely]] {
     if (m_has_enqueue_waiters.load(std::memory_order_relaxed)) [[unlikely]] {
       notify_enqueue_waiters(1);
     }
     m_n_dequeued.fetch_add(1, std::memory_order_relaxed);
     return result;
   }
   return std::nullopt;
 }

  /**
   * Awaitable for coroutine-based enqueue when queue is full
   * Suspends the coroutine until space becomes available
   */
  struct Enqueue_awaitable {
    bool await_ready() const noexcept {
      /* INVARIANT: If queue is NOT full, producer CAN enqueue (should never suspend) */
      /* INVARIANT: If queue is full AND no consumers waiting, producer MUST suspend */
      
      /* Try enqueue once - fast path */
      if constexpr (has_handle_v<Queue>) {
        m_already_enqueued = m_channel->m_queue.enqueue(m_handle, *m_data_ptr);
      } else {
        m_already_enqueued = m_channel->m_queue.enqueue(*m_data_ptr);
      }
      if (m_already_enqueued) {
        m_channel->notify_dequeue_waiters(1);
        return true;
      }
      
      /* Enqueue failed - check queue state to determine if we should suspend */
      bool queue_full = m_channel->m_queue.full();
      bool has_consumers_waiting = m_channel->m_has_dequeue_waiters.load(std::memory_order_relaxed) > 0;
      
      /* Only suspend if queue is full AND no consumers waiting.
       * If queue is not full, we should be able to enqueue - don't suspend.
       * If consumers are waiting, they will make space - don't suspend */
      if (queue_full && !has_consumers_waiting) {
        return false;  /* Suspend - queue is full and no consumers to make space */
      }
      
      /* Queue is not full OR consumers are waiting - we should be able to enqueue
       * This should not happen if the queue implementation is correct, but if it does,
       * we retry once more to handle transient races */

      if constexpr (has_handle_v<Queue>) {
        m_already_enqueued = m_channel->m_queue.enqueue(m_handle, *m_data_ptr);
      } else {
        m_already_enqueued = m_channel->m_queue.enqueue(*m_data_ptr);
      }

      if (m_already_enqueued) {
        m_channel->notify_dequeue_waiters(1);
        return true;
      }
      
      /* Still failed after retry - check state again */
      queue_full = m_channel->m_queue.full();
      has_consumers_waiting = m_channel->m_has_dequeue_waiters.load(std::memory_order_relaxed) > 0;
      
      assert(!m_already_enqueued);

      /* Only suspend if queue is truly full and no consumers
       * If queue is not full or consumers are waiting, we should be able to enqueue
       * But if enqueue() still fails, it's a transient race - suspend and let notifications handle it */
      if (queue_full && !has_consumers_waiting) {
        return false;  /* Suspend - queue is full and no consumers */
      }
      

      /* Queue state says we should be able to enqueue but operation failed
       * This is a transient race condition - suspend and let notifications handle it
       * The notification mechanism will wake us when state changes */
      return false;
    }

    bool await_suspend(std::coroutine_handle<> h) noexcept {
      if (!m_channel->m_enqueue_waiters) {
        return false;
      }
      
      /* Don't wake consumers here - they will wake naturally when they try to dequeue
       * Waking here can cause premature wake-ups and races */
      
      return m_channel->suspend_waiters(m_channel->m_enqueue_waiters, h);
    }

    bool await_resume() noexcept {
      /* If we already enqueued in await_ready(), we're done */
      if (m_already_enqueued) {
        return true;
      }

      /* We were woken because space should be available - try once.
       * If it fails, the coroutine code will loop and re-await, which will suspend again */
      if constexpr (has_handle_v<Queue>) {
        if (m_is_move) {
          m_already_enqueued = m_channel->m_queue.enqueue(m_handle, std::move(*const_cast<T*>(m_data_ptr)));
        } else {
          m_already_enqueued = m_channel->m_queue.enqueue(m_handle, *m_data_ptr);
        }
      } else {
        if (m_is_move) {
          m_already_enqueued = m_channel->m_queue.enqueue(std::move(*const_cast<T*>(m_data_ptr)));
        } else {
          m_already_enqueued = m_channel->m_queue.enqueue(*m_data_ptr);
        }
      }
      if (m_already_enqueued) [[likely]] {
        m_channel->notify_dequeue_waiters(1);
        return true;
      }

      /* Still full - can happen due to races when multiple producers are woken.
       * The coroutine will check try_enqueue() again, fail, and re-await.
       * This allows proper re-suspension instead of busy-waiting here */
      return false;
    }

    [[nodiscard]] bool success() const noexcept {
      return m_already_enqueued;
    }

    Bounded_queue_coro* m_channel;

    const T* m_data_ptr;
    bool m_is_move;

    /** Track if we already enqueued in await_ready() */
    mutable bool m_already_enqueued{false};

    /** Handle for queues that require it (e.g., Segmented_queue)
     * Must be mutable because enqueue() may modify it (e.g., rolling to next stripe) */
    [[no_unique_address]] mutable queue_handle_t<Queue> m_handle{};
  };

  /**
   * Awaitable for coroutine-based dequeue when queue is empty
   * Suspends the coroutine until data becomes available
   */
  struct Dequeue_awaitable {
    bool await_ready() const noexcept {
      /* INVARIANT: If queue is NOT empty, consumer CAN dequeue (should never suspend) */
      /* INVARIANT: If queue is empty AND no producers waiting, consumer MUST suspend */
      
      /* Try dequeue once - fast path */
      if constexpr (has_handle_v<Queue>) {
        m_already_dequeued = m_channel->m_queue.dequeue(m_stripe_idx, *m_data_ptr);
      } else {
        m_already_dequeued = m_channel->m_queue.dequeue(*m_data_ptr);
      }
      if (m_already_dequeued) {
        /* Wake one producer after dequeue */
        m_channel->notify_enqueue_waiters(1);
        return true;
      }
      
      /* Dequeue failed - check queue state to determine if we should suspend */
      bool queue_empty = m_channel->m_queue.empty();
      bool has_producers_waiting = m_channel->m_has_enqueue_waiters.load(std::memory_order_relaxed);
      
      /* Only suspend if queue is empty AND no producers waiting.
       * If queue is not empty, we should be able to dequeue - don't suspend.
       * If producers are waiting, they will add data - don't suspend */
      if (queue_empty && !has_producers_waiting) {
        /* Suspend - queue is empty and no producers to add data */
        return false;
      }
      
      /* Queue is not empty OR producers are waiting - we should be able to dequeue.
       * This should not happen if the queue implementation is correct, but if it does,
       * we retry once more to handle transient races */
      assert(!m_already_dequeued);

      if constexpr (has_handle_v<Queue>) {
        m_already_dequeued = m_channel->m_queue.dequeue(m_stripe_idx, *m_data_ptr);
      } else {
        m_already_dequeued = m_channel->m_queue.dequeue(*m_data_ptr);
      }
      if (m_already_dequeued) {
        /* Wake one producer after dequeue */
        m_channel->notify_enqueue_waiters(1);
        return true;
      }
      
      /* Still failed after retry - check state again */
      queue_empty = m_channel->m_queue.empty();
      has_producers_waiting = m_channel->m_has_enqueue_waiters.load(std::memory_order_relaxed);
      
      assert(!m_already_dequeued);

      /* Only suspend if queue is truly empty and no producers.
       * If queue is not empty or producers are waiting, we should be able to dequeue.
       * But if dequeue() still fails, it's a transient race - suspend and let notifications handle it */
      if (queue_empty && !has_producers_waiting) {
        /* Suspend - queue is empty and no producers */
        return false;
      }
      
      /* Queue state says we should be able to dequeue but operation failed.
       * This is a transient race condition - suspend and let notifications handle it.
       * The notification mechanism will wake us when state changes */
      return false;
    }

    bool await_suspend(std::coroutine_handle<> h) noexcept {
      if (!m_channel->m_dequeue_waiters) {
        return false;
      }
      
      /* Producers will wake us when they enqueue */
      return m_channel->suspend_waiters(m_channel->m_dequeue_waiters, h);
    }

    void await_resume() noexcept {
      /* If we already dequeued in await_ready(), we're done */
      if (m_already_dequeued) {
        return;
      }

      /* We were woken because data should be available - try once */
      /* If it fails, the coroutine code will loop and re-await, which will suspend again */
      if constexpr (has_handle_v<Queue>) {
        m_already_dequeued = m_channel->m_queue.dequeue(m_stripe_idx, *m_data_ptr);
      } else {
        m_already_dequeued = m_channel->m_queue.dequeue(*m_data_ptr);
      }
      if (m_already_dequeued) [[likely]] {
        /* Wake one producer after dequeue */
        m_channel->notify_enqueue_waiters(1);
        return;
      }

      /* Still empty - can happen due to races when multiple consumers are woken.
       * The coroutine will check try_dequeue() again, fail, and re-await.
       * This allows proper re-suspension instead of busy-waiting here */
    }

    [[nodiscard]] bool success() const noexcept {
      return m_already_dequeued;
    }

    Bounded_queue_coro* m_channel;
    T* m_data_ptr;

    /** Track if we already dequeued in await_ready */
    mutable bool m_already_dequeued{false};

    /** Stripe index for queues that require it (e.g., Segmented_queue sticky round-robin).
     *  Must be mutable because dequeue() may modify it (sticky round-robin) */
    [[no_unique_address]] mutable std::conditional_t<has_handle_v<Queue>, std::size_t, std::monostate> m_stripe_idx{};
  };

  /**
   * Create an awaitable for enqueueing with copy semantics
   * Use this when the queue is full to suspend until space is available
   *
   * @param[in] data Reference to data to enqueue (must remain valid until await completes)
   * @return Awaitable that suspends until space is available
   */
  [[nodiscard]] Enqueue_awaitable enqueue_awaitable(const T& data) noexcept {
    return Enqueue_awaitable{this, &data, false};
  }

  /**
   * Create an awaitable for enqueueing with move semantics
   * Use this when the queue is full to suspend until space is available
   *
   * @param[in] data Rvalue reference to data to enqueue (will be moved)
   * @return Awaitable that suspends until space is available
   */
  [[nodiscard]] Enqueue_awaitable enqueue_awaitable(T&& data) noexcept {
    return Enqueue_awaitable{this, &data, true};
  }

  /**
   * Create an awaitable for dequeueing
   * Use this when the queue is empty to suspend until data is available
   *
   * @param[out] data Reference to output location for dequeued data
   * @return Awaitable that suspends until data is available
   */
  [[nodiscard]] Dequeue_awaitable dequeue_awaitable(T& data) noexcept {
    return Dequeue_awaitable{this, &data};
  }

  /**
   * Generator type for yielding dequeued values
   * Yields values from the queue, suspending when empty
   */
  struct Dequeue_generator {
    struct promise_type {
      T m_value;
      std::coroutine_handle<> m_continuation;

      Dequeue_generator get_return_object() noexcept {
        return Dequeue_generator{std::coroutine_handle<promise_type>::from_promise(*this)};
      }

      std::suspend_always initial_suspend() const noexcept { return {}; }
      std::suspend_always final_suspend() const noexcept { return {}; }

      void unhandled_exception() noexcept {
        std::terminate();
      }

      void return_void() noexcept {}

      std::suspend_always yield_value(T&& value) noexcept {
        m_value = std::move(value);
        return {};
      }
    };

    using promise_type = promise_type;

    Dequeue_generator() = default;
    explicit Dequeue_generator(std::coroutine_handle<promise_type> h) : m_h(h) {}
    Dequeue_generator(Dequeue_generator&& rhs) noexcept : m_h(std::exchange(rhs.m_h, nullptr)) {}

    Dequeue_generator& operator=(Dequeue_generator&& rhs) noexcept {
      if (this != &rhs) {
        if (m_h) {
          m_h.destroy();
        }
        m_h = std::exchange(rhs.m_h, nullptr);
      }
      return *this;
    }

    Dequeue_generator(const Dequeue_generator&) = delete;
    Dequeue_generator& operator=(const Dequeue_generator&) = delete;

    ~Dequeue_generator() {
      if (m_h) {
        m_h.destroy();
      }
    }

    /**
     * Iterator for the generator
     */
    struct iterator {
      std::coroutine_handle<promise_type> m_h{nullptr};
      bool m_done{true};

      iterator() = default;
      explicit iterator(std::coroutine_handle<promise_type> h) : m_h(h) {
        if (m_h && !m_h.done()) {
          m_h.resume();
          m_done = m_h.done();
        } else {
          m_done = true;
        }
      }

      iterator& operator++() {
        if (m_h && !m_h.done()) {
          m_h.resume();
          m_done = m_h.done();
        } else {
          m_done = true;
        }
        return *this;
      }

      const T& operator*() const noexcept {
        return m_h.promise().m_value;
      }

      bool operator==(const iterator& other) const noexcept {
        return m_done == other.m_done;
      }
    };

    iterator begin() {
      if (!m_h || m_h.done()) {
        return iterator{};
      }
      return iterator{m_h};
    }

    iterator end() {
      return iterator{};
    }

    std::coroutine_handle<promise_type> m_h{nullptr};
  };

  /**
   * Create a generator that yields dequeued values
   * Use co_yield to yield values from the queue
   *
   * Example:
   *   for (auto value : ch.dequeue_generator()) {
   *     // process value
   *   }
   *
   * @return Generator that yields values until the queue is empty
   */
  [[nodiscard]] Dequeue_generator dequeue_generator() noexcept {
    return dequeue_generator_impl();
  }

  /**
   * Check if the channel is empty
   * @return true if empty, false otherwise
   */
  [[nodiscard]] bool empty() const noexcept {
    return m_queue.empty();
  }

  /**
   * Check if the channel is full
   * @return true if full, false otherwise
   */
  [[nodiscard]] bool full() const noexcept {
    return m_queue.full();
  }

  /**
   * Get the capacity of the channel
   * @return capacity
   */
  [[nodiscard]] std::size_t capacity() const noexcept {
    return m_queue.capacity();
  }

private:
  void initialize_waiters(std::size_t max_waiters) {
    if (max_waiters == 0) {
      throw std::invalid_argument("max_waiters must be > 0");
    }

    m_storage.resize(max_waiters);

    auto begin = m_storage.data();
    auto end = m_storage.data() + m_storage.size();

    m_free_list = std::make_unique<Waiter_list>(begin, end);

    for (std::size_t i = 0; i < m_storage.size(); ++i) {
      begin[i] = Waiter{};

      auto ret = m_free_list->push_back(begin[i]);
      if (!ret) {
        std::terminate();
      }
    }

    assert(m_free_list->size() == m_storage.size());

    m_enqueue_waiters = std::make_unique<Waiter_list>(begin, end);
    m_dequeue_waiters = std::make_unique<Waiter_list>(begin, end);
  }

  /**
   * Internal coroutine implementation for dequeue generator
   */
  Dequeue_generator dequeue_generator_impl() {
    T value{};
    std::size_t stripe_idx = 0;
    for (;;) {
      bool dequeued = false;
      if constexpr (has_handle_v<Queue>) {
        dequeued = m_queue.dequeue(stripe_idx, value);
      } else {
        dequeued = m_queue.dequeue(value);
      }
      if (dequeued) {
        /* Wake one producer after dequeue */
        notify_enqueue_waiters(1);
        co_yield std::move(value);
      } else {
        /* Queue is empty - suspend and wait */
        co_await dequeue_awaitable(value);
        bool dequeued = false;
        if constexpr (has_handle_v<Queue>) {
          dequeued = m_queue.dequeue(stripe_idx, value);
        } else {
          dequeued = m_queue.dequeue(value);
        }
        if (dequeued) {
          /* Wake one producer after dequeue */
          notify_enqueue_waiters(1);
          co_yield std::move(value);
        } else {
          /* Still empty after wake - break */
          co_return;
        }
      }
    }
  }

  void resume_dequeue_waiters() noexcept {
    notify_dequeue_waiters(1);
  }

  void resume_enqueue_waiters() noexcept {
    notify_enqueue_waiters(1);
  }

private:

  template <typename Waiter_list>
  bool suspend_waiters(Waiter_list& waiters, std::coroutine_handle<> h) noexcept {
    auto slot = m_free_list->pop_front();

    if (slot == nullptr) [[unlikely]] {
      return false;
    }

    *slot = Waiter(h);

    /* Use backoff policy to handle retries - no hardcoded spin counts */
    Backoff_context ctx{};
    ctx.m_queue_full = false;  // Not queue-related, but use backoff policy
    ctx.m_has_waiters = true;  // We're adding a waiter
    ctx.m_queue_capacity = 0;  // Not applicable for list operations

    /* Try to push, using backoff policy for retries */
    while (!waiters->push_front(*slot)) {
      m_backoff_policy(ctx);
      /* Backoff policy will yield when appropriate - no hardcoded limit */
    }

    /* Successfully added waiter - set flag */
    if (waiters.get() == m_enqueue_waiters.get()) {
      m_has_enqueue_waiters.store(true, std::memory_order_relaxed);
    } else if (waiters.get() == m_dequeue_waiters.get()) {
      m_has_dequeue_waiters.fetch_add(1, std::memory_order_relaxed);
    }

    return true;
  }

  template <typename Waiter_list>
  void notify_waiters(Waiter_list& waiters, std::size_t count) noexcept {
    /* Limit wakeups to number of CPU cores to avoid thundering herd */
    const std::size_t max_wakeups = max_concurrent_wakeups();
    std::size_t target_count = count;
    
    if (count == 0) {
      /* Wake all waiters, but cap at CPU core count */
      target_count = max_wakeups;
    } else {
      /* Wake up to count waiters, but don't exceed CPU core count */
      target_count = std::min(count, max_wakeups);
    }
    
    /* Fast path for single waiter (most common case) */
    if (target_count == 1) [[likely]] {
      auto waiter = waiters->pop_front();

      if (waiter == nullptr) [[unlikely]] {
        /* Clear flag if list is now empty */
        if (waiters.get() == m_enqueue_waiters.get()) {
          m_has_enqueue_waiters.store(false, std::memory_order_relaxed);
        } else if (waiters.get() == m_dequeue_waiters.get()) {
          m_has_dequeue_waiters.fetch_sub(1, std::memory_order_relaxed);
        }
        return;
      }

      if (!waiter->m_resumed) [[likely]] {
        waiter->m_resumed = true;
        waiter->m_handle.resume();
      }

      if (!m_free_list->push_back(*waiter)) [[unlikely]] {
        std::terminate();
      }

      /* Clear flag if list is now empty after popping */
      if (waiters->size() == 0) {
        if (waiters.get() == m_enqueue_waiters.get()) {
          m_has_enqueue_waiters.store(false, std::memory_order_relaxed);
        } else if (waiters.get() == m_dequeue_waiters.get()) {
          m_has_dequeue_waiters.fetch_sub(1, std::memory_order_relaxed);
        }
      }
      return;
    }

    /* Slow path for multiple waiters - wake up to target_count */
    std::size_t woken = 0;

    do {
      auto waiter = waiters->pop_front();

      if (waiter == nullptr) [[unlikely]] {
        break;
      }

      if (!waiter->m_resumed) {
        waiter->m_resumed = true;
        waiter->m_handle.resume();
        ++woken;
      }

      if (!m_free_list->push_back(*waiter)) [[unlikely]] {
        std::terminate();
      }
    } while (woken < target_count && waiters->size() > 0);

    /* Clear flag if list is now empty */
    if (waiters->size() == 0) {
      if (waiters.get() == m_enqueue_waiters.get()) {
        m_has_enqueue_waiters.store(false, std::memory_order_relaxed);
      } else if (waiters.get() == m_dequeue_waiters.get()) {
        m_has_dequeue_waiters.fetch_sub(1, std::memory_order_relaxed);
      }
    }
  }

  /**
   * Notify enqueue waiters that space is available
   * Wakes up to count waiters (or all if count is 0)
   */
  void notify_enqueue_waiters(std::size_t count) noexcept {
    if (!m_enqueue_waiters) {
      return;
    }

    /* Fast path: skip notify if no waiters */
    if (m_enqueue_waiters->size() == 0) [[likely]] {
      return;
    }

    notify_waiters(m_enqueue_waiters, count);
  }

  /**
   * Notify dequeue waiters that data is available
   * Wakes up to count waiters (or all if count is 0)
   */
  void notify_dequeue_waiters(std::size_t count) noexcept {
    if (!m_dequeue_waiters) {
      return;
    }

    /* Fast path: skip notify if no waiters */
    if (m_dequeue_waiters->size() == 0) [[likely]] {
      return;
    }

    notify_waiters(m_dequeue_waiters, count);
  }

private:
  /** Waiter structure for coroutine suspension */
  struct Waiter {
    Waiter() = default;

    explicit Waiter(std::coroutine_handle<> h)
      : m_handle(h) {}

    Node& get_node() noexcept {
      return m_node;
    }

    Node m_node{};
    bool m_resumed{false};
    std::coroutine_handle<> m_handle{};
  };

  /** Lock-free list type for waiters */
  constexpr static auto waiter_node = &Waiter::get_node;

  using Waiter_list = List<Waiter, waiter_node>;

  /** Underlying lock-free queue */
  Queue m_queue;

  /** Waiter storage and management for enqueue operations (when queue is full) */
  alignas(64) std::vector<Waiter> m_storage;

  std::unique_ptr<Waiter_list> m_free_list;
  std::unique_ptr<Waiter_list> m_dequeue_waiters;
  std::unique_ptr<Waiter_list> m_enqueue_waiters;

  /** Fast check flags to avoid expensive waiter list checks */
  alignas(64) std::atomic<bool> m_has_enqueue_waiters{false};
  alignas(64) std::atomic<std::size_t> m_has_dequeue_waiters{0};

  std::atomic<std::size_t> m_n_enqueued{0};
  std::atomic<std::size_t> m_n_dequeued{0};
};

} // namespace util
