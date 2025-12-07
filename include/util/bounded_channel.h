#pragma once

#include <atomic>
#include <bit>
#include <cassert>
#include <cstring>
#include <concepts>
#include <memory>
#include <new>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <utility>

#include <print>

#include "atomics.h"

#if defined(__GNUC__) || defined(__clang__)
  #define PREFETCH_FOR_READ(addr) __builtin_prefetch((addr), 0, 3)
  #define PREFETCH_FOR_WRITE(addr) __builtin_prefetch((addr), 1, 3)
#else
  #define PREFETCH_FOR_READ(addr) ((void)0)
  #define PREFETCH_FOR_WRITE(addr) ((void)0)
#endif

namespace util {

/* Type traits for Bounded_queue element types */
template <typename T>
concept Bounded_queue_value = 
  !std::is_reference_v<T> &&                    /* Cannot store references */
  !std::is_void_v<T> &&                         /* Cannot store void */
  std::is_object_v<T> &&                        /* Must be an object type */
  std::is_destructible_v<T> &&                  /* Must be destructible */
  std::is_default_constructible_v<T> &&          /* Must be default constructible (used in Cell) */
  (std::is_copy_assignable_v<T> || std::is_move_assignable_v<T>); /* Must be assignable */

using Handle = std::size_t;

/* Type trait for checking if a type is valid for Bounded_queue */
template <typename T>
struct is_bounded_queue_value : std::bool_constant<Bounded_queue_value<T>> {};

template <typename T>
inline constexpr bool is_bounded_queue_value_v = is_bounded_queue_value<T>::value;

template <Bounded_queue_value T>
struct Bounded_queue {
  using Type = T;
  using Pos = std::size_t;
  using Config = std::size_t;
  static constexpr bool is_lock_free = std::atomic<Pos>::is_always_lock_free;

  explicit Bounded_queue(std::size_t n) 
    : m_ring(std::make_unique<Cell[]>(n)),
      m_capacity(n - 1) {

    if (!std::has_single_bit(n)) {
      throw std::invalid_argument("n must be a power of 2");
    }

    for (std::size_t i{}; i < n; ++i) {
      atomic_store_const<Pos, std::memory_order_relaxed>(&m_ring[i].m_pos, i);
    }

    atomic_store_const<Pos, std::memory_order_relaxed>(&m_epos, Pos(0));
    atomic_store_const<Pos, std::memory_order_relaxed>(&m_dpos, Pos(0));
  }

  Bounded_queue(Bounded_queue &&) = delete;
  Bounded_queue(const Bounded_queue &) = delete;
  Bounded_queue &operator=(Bounded_queue &&) = delete;
  Bounded_queue &operator=(const Bounded_queue &) = delete;

  ~Bounded_queue() = default;

  [[nodiscard]] bool enqueue(T const &data) noexcept(noexcept(std::declval<T &>() = std::declval<T const &>())) requires std::assignable_from<T &, T const &> {
    if constexpr (std::is_trivially_copyable_v<T>) {
      return enqueue_impl([&](T &slot) { std::memcpy(&slot, &data, sizeof(T)); });
    } else {
      return enqueue_impl([&](T &slot) { slot = data; });
    }
  }

  [[nodiscard]] bool enqueue(T &&data) noexcept(noexcept(std::declval<T &>() = std::declval<T &&>())) requires std::assignable_from<T &, T &&> {
    return enqueue_impl([&](T &slot) { slot = std::move(data); });
  }

  template <typename... Args>
  [[nodiscard]] bool emplace(Args &&...args) noexcept(std::is_nothrow_constructible_v<T, Args...>) requires std::constructible_from<T, Args...> {
    return enqueue_impl([&](T &slot) { slot = T(std::forward<Args>(args)...); });
  }

  [[nodiscard]] bool dequeue(T &data) noexcept {
    Cell *cell;
    Cell *ring_ptr = m_ring.get();
    auto pos{atomic_load_const<Pos, std::memory_order_relaxed>(&m_dpos)};

    for (;;) {
      cell = &ring_ptr[pos & m_capacity];
      /* Prefetch next cell - for small queues (L1 cache), single prefetch is optimal */
      PREFETCH_FOR_READ(&ring_ptr[(pos + 1) & m_capacity]);
      const auto seq{atomic_load_const<Pos, std::memory_order_acquire>(&cell->m_pos)};
      const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + 1);

      if (diff == 0) [[likely]] {
        /* Atomically move head to claim spot. Weak CAS may fail spuriously or
         * if another thread updated m_dpos; loop retries in both cases. */
        Pos expected = pos;
        if (atomic_compare_exchange_weak_const<Pos, std::memory_order_relaxed, std::memory_order_relaxed>(&m_dpos, &expected, pos + 1)) {
          break;
        }
        pos = expected;  /* Update pos from expected after failed CAS */

      } else if (diff < 0) [[unlikely]] {
        /* The Bounded_queue is empty */
        return false;

      } else {
        /* Under normal circumstances this branch should never be taken. */
        pos = atomic_load_const<Pos, std::memory_order_relaxed>(&m_dpos);
      }
    }

    if constexpr (std::is_trivially_copyable_v<T>) {
      std::memcpy(&data, &cell->m_data, sizeof(T));
    } else {
      data = cell->m_data;
    }

    /* Set the sequence to what the head sequence should be next time around */
    atomic_store_const<Pos, std::memory_order_release>(&cell->m_pos, pos + m_capacity + 1);

    return true;
  }

  /* Move data out of the queue (more efficient for move-only or expensive-to-copy types) */
  /* Returns std::optional<T> - empty if queue is empty, otherwise contains moved value */
  [[nodiscard]] std::optional<T> try_dequeue() noexcept requires std::move_constructible<T> {
    Cell *cell;
    Cell *ring_ptr = m_ring.get();
    auto pos{atomic_load_const<Pos, std::memory_order_relaxed>(&m_dpos)};

    for (;;) {
      cell = &ring_ptr[pos & m_capacity];
      /* Prefetch next cell - for small queues (L1 cache), single prefetch is optimal */
      PREFETCH_FOR_READ(&ring_ptr[(pos + 1) & m_capacity]);
      const auto seq{atomic_load_const<Pos, std::memory_order_acquire>(&cell->m_pos)};
      const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + 1);

      if (diff == 0) [[likely]] {
        /* Atomically move head to claim spot. Weak CAS may fail spuriously or
         * if another thread updated m_dpos; loop retries in both cases. */
        Pos expected = pos;
        if (atomic_compare_exchange_weak_const<Pos, std::memory_order_relaxed, std::memory_order_relaxed>(&m_dpos, &expected, pos + 1)) {
          break;
        }
        pos = expected;  /* Update pos from expected after failed CAS */

      } else if (diff < 0) [[unlikely]] {
        /* The Bounded_queue is empty */
        return std::nullopt;

      } else {
        /* Under normal circumstances this branch should never be taken. */
        pos = atomic_load_const<Pos, std::memory_order_relaxed>(&m_dpos);
      }
    }

    /* Move-construct the result from the cell's data */
    T result = [&]() -> T {
      if constexpr (std::is_trivially_copyable_v<T>) {
        /* For trivially copyable types, copy is as efficient as move */
        T tmp;
        std::memcpy(&tmp, &cell->m_data, sizeof(T));
        return tmp;
      } else {
        /* Move-construct from the cell's data (return value optimization) */
        return std::move(cell->m_data);
      }
    }();

    /* Set the sequence to what the head sequence should be next time around */
    atomic_store_const<Pos, std::memory_order_release>(&cell->m_pos, pos + m_capacity + 1);

    return result;
  }

  /**
   * Dequeue multiple items at once (batch dequeue)
   * Uses SIMD optimization for trivially copyable types
   * @param values Pointer to output array (must have space for at least max_count items)
   * @param max_count Maximum number of items to dequeue
   * @return Number of items actually dequeued (0 if queue is empty)
   */
  [[nodiscard]] std::size_t dequeue_batch(T* values, std::size_t max_count) noexcept {
    if (max_count == 0) {
      return 0;
    }

    Cell *ring_ptr = m_ring.get();
    auto pos{atomic_load_const<Pos, std::memory_order_relaxed>(&m_dpos)};
    std::size_t claimed = 0;

    // Try to claim as many positions as possible (up to max_count)
    for (;;) {
      // Check how many items are available starting from pos
      std::size_t available = 0;
      for (std::size_t i = 0; i < max_count; ++i) {
        Cell *cell = &ring_ptr[(pos + i) & m_capacity];
        const auto seq{atomic_load_const<Pos, std::memory_order_acquire>(&cell->m_pos)};
        const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + i + 1);

        if (diff == 0) {
          // This item is available
          available++;
        } else {
          // No more items available
          break;
        }
      }

      if (available == 0) {
        // Queue is empty or another thread consumed items
        return 0;
      }

      // Try to atomically claim 'available' positions
      Pos expected = pos;
      if (atomic_compare_exchange_weak_const<Pos, std::memory_order_relaxed, std::memory_order_relaxed>(
            &m_dpos, &expected, pos + available)) {
        claimed = available;
        break;
      }

      // CAS failed, retry with updated pos
      pos = expected;
    }

    // Now we have claimed 'claimed' items starting at position 'pos'
    // Copy them to the output array
    // Note: Cell layout (T data, atomic<Pos> pos) means data is not contiguous,
    // so we copy item by item. Main benefit is reducing atomic operations.
    for (std::size_t i = 0; i < claimed; ++i) {
      Cell *cell = &ring_ptr[(pos + i) & m_capacity];

      if constexpr (std::is_trivially_copyable_v<T>) {
        std::memcpy(&values[i], &cell->m_data, sizeof(T));
      } else {
        values[i] = cell->m_data;
      }

      // Set the sequence to what it should be next time around
      atomic_store_const<Pos, std::memory_order_release>(&cell->m_pos, pos + i + m_capacity + 1);
    }

    return claimed;
  }

  /**
   * Dequeue multiple items at once using a callback (zero-copy batch dequeue)
   * Processes items in-place without copying to a separate buffer
   * @param max_count Maximum number of items to dequeue
   * @param callback Function to call for each dequeued item: bool callback(T&)
   *                 Return false from callback to stop early
   * @return Number of items actually dequeued (0 if queue is empty)
   */
  template<typename F>
  [[nodiscard]] std::size_t dequeue_batch(std::size_t max_count, F&& callback) noexcept {
    if (max_count == 0) {
      return 0;
    }

    Cell *ring_ptr = m_ring.get();
    auto pos{atomic_load_const<Pos, std::memory_order_relaxed>(&m_dpos)};
    std::size_t claimed = 0;

    // Try to claim as many positions as possible (up to max_count)
    for (;;) {
      // Check how many items are available starting from pos
      std::size_t available = 0;
      for (std::size_t i = 0; i < max_count; ++i) {
        Cell *cell = &ring_ptr[(pos + i) & m_capacity];
        const auto seq{atomic_load_const<Pos, std::memory_order_acquire>(&cell->m_pos)};
        const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + i + 1);

        if (diff == 0) {
          // This item is available
          available++;
        } else {
          // No more items available
          break;
        }
      }

      if (available == 0) {
        // Queue is empty or another thread consumed items
        return 0;
      }

      // Try to atomically claim 'available' positions
      Pos expected = pos;
      if (atomic_compare_exchange_weak_const<Pos, std::memory_order_relaxed, std::memory_order_relaxed>(
            &m_dpos, &expected, pos + available)) {
        claimed = available;
        break;
      }

      // CAS failed, retry with updated pos
      pos = expected;
    }

    // Now we have claimed 'claimed' items starting at position 'pos'
    // Process them in-place with the callback
    std::size_t processed = 0;
    for (std::size_t i = 0; i < claimed; ++i) {
      Cell *cell = &ring_ptr[(pos + i) & m_capacity];

      // Call callback with the cell's data
      if (!callback(cell->m_data)) {
        // Early exit requested by callback
        processed = i + 1;

        // Still need to release all claimed cells
        for (std::size_t j = 0; j < claimed; ++j) {
          Cell *release_cell = &ring_ptr[(pos + j) & m_capacity];
          atomic_store_const<Pos, std::memory_order_release>(&release_cell->m_pos, pos + j + m_capacity + 1);
        }

        return processed;
      }

      // Set the sequence to what it should be next time around
      atomic_store_const<Pos, std::memory_order_release>(&cell->m_pos, pos + i + m_capacity + 1);
      processed++;
    }

    return processed;
  }

  /** @return the capacity of the Bounded_queue */
  [[nodiscard]] auto capacity() const noexcept { return m_capacity + 1; }

  /** @return true if the Bounded_queue has room for at least one free slot. */
  [[nodiscard]] bool empty() const noexcept {
    auto pos{atomic_load_const<Pos, std::memory_order_relaxed>(&m_dpos)};

    for (;;) {
      auto cell{&m_ring.get()[pos & m_capacity]};
      const auto seq{atomic_load_const<Pos, std::memory_order_acquire>(&cell->m_pos)};
      const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + 1);

      if (diff == 0) [[likely]] {
        /* seq == pos + 1: data is ready, queue is not empty */
        return false;
      } else if (diff < 0) [[likely]] {
        /* seq < pos + 1: slot is empty or not ready, queue is empty */
        return true;
      } else if (seq >= pos + m_capacity + 1) [[likely]] {
        /* diff > 0: seq > pos + 1
         * This can happen if seq = pos + m_capacity + 1 (slot was freed in previous cycle)
         * In that case, the slot is empty, but we need to check if pos has advanced
         * If seq >= pos + m_capacity + 1, the slot is definitely empty
         * Slot was freed, it's empty - queue is empty */
        return true;
      } else [[unlikely]] {
        /* Otherwise, pos might have advanced, reload and retry */
        auto new_pos = atomic_load_const<Pos, std::memory_order_relaxed>(&m_dpos);

        if (new_pos == pos) {
          /* pos hasn't changed, but seq > pos + 1 and seq < pos + m_capacity + 1
           * This is an invariant violation - seq should only be:
           * - pos (initial/empty)
           * - pos + 1 (data ready)
           * - pos + m_capacity + 1 (freed)
           * Any other value indicates a bug */
          assert((seq == pos || seq == pos + 1 || seq == pos + m_capacity + 1) &&
                 "Invalid sequence number state: invariant violation");
          return true;
        }
        pos = new_pos;
      }
    }

    std::unreachable();
  }

  /**
   * Peek at the next item without removing it
   * @param[out] data Output parameter for the peeked data
   * @return true if peeked successfully, false if queue is empty
   */
  [[nodiscard]] bool peek(T& data) const noexcept {
    Cell *cell;
    Cell *ring_ptr = m_ring.get();
    auto pos{atomic_load_const<Pos, std::memory_order_relaxed>(&m_dpos)};

    for (;;) {
      cell = &ring_ptr[pos & m_capacity];
      const auto seq{atomic_load_const<Pos, std::memory_order_acquire>(&cell->m_pos)};
      const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + 1);

      if (diff == 0) [[likely]] {
        /* Item is available - read it without modifying state */
        if constexpr (std::is_trivially_copyable_v<T>) {
          std::memcpy(&data, &cell->m_data, sizeof(T));
        } else {
          data = cell->m_data;
        }
        return true;
      } else if (diff < 0) [[unlikely]] {
        /* The Bounded_queue is empty */
        return false;
      } else {
        /* Under normal circumstances this branch should never be taken. */
        pos = atomic_load_const<Pos, std::memory_order_relaxed>(&m_dpos);
      }
    }
  }

  /** @return true if the Bounded_queue is full. */
  [[nodiscard]] bool full() const noexcept {
    auto pos{atomic_load_const<Pos, std::memory_order_relaxed>(&m_epos)};

    for (;;) {
      auto cell = &m_ring.get()[pos & m_capacity];
      const auto seq{atomic_load_const<Pos, std::memory_order_acquire>(&cell->m_pos)};
      const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos);

      if (diff == 0) [[likely]] {
        /* seq == pos: cell is empty, queue is not full */
        return false;
      } else if (diff < 0) [[likely]] {
        /* seq < pos: queue is full */
        return true;
      } else {
        /* diff > 0: seq > pos
         * This can happen if seq = pos + 1 (slot has data) or seq = pos + m_capacity + 1 (slot was freed)
         * In both cases, the queue is not full */
        if (seq >= pos + m_capacity + 1) {
          /* Slot was freed in previous cycle, it's empty - queue is not full */
          return false;
        }
        /* Otherwise, pos might have advanced, reload and retry */
        const auto new_pos = atomic_load_const<Pos, std::memory_order_relaxed>(&m_epos);

        if (new_pos == pos) {
          /* pos hasn't changed, but seq > pos and seq < pos + m_capacity + 1
           * This is an invariant violation - seq should only be:
           * - pos (empty, can enqueue)
           * - pos + 1 (has data, full)
           * - pos + m_capacity + 1 (freed, can enqueue)
           * In this range, only seq = pos + 1 is valid */
          assert(seq == pos + 1 && "Invalid sequence number state in full(): invariant violation");

          /* seq = pos + 1 means slot has data, so queue is not full */
          return false;
        }
        pos = new_pos;
      }
    }

    std::unreachable();
  }

 private:
  template <typename Writer>
  [[nodiscard]] bool enqueue_impl(Writer &&write) noexcept(noexcept(write(std::declval<T &>()))) {
    Cell *cell;
    Cell *ring_ptr = m_ring.get();

    /* m_epos only wraps at MAX(m_epos); use capacity as mask for index. */
    auto pos{atomic_load_const<Pos, std::memory_order_relaxed>(&m_epos)};

    for (;;) {
      cell = &ring_ptr[pos & m_capacity];
      /* Prefetch next cell - for small queues (L1 cache), single prefetch is optimal */
      PREFETCH_FOR_WRITE(&ring_ptr[(pos + 1) & m_capacity]);
      const auto seq{atomic_load_const<Pos, std::memory_order_acquire>(&cell->m_pos)};
      const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos);

      if (diff == 0) [[likely]] {
        Pos expected = pos;
        if (atomic_compare_exchange_weak_const<Pos, std::memory_order_relaxed, std::memory_order_relaxed>(&m_epos, &expected, pos + 1)) {
          break;
        }
        pos = expected;  /* Update pos from expected after failed CAS */
      } else if (diff < 0) [[unlikely]] {
        /* The Bounded_queue is full */
        return false;
      } else {
        pos = atomic_load_const<Pos, std::memory_order_relaxed>(&m_epos);
      }
    }

    write(cell->m_data);

    /* Publish: increment sequence so consumers can observe data. */
    atomic_store_const<Pos, std::memory_order_release>(&cell->m_pos, pos + 1);

    return true;
  }

 private:
  using Pad = std::byte[std::hardware_constructive_interference_size];

  struct Cell {
    alignas(std::max(alignof(T), alignof(std::atomic<Pos>))) T m_data{};
    std::atomic<Pos> m_pos{};
  };

  void *m_ptr{};
  [[no_unique_address]] Pad m_pad0;
  std::unique_ptr<Cell[]> m_ring{};
  Pos const m_capacity{};
  [[no_unique_address]] Pad m_pad1;
  std::atomic<Pos> m_epos{};
  [[no_unique_address]] Pad m_pad2;
  std::atomic<Pos> m_dpos{};
  [[no_unique_address]] Pad m_pad3;
};

} // namespace util
