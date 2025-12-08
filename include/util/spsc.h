/*
 * Copyright (C) 2025 Sunny Bains
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <cstring>
#include <type_traits>
#include <algorithm>

#include "atomics.h"

#if defined(__GNUC__) || defined(__clang__)
  #define PREFETCH_FOR_READ(addr) __builtin_prefetch((addr), 0, 3)
  #define PREFETCH_FOR_WRITE(addr) __builtin_prefetch((addr), 1, 3)
#else
  #define PREFETCH_FOR_READ(addr) ((void)0)
  #define PREFETCH_FOR_WRITE(addr) ((void)0)
#endif

namespace util {
template <typename T>
struct Spsc_bounded_queue {
  explicit Spsc_bounded_queue(std::size_t capacity)
    : m_capacity(capacity),
      m_mask(capacity - 1),
      m_storage(std::make_unique<T[]>(capacity)),
      m_tail_cache(0),
      m_head_cache(0) {
    if (!std::has_single_bit(capacity)) {
      throw std::invalid_argument("Capacity must be a power of two");
    }
  }

  // Enqueue with copy
  [[nodiscard]] bool enqueue(const T& value) noexcept {
    auto head = m_head.load(std::memory_order_relaxed);
    auto next = (head + 1) & m_mask;
    
    // Check if queue is full using cached tail
    if (next == m_tail_cache) {
      // Refresh cache - need acquire to see consumer's updates
      m_tail_cache = m_tail.load(std::memory_order_acquire);
      if (next == m_tail_cache) {
        return false;  // Queue is full
      }
    }
    
    // Prefetch the next slot to reduce store latency
    PREFETCH_FOR_WRITE(&m_storage[next]);

    // Write data
    if constexpr (std::is_trivially_copyable_v<T>) {
      std::memcpy(&m_storage[head], &value, sizeof(T));
    } else {
      m_storage[head] = value;
    }
    
    // Publish: release ensures consumer sees the data
    m_head.store(next, std::memory_order_release);
    return true;
  }

  // Enqueue with move
  [[nodiscard]] bool enqueue(T&& value) noexcept {
    auto head = m_head.load(std::memory_order_relaxed);
    auto next = (head + 1) & m_mask;
    
    // Check if queue is full using cached tail
    if (next == m_tail_cache) {
      // Refresh cache - need acquire to see consumer's updates
      m_tail_cache = m_tail.load(std::memory_order_acquire);
      if (next == m_tail_cache) {
        return false;  // Queue is full
      }
    }
    
    PREFETCH_FOR_WRITE(&m_storage[next]);

    // Move data
    m_storage[head] = std::move(value);
    
    // Publish: release ensures consumer sees the data
    m_head.store(next, std::memory_order_release);
    return true;
  }

  [[nodiscard]] bool dequeue(T& value) noexcept {
    auto tail = m_tail.load(std::memory_order_relaxed);

    // Check if queue is empty using cached head
    if (tail == m_head_cache) {
      // Refresh cache - need acquire to see producer's updates
      m_head_cache = m_head.load(std::memory_order_acquire);
      if (tail == m_head_cache) {
        return false;  // Queue is empty
      }
    }

    // Read data
    if constexpr (std::is_trivially_copyable_v<T>) {
      std::memcpy(&value, &m_storage[tail], sizeof(T));
    } else {
      // Move out to avoid an extra copy for non-trivial types
      value = std::move(m_storage[tail]);
    }

    // Advance tail: release ensures producer sees we've consumed
    const auto next = (tail + 1) & m_mask;
    m_tail.store(next, std::memory_order_release);
    return true;
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

    auto tail = m_tail.load(std::memory_order_relaxed);

    // Always refresh head cache to get accurate count of available items
    m_head_cache = m_head.load(std::memory_order_acquire);

    if (tail == m_head_cache) {
      return 0;  // Queue is empty
    }

    // Calculate how many items are available
    const std::size_t available = (m_head_cache - tail) & m_mask;
    const std::size_t to_dequeue = std::min(max_count, available);

    if (to_dequeue == 0) {
      return 0;
    }

    // Handle wrap-around case: we need to copy in two chunks
    const std::size_t first_chunk = std::min(to_dequeue, m_capacity - tail);
    const std::size_t second_chunk = to_dequeue - first_chunk;

    // Copy first chunk (from tail to end of buffer or all items if no wrap)
    if constexpr (std::is_trivially_copyable_v<T>) {
      simd_batch_copy(&values[0], &m_storage[tail], first_chunk);
    } else {
      for (std::size_t i = 0; i < first_chunk; ++i) {
        values[i] = m_storage[tail + i];
      }
    }

    // Copy second chunk if we wrapped around
    if (second_chunk > 0) {
      if constexpr (std::is_trivially_copyable_v<T>) {
        simd_batch_copy(&values[first_chunk], &m_storage[0], second_chunk);
      } else {
        for (std::size_t i = 0; i < second_chunk; ++i) {
          values[first_chunk + i] = m_storage[i];
        }
      }
    }

    // Advance tail: release ensures producer sees we've consumed
    const auto new_tail = (tail + to_dequeue) & m_mask;
    m_tail.store(new_tail, std::memory_order_release);

    return to_dequeue;
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

    auto tail = m_tail.load(std::memory_order_relaxed);

    // Always refresh head cache to get accurate count of available items
    m_head_cache = m_head.load(std::memory_order_acquire);

    if (tail == m_head_cache) {
      return 0;  // Queue is empty
    }

    // Calculate how many items are available
    const std::size_t available = (m_head_cache - tail) & m_mask;
    const std::size_t to_dequeue = std::min(max_count, available);

    if (to_dequeue == 0) {
      return 0;
    }

    std::size_t processed = 0;

    // Process first chunk (from tail to end of buffer or all items if no wrap)
    const std::size_t first_chunk = std::min(to_dequeue, m_capacity - tail);
    for (std::size_t i = 0; i < first_chunk; ++i) {
      if (!callback(m_storage[tail + i])) {
        // Early exit requested by callback
        processed = i + 1;
        const auto new_tail = (tail + processed) & m_mask;
        m_tail.store(new_tail, std::memory_order_release);
        return processed;
      }
    }
    processed += first_chunk;

    // Process second chunk if we wrapped around
    const std::size_t second_chunk = to_dequeue - first_chunk;
    if (second_chunk > 0) {
      for (std::size_t i = 0; i < second_chunk; ++i) {
        if (!callback(m_storage[i])) {
          // Early exit requested by callback
          processed = first_chunk + i + 1;
          const auto new_tail = (tail + processed) & m_mask;
          m_tail.store(new_tail, std::memory_order_release);
          return processed;
        }
      }
      processed += second_chunk;
    }

    // Advance tail: release ensures producer sees we've consumed
    const auto new_tail = (tail + to_dequeue) & m_mask;
    m_tail.store(new_tail, std::memory_order_release);

    return processed;
  }

  [[nodiscard]] std::size_t capacity() const noexcept {
    return m_capacity;
  }

private:
  const std::size_t m_capacity;
  const std::size_t m_mask;
  std::unique_ptr<T[]> m_storage;
  // Producer side (enqueue)
  alignas(64) std::atomic<std::size_t> m_head{0};
  alignas(64) std::size_t m_tail_cache{0};  // Cached consumer position
  // Consumer side (dequeue)
  alignas(64) std::atomic<std::size_t> m_tail{0};
  alignas(64) std::size_t m_head_cache{0};  // Cached producer position
};

}
