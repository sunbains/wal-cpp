#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <memory>
#include <stdexcept>
#include <cstring>
#include <type_traits>

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
    value = m_storage[tail];
    }
    
    // Advance tail: release ensures producer sees we've consumed
    const auto next = (tail + 1) & m_mask;
    m_tail.store(next, std::memory_order_release);
    return true;
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