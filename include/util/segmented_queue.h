#pragma once

#include <atomic>
#include <thread>
#include <cstddef>
#include <bit>
#include <memory>
#include <optional>
#include <vector>

#include "bounded_channel.h"
#include "backoff.h"

namespace util {

/**
 * Segmented_queue - High-performance MPMC queue using multiple Bounded_queue stripes.
 * @note The queue doesn't give any ordering guarantees. It's NOT a FIFO queue.
 *
 * Design:
 * - Each stripe is an independent Bounded_queue
 * - Producers select a stripe via handle
 * - Consumers use sticky round-robin across stripes
 * - No global counters for maximum concurrency
 */
template <typename T, typename Backoff_policy = Exponential_backoff>
struct Segmented_queue {
  using Type = T;

  struct Config {
    std::size_t m_slots_per_segment;
    std::size_t m_num_segments;
    std::size_t m_num_stripes{8};
  };

  /**
   * Handle for enqueue operations - stores stripe index
   */
  struct Handle {
    std::size_t m_stripe_idx{0};
    std::size_t m_position{0};  // Updated after enqueue for compatibility
  };

  /**
   * Construct a segmented queue.
   * @param[in] config Configuration with slots_per_segment and num_segments
   */
  explicit Segmented_queue(const Config& config)
    : m_num_stripes(config.m_num_stripes),
      m_stripe_mask(config.m_num_stripes - 1) {

    if (!std::has_single_bit(config.m_slots_per_segment)) {
      throw std::invalid_argument("slots_per_segment must be a power of 2");
    }
    if (config.m_num_segments == 0 || !std::has_single_bit(config.m_num_segments)) {
      throw std::invalid_argument("num_segments must be a power of 2");
    }
    if (config.m_num_stripes == 0 || !std::has_single_bit(config.m_num_stripes)) {
      throw std::invalid_argument("num_stripes must be a power of 2");
    }

    std::size_t total_capacity = config.m_slots_per_segment * config.m_num_segments;
    std::size_t stripe_capacity = total_capacity / config.m_num_stripes;
    
    // Bounded_queue requires capacity to be a power of 2 and at least 2
    // (since it uses n-1 internally, n=1 gives m_capacity=0 which may cause issues)
    if (stripe_capacity < 2) {
      stripe_capacity = 2;
    } else if (!std::has_single_bit(stripe_capacity)) {
      // Round up to next power of 2
      stripe_capacity = std::bit_ceil(stripe_capacity);
    }

    // Create independent Bounded_queue for each stripe
    m_stripes.reserve(config.m_num_stripes);
    for (std::size_t i = 0; i < config.m_num_stripes; ++i) {
      m_stripes.push_back(std::make_unique<Bounded_queue<T>>(stripe_capacity));
    }
  }

  Segmented_queue(Segmented_queue&&) = delete;
  Segmented_queue(const Segmented_queue&) = delete;
  Segmented_queue& operator=(Segmented_queue&&) = delete;
  Segmented_queue& operator=(const Segmented_queue&) = delete;

  ~Segmented_queue() = default;

  [[nodiscard]] bool enqueue(Handle& handle, const T& data) noexcept
    requires std::is_copy_assignable_v<T> {
    handle.m_stripe_idx &= m_stripe_mask;
    const std::size_t start_stripe = handle.m_stripe_idx;

    // Try stripes in rolling fashion until we find one with space
    do {
      if (m_stripes[handle.m_stripe_idx]->enqueue(data)) {
        return true;
      }
      // Current stripe full, try next
      handle.m_stripe_idx = (handle.m_stripe_idx + 1) & m_stripe_mask;
    } while (handle.m_stripe_idx != start_stripe);

    return false;  // All stripes full
  }

  [[nodiscard]] bool enqueue(Handle& handle, T&& data) noexcept
    requires std::is_move_assignable_v<T> {
    // For rvalue reference with rolling, use copy semantics to avoid
    // moving from data multiple times when trying different stripes
    return enqueue(handle, static_cast<const T&>(data));
  }

  void enqueue_wait(Handle& handle, const T& data) noexcept
    requires std::is_copy_assignable_v<T> {
    handle.m_stripe_idx &= m_stripe_mask;
    Backoff_policy backoff;
    std::size_t spins = 0;

    // Keep trying with rolling stripe selection until we succeed
    for (;;) {
      const std::size_t start_stripe = handle.m_stripe_idx;
      do {
        if (m_stripes[handle.m_stripe_idx]->enqueue(data)) {
          return;
        }
        handle.m_stripe_idx = (handle.m_stripe_idx + 1) & m_stripe_mask;
      } while (handle.m_stripe_idx != start_stripe);

      // All stripes full, backoff and retry
      backoff(spins);
    }
  }

  void enqueue_wait(Handle& handle, T&& data) noexcept
    requires std::is_move_assignable_v<T> {
    // For rvalue reference with rolling, use copy semantics to avoid
    // moving from data multiple times when trying different stripes
    enqueue_wait(handle, static_cast<const T&>(data));
  }

  template <typename... Args>
  [[nodiscard]] bool emplace(Handle& handle, Args&&... args) noexcept
    requires std::constructible_from<T, Args...> {
    handle.m_stripe_idx &= m_stripe_mask;
    const std::size_t start_stripe = handle.m_stripe_idx;

    // Try stripes in rolling fashion until we find one with space
    do {
      if (m_stripes[handle.m_stripe_idx]->emplace(std::forward<Args>(args)...)) {
        return true;
      }
      // Current stripe full, try next
      handle.m_stripe_idx = (handle.m_stripe_idx + 1) & m_stripe_mask;
    } while (handle.m_stripe_idx != start_stripe);

    return false;  // All stripes full
  }

  /**
   * Dequeue using sticky round-robin strategy across stripes
   * @param stripe_idx Current stripe index (updated to next stripe if current is empty)
   * @param data Output parameter for dequeued data
   * @return true if data was dequeued, false if all stripes are empty
   */
  [[nodiscard]] bool dequeue(std::size_t& stripe_idx, T& data) noexcept {
    stripe_idx &= m_stripe_mask;
    const std::size_t start_stripe = stripe_idx;

    // Sticky round-robin: try current stripe first, then others
    do {
      if (m_stripes[stripe_idx]->dequeue(data)) {
        // Success - stay on this stripe (don't update stripe_idx)
        return true;
      }
      // Current stripe empty, try next
      stripe_idx = (stripe_idx + 1) & m_stripe_mask;
    } while (stripe_idx != start_stripe);

    return false;
  }

  [[nodiscard]] std::optional<T> try_dequeue(std::size_t& stripe_idx) noexcept
    requires std::move_constructible<T> {
    T data;
    if (dequeue(stripe_idx, data)) {
      return std::move(data);
    }
    return std::nullopt;
  }

  void dequeue_wait(std::size_t& stripe_idx, T& data) noexcept {
    stripe_idx &= m_stripe_mask;
    const std::size_t start_stripe = stripe_idx;
    Backoff_policy backoff;
    std::size_t spins = 0;

    // Try each stripe until we find data
    for (;;) {
      if (m_stripes[stripe_idx]->dequeue(data)) {
        return;
      }
      stripe_idx = (stripe_idx + 1) & m_stripe_mask;
      if (stripe_idx == start_stripe) {
        backoff(spins);
      }
    }
  }

  [[nodiscard]] bool empty() const noexcept {
    for (const auto& stripe : m_stripes) {
      if (!stripe->empty()) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] bool full() const noexcept {
    for (const auto& stripe : m_stripes) {
      if (!stripe->full()) {
        return false;
      }
    }
    return true;
  }

  [[nodiscard]] std::size_t capacity() const noexcept {
    std::size_t total = 0;
    for (const auto& stripe : m_stripes) {
      total += stripe->capacity();
    }
    return total;
  }

  [[nodiscard]] std::size_t size() const noexcept {
    std::size_t total = 0;
    for (const auto& stripe : m_stripes) {
      total += stripe->size();
    }
    return total;
  }

private:
  std::size_t m_num_stripes;
  std::size_t m_stripe_mask;
  std::vector<std::unique_ptr<Bounded_queue<T>>> m_stripes;
};

} // namespace util
