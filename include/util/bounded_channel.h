#pragma once

#include <atomic>
#include <bit>
#include <concepts>
#include <new>
#include <type_traits>
#include <utility>

namespace util {

template <typename T>
struct Bounded_queue {
  using Type = T;
  using Pos = std::size_t;
  static constexpr bool is_lock_free = std::atomic<Pos>::is_always_lock_free;

  explicit Bounded_queue(std::size_t n) 
    : m_ring(std::make_unique<Cell[]>(n)),
      m_capacity(n - 1) {

    if (!std::has_single_bit(n)) {
      throw std::invalid_argument("n must be a power of 2");
    }

    for (std::size_t i{}; i < n; ++i) {
      m_ring[i].m_pos.store(i, std::memory_order_relaxed);
    }

    m_epos.store(0, std::memory_order_relaxed);
    m_dpos.store(0, std::memory_order_relaxed);
  }

  Bounded_queue(Bounded_queue &&) = delete;
  Bounded_queue(const Bounded_queue &) = delete;
  Bounded_queue &operator=(Bounded_queue &&) = delete;
  Bounded_queue &operator=(const Bounded_queue &) = delete;

  ~Bounded_queue() = default;

  [[nodiscard]] bool enqueue(T const &data) noexcept(noexcept(std::declval<T &>() = std::declval<T const &>())) requires std::assignable_from<T &, T const &> {
    return enqueue_impl([&](T &slot) { slot = data; });
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
    auto pos{m_dpos.load(std::memory_order_relaxed)};

    for (;;) {
      cell = &m_ring.get()[pos & m_capacity];
      const auto seq{cell->m_pos.load(std::memory_order_acquire)};
      const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + 1);

      if (diff == 0) [[likely]] {
        /* Atomically move head to claim spot. Weak CAS may fail spuriously or
         * if another thread updated m_dpos; loop retries in both cases. */
        if (m_dpos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }

      } else if (diff < 0) [[unlikely]] {
        /* The Bounded_queue is empty */
        return false;

      } else {
        /* Under normal circumstances this branch should never be taken. */
        pos = m_dpos.load(std::memory_order_relaxed);
      }
    }

    data = cell->m_data;

    /* Set the sequence to what the head sequence should be next time around */
    cell->m_pos.store(pos + m_capacity + 1, std::memory_order_release);

    return true;
  }

  /** @return the capacity of the Bounded_queue */
  [[nodiscard]] auto capacity() const noexcept { return m_capacity + 1; }

  /** @return true if the Bounded_queue is empty. */
  [[nodiscard]] bool empty() const noexcept {
    auto pos{m_dpos.load(std::memory_order_relaxed)};

    for (;;) {
      auto cell{&m_ring.get()[pos & m_capacity]};
      const auto seq{cell->m_pos.load(std::memory_order_acquire)};
      const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos + 1);

      if (diff == 0) [[likely]] {
        return false;
      } else if (diff < 0) [[likely]] {
        return true;
      } else {
        pos = m_dpos.load(std::memory_order_relaxed);
      }
    }

    std::unreachable();
    return false;
  }

  /** @return true if the Bounded_queue is full. */
  [[nodiscard]] bool full() const noexcept {
    auto pos{m_epos.load(std::memory_order_relaxed)};

    for (;;) {
      auto cell = &m_ring.get()[pos & m_capacity];
      const auto seq{cell->m_pos.load(std::memory_order_acquire)};
      const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos);

      if (diff == 0) [[likely]] {
        /* Cell is empty -> queue not full */
        return false;
      } else if (diff < 0) [[likely]] {
        /* The Bounded_queue is full */
        return true;
      } else {
        pos = m_epos.load(std::memory_order_relaxed);
      }
    }

    std::unreachable();
    return false;
  }

 private:
  template <typename Writer>
  [[nodiscard]] bool enqueue_impl(Writer &&write) noexcept(noexcept(write(std::declval<T &>()))) {
    Cell *cell;

    /* m_epos only wraps at MAX(m_epos); use capacity as mask for index. */
    auto pos{m_epos.load(std::memory_order_relaxed)};

    for (;;) {
      cell = &m_ring[pos & m_capacity];
      const auto seq{cell->m_pos.load(std::memory_order_acquire)};
      const auto diff = static_cast<std::ptrdiff_t>(seq) - static_cast<std::ptrdiff_t>(pos);

      if (diff == 0) [[likely]] {
        if (m_epos.compare_exchange_weak(pos, pos + 1, std::memory_order_relaxed)) {
          break;
        }
      } else if (diff < 0) [[unlikely]] {
        /* The Bounded_queue is full */
        return false;
      } else {
        pos = m_epos.load(std::memory_order_relaxed);
      }
    }

    write(cell->m_data);

    /* Publish: increment sequence so consumers can observe data. */
    cell->m_pos.store(pos + 1, std::memory_order_release);

    return true;
  }

 private:
  using Pad = std::byte[std::hardware_constructive_interference_size];

  struct alignas(T) Cell {
    T m_data{};
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
