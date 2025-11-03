#pragma once

#include <atomic>
#include <memory>

/**
 * Atomic operation wrappers
 *
 * This header provides two implementations for atomic operations:
 *
 * 1. std::atomic (default, recommended)
 *    - Standards-compliant, portable
 *    - Well-optimized by modern compilers
 *    - Enabled when WAL_USE_ATOMIC_BUILTINS is NOT defined
 *
 * 2. __atomic builtins (experimental, opt-in)
 *    - Uses GCC/Clang compiler builtins
 *    - Contains undefined behavior (reinterpret_cast of std::atomic)
 *    - Works on GCC/Clang x86_64 but not portable
 *    - Provides zero performance benefit over std::atomic
 *    - Enabled when WAL_USE_ATOMIC_BUILTINS is defined
 *
 * See ATOMIC_ANALYSIS.md for detailed analysis.
 *
 * Usage:
 *   std::atomic<size_t> counter{0};
 *
 *   // Runtime memory order:
 *   auto val = atomic_load(&counter, std::memory_order_acquire);
 *   atomic_store(&counter, 42, std::memory_order_release);
 *
 *   // Compile-time memory order (may be slightly more efficient):
 *   auto val = atomic_load_const<size_t, std::memory_order_acquire>(&counter);
 *   atomic_store_const<size_t, std::memory_order_release>(&counter, 42);
 */

#ifdef WAL_USE_ATOMIC_BUILTINS

/* ============================================================================
 * HOMEBREW ATOMIC BUILTINS (EXPERIMENTAL)
 * ============================================================================
 * WARNING: Contains undefined behavior. Use at your own risk.
 * See ATOMIC_ANALYSIS.md for details.
 */

namespace util::detail {
  /** Convert std::memory_order to __ATOMIC_* constants at compile time */
  template <std::memory_order Order>
  constexpr int atomic_order_constant() noexcept {
    if constexpr (Order == std::memory_order_relaxed) {
      return __ATOMIC_RELAXED;
    } else if constexpr (Order == std::memory_order_acquire) {
      return __ATOMIC_ACQUIRE;
    } else if constexpr (Order == std::memory_order_release) {
      return __ATOMIC_RELEASE;
    } else if constexpr (Order == std::memory_order_acq_rel) {
      return __ATOMIC_ACQ_REL;
    } else if constexpr (Order == std::memory_order_seq_cst) {
      return __ATOMIC_SEQ_CST;
    } else if constexpr (Order == std::memory_order_consume) {
      return __ATOMIC_CONSUME;
    } else {
      return __ATOMIC_SEQ_CST;
    }
  }

  /** Runtime conversion for non-constant memory orders */
  [[nodiscard]] constexpr int to_atomic_order_runtime(std::memory_order order) noexcept {
    switch (order) {
      case std::memory_order_relaxed: return __ATOMIC_RELAXED;
      case std::memory_order_acquire: return __ATOMIC_ACQUIRE;
      case std::memory_order_release: return __ATOMIC_RELEASE;
      case std::memory_order_acq_rel: return __ATOMIC_ACQ_REL;
      case std::memory_order_seq_cst: return __ATOMIC_SEQ_CST;
      case std::memory_order_consume: return __ATOMIC_CONSUME;
      default: return __ATOMIC_SEQ_CST;
    }
  }
} // namespace util::detail

/**
 * Atomic load wrapper - uses __atomic builtins
 * WARNING: Contains UB (reinterpret_cast of std::atomic)
 */
template <typename T>
[[nodiscard]] inline T atomic_load(const std::atomic<T>* ptr, std::memory_order order = std::memory_order_seq_cst) noexcept {
  /* WARNING: This assumes std::atomic<T> has standard layout with value at offset 0.
   * This is NOT guaranteed by the C++ standard and is undefined behavior.
   * Works on GCC/Clang x86_64 but may break on other platforms. */
  return __atomic_load_n(reinterpret_cast<const T*>(ptr), util::detail::to_atomic_order_runtime(order));
}

/**
 * Atomic load wrapper with compile-time constant memory order
 */
template <typename T, std::memory_order Order>
[[nodiscard]] inline T atomic_load_const(const std::atomic<T>* ptr) noexcept {
  return __atomic_load_n(reinterpret_cast<const T*>(ptr), util::detail::atomic_order_constant<Order>());
}

/**
 * Atomic store wrapper - uses __atomic builtins
 * WARNING: Contains UB (reinterpret_cast of std::atomic)
 */
template <typename T>
inline void atomic_store(std::atomic<T>* ptr, T desired, std::memory_order order = std::memory_order_seq_cst) noexcept {
  /* WARNING: This assumes std::atomic<T> has standard layout with value at offset 0.
   * This is NOT guaranteed by the C++ standard and is undefined behavior.
   * Works on GCC/Clang x86_64 but may break on other platforms. */
  __atomic_store_n(reinterpret_cast<T*>(ptr), desired, util::detail::to_atomic_order_runtime(order));
}

/**
 * Atomic store wrapper with compile-time constant memory order
 */
template <typename T, std::memory_order Order>
inline void atomic_store_const(std::atomic<T>* ptr, T desired) noexcept {
  __atomic_store_n(reinterpret_cast<T*>(ptr), desired, util::detail::atomic_order_constant<Order>());
}

/**
 * Atomic compare-exchange wrapper - uses __atomic builtins
 * WARNING: Contains UB (reinterpret_cast of std::atomic)
 */
template <typename T>
[[nodiscard]] inline bool atomic_compare_exchange_weak(
    std::atomic<T>* ptr,
    T* expected,
    T desired,
    std::memory_order success_order,
    std::memory_order failure_order) noexcept {
  /* WARNING: This assumes std::atomic<T> has standard layout with value at offset 0.
   * This is NOT guaranteed by the C++ standard and is undefined behavior.
   * Works on GCC/Clang x86_64 but may break on other platforms. */
  return __atomic_compare_exchange_n(
      reinterpret_cast<T*>(ptr), expected, desired, true, /* weak */
      util::detail::to_atomic_order_runtime(success_order),
      util::detail::to_atomic_order_runtime(failure_order));
}

/**
 * Atomic compare-exchange wrapper with compile-time constant memory orders
 */
template <typename T, std::memory_order SuccessOrder, std::memory_order FailureOrder>
[[nodiscard]] inline bool atomic_compare_exchange_weak_const(
    std::atomic<T>* ptr,
    T* expected,
    T desired) noexcept {
  return __atomic_compare_exchange_n(
      reinterpret_cast<T*>(ptr), expected, desired, true, /* weak */
      util::detail::atomic_order_constant<SuccessOrder>(),
      util::detail::atomic_order_constant<FailureOrder>());
}

#else

/* ============================================================================
 * STD::ATOMIC WRAPPERS (DEFAULT, RECOMMENDED)
 * ============================================================================
 * Standards-compliant, portable, well-optimized.
 * Produces identical assembly to __atomic builtins on modern compilers.
 */

/**
 * Atomic load wrapper - uses std::atomic methods
 */
template <typename T>
[[nodiscard]] inline T atomic_load(const std::atomic<T>* ptr, std::memory_order order = std::memory_order_seq_cst) noexcept {
  return ptr->load(order);
}

/**
 * Atomic load wrapper with compile-time constant memory order
 */
template <typename T, std::memory_order Order>
[[nodiscard]] inline T atomic_load_const(const std::atomic<T>* ptr) noexcept {
  return ptr->load(Order);
}

/**
 * Atomic store wrapper - uses std::atomic methods
 */
template <typename T>
inline void atomic_store(std::atomic<T>* ptr, T desired, std::memory_order order = std::memory_order_seq_cst) noexcept {
  ptr->store(desired, order);
}

/**
 * Atomic store wrapper with compile-time constant memory order
 */
template <typename T, std::memory_order Order>
inline void atomic_store_const(std::atomic<T>* ptr, T desired) noexcept {
  ptr->store(desired, Order);
}

/**
 * Atomic compare-exchange wrapper - uses std::atomic methods
 */
template <typename T>
[[nodiscard]] inline bool atomic_compare_exchange_weak(
    std::atomic<T>* ptr,
    T* expected,
    T desired,
    std::memory_order success_order,
    std::memory_order failure_order) noexcept {
  return ptr->compare_exchange_weak(*expected, desired, success_order, failure_order);
}

/**
 * Atomic compare-exchange wrapper with compile-time constant memory orders
 */
template <typename T, std::memory_order SuccessOrder, std::memory_order FailureOrder>
[[nodiscard]] inline bool atomic_compare_exchange_weak_const(
    std::atomic<T>* ptr,
    T* expected,
    T desired) noexcept {
  return ptr->compare_exchange_weak(*expected, desired, SuccessOrder, FailureOrder);
}

#endif // WAL_USE_ATOMIC_BUILTINS
