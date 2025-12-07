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

/* ============================================================================
 * SIMD UTILITIES FOR BATCH OPERATIONS
 * ============================================================================
 * SIMD intrinsics for optimized batch copying in queues
 */

// SIMD intrinsics detection and includes
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
  #if defined(__AVX512F__)
    #include <immintrin.h>
    #define UTIL_HAS_AVX512
  #elif defined(__AVX2__)
    #include <immintrin.h>
    #define UTIL_HAS_AVX2
  #elif defined(__SSE2__) || (defined(_M_IX86_FP) && _M_IX86_FP >= 2)
    #include <emmintrin.h>
    #define UTIL_HAS_SSE2
  #endif
#elif defined(__ARM_NEON) || defined(__aarch64__)
  #include <arm_neon.h>
  #define UTIL_HAS_NEON
#endif

#include <cstring>
#include <type_traits>

namespace util {

/**
 * SIMD-optimized batch copy for trivially copyable types
 * Falls back to memcpy for non-SIMD or when size is too small
 */
template <typename T>
inline void simd_batch_copy(T* dest, const T* src, std::size_t count) noexcept {
  static_assert(std::is_trivially_copyable_v<T>, "Type must be trivially copyable for SIMD copy");

  const std::size_t total_bytes = count * sizeof(T);
  auto* dest_bytes = reinterpret_cast<char*>(dest);
  const auto* src_bytes = reinterpret_cast<const char*>(src);

  std::size_t offset = 0;

#if defined(UTIL_HAS_AVX512)
  // AVX-512: 64 bytes (512 bits) at a time
  constexpr std::size_t simd_width = 64;
  while (offset + simd_width <= total_bytes) {
    __m512i data = _mm512_loadu_si512(reinterpret_cast<const __m512i*>(src_bytes + offset));
    _mm512_storeu_si512(reinterpret_cast<__m512i*>(dest_bytes + offset), data);
    offset += simd_width;
  }
#elif defined(UTIL_HAS_AVX2)
  // AVX2: 32 bytes (256 bits) at a time
  constexpr std::size_t simd_width = 32;
  while (offset + simd_width <= total_bytes) {
    __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src_bytes + offset));
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest_bytes + offset), data);
    offset += simd_width;
  }
#elif defined(UTIL_HAS_SSE2)
  // SSE2: 16 bytes (128 bits) at a time
  constexpr std::size_t simd_width = 16;
  while (offset + simd_width <= total_bytes) {
    __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src_bytes + offset));
    _mm_storeu_si128(reinterpret_cast<__m128i*>(dest_bytes + offset), data);
    offset += simd_width;
  }
#elif defined(UTIL_HAS_NEON)
  // NEON: 16 bytes (128 bits) at a time
  constexpr std::size_t simd_width = 16;
  while (offset + simd_width <= total_bytes) {
    uint8x16_t data = vld1q_u8(reinterpret_cast<const uint8_t*>(src_bytes + offset));
    vst1q_u8(reinterpret_cast<uint8_t*>(dest_bytes + offset), data);
    offset += simd_width;
  }
#endif

  // Copy remaining bytes with memcpy (handles tail and fallback for non-SIMD)
  if (offset < total_bytes) {
    std::memcpy(dest_bytes + offset, src_bytes + offset, total_bytes - offset);
  }
}

} // namespace util
