#pragma once

#include <cstddef>

namespace util {

/**
 * @brief Execute a CPU pause instruction for busy-wait loops.
 *
 * The pause instruction is used in spin-wait loops to:
 * - Reduce CPU power consumption
 * - Improve performance on hyperthreaded processors
 * - Hint to the CPU that this is a spin-wait loop
 *
 * On non-x86 architectures, this is a no-op.
 */
inline void cpu_pause() noexcept {
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
  __asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__) || defined(_M_ARM64)
  __asm__ __volatile__("yield" ::: "memory");
#elif defined(__powerpc64__) || defined(__ppc64__)
  __asm__ __volatile__("or 27,27,27" ::: "memory");
#else
  // No-op on other architectures
  (void)0;
#endif
}

/**
 * @brief Execute N CPU pause instructions in a loop.
 *
 * @param n Number of pause instructions to execute (runtime value)
 */
inline void cpu_pause_n(std::size_t n) noexcept {
  for (std::size_t i = 0; i < n; ++i) {
    cpu_pause();
  }
}

} // namespace util

