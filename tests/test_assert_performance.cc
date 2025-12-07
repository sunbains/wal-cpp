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

// Performance test for assertion overhead
// This test verifies that:
// 1. Disabled assertions have zero overhead (compile to nothing)
// 2. Enabled assertions only add the cost of the condition check
//
// Build configurations to test:
//   Release with all asserts disabled:
//     g++ -std=c++23 -O3 -DNDEBUG -DDISABLE_ALL_ASSERTS -I../include test_assert_performance.cc -S -o perf_disabled.s
//   Release with default asserts enabled:
//     g++ -std=c++23 -O3 -DNDEBUG -I../include test_assert_performance.cc -S -o perf_enabled.s
//   Debug with all asserts enabled:
//     g++ -std=c++23 -O3 -I../include test_assert_performance.cc -S -o perf_debug.s
//
// Compare assembly output to verify overhead

#include "util/assert.h"
#include "util/logger.h"
#include <chrono>
#include <iostream>
#include <vector>

// Define the global logger instance
util::Logger<util::MT_logger_writer> g_logger(util::MT_logger_writer{std::cerr},
                                               util::Log_level::Err);

// Prevent compiler from optimizing away our test functions
template <typename T>
__attribute__((noinline)) void do_not_optimize(T&& value) {
  asm volatile("" : "+r,m"(value) : : "memory");
}

// Test 1: Tight loop with audit assertions (disabled by NDEBUG)
__attribute__((noinline)) int test_audit_assert_loop(const int* data, size_t size) {
  int sum = 0;
  for (size_t i = 0; i < size; ++i) {
    audit_assert(i < size, "Index out of bounds");
    audit_assert(data != nullptr, "Null pointer");
    sum += data[i];
  }
  return sum;
}

// Test 2: Tight loop with contract assertions (always enabled unless DISABLE_ALL_ASSERTS)
__attribute__((noinline)) int test_contract_assert_loop(const int* data, size_t size) {
  int sum = 0;
  for (size_t i = 0; i < size; ++i) {
    contract_assert(i < size, "Index out of bounds");
    contract_assert(data != nullptr, "Null pointer");
    sum += data[i];
  }
  return sum;
}

// Test 3: Tight loop with expects (precondition - always enabled)
__attribute__((noinline)) int test_expects_loop(const int* data, size_t size) {
  expects(data != nullptr, "Data must not be null");
  expects(size > 0, "Size must be positive");

  int sum = 0;
  for (size_t i = 0; i < size; ++i) {
    sum += data[i];
  }
  return sum;
}

// Test 4: Tight loop with assume (optimizer hint - zero cost)
__attribute__((noinline)) int test_assume_loop(const int* data, size_t size) {
  assume(data != nullptr);
  assume(size > 0);
  assume(size < 1000000);

  int sum = 0;
  for (size_t i = 0; i < size; ++i) {
    assume(i < size);
    sum += data[i];
  }
  return sum;
}

// Test 5: Control - no assertions
__attribute__((noinline)) int test_no_assert_loop(const int* data, size_t size) {
  int sum = 0;
  for (size_t i = 0; i < size; ++i) {
    sum += data[i];
  }
  return sum;
}

// Benchmark helper
template <typename Func>
void benchmark(const char* name, Func func, const int* data, size_t size, int iterations) {
  auto start = std::chrono::high_resolution_clock::now();

  int result = 0;
  for (int iter = 0; iter < iterations; ++iter) {
    result += func(data, size);
    do_not_optimize(result);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  std::cout << name << ": " << duration.count() << " μs (result: " << result << ")\n";
}

int main() {
  constexpr size_t SIZE = 10000;
  constexpr int ITERATIONS = 10000;

  std::vector<int> data(SIZE);
  for (size_t i = 0; i < SIZE; ++i) {
    data[i] = static_cast<int>(i);
  }

  std::cout << "Assertion Performance Test\n";
  std::cout << "==========================\n";
  std::cout << "Build configuration:\n";
#ifdef NDEBUG
  std::cout << "  NDEBUG: defined (release build)\n";
#else
  std::cout << "  NDEBUG: undefined (debug build)\n";
#endif
#ifdef DISABLE_ALL_ASSERTS
  std::cout << "  DISABLE_ALL_ASSERTS: defined\n";
#else
  std::cout << "  DISABLE_ALL_ASSERTS: undefined\n";
#endif
  std::cout << "\n";

  std::cout << "Running " << ITERATIONS << " iterations on " << SIZE << " elements\n\n";

  // Run benchmarks
  benchmark("Control (no asserts)      ", test_no_assert_loop, data.data(), SIZE, ITERATIONS);
  benchmark("audit_assert (NDEBUG)     ", test_audit_assert_loop, data.data(), SIZE, ITERATIONS);
  benchmark("contract_assert (default) ", test_contract_assert_loop, data.data(), SIZE, ITERATIONS);
  benchmark("expects (precondition)    ", test_expects_loop, data.data(), SIZE, ITERATIONS);
  benchmark("assume (optimizer hint)   ", test_assume_loop, data.data(), SIZE, ITERATIONS);

  std::cout << "\nExpected results:\n";
  std::cout << "  - With NDEBUG: audit_assert should match control\n";
  std::cout << "  - With DISABLE_ALL_ASSERTS: all asserts should match control\n";
  std::cout << "  - assume should always match or beat control (optimizer hints)\n";
  std::cout << "  - contract_assert/expects should show minimal overhead (condition check only)\n";

  std::cout << "\nTo examine assembly output:\n";
  std::cout << "  objdump -d -C -S <binary> | grep -A 30 'test_.*_loop'\n";
  std::cout << "  Or compile with -S flag to generate assembly (.s) file\n";

  return 0;
}
