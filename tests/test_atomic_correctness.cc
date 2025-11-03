#include <atomic>
#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>
#include <vector>

/* Test the homebrew atomic wrappers for correctness issues */

/* Include the atomic wrappers */
#ifdef WAL_USE_ATOMIC_BUILTINS
  #undef WAL_USE_ATOMIC_BUILTINS
#endif /* WAL_USE_ATOMIC_BUILTINS */

/* NOTE: These will only work on x86_64 architecture, it will most likely
 * not work on other architectures.
 *
 * The only reason they exist is because I was desperate for performance
 * gains, not matter how small and decided to try this. */
 
/* First, let's check if the reinterpret_cast assumption holds */
template <typename T>
struct LayoutCheck {
  static bool check() {
    std::atomic<T> atom{};
    T value{42};

    /* Check if std::atomic<T> has the same size as T (unlikely but possible) */
    bool same_size = sizeof(std::atomic<T>) == sizeof(T);

    /* Check if we can extract the value using reinterpret_cast */
    atom.store(value, std::memory_order_relaxed);

    /* This is UB, but let's see if it "works" on this platform */
    T extracted;
    std::memcpy(&extracted, &atom, sizeof(T));

    bool value_matches = (extracted == value);

    std::cout << "Type: " << __PRETTY_FUNCTION__ << "\n";
    std::cout << "  sizeof(std::atomic<T>): " << sizeof(std::atomic<T>) << "\n";
    std::cout << "  sizeof(T): " << sizeof(T) << "\n";
    std::cout << "  alignof(std::atomic<T>): " << alignof(std::atomic<T>) << "\n";
    std::cout << "  alignof(T): " << alignof(T) << "\n";
    std::cout << "  Same size: " << (same_size ? "yes" : "no") << "\n";
    std::cout << "  Value matches via memcpy: " << (value_matches ? "yes" : "no") << "\n";

    /* Check if the address of the atomic equals the address after reinterpret_cast */
    void* atomic_addr = static_cast<void*>(&atom);
    void* reinterpreted_addr = static_cast<void*>(reinterpret_cast<T*>(&atom));
    std::cout << "  Addresses match: " << (atomic_addr == reinterpreted_addr ? "yes" : "no") << "\n";

    return same_size && value_matches;
  }
};

/* Now define the atomic wrappers WITH builtins enabled */
#define WAL_USE_ATOMIC_BUILTINS

namespace util::detail {
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
    } else {
      return __ATOMIC_SEQ_CST;
    }
  }

  [[nodiscard]] constexpr int to_atomic_order_runtime(std::memory_order order) noexcept {
    switch (order) {
      case std::memory_order_relaxed: return __ATOMIC_RELAXED;
      case std::memory_order_acquire: return __ATOMIC_ACQUIRE;
      case std::memory_order_release: return __ATOMIC_RELEASE;
      case std::memory_order_acq_rel: return __ATOMIC_ACQ_REL;
      case std::memory_order_seq_cst: return __ATOMIC_SEQ_CST;
      default: return __ATOMIC_SEQ_CST;
    }
  }
}

/* Builtin versions */
template <typename T>
[[nodiscard]] inline T atomic_load_builtin(const std::atomic<T>* ptr, std::memory_order order = std::memory_order_seq_cst) noexcept {
  return __atomic_load_n(reinterpret_cast<const T*>(ptr), util::detail::to_atomic_order_runtime(order));
}

template <typename T>
inline void atomic_store_builtin(std::atomic<T>* ptr, T desired, std::memory_order order = std::memory_order_seq_cst) noexcept {
  __atomic_store_n(reinterpret_cast<T*>(ptr), desired, util::detail::to_atomic_order_runtime(order));
}

template <typename T>
[[nodiscard]] inline bool atomic_compare_exchange_weak_builtin(
    std::atomic<T>* ptr,
    T* expected,
    T desired,
    std::memory_order success_order,
    std::memory_order failure_order) noexcept {
  return __atomic_compare_exchange_n(
      reinterpret_cast<T*>(ptr), expected, desired, true,
      util::detail::to_atomic_order_runtime(success_order),
      util::detail::to_atomic_order_runtime(failure_order));
}

/* Standard versions */
template <typename T>
[[nodiscard]] inline T atomic_load_std(const std::atomic<T>* ptr, std::memory_order order = std::memory_order_seq_cst) noexcept {
  return ptr->load(order);
}

template <typename T>
inline void atomic_store_std(std::atomic<T>* ptr, T desired, std::memory_order order = std::memory_order_seq_cst) noexcept {
  ptr->store(desired, order);
}

template <typename T>
[[nodiscard]] inline bool atomic_compare_exchange_weak_std(
    std::atomic<T>* ptr,
    T* expected,
    T desired,
    std::memory_order success_order,
    std::memory_order failure_order) noexcept {
  return ptr->compare_exchange_weak(*expected, desired, success_order, failure_order);
}

/* Test equivalence between builtin and std versions */
template <typename T>
void test_atomic_equivalence() {
  std::cout << "\n=== Testing atomic equivalence for " << __PRETTY_FUNCTION__ << " ===\n";

  /* Test load/store */
  {
    std::atomic<T> atom1{};
    std::atomic<T> atom2{};
    T value = 42;

    atomic_store_builtin(&atom1, value, std::memory_order_relaxed);
    atomic_store_std(&atom2, value, std::memory_order_relaxed);

    T read1 = atomic_load_builtin(&atom1, std::memory_order_relaxed);
    T read2 = atomic_load_std(&atom2, std::memory_order_relaxed);

    std::cout << "Load/Store test: " << ((read1 == value && read2 == value) ? "PASS" : "FAIL") << "\n";
    std::cout << "  Builtin: " << read1 << ", Std: " << read2 << ", Expected: " << value << "\n";
  }

  /* Test compare-exchange */
  {
    std::atomic<T> atom1{};
    std::atomic<T> atom2{};

    atomic_store_builtin(&atom1, T{10}, std::memory_order_relaxed);
    atomic_store_std(&atom2, T{10}, std::memory_order_relaxed);

    T expected1 = 10;
    T expected2 = 10;

    bool result1 = atomic_compare_exchange_weak_builtin(&atom1, &expected1, T{20}, std::memory_order_relaxed, std::memory_order_relaxed);
    bool result2 = atomic_compare_exchange_weak_std(&atom2, &expected2, T{20}, std::memory_order_relaxed, std::memory_order_relaxed);

    std::cout << "CAS success test: " << ((result1 && result2) ? "PASS" : "FAIL") << "\n";
    std::cout << "  Builtin: " << result1 << ", Std: " << result2 << "\n";

    /* Test failed CAS */
    expected1 = 999;
    expected2 = 999;

    result1 = atomic_compare_exchange_weak_builtin(&atom1, &expected1, T{30}, std::memory_order_relaxed, std::memory_order_relaxed);
    result2 = atomic_compare_exchange_weak_std(&atom2, &expected2, T{30}, std::memory_order_relaxed, std::memory_order_relaxed);

    std::cout << "CAS failure test: " << ((!result1 && !result2 && expected1 == 20 && expected2 == 20) ? "PASS" : "FAIL") << "\n";
    std::cout << "  Builtin result: " << result1 << " (expected=" << expected1 << ")\n";
    std::cout << "  Std result: " << result2 << " (expected=" << expected2 << ")\n";
  }
}

/* Multi-threaded stress test */
template <typename T>
void test_concurrent_operations() {
  std::cout << "\n=== Concurrent operations test for " << __PRETTY_FUNCTION__ << " ===\n";

  std::atomic<T> counter_builtin{};
  std::atomic<T> counter_std{};

  constexpr int num_threads = 4;
  constexpr int iterations = 10000;

  auto worker_builtin = [&]() {
    for (int i = 0; i < iterations; ++i) {
      T expected = atomic_load_builtin(&counter_builtin, std::memory_order_relaxed);
      while (!atomic_compare_exchange_weak_builtin(&counter_builtin, &expected, expected + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
        /* Retry */
      }
    }
  };

  auto worker_std = [&]() {
    for (int i = 0; i < iterations; ++i) {
      T expected = atomic_load_std(&counter_std, std::memory_order_relaxed);
      while (!atomic_compare_exchange_weak_std(&counter_std, &expected, expected + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
        /* Retry */
      }
    }
  };

  std::vector<std::thread> threads;

  /* Start builtin threads */
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker_builtin);
  }

  /* Start std threads */
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker_std);
  }

  for (auto& t : threads) {
    t.join();
  }

  T final_builtin = atomic_load_builtin(&counter_builtin, std::memory_order_relaxed);
  T final_std = atomic_load_std(&counter_std, std::memory_order_relaxed);
  T expected = num_threads * iterations;

  std::cout << "Concurrent increment test:\n";
  std::cout << "  Builtin final value: " << final_builtin << " (expected: " << expected << ") - "
            << ((final_builtin == expected) ? "PASS" : "FAIL") << "\n";
  std::cout << "  Std final value: " << final_std << " (expected: " << expected << ") - "
            << ((final_std == expected) ? "PASS" : "FAIL") << "\n";
}

int main() {
  std::cout << "=== Atomic Operations Correctness Tests ===\n\n";

  /* Check layout assumptions */
  std::cout << "Checking layout assumptions for reinterpret_cast:\n";
  std::cout << "\nChecking uint32_t:\n";
  LayoutCheck<uint32_t>::check();

  std::cout << "\nChecking uint64_t:\n";
  LayoutCheck<uint64_t>::check();

  std::cout << "\nChecking size_t:\n";
  LayoutCheck<std::size_t>::check();

  /* Test equivalence */
  test_atomic_equivalence<uint32_t>();
  test_atomic_equivalence<uint64_t>();
  test_atomic_equivalence<std::size_t>();

  /* Concurrent tests */
  test_concurrent_operations<uint32_t>();
  test_concurrent_operations<uint64_t>();
  test_concurrent_operations<std::size_t>();

  std::cout << "\n=== All tests completed ===\n";

  return 0;
}
