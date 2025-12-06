// Example usage of the C++26-forward compatible assertion system

#include "util/assert.h"
#include "util/logger.h"
#include <iostream>

// Define the global logger instance
util::Logger<util::MT_logger_writer> g_logger{
    util::MT_logger_writer{std::cerr}, util::Log_level::Trace};

// Example function using preconditions
int divide(int a, int b) {
  expects(b != 0, "Division by zero: divisor must not be zero");
  return a / b;
}

// Example function using postconditions
int* allocate_buffer(size_t size) {
  expects(size > 0, "Buffer size must be positive, got {}", size);

  int* buffer = new int[size];

  ensures(buffer != nullptr, "Failed to allocate buffer of size {}", size);
  return buffer;
}

// Example function using runtime assertions
void process_data(int* data, size_t size) {
  expects(data != nullptr, "Data pointer must not be null");
  expects(size > 0, "Size must be positive");

  for (size_t i = 0; i < size; ++i) {
    // Invariant check
    contract_assert(i < size, "Index {} out of bounds (size: {})", i, size);
    data[i] *= 2;
  }

  // Postcondition: all values should be even
  for (size_t i = 0; i < size; ++i) {
    ensures(data[i] % 2 == 0, "Result at index {} is not even: {}", i, data[i]);
  }
}

// Example using audit assertions (only checked in debug builds)
void expensive_validation(const int* data, size_t size) {
  // This check is expensive and only runs when NDEBUG is not defined
  audit_assert(data != nullptr, "Data must not be null");

  // Expensive O(n^2) check only in debug builds
  for (size_t i = 0; i < size; ++i) {
    for (size_t j = i + 1; j < size; ++j) {
      audit_assert(data[i] + data[j] >= 0,
                   "Sum overflow check failed at indices {}, {}", i, j);
    }
  }
}

// Example using assume for optimizer hints
int fast_lookup(const int* sorted_array, size_t size, int value) {
  expects(sorted_array != nullptr, "Array must not be null");
  expects(size > 0, "Array must not be empty");

  // Tell the compiler this array is sorted (helps with optimizations)
  // CAUTION: If this assumption is false, behavior is undefined!
  for (size_t i = 1; i < size; ++i) {
    assume(sorted_array[i] >= sorted_array[i - 1]);
  }

  // Binary search implementation would go here...
  return 0;
}

int main() {
  std::cout << "Testing C++26-forward compatible assertions\n";

  // Test preconditions
  int result = divide(10, 2);
  std::cout << "10 / 2 = " << result << "\n";

  // This would trigger an assertion:
  // int bad_result = divide(10, 0);  // expects(b != 0, ...) would fail

  // Test postconditions
  int* buffer = allocate_buffer(10);
  delete[] buffer;

  // Test runtime assertions
  int data[] = {1, 2, 3, 4, 5};
  process_data(data, 5);

  // Test audit assertions (only checked in debug builds)
  expensive_validation(data, 5);

  std::cout << "All assertions passed!\n";
  return 0;
}
