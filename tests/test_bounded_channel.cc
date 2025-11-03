// Minimal unit tests for util::Bounded_queue
// Provide required macros used by the header in this TU only.

#include <cassert>
#include <atomic>
#include <thread>
#include <vector>
#include <optional>
#include <chrono>
#include <numeric>
#include <mutex>
#include <string>
#include <algorithm>
#include <cstdlib>
#include <cstdio>

#include "util/bounded_channel.h"

using util::Bounded_queue;

static void test_basic_single_thread() {
  std::fprintf(stderr, "[test_basic_single_thread] start\n");
  Bounded_queue<int> q(8); // ring size is 8, usable slots 8
  assert(q.capacity() == 8);
  assert(q.empty());
  assert(!q.full());

  // Enqueue 1..8
  for (int i = 1; i <= 8; ++i) {
    if (!q.enqueue(i)) { std::fprintf(stderr, "enqueue fail at i=%d\n", i); std::abort(); }
  }
  assert(q.full());
  assert(q.full());

  // Dequeue and check order
  for (int i = 1; i <= 8; ++i) {
    int v = 0;
    if (!q.dequeue(v)) { std::fprintf(stderr, "dequeue fail at i=%d\n", i); std::abort(); }
    if (v != i) { std::fprintf(stderr, "order mismatch got=%d expect=%d\n", v, i); std::abort(); }
  }
  assert(q.empty());
  std::fprintf(stderr, "[test_basic_single_thread] done\n");
}

static void test_wraparound() {
  std::fprintf(stderr, "[test_wraparound] start\n");
  Bounded_queue<int> q(8); // ring size 8

  // Fill 0..7
  for (int i = 0; i < 8; ++i) if (!q.enqueue(i)) { std::fprintf(stderr, "initial enqueue fail i=%d\n", i); std::abort(); }

  // Dequeue 0..3
  for (int i = 0; i < 4; ++i) {
    int v{}; if (!q.dequeue(v)) { std::fprintf(stderr, "dequeue(phase1) fail i=%d\n", i); std::abort(); }
    if (v != i) { std::fprintf(stderr, "order(phase1) mismatch got=%d expect=%d\n", v, i); std::abort(); }
  }

  // Enqueue 8..11 to force wrap-around positions
  for (int i = 8; i < 12; ++i) if (!q.enqueue(i)) { std::fprintf(stderr, "enqueue(phase2) fail i=%d\n", i); std::abort(); }

  // Drain remaining in order: 4..11
  std::vector<int> out;
  int v{};
  while (q.dequeue(v)) out.push_back(v);

  std::vector<int> expect{4,5,6,7,8,9,10,11};
  if (out != expect) {
    std::fprintf(stderr, "wrap result mismatch\n");
    std::fprintf(stderr, "got: "); for (auto x: out) std::fprintf(stderr, "%d ", x); std::fprintf(stderr, "\n");
    std::fprintf(stderr, "exp: "); for (auto x: expect) std::fprintf(stderr, "%d ", x); std::fprintf(stderr, "\n");
    std::abort();
  }
  assert(q.empty());
  std::fprintf(stderr, "[test_wraparound] done\n");
}

static void test_emplace_and_types() {
  std::fprintf(stderr, "[test_emplace_and_types] start\n");
  Bounded_queue<std::pair<int, std::string>> q(4);
  assert(q.capacity() == 4);

  if (!q.emplace(1, "a")) { std::fprintf(stderr, "emplace #1 failed\n"); std::abort(); }
  if (!q.emplace(2, std::string{"bb"})) { std::fprintf(stderr, "emplace #2 failed\n"); std::abort(); }
  std::pair<int, std::string> p;
  if (!q.dequeue(p)) { std::fprintf(stderr, "dequeue p1 fail\n"); std::abort(); }
  assert(p.first == 1 && p.second == "a");
  if (!q.dequeue(p)) { std::fprintf(stderr, "dequeue p2 fail\n"); std::abort(); }
  assert(p.first == 2 && p.second == "bb");
  std::fprintf(stderr, "[test_emplace_and_types] done\n");
}

static void test_mpmc_basic() {
  std::fprintf(stderr, "[test_mpmc_basic] start\n");
  // Keep this small and robust; validate all items observed exactly once.
  constexpr int producers = 2;
  // consumers count implicitly derived from threads
  constexpr int items_per_producer = 1000;
  Bounded_queue<int> q(1024);

  std::atomic<int> produced{0};
  std::atomic<int> finished_producers{0};
  std::vector<int> consumed; consumed.reserve(producers * items_per_producer);
  std::mutex mx;

  auto prod = [&](int base) {
    for (int i = 0; i < items_per_producer; ++i) {
      int val = base + i;
      while (!q.enqueue(val)) {
        std::this_thread::yield();
      }
      produced.fetch_add(1, std::memory_order_relaxed);
    }
    finished_producers.fetch_add(1, std::memory_order_release);
  };

  auto cons = [&] {
    for (;;) {
      int v{};
      if (q.dequeue(v)) {
        std::scoped_lock lk(mx);
        consumed.push_back(v);
      } else {
        if (finished_producers.load(std::memory_order_acquire) == producers) {
          if (q.empty()) break; // drain fully
        }
        std::this_thread::yield();
      }
    }
  };

  std::thread tp1(prod, 0);
  std::thread tp2(prod, items_per_producer);
  std::thread tc1(cons);
  std::thread tc2(cons);
  tp1.join(); tp2.join();
  tc1.join(); tc2.join();

  assert(static_cast<int>(consumed.size()) == producers * items_per_producer);

  // Verify set equality by sorting and comparing to expected range
  std::sort(consumed.begin(), consumed.end());
  for (int i = 0; i < producers * items_per_producer; ++i) {
    assert(consumed[i] == i);
  }
  std::fprintf(stderr, "[test_mpmc_basic] done\n");
}

int main() {
  test_basic_single_thread();
  test_wraparound();
  test_emplace_and_types();
  test_mpmc_basic();
  return 0;
}
