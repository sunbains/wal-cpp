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
#include <print>

#include "util/bounded_channel.h"

using util::Bounded_queue;

static void test_basic_single_thread() {
  std::println(stderr, "[test_basic_single_thread] start");
  Bounded_queue<int> q(8); // ring size is 8, usable slots 8
  assert(q.capacity() == 8);
  assert(q.empty());
  assert(!q.full());

  // Enqueue 1..8
  for (int i = 1; i <= 8; ++i) {
    if (!q.enqueue(i)) { std::println(stderr, "enqueue fail at i={}", i); std::abort(); }
  }
  assert(q.full());
  assert(q.full());

  // Dequeue and check order
  for (int i = 1; i <= 8; ++i) {
    int v = 0;
    if (!q.dequeue(v)) { std::println(stderr, "dequeue fail at i={}", i); std::abort(); }
    if (v != i) { std::println(stderr, "order mismatch got={} expect={}", v, i); std::abort(); }
  }
  assert(q.empty());
  std::println(stderr, "[test_basic_single_thread] done");
}

static void test_wraparound() {
  std::println(stderr, "[test_wraparound] start");
  Bounded_queue<int> q(8); // ring size 8

  // Fill 0..7
  for (int i = 0; i < 8; ++i) if (!q.enqueue(i)) { std::println(stderr, "initial enqueue fail i={}", i); std::abort(); }

  // Dequeue 0..3
  for (int i = 0; i < 4; ++i) {
    int v{}; if (!q.dequeue(v)) { std::println(stderr, "dequeue(phase1) fail i={}", i); std::abort(); }
    if (v != i) { std::println(stderr, "order(phase1) mismatch got={} expect={}", v, i); std::abort(); }
  }

  // Enqueue 8..11 to force wrap-around positions
  for (int i = 8; i < 12; ++i) if (!q.enqueue(i)) { std::println(stderr, "enqueue(phase2) fail i={}", i); std::abort(); }

  // Drain remaining in order: 4..11
  std::vector<int> out;
  int v{};
  while (q.dequeue(v)) out.push_back(v);

  std::vector<int> expect{4,5,6,7,8,9,10,11};
  if (out != expect) {
    std::println(stderr, "wrap result mismatch");
    std::print(stderr, "got: "); for (auto x: out) std::print(stderr, "{} ", x); std::println(stderr);
    std::print(stderr, "exp: "); for (auto x: expect) std::print(stderr, "{} ", x); std::println(stderr);
    std::abort();
  }
  assert(q.empty());
  std::println(stderr, "[test_wraparound] done");
}

static void test_emplace_and_types() {
  std::println(stderr, "[test_emplace_and_types] start");
  Bounded_queue<std::pair<int, std::string>> q(4);
  assert(q.capacity() == 4);

  if (!q.emplace(1, "a")) { std::println(stderr, "emplace #1 failed"); std::abort(); }
  if (!q.emplace(2, std::string{"bb"})) { std::println(stderr, "emplace #2 failed"); std::abort(); }
  std::pair<int, std::string> p;
  if (!q.dequeue(p)) { std::println(stderr, "dequeue p1 fail"); std::abort(); }
  assert(p.first == 1 && p.second == "a");
  if (!q.dequeue(p)) { std::println(stderr, "dequeue p2 fail"); std::abort(); }
  assert(p.first == 2 && p.second == "bb");
  std::println(stderr, "[test_emplace_and_types] done");
}

static void test_mpmc_basic() {
  std::println(stderr, "[test_mpmc_basic] start");
  // Keep this small and robust; validate all items observed exactly once.
  constexpr int producers = 2;
  // consumers count implicitly derived from threads
  constexpr int items_per_producer = 1000;
  Bounded_queue<std::size_t> q(1024);

  std::atomic<std::size_t> produced{0};
  std::atomic<std::size_t> finished_producers{0};
  std::vector<std::size_t> consumed; consumed.reserve(producers * items_per_producer);
  std::mutex mx;

  auto prod = [&](std::size_t base) {
    for (std::size_t i = 0; i < items_per_producer; ++i) {
      std::size_t val = base + i;
      while (!q.enqueue(val)) {
        std::this_thread::yield();
      }
      produced.fetch_add(1, std::memory_order_relaxed);
    }
    finished_producers.fetch_add(1, std::memory_order_release);
  };

  auto cons = [&] {
    for (;;) {
      std::size_t v{};
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

  assert(static_cast<std::size_t>(consumed.size()) == producers * items_per_producer);

  // Verify set equality by sorting and comparing to expected range
  std::sort(consumed.begin(), consumed.end());
  for (std::size_t i = 0; i < producers * items_per_producer; ++i) {
    assert(consumed[i] == i);
  }
  std::println(stderr, "[test_mpmc_basic] done");
}

int main() {
  test_basic_single_thread();
  test_wraparound();
  test_emplace_and_types();
  test_mpmc_basic();
  return 0;
}
