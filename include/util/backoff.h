#pragma once

#include <algorithm>
#include <cstddef>
#include <thread>

#include "util.h"

namespace util {

/**
 * Context passed to backoff policy for custom backoff strategies in coroutines
 */
struct Coro_backoff_context {
  std::size_t m_retries{0};           // Number of retries so far
  bool m_queue_full{false};          // Whether queue is full (for enqueue) or empty (for dequeue)
  bool m_has_waiters{false};         // Whether there are waiters on the other side
  std::size_t m_queue_size{0};       // Current queue size (if available)
  std::size_t m_queue_capacity{0};    // Queue capacity
};

/**
 * Default backoff policy - pause then yield
 * Works with std::size_t& spins signature
 */
struct Default_backoff {
  void operator()(std::size_t& spins) noexcept {
    if (spins++ < 100) {
      cpu_pause();
    } else {
      spins = 0;
      std::this_thread::yield();
    }
  }
};

/**
 * Exponential backoff policy - exponentially increases delay between retries
 * Works with std::size_t& spins signature
 * Includes jitter to spread out producer attempts and reduce contention
 */
struct Exponential_backoff {
  void operator()(std::size_t& spins) noexcept {
    if (spins == 0) {
      /* Initial attempt - add small jitter based on thread ID to spread out producers */
      /* Hash thread ID for pseudo-random jitter without thread_local */
      auto thread_hash = std::hash<std::thread::id>{}(std::this_thread::get_id());
      std::size_t jitter = (thread_hash % 16);  /* 0-15 pause cycles */
      for (std::size_t i = 0; i < jitter; ++i) {
        cpu_pause();
      }
      spins = 1;
    } else {
      std::size_t backoff = std::min(spins, static_cast<std::size_t>(1024));
      for (std::size_t i = 0; i < backoff; ++i) {
        cpu_pause();
      }
      if (spins < 1024) {
        spins *= 2;
      } else {
        std::this_thread::yield();
        spins = 0;
      }
    }
  }
};

/**
 * Spin-only backoff policy - no yielding
 * Works with std::size_t& spins signature
 */
struct Spin_backoff {
  void operator()(std::size_t&) noexcept {
    cpu_pause();
  }
};

/**
 * Default backoff policy for coroutines - uses context for adaptive backoff
 * Works with Coro_backoff_context& signature
 */
struct Default_coro_backoff {
  void operator()(Coro_backoff_context& ctx) noexcept {
    if (ctx.m_retries < 100) {
      cpu_pause();
    } else {
      std::this_thread::yield();
    }
    ++ctx.m_retries;
  }
};

/**
 * Exponential backoff policy for coroutines - exponentially increases delay between retries
 * Works with Coro_backoff_context& signature
 */
struct Exponential_coro_backoff {
  void operator()(Coro_backoff_context& ctx) noexcept {
    if (ctx.m_retries == 0) {
      cpu_pause();
      ctx.m_retries = 1;
    } else {
      std::size_t backoff = std::min(ctx.m_retries, static_cast<std::size_t>(1024));
      for (std::size_t i = 0; i < backoff; ++i) {
        cpu_pause();
      }
      if (ctx.m_retries < 1024) {
        ctx.m_retries *= 2;
      } else {
        std::this_thread::yield();
        ctx.m_retries = 0;  // Reset after yield
      }
    }
  }
};

/**
 * Adapter to convert coro backoff policy (Coro_backoff_context&) to queue backoff policy (std::size_t&)
 * This allows the same backoff policy to be used in both Bounded_queue_coro awaitables and the underlying queue
 */
template <typename Coro_backoff_policy>
struct Queue_backoff_adapter {
  Coro_backoff_policy m_coro_backoff;
  Coro_backoff_context m_ctx;

  void operator()(std::size_t& spins) noexcept {
    m_ctx.m_retries = spins;
    m_coro_backoff(m_ctx);
    spins = m_ctx.m_retries;
  }
};

} // namespace util

