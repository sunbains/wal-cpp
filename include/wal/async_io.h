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

#pragma once

#include <span>
#include <sys/uio.h>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include "util/thread_pool.h"
#include "coro/task.h"

/* Forward declarations to break circular dependency */
namespace wal {

enum class Status;

template<typename T> using Result = std::expected<T, Status>;

/**
 * Execute I/O operation asynchronously or synchronously.
 *
 * @param callback The callback function to execute.
 * @param span The I/O vector span to pass to the callback.
 * @param pool Optional thread pool. If nullptr, executes synchronously.
 * @return A Task containing the result of the I/O operation.
 */
template<typename ResultType>
[[nodiscard]] Task<Result<ResultType>> execute_io_async(
    std::function<Result<ResultType>(std::span<struct iovec>)> callback,
    std::span<struct iovec> span,
    util::Thread_pool* pool = nullptr) {

  if (pool == nullptr) {
    /* Synchronous execution - return result immediately */
    co_return callback(span);
  }

  /* Asynchronous execution - post to thread pool */
  struct Awaiter {
    bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> continuation) {
      /* Use high priority for I/O syscalls to ensure they get CPU time */
      bool posted = m_pool->post([this, continuation]() mutable {
        m_result = m_callback(m_span);
        continuation.resume();
      });

      /* If posting failed, execute synchronously by returning false */
      if (!posted) {
        m_result = m_callback(m_span);
        return false;  /* Don't suspend - resume immediately */
      }

      return true;  /* Suspend - will be resumed by the lambda */
    }

    Result<ResultType> await_resume() noexcept {
      return std::move(*m_result);
    }

    std::function<Result<ResultType>(std::span<struct iovec>)> m_callback;
    std::span<struct iovec> m_span;
    util::Thread_pool* m_pool;
    std::optional<Result<ResultType>> m_result;
  };

  co_return co_await Awaiter{std::move(callback), span, pool, std::nullopt};
}

} // namespace wal

