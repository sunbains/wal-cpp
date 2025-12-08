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

#include "util/metrics.h"
#include "wal/buffer_pool.h"

namespace wal {

struct [[nodiscard]] Log {
  using Sync_type = wal::Sync_type;  /* Alias to namespace-level Sync_type */

  using Slot = Buffer::Slot;
  using IO_vecs = Buffer::IO_vecs;
  using Write_callback = Buffer::Write_callback;

  /**
   * Configuration for the Log.
   */
  struct Config {
    /** Configuration for the buffer pool */
    Pool::Config m_pool_config;
    
    /** Configuration for each buffer */
    Buffer::Config m_buffer_config;
  };

  /**
   * Constructor.
   *
   * @param[in]  lsn The LSN to start the log from.
   * @param[in]  config The configuration for the log (pool and buffer).
   */
  Log(lsn_t lsn, const Config &config);

  ~Log() noexcept;

  /* Log is not copyable or movable */
  Log(const Log&) = delete;
  Log& operator=(const Log&) = delete;
  Log(Log&&) = delete;
  Log& operator=(Log&&) = delete;

  /** Reserve and write the data in the span to the buffer.
   * @param[in] span The span of the data to write.
   * @return The slot that was reserved.
   */
  [[nodiscard]] inline Result<Slot> append(std::span<const std::byte> span, util::Thread_pool* thread_pool = nullptr) noexcept;
  
  [[nodiscard]] bool is_full() const noexcept;
  [[nodiscard]] bool is_empty() const noexcept;
  [[nodiscard]] std::size_t margin() const noexcept;
  
  /**
   * Get the number of synchronous writes that occurred due to no free buffer being available.
   * @return The count of synchronous writes.
   */
  [[nodiscard]] std::size_t get_sync_write_count() const noexcept {
    return m_pool->get_sync_write_count();
  }
  
  /**
   * Get the type of the last operation enqueued to the IO queue.
   * @return The type of the last IO operation (None, Write, or Sync).
   */
  [[nodiscard]] Pool::Last_io_op_type get_last_io_op_type() const noexcept {
    return m_pool->get_last_io_op_type();
  }

  /** Start the background I/O coroutine that continuously processes buffers.
   * 
   * @param[in] callback The callback function to write the data.
   * @param[in] pool Thread pool for executing I/O operations (optional).
   */
  void start_io(Write_callback callback, util::Thread_pool* pool = nullptr) noexcept;

  /** Write all pending buffers in the pool to the store using Tasks.

   * @param[in] callback The callback function to write the data.
   * @param[in] thread_pool Thread pool for executing I/O operations.
   * @return A Task that yields true if the write was successful, false otherwise.
   */
  [[nodiscard]] Result<bool> write_to_store(Write_callback callback) noexcept;

  /** Flush all pending and active buffers to the store and wait for them to complete.

   * @param[in] callback The callback function to write the data.
   * @return A Task that yields true if the write was successful, false otherwise.
   */
  [[nodiscard]] inline Result<bool> shutdown(Write_callback callback) noexcept {
    WAL_ASSERT(m_pool->m_active != nullptr);

    /* Disable writes after this. */
    m_pool->shutdown();

    WAL_ASSERT(m_pool->m_active == nullptr);

    /* Stop the I/O coroutine and wait for any remaining buffers to be processed */
    m_pool->stop_io_coroutine();

    /* Process any remaining buffers synchronously */
    auto adapter = [callback](Buffer& buffer) -> Result<bool> {
     return  buffer.write_to_store(callback);
    };

    auto result = m_pool->write_to_store(adapter);
    
    /* Clear callbacks to break circular references before Log destruction.
     * The adapter lambda in m_io_callback captures 'this' and m_write_callback,
     * which can create cycles if the callbacks are not cleared */
    m_io_callback = {};
    m_write_callback = {};
    
    return result;
  }

  /** Flush all pending and active buffers to the store and wait for them to complete.

   * @param[in] callback The callback function to write the data.
   * @param[in] sync_callable Sync callback to execute after the last write.
   * @return A Task that yields true if the write was successful, false otherwise.
   */
  template<typename CallableType>
  [[nodiscard]] Result<bool> shutdown(Write_callback callback, CallableType* sync_callable) noexcept {
    WAL_ASSERT(m_pool->m_active != nullptr);

    /* Disable writes after this. */
    m_pool->shutdown();

    WAL_ASSERT(m_pool->m_active == nullptr);

    /* Stop the I/O coroutine and wait for any remaining buffers to be processed */
    m_pool->stop_io_coroutine();

    /* Process any remaining buffers synchronously */
    auto adapter = [callback](Buffer& buffer) -> Result<bool> {
     return  buffer.write_to_store(callback);
    };

    auto result = m_pool->write_to_store(adapter);
    
    /* If write was successful and sync callback is provided, execute sync */
    if (result.has_value() && sync_callable != nullptr) {
      auto sync_result = (*sync_callable)();
      if (!sync_result.has_value()) {
        /* Sync failed, return error */
        return Result<bool>(sync_result.error());
      }
    }
    
    /* Clear callbacks to break circular references before Log destruction.
     * The adapter lambda in m_io_callback captures 'this' and m_write_callback,
     * which can create cycles if the callbacks are not cleared */
    m_io_callback = {};
    m_write_callback = {};
    
    return result;
  }

  /**
   * Request a sync operation (fsync/fdatasync) to be executed after all pending writes.
   * The sync operation will be serialized with other IO operations.
   * 
   * @param sync_type Type of sync operation (Fdatasync or Fsync).
   * @param sync_callback Callback that performs the sync operation (returns Result<bool>).
   */
  template<typename CallableType>
  void request_sync(Sync_type sync_type, CallableType* sync_callable) noexcept {
    if (sync_type == Sync_type::None || m_thread_pool == nullptr) {
      return;
    }
    
    m_pool->enqueue_sync_operation(sync_type, sync_callable, *m_thread_pool);
  }

  /**
   * Convert the log state to a string.
   * @return The string representation of the log state.
   */
  [[nodiscard]] std::string to_string() const noexcept;

  /**
   * Set metrics collector for all buffers in the pool.
   * All buffers will share the same metrics instance for consolidated statistics.
   * @param metrics Pointer to metrics collector (can be nullptr to disable)
   */
  void set_metrics(util::Metrics* metrics) noexcept;

  /**
   * Get the metrics collector (if set).
   * @return Pointer to metrics collector or nullptr if not set
   */
  [[nodiscard]] util::Metrics* get_metrics() const noexcept {
    return m_metrics;
  }

  /** The thread pool to use for the write. */
  util::Thread_pool* m_thread_pool{nullptr};

  /* We want to avoid a circular dependency, buffer_pool.h includes this file. */
  std::unique_ptr<Pool> m_pool;

  /** Metrics collector shared by all buffers in the pool. */
  util::Metrics* m_metrics{nullptr};
  
  /** Original write callback stored for background I/O. */
  Write_callback m_write_callback{};
  
  /** I/O adapter callback - orchestrates buffer writes with sync handling. */
  std::function<Result<lsn_t>(Buffer&)> m_io_callback{};

  /** We have synced up to this LSN. */
  std::atomic<lsn_t> m_flushed_lsn{0};
};

using Log_writer = Log::Write_callback;
} // namespace wal

// Include Pool definition for inline Log::append
#include "wal/buffer_pool.h"

namespace wal {

inline Result<Log::Slot> Log::append(std::span<const std::byte> span, [[maybe_unused]] util::Thread_pool* thread_pool) noexcept {
  WAL_ASSERT(span.size() > 0);
  WAL_ASSERT(span.size() <= std::numeric_limits<std::uint16_t>::max());
  WAL_ASSERT(m_pool->m_active != nullptr);

  auto entry_ptr = m_pool->acquire_buffer();
  auto buffer{&entry_ptr->m_buffer};
  auto result = buffer->append(span);

  Slot slot;

  if (!result.has_value()) [[unlikely]] {
    slot.m_len = 0;
    slot.m_lsn = buffer->m_hwm;
    WAL_ASSERT(result.error() == Status::Not_enough_space);
  } else if (result.value().m_len == span.size()) [[likely]] {
    return result;
  } else {
    slot = result.value();
  }

  /* Only the start LSN is required by the caller. */
  const auto lsn = slot.m_lsn;

  /* Span should fit in 64K a within a single buffer, it can overflow
   * a single buffer but the  remaining bytes must fit in the next buffer. */

  m_pool->prepare_buffer_for_io(entry_ptr, *thread_pool, m_write_callback);

   /* Get the next buffer */
  entry_ptr = m_pool->acquire_buffer();
  buffer = &entry_ptr->m_buffer;
  result = buffer->append(span.subspan(slot.m_len));

  WAL_ASSERT(result.has_value());
  WAL_ASSERT(result.value().m_len == span.size() - slot.m_len);

  return Slot { .m_lsn = lsn, .m_len = uint16_t(span.size()) };
}

} // namespace wal
