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

#include <atomic>
#include <bit>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <format>
#include <functional>
#include <iostream>
#include <memory>
#include <print>
#include <sstream>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "coro/task.h"
#include "util/bounded_channel.h"
#include "util/logger.h"
#include "util/thread_pool.h"
#include "wal/buffer.h"
#include "wal/types.h"

extern util::Logger<util::MT_logger_writer> g_logger;

namespace wal {

using Config = Buffer::Config;

/**
 * Manages a pool of Buffer instances for concurrent I/O operations.
 *
 * When a buffer becomes full, it's moved to an I/O queue for async writing,
 * and operations continue on the next available buffer from the pool.
 *
 * Note: Padding is intentional for cache line alignment to prevent false sharing.
 */
// NOLINTBEGIN(clang-analyzer-optin.performance.Padding)
struct Pool {
  /**
   * Configuration for the buffer pool.
   */
  struct Config {
    /** Number of buffers in the pool (must be a power of 2, default: 32) */
    std::size_t m_pool_size{32};
    
    /** Size of the IO operations queue (must be a power of 2, default: pool_size * 2) */
    std::size_t m_io_queue_size{0};  // 0 means use pool_size * 2
    
    /**
     * Get the actual IO queue size to use.
     */
    [[nodiscard]] std::size_t get_io_queue_size() const noexcept {
      return m_io_queue_size == 0 ? m_pool_size * 2 : m_io_queue_size;
    }
  };
  
  struct Entry;
  
  /**
   * IO operation types that can be queued for serialized execution.
   * All operations are serialized through a single queue since we only configure one IO thread.
   */
  struct Io_operation {
    /* Write operation - buffer entry to write */
    struct Write_op {
      Entry* m_entry_ptr;
      Buffer::Write_callback* m_write_callback;
    };
    
    /* Sync operation - fsync or fdatasync */
    struct Sync_op {
      Sync_type m_sync_type;
      /* Pointer to callable that performs the sync operation */
      void* m_sync_callable_ptr;
      /* Function pointer to invoke the callable */
      Result<bool>(*m_sync_invoke)(void*);
    };
    
    /* Read operation - placeholder for future use */
    struct Read_op {
      /* TODO: Add read operation parameters */
    };
    
    std::variant<Write_op, Sync_op, Read_op> m_op;
  };

  struct alignas(kCLS) [[nodiscard]] Entry {
    /** State transitions are :
     * Free -> In_use -> Ready_for_io -> Free
     */
    enum class State : std::uint8_t { Free, In_use, Ready_for_io };
    
    /** Constructor.
     * 
     * @param[in] buffer The buffer to store the data.
     */
    explicit Entry(Buffer &&buffer, State state)
      : m_buffer(std::move(buffer)),
        m_state(state) {}
    
    /* Entry is not copyable or movable due to std::atomic member */
    Entry(Entry&&) = delete;
    Entry(const Entry&) = delete;
    Entry& operator=(Entry&&) = delete;
    Entry& operator=(const Entry&) = delete;
    ~Entry() = default;

    [[nodiscard]] std::string to_string() const noexcept {
      auto get_state = [&](State state) -> std::string_view {
        switch (state) {
        case State::Free:
          return "Free";
        case State::In_use:
          return "In use";
        case State::Ready_for_io:
          return "Ready for IO";
        }
        return "Unknown";
      };

      return std::format("Entry: buffer: {}, state: {}", m_buffer.to_string(), get_state(m_state));
    }

    Buffer m_buffer;

    /** For debugging purposes. */
    std::atomic<State> m_state{State::Free};
    
    /** Sync type for this buffer when it's ready for I/O (used for async sync operations). */
    wal::Sync_type m_sync_type{wal::Sync_type::None};
  };

  /**
  * Constructor.
  * 
  * @param[in] pool_config Configuration for the buffer pool
  * @param[in] buffer_config Configuration for each buffer
  * @param[in] hwm Starting LSN for the first buffer
  */
  Pool(const Config& pool_config, const Buffer::Config& buffer_config, lsn_t hwm)
    : m_free_buffers(pool_config.m_pool_size),
      m_ready_for_io_buffers(pool_config.m_pool_size),
      m_io_thread_running(false),
      m_io_operations(pool_config.get_io_queue_size()) {

    if (!std::has_single_bit(pool_config.m_pool_size)) {
      throw std::invalid_argument("Pool size must be a power of 2");
    }

    const auto io_queue_size = pool_config.get_io_queue_size();
    if (!std::has_single_bit(io_queue_size)) {
      throw std::invalid_argument("IO queue size must be a power of 2");
    }

    m_buffers.reserve(pool_config.m_pool_size);

    for (std::size_t i = 0; i < pool_config.m_pool_size; ++i) {
      m_buffers.emplace_back(std::make_unique<Entry>(Buffer(buffer_config), Entry::State::Free));
      if (!m_free_buffers.enqueue(m_buffers.back().get())) {
        std::terminate();
      }
    }

    [[maybe_unused]] auto ret = m_free_buffers.dequeue(m_active);
    WAL_ASSERT(ret);
    WAL_ASSERT(m_active->m_state == Entry::State::Free);

    m_active->m_buffer.initialize(hwm);
    m_active->m_state = Entry::State::In_use;
  }

  ~Pool() noexcept {
    if (m_io_thread_running.load(std::memory_order_acquire)) {
      stop_io_coroutine();
    }
    WAL_ASSERT(m_active == nullptr);
    for ([[maybe_unused]] auto& entry : m_buffers) {
      WAL_ASSERT(entry->m_buffer.is_empty());
      WAL_ASSERT(entry->m_state == Entry::State::Free);
    }
  }

  /* Delete copy and move operations - Pool is not copyable or movable */
  Pool(Pool&&) = delete;
  Pool(const Pool&) = delete;
  Pool& operator=(Pool&&) = delete;
  Pool& operator=(const Pool&) = delete;

  /**
  * Acquire a buffer from the pool for writing.
  * Blocks if no buffer is available.
  * 
  * @return Pointer to buffer entry, or nullptr if pool is shutting down
  */
  [[nodiscard]] Entry* acquire_buffer() const noexcept {
    WAL_ASSERT(m_active != nullptr);
    return m_active;
  }

  /** Release the buffer back and signal that we want to write it to storage.
   * Synchronous version - blocks if no buffers are available.
   * 
   * @param[in] entry_ptr The buffer entry to prepare for IO.
   * @param[in] thread_pool Thread pool for executing I/O operations.
   * @param[in] write_callback Callback for writing data (should only do writes, not syncs).
   *
   * @return Pointer to buffer entry, or nullptr if pool is shutting down
   */
  Entry* prepare_buffer_for_io(Entry* entry_ptr, util::Thread_pool& thread_pool, Buffer::Write_callback& write_callback) noexcept {
    WAL_ASSERT(entry_ptr == m_active);
    WAL_ASSERT(!entry_ptr->m_buffer.is_empty());  /* Buffer must have data, but doesn't need to be full */
    WAL_ASSERT(entry_ptr->m_state == Entry::State::In_use);

    const auto hwm = entry_ptr->m_buffer.m_hwm;

    entry_ptr->m_state = Entry::State::Ready_for_io;

    Entry* free_entry_ptr{};

    if (m_free_buffers.dequeue(free_entry_ptr)) {
      /* Enqueue write operation to IO queue */
      Io_operation io_op;
      io_op.m_op = Io_operation::Write_op{.m_entry_ptr = entry_ptr, .m_write_callback = &write_callback};
      [[maybe_unused]] auto ret = m_io_operations.enqueue(io_op);
      WAL_ASSERT(ret);
      
      /* Start IO coroutine if not already running */
      bool expected = false;
      if (m_io_task_running.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        io_coroutine_function(this, &thread_pool).start_on_pool(thread_pool);
      }
      
      m_active = free_entry_ptr;
      WAL_ASSERT(m_active->m_state == Entry::State::Free);
    } else {
      /* Increment counter for synchronous writes */
      m_sync_write_count.fetch_add(1, std::memory_order_relaxed);

      WAL_ASSERT(write_callback && "I/O callback must be set via start_io");

      /* For synchronous writes, use write callback directly */
      auto result = entry_ptr->m_buffer.write_to_store(write_callback);

      if (!result.has_value()) {
        log_fatal("IO task failed");
      }
    }

    m_active->m_state = Entry::State::In_use;
    m_active->m_buffer.initialize(hwm);

    return m_active;
  }

  /**
   * Helper function template to invoke a callable through a void pointer.
   * This allows type-erased storage while maintaining type safety at call site.
   */
  template<typename CallableType>
  static Result<bool> invoke_sync_callable(void* ptr) noexcept {
    return (*static_cast<CallableType*>(ptr))();
  }

  /**
   * Enqueue a sync operation (fsync/fdatasync) to be executed after all pending writes.
   * The sync operation will be serialized with other IO operations through the IO queue.
   * 
   * @tparam CallableType Type of callable that performs the sync operation (must have operator()() -> Result<bool>).
   * @param[in] sync_type Type of sync operation (Fdatasync or Fsync).
   * @param[in] sync_callable Pointer to callable that performs the sync operation.
   * @param[in] thread_pool Thread pool for executing I/O operations.
   */
  template<typename CallableType>
  void enqueue_sync_operation(Sync_type sync_type, CallableType* sync_callable, util::Thread_pool& thread_pool) noexcept {
    if (sync_type == Sync_type::None) {
      return;
    }
    
    Io_operation io_op;
    io_op.m_op = Io_operation::Sync_op{
      .m_sync_type = sync_type,
      .m_sync_callable_ptr = sync_callable,
      .m_sync_invoke = &invoke_sync_callable<CallableType>
    };
    [[maybe_unused]] auto ret = m_io_operations.enqueue(io_op);
    WAL_ASSERT(ret);
    
    /* Start IO coroutine if not already running */
    bool expected = false;
    if (m_io_task_running.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      io_coroutine_function(this, &thread_pool).start_on_pool(thread_pool);
    }
  }

  /* I/O coroutine function - processes all IO operations (writes, syncs, reads) from the queue.
   * All operations are serialized through the single queue since we only configure one IO thread.
   * 
   * @param[in] pool The pool instance to process the IO operations.
   * @param[in] thread_pool The thread pool to execute the IO operations.
   * @return A Task that yields true if the IO operations were successful, false otherwise.
   */
  static Task<void> io_coroutine_function(Pool* pool, [[maybe_unused]] util::Thread_pool* thread_pool) {
    Io_operation io_op;
    
    while (pool->m_io_operations.dequeue(io_op)) {
      if (std::holds_alternative<Io_operation::Write_op>(io_op.m_op)) {
        /* Handle write operation */
        auto& write_op = std::get<Io_operation::Write_op>(io_op.m_op);
        auto entry_ptr = write_op.m_entry_ptr;
        auto write_callback = write_op.m_write_callback;
        
        WAL_ASSERT(write_callback && "I/O callback must be set via start_io");
        
        auto result = entry_ptr->m_buffer.write_to_store(*write_callback);

        if (!result.has_value()) {
          log_fatal("IO task failed");
        }
        
        entry_ptr->m_buffer.initialize(entry_ptr->m_buffer.m_hwm);
        entry_ptr->m_state = Entry::State::Free;

        [[maybe_unused]] auto ret = pool->m_free_buffers.enqueue(entry_ptr);
        WAL_ASSERT(ret);
        
      } else if (std::holds_alternative<Io_operation::Sync_op>(io_op.m_op)) {
        /* Handle sync operation */
        auto& sync_op = std::get<Io_operation::Sync_op>(io_op.m_op);
        
        if (sync_op.m_sync_type != Sync_type::None && sync_op.m_sync_callable_ptr != nullptr && sync_op.m_sync_invoke != nullptr) {
          auto sync_result = sync_op.m_sync_invoke(sync_op.m_sync_callable_ptr);
          if (!sync_result.has_value()) {
            log_fatal("Sync operation failed");
          }
        }
        
      } else if (std::holds_alternative<Io_operation::Read_op>(io_op.m_op)) {
        /* Handle read operation - placeholder for future use */
        /* TODO: Implement read operation */
        std::abort();
      }
    }
    
    /* No more operations pending - mark IO task as not running */
    pool->m_io_task_running.store(false, std::memory_order_release);
    
    co_return;
  }

  /**
   * Stop the background I/O coroutine.
   */
  void stop_io_coroutine() noexcept {
    WAL_ASSERT(m_io_thread_running.load(std::memory_order_acquire));

    m_io_thread_running = false;
  }

  /**
   * Write data to storage using Tasks.
   *
   * @param[in] callback The callback function to write the data. Takes Buffer& and Thread_pool* and returns Task<Result<bool>>.
   * @param[in] pool Thread pool for executing I/O operations.
   * @return result of the write operation.
   */
  template<typename CallbackType>
  [[nodiscard]] Result<bool> write_to_store(CallbackType callback) noexcept {
    Entry* entry_ptr{};

    while (m_ready_for_io_buffers.dequeue(entry_ptr)) {
      WAL_ASSERT(entry_ptr->m_state == Entry::State::Ready_for_io);

      auto result = callback(entry_ptr->m_buffer);

      if (!result.has_value() || !result.value()) {
        return result;
      }

      entry_ptr->m_state = Entry::State::Free;

      [[maybe_unused]] auto ret = m_free_buffers.enqueue(entry_ptr);
      WAL_ASSERT(ret);
    }
    
    return Result<bool>{true};
  }

  void shutdown() noexcept {
    WAL_ASSERT(m_active->m_state == Entry::State::In_use);

    Entry* active = m_active;

    m_active = nullptr;

    if (!active->m_buffer.is_empty()) {
      active->m_state = Entry::State::Ready_for_io;
      [[maybe_unused]] auto ret = m_ready_for_io_buffers.enqueue(active);
      WAL_ASSERT(ret);
    } else {
      active->m_state = Entry::State::Free;
      [[maybe_unused]] auto ret = m_free_buffers.enqueue(active);
      WAL_ASSERT(ret);
    }
  }

  [[nodiscard]] std::string to_string() const noexcept {
    std::stringstream stream;

    stream << "Pool: " << std::endl;
    for (const auto& entry : m_buffers) {
      stream << "  " << entry->to_string() << std::endl;
    }
    return stream.str();
  }
  
  /**
   * Get the number of synchronous writes that occurred due to no free buffer being available.
   * @return The count of synchronous writes.
   */
  [[nodiscard]] std::size_t get_sync_write_count() const noexcept {
    return m_sync_write_count.load(std::memory_order_acquire);
  }

  /* Background I/O coroutine that continuously processes buffers on the thread pool */
  /* Currently active buffer - align to cache line to avoid false sharing with I/O threads */
  alignas(kCLS) Entry* m_active{nullptr};

  /* Storage for all buffer entries - manages lifetime */
  std::vector<std::unique_ptr<Entry>> m_buffers;

  /* Pool of available buffers */
  util::Bounded_queue<Entry*> m_free_buffers;
  
  /* Queue of buffers waiting for I/O */
  util::Bounded_queue<Entry*> m_ready_for_io_buffers;

  /* I/O process state - align to cache line to avoid false sharing */
  alignas(kCLS) std::atomic<bool> m_io_thread_running{false};

  /* Synchronous write callback for inline I/O when buffers exhausted */
  Buffer::Write_callback m_sync_write_callback;

  /* Unified queue for all IO operations (writes, syncs, reads) - serialized execution */
  util::Bounded_queue<Io_operation> m_io_operations;

  /* Flag to track if IO coroutine is running */
  alignas(kCLS) std::atomic<bool> m_io_task_running{false};
  
  /* Counter for synchronous writes (when no free buffer available) */
  alignas(kCLS) std::atomic<std::size_t> m_sync_write_count{0};
};
// NOLINTEND(clang-analyzer-optin.performance.Padding)

} // namespace wal

