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
  struct Entry;
  
  /**
   * IO operation types that can be queued for serialized execution.
   * All operations are serialized through a single queue since we only configure one IO thread.
   */
  struct Io_operation {
    /* Write operation - buffer entry to write */
    struct Write_op {
      Entry* entry_ptr;
      Buffer::Write_callback* write_callback;
    };
    
    /* Sync operation - fsync or fdatasync */
    struct Sync_op {
      Sync_type sync_type;
      std::function<Result<bool>(Sync_type)>* sync_callback;
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
  * @param[in] pool_size Number of buffers in the pool (must be a power of 2)
  * @param[in] config Configuration for each buffer
  * @param[in] hwm Starting LSN for the first buffer
  */
  Pool(std::size_t pool_size, const Config& config, lsn_t hwm)
    : m_free_buffers(pool_size),
      m_ready_for_io_buffers(pool_size),
      m_io_thread_running(false),
      m_io_operations(pool_size * 2) {

    if (!std::has_single_bit(pool_size)) {
      throw std::invalid_argument("Pool size must be a power of 2");
    }

    m_buffers.reserve(pool_size);

    for (std::size_t i = 0; i < pool_size; ++i) {
      m_buffers.emplace_back(std::make_unique<Entry>(Buffer(config), Entry::State::Free));
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

   @return Pointer to buffer entry, or nullptr if pool is shutting down
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
      io_op.m_op = Io_operation::Write_op{.entry_ptr = entry_ptr, .write_callback = &write_callback};
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
      std::println("No free buffer available, doing synchronous write");

      WAL_ASSERT(write_callback && "I/O callback must be set via start_io");

      /* For synchronous writes, use write-only callback */
      auto write_only_callback = [&write_callback](std::span<struct iovec> span, Sync_type) -> Result<lsn_t> {
        return write_callback(span, Sync_type::None);
      };
      
      auto result = entry_ptr->m_buffer.write_to_store(write_only_callback);

      if (!result.has_value()) {
        log_fatal("IO task failed");
      }
    }

    m_active->m_state = Entry::State::In_use;
    m_active->m_buffer.initialize(hwm);

    return m_active;
  }

  /**
   * Enqueue a sync operation (fsync/fdatasync) to be executed after all pending writes.
   * The sync operation will be serialized with other IO operations through the IO queue.
   * 
   * @param[in] sync_type Type of sync operation (Fdatasync or Fsync).
   * @param[in] sync_callback Callback that performs the sync operation.
   * @param[in] thread_pool Thread pool for executing I/O operations.
   */
  void enqueue_sync_operation(Sync_type sync_type, std::function<Result<bool>(Sync_type)>& sync_callback, util::Thread_pool& thread_pool) noexcept {
    if (sync_type == Sync_type::None) {
      return;
    }
    
    Io_operation io_op;
    io_op.m_op = Io_operation::Sync_op{.sync_type = sync_type, .sync_callback = &sync_callback};
    [[maybe_unused]] auto ret = m_io_operations.enqueue(io_op);
    WAL_ASSERT(ret);
    
    /* Start IO coroutine if not already running */
    bool expected = false;
    if (m_io_task_running.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      io_coroutine_function(this, &thread_pool).start_on_pool(thread_pool);
    }
  }

  /* Delete copy and move operations - Pool is not copyable or movable */
  Pool(const Pool&) = delete;
  Pool& operator=(const Pool&) = delete;
  Pool(Pool&&) = delete;
  Pool& operator=(Pool&&) = delete;

  /* I/O coroutine function - processes all IO operations (writes, syncs, reads) from the queue.
   * All operations are serialized through the single queue since we only configure one IO thread.
   */
  static Task<void> io_coroutine_function(Pool* pool, [[maybe_unused]] util::Thread_pool* thread_pool) {
    Io_operation io_op;
    
    while (pool->m_io_operations.dequeue(io_op)) {
      if (std::holds_alternative<Io_operation::Write_op>(io_op.m_op)) {
        /* Handle write operation */
        auto& write_op = std::get<Io_operation::Write_op>(io_op.m_op);
        Entry* entry_ptr = write_op.entry_ptr;
        Buffer::Write_callback* write_callback = write_op.write_callback;
        
        WAL_ASSERT(write_callback && "I/O callback must be set via start_io");
        
        /* Use write-only callback (no syncs in write path) */
        auto write_only_callback = [write_callback](std::span<struct iovec> span, Sync_type) -> Result<lsn_t> {
          return (*write_callback)(span, Sync_type::None);
        };
        
        auto result = entry_ptr->m_buffer.write_to_store(write_only_callback);
        
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
        
        if ((sync_op.sync_callback != nullptr) && sync_op.sync_type != Sync_type::None) {
          auto sync_result = (*sync_op.sync_callback)(sync_op.sync_type);
          if (!sync_result.has_value()) {
            log_fatal("Sync operation failed");
          }
        }
        
      } else if (std::holds_alternative<Io_operation::Read_op>(io_op.m_op)) {
        /* Handle read operation - placeholder for future use */
        /* TODO: Implement read operation */
      }
    }
    
    /* No more operations pending - mark IO task as not running */
    pool->m_io_task_running.store(false, std::memory_order_release);
    
    co_return;
  }

  /**
   * Start the background I/O coroutine that continuously processes buffers.
   * The coroutine runs on the thread pool and processes buffers from m_ready_for_io_buffers.
   * 
   * NOTE: This method is deprecated. I/O orchestration is now controlled by Log.
   * The Log sets m_io_thread_running directly.
   */
  template<typename CallbackType>
  void start_io_coroutine(CallbackType callback) noexcept {
    WAL_ASSERT(!m_io_thread_running.load(std::memory_order_acquire));
    /* I/O callback is now stored in Log, not Pool */
    m_io_thread_running = true;
    (void)callback; /* Ignored - callback is stored in Log */
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
  std::function<Result<std::size_t>(std::span<struct iovec>, wal::Sync_type)> m_sync_write_callback;

  /* Unified queue for all IO operations (writes, syncs, reads) - serialized execution */
  util::Bounded_queue<Io_operation> m_io_operations;

  /* Flag to track if IO coroutine is running */
  alignas(kCLS) std::atomic<bool> m_io_task_running{false};
};
// NOLINTEND(clang-analyzer-optin.performance.Padding)

} // namespace wal

