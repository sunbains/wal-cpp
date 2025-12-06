#pragma once

#include <memory>
#include <atomic>
#include <thread>
#include <functional>
#include <vector>

#include "coro/task.h"
#include "util/bounded_channel.h"
#include "util/logger.h"
#include "util/thread_pool.h"
#include "util/util.h"
#include "wal/buffer.h"

extern util::Logger<util::MT_logger_writer> g_logger;

namespace wal {

using Config = Buffer::Config;

/**
 * Manages a pool of Buffer instances for concurrent I/O operations.
 * 
 * When a buffer becomes full, it's moved to an I/O queue for async writing,
 * and operations continue on the next available buffer from the pool.
 */
struct Pool {
  struct Entry;

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
    : m_buffers(),
      m_free_buffers(pool_size),
      m_ready_for_io_buffers(pool_size),
      m_io_thread_running(false) {

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
  [[nodiscard]] Entry* acquire_buffer() noexcept {
    WAL_ASSERT(m_active != nullptr);
    return m_active;
  }

  /** Release the buffer back and signal that we want to write it to storage.
   * Synchronous version - blocks if no buffers are available.
   * 
   * @param buffer The buffer to prepare for IO.
   */
  Entry* prepare_buffer_for_io(Entry* entry_ptr, util::Thread_pool& thread_pool, Buffer::Write_callback& write_callback) noexcept {
    WAL_ASSERT(entry_ptr == m_active);
    WAL_ASSERT(!entry_ptr->m_buffer.is_empty());  /* Buffer must have data, but doesn't need to be full */
    WAL_ASSERT(entry_ptr->m_state == Entry::State::In_use);

    const auto hwm = entry_ptr->m_buffer.m_hwm;

    entry_ptr->m_state = Entry::State::Ready_for_io;

    Entry* free_entry_ptr{};

    if (m_free_buffers.dequeue(free_entry_ptr)) {
      post_io_task_for_buffer(entry_ptr, thread_pool, write_callback);
      m_active = free_entry_ptr;
      WAL_ASSERT(m_active->m_state == Entry::State::Free);
    } else {
      std::println("No free buffer available, doing synchronous write");

      WAL_ASSERT(write_callback && "I/O callback must be set via start_io");

      auto result = entry_ptr->m_buffer.write_to_store(write_callback);

      if (!result.has_value()) {
        log_fatal("IO task failed");
      }
    }

    m_active->m_state = Entry::State::In_use;
    m_active->m_buffer.initialize(hwm);

    return m_active;
  }

  /* I/O coroutine function - takes * and Entry* as parameters.
   * Log is the coordinator and controls orchestration, so it provides the I/O callback.
   * This ensures the parameters are stored in the coroutine frame, not in a closure on the stack.
   */
  static Task<void> io_coroutine_function(Buffer::Write_callback write_callback, Pool* pool, Entry* entry_ptr) {
    WAL_ASSERT(write_callback && "I/O callback must be set via start_io");
    
    auto result = entry_ptr->m_buffer.write_to_store(write_callback);
    
    if (!result.has_value()) {
      log_fatal("IO task failed");
    }
    
    entry_ptr->m_buffer.initialize(entry_ptr->m_buffer.m_hwm);
    entry_ptr->m_state = Entry::State::Free;

    [[maybe_unused]] auto ret = pool->m_free_buffers.enqueue(entry_ptr);
    WAL_ASSERT(ret);

    co_return;
  }

  void post_io_task_for_buffer(Entry* entry_ptr, util::Thread_pool& thread_pool, Buffer::Write_callback write_callback) noexcept {
    WAL_ASSERT(!entry_ptr->m_buffer.is_empty());  /* Buffer must have data, but doesn't need to be full */
    WAL_ASSERT(entry_ptr->m_state == Entry::State::Ready_for_io);
    WAL_ASSERT(write_callback && "I/O callback must be set via start_io");

    /* Post a task to the I/O pool that will:
     * 1. Write the buffer to storage using write_callback
     * 2. Put the buffer back on the free list
     * 3. Notify any waiters
     * 
     * Use a static function instead of lambda to avoid closure storage issues.
     * The parameters (log, pool, entry_ptr) are passed directly and stored in the coroutine frame,
     * not in a closure on the stack that could be overwritten.
     */
    io_coroutine_function(write_callback, this, entry_ptr).start_on_pool(thread_pool);
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

    return Result<bool>(true);
  }

  void shutdown() noexcept {
    WAL_ASSERT(m_active->m_state == Entry::State::In_use);

    auto active = m_active;

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
    std::stringstream ss;

    ss << "Pool: " << std::endl;
    for (auto& entry : m_buffers) {
      ss << "  " << entry->to_string() << std::endl;
    }
    return ss.str();
  }

  /* Background I/O coroutine that continuously processes buffers on the thread pool */
  /* Currently active buffer - align to cache line to avoid false sharing with I/O threads */
  alignas(64) Entry* m_active{nullptr};

  /* Storage for all buffer entries - manages lifetime */
  std::vector<std::unique_ptr<Entry>> m_buffers;

  /* Pool of available buffers */
  util::Bounded_queue<Entry*> m_free_buffers;
  
  /* Queue of buffers waiting for I/O */
  util::Bounded_queue<Entry*> m_ready_for_io_buffers;

  /* I/O process state - align to cache line to avoid false sharing */
  alignas(64) std::atomic<bool> m_io_thread_running{false};

  /* Synchronous write callback for inline I/O when buffers exhausted */
  std::function<Result<std::size_t>(std::span<struct iovec>, wal::Sync_type)> m_sync_write_callback;
};

} // namespace wal

