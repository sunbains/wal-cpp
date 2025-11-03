#pragma once

#include <thread>
#include <atomic>
#include <vector>
#include <memory>
#include <variant>
#include <bit>
#include <chrono>
#include <print>
#include <array>
#include <type_traits>

#include "util/bounded_channel.h"
#include "util/spsc.h"
#include "util/thread_pool.h"
#include "util/util.h"
#include "coro/task.h"

/**
 * Common Actor Model Infrastructure
 * 
 * This header provides the shared actor model components used by both
 * test_actor_model.cc and test_log_service_actor.cc:
 * - Thread mailboxes for work distribution
 * - Process scheduling
 * - Scheduler for managing processes
 */

using Pid = std::size_t;

/**
 * Helper to process payload message with optional force_flush flag.
 * Optimized to minimize hot path overhead.
 */
template<typename PayloadType, typename MessageProcessor, typename MessageEnvelope>
inline void process_payload_message(
    MessageProcessor& process_msg,
    const PayloadType& payload,
    const MessageEnvelope& msg) noexcept {
  /* Extract send time if available */
  std::chrono::steady_clock::time_point send_time{};
  if constexpr (requires { msg.m_send_time; }) {
    send_time = msg.m_send_time;
  }
  
  if constexpr (requires { process_msg(payload, bool{}, std::chrono::steady_clock::time_point{}); }) {
    /* Processor supports three-parameter version (payload, force_flush, send_time) */
    bool force_flush = false;
    if constexpr (requires { msg.m_force_flush; }) {
      force_flush = msg.m_force_flush;
    }
    process_msg(payload, force_flush, send_time);
  } else if constexpr (requires { process_msg(payload, bool{}); }) {
    /* Processor supports two-parameter version - read force_flush only if envelope supports it */
    bool force_flush = false;
    if constexpr (requires { msg.m_force_flush; }) {
      force_flush = msg.m_force_flush;
    }
    process_msg(payload, force_flush);
  } else {
    /* Processor only supports single-parameter version */
    process_msg(payload);
    /* Check force_flush only if both processor and envelope support it */
    if constexpr (requires { process_msg.flush_batch(); } && requires { msg.m_force_flush; }) {
      if (msg.m_force_flush) [[unlikely]] {
        process_msg.flush_batch();
      }
    }
  }
}

// Forward declaration
template<typename PayloadType>
struct Process;
struct Poison_pill {};
struct Fdatasync {};
struct Fsync {};

/**
 * Message envelope that can hold payload or poison pill
 */
template<typename PayloadType>
struct Message_envelope {
  Pid m_sender;
  std::variant<PayloadType, Poison_pill> m_payload;
};

/**
 * Per-thread mailbox with tracking for efficient consumer scanning
 */
template<typename PayloadType>
struct Thread_mailbox {
  explicit Thread_mailbox(std::size_t capacity)
    : m_queue(capacity), m_has_messages(false) {}

  util::Bounded_queue<Process<PayloadType>*> m_queue;
  /* Atomic flag to indicate if this mailbox has messages (reduces scanning overhead) */
  alignas(64) std::atomic<bool> m_has_messages{false};
};

/**
 * Global array of per-thread mailboxes (indexed by thread pool worker ID)
 * Also includes a notification queue to track which mailboxes have messages
 */
template<typename PayloadType>
struct Thread_mailboxes {
  using Mailbox_type = Thread_mailbox<PayloadType>;
  
  void initialize(std::size_t num_threads, std::size_t capacity, std::size_t num_producers = 0) {
    m_mailboxes.clear();

    for (std::size_t i = 0; i < num_threads; ++i) {
      m_mailboxes.push_back(std::make_unique<Mailbox_type>(capacity));
    }

    /* Notification queue capacity: scale dynamically with number of producers
     * For high producer counts, we need more capacity to handle burst notifications
     * Base capacity on threads, but scale up significantly for many producers
     * Use power of 2 for efficient masking */
    std::size_t notify_capacity;
    if (num_producers > 0) {
      /* Scale with producers: allow for bursts where many producers schedule simultaneously
       * For 8K producers, we need significant capacity to handle all notifications
       * Cap at reasonable limit to avoid excessive memory usage */
      notify_capacity = std::min<std::size_t>(131072, std::max<std::size_t>(num_threads * 16, num_producers / 2));
    } else {
      /* Fallback: scale with threads only */
      notify_capacity = std::max<std::size_t>(num_threads * 8, 1024);
    }
    notify_capacity = std::bit_ceil(notify_capacity);
    m_notify_queue = std::make_unique<util::Spsc_bounded_queue<std::size_t>>(notify_capacity);
  }

  Mailbox_type* get_for_thread(std::size_t thread_id) {
    if (thread_id < m_mailboxes.size()) {
      return m_mailboxes[thread_id].get();
    }
    return nullptr;
  }

  std::size_t size() const { return m_mailboxes.size(); }

  /* Notify consumer that a mailbox has messages (level-triggered)
   * Only notifies when transitioning from empty (consumer finished) to non-empty (new messages)
   * Returns true if notification was sent, false if already notified */
  bool notify_mailbox_has_messages(std::size_t thread_id) {
    if (thread_id >= m_mailboxes.size()) {
      return false;
    }
    
    auto* mbox = m_mailboxes[thread_id].get();

    /* Level-triggered: only notify when flag transitions from false (consumer finished) to true (new messages)
     * This ensures we only notify when consumer has read all entries from the SPSC queue */
    bool expected = false;

    if (mbox->m_has_messages.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      /* Try to enqueue notification (non-blocking - if queue is full, retry a few times)
       * If still full, the consumer will find it via the m_has_messages flag during scanning */
      bool notified = false;
      for (int retry = 0; retry < 3 && !notified; ++retry) {
        if (m_notify_queue->enqueue(thread_id)) {
          notified = true;
        } else if (retry < 2) {
          /* Brief pause before retry to let consumer drain */
          util::cpu_pause();
        }
      }
      /* If notification queue is full after retries, consumer will find via flag during scan */
      return true;
    }

    return false;  /* Already notified (consumer hasn't finished processing yet) */
  }

  std::vector<std::unique_ptr<Mailbox_type>> m_mailboxes;
  std::unique_ptr<util::Spsc_bounded_queue<std::size_t>> m_notify_queue;
};

/**
 * Process - aligned to cache line boundary to avoid false sharing between processes
 * This is critical when multiple threads access different processes
 */
template<typename PayloadType>
struct alignas(64) Process {
  explicit Process(Pid p, std::size_t mailbox_size)
    : m_pid(p), m_mailbox(mailbox_size) {}

  /* Self-schedule into a thread mailbox for better work distribution
   * Uses hash-based distribution instead of current thread ID to balance load
   * Also notifies consumer via notification queue
   * Returns true if successfully scheduled, false if already scheduled or needs retry */
  bool schedule_self(Thread_mailboxes<PayloadType>* mailboxes, std::size_t num_threads) {
    if (num_threads == 0){
      return false;
    }
    
    /* Hash-based distribution: use process ID to consistently map to a thread mailbox
     * This distributes work evenly across threads regardless of where producer is running
     * Use a better hash function (multiply by prime) to improve distribution quality
     * This reduces clustering when many producers hash to the same thread */
    std::size_t target_thread_id = (m_pid * 2654435761ULL) % num_threads;

    auto thread_mbox = mailboxes->get_for_thread(target_thread_id);

    if (thread_mbox == nullptr) {
      return false;
    }

    /* Only enqueue if not already in this thread's mailbox */
    if (!m_in_thread_mailbox.exchange(true, std::memory_order_acq_rel)) {
      /* Try with limited yields for back-pressure */
      constexpr int max_yields = 10;  /* More persistent but still limited */

      /* Use CPU pause for retries - avoid syscalls */
      for (int i = 0; i < max_yields; ++i) {
        if (thread_mbox->m_queue.enqueue(this)) {
          /* Notify consumer that this mailbox has messages (only first time) */
          mailboxes->notify_mailbox_has_messages(target_thread_id);
          return true;
        }
        /* Use minimal CPU pause to avoid syscalls */
        util::cpu_pause();
      }

      /* Failed after retries - reset flag and let caller co_await */
      m_in_thread_mailbox.store(false, std::memory_order_release);
      return false;
    }
    return false;  /* Already scheduled */
  }

  Pid m_pid;
  util::Spsc_bounded_queue<Message_envelope<PayloadType>> m_mailbox;
  /* Align atomic flag to separate cache line to avoid false sharing */
  alignas(64) std::atomic<bool> m_in_thread_mailbox{false};
};

/**
 * Base scheduler structure - can be extended by specific tests
 */
template<typename PayloadType>
struct Scheduler_base {
  using Process_type = Process<PayloadType>;
  using Mailboxes_type = Thread_mailboxes<PayloadType>;
  
  Process_type* get_process(Pid pid) {
    if (pid >= m_processes.size()) {
      return nullptr;
    }

    return m_processes[pid].get();
  }

  Pid spawn(std::size_t mailbox_size) {
    auto pid = m_processes.size();
    auto proc = std::make_unique<Process_type>(pid, mailbox_size);
    m_processes.push_back(std::move(proc));
    return pid;
  }

  util::Thread_pool* m_producer_pool;
  util::Thread_pool* m_consumer_pool;
  Mailboxes_type* m_mailboxes{nullptr};
  std::vector<std::unique_ptr<Process_type>> m_processes;
};

/**
 * Common producer context - used by both tests
 */
template<typename PayloadType, typename SchedulerType>
struct Producer_context_base {
  void send(const PayloadType& payload) {
    Message_envelope<PayloadType> env{
      .m_sender = m_self,
      .m_payload = payload
    };

    if (m_proc->m_mailbox.enqueue(std::move(env))) [[likely]] {
      return;
    }

    /* Slow path: queue full - just spin with CPU pause */
    while (!m_proc->m_mailbox.enqueue(std::move(env))) {
      util::cpu_pause();
    }

    /* Always schedule self after writing (consumer will drain when ready) */
    m_proc->schedule_self(m_sched->m_mailboxes, m_sched->m_mailboxes->size());
  }

  Pid m_self;
  Process<PayloadType>* m_proc;
  SchedulerType* m_sched;
  /** Consumer's mailbox for poison pills */
  Process<PayloadType>* m_consumer_proc;
  std::size_t m_fdatasync_interval{0};
};

/**
 * Common test setup - creates thread pool, mailboxes, and scheduler
 */
template<typename PayloadType, typename SchedulerType>
struct Actor_test_setup {
  Actor_test_setup(std::size_t num_producers)
    : m_producer_pool(create_producer_pool_config(num_producers)),
      m_consumer_pool(create_consumer_pool_config()),
      m_io_pool(create_io_pool_config()) {
    std::size_t hw_threads = std::thread::hardware_concurrency();

    if (hw_threads == 0) {
      hw_threads = 4;
    }

    std::size_t thread_mailbox_capacity = std::max<std::size_t>(256, num_producers / m_producer_pool.m_workers.size() * 2);

    if (!std::has_single_bit(thread_mailbox_capacity)) {
      thread_mailbox_capacity = std::bit_ceil(thread_mailbox_capacity);
    }
    m_thread_mailboxes.initialize(m_producer_pool.m_workers.size(), thread_mailbox_capacity);

    m_sched.m_producer_pool = &m_producer_pool;
    m_sched.m_consumer_pool = &m_consumer_pool;
    m_sched.m_mailboxes = &m_thread_mailboxes;

    /* Pre-warm coroutine memory pool */
   {
      std::size_t needed_coro_frames = m_producer_pool.m_workers.size() + m_consumer_pool.m_workers.size() + m_io_pool.m_workers.size() + 1;
      std::size_t arenas_needed = (needed_coro_frames + 2047) / 2048;

      auto& coro_pool = util::get_coroutine_pool();

      for (std::size_t i = 1; i < arenas_needed; ++i) {
        coro_pool.grow();
      }
    }
  }

private:
  static util::Thread_pool::Config create_producer_pool_config(std::size_t num_producers) {
    util::Thread_pool::Config producer_pool_config;

    std::size_t hw_threads = std::thread::hardware_concurrency();

    if (hw_threads == 0) {
      hw_threads = 4;
    }

    producer_pool_config.m_num_threads = std::max<std::size_t>(1, hw_threads / 2);
    producer_pool_config.m_queue_capacity = std::min<std::size_t>(131072, std::max<std::size_t>(4096, num_producers * 4));

    if (!std::has_single_bit(producer_pool_config.m_queue_capacity)) {
      producer_pool_config.m_queue_capacity = std::bit_ceil(producer_pool_config.m_queue_capacity);
    }
    return producer_pool_config;
  }

  static util::Thread_pool::Config create_consumer_pool_config() {
    util::Thread_pool::Config consumer_pool_config;

    std::size_t hw_threads = std::thread::hardware_concurrency();

    if (hw_threads == 0) {
      hw_threads = 4;
    }

    /* Dedicated pool for consumer coroutine */
    consumer_pool_config.m_num_threads = 2;
    consumer_pool_config.m_queue_capacity = 1024;

    if (!std::has_single_bit(consumer_pool_config.m_queue_capacity)) {
      consumer_pool_config.m_queue_capacity = std::bit_ceil(consumer_pool_config.m_queue_capacity);
    }

    return consumer_pool_config;
  }

  static util::Thread_pool::Config create_io_pool_config() {
    util::Thread_pool::Config io_pool_config;

    std::size_t hw_threads = std::thread::hardware_concurrency();

    if (hw_threads == 0) {
      hw_threads = 4;
    }

    /* Dedicated pool for I/O operations - match test_log_io_simple.cc configuration */
    io_pool_config.m_num_threads = std::max<std::size_t>(1, hw_threads / 2);
    io_pool_config.m_queue_capacity = 32768;

    if (!std::has_single_bit(io_pool_config.m_queue_capacity)) {
      io_pool_config.m_queue_capacity = std::bit_ceil(io_pool_config.m_queue_capacity);
    }

    return io_pool_config;
  }

public:
  Thread_mailboxes<PayloadType> m_thread_mailboxes;
  util::Thread_pool m_producer_pool;
  util::Thread_pool m_consumer_pool;
  util::Thread_pool m_io_pool;
  SchedulerType m_sched;
};
