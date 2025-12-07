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
 * - Process mailboxes for work distribution
 * - Process scheduling
 * - Scheduler for managing processes
 */

namespace util {

using Pid = std::size_t;

/**
 * Helper to process payload message with optional force_flush flag.
 * Optimized to minimize hot path overhead.
 */
template<typename PayloadType, typename MessageProcessor, typename MessageEnvelope>
inline void process_payload_message(MessageProcessor& process_msg, const PayloadType& payload, const MessageEnvelope& msg) noexcept {
  /* Extract send time if available */
  std::chrono::steady_clock::time_point send_time{};

  if constexpr (requires { msg.m_send_time; }) {
    send_time = msg.m_send_time;
  }
  
  if constexpr (requires { process_msg(payload, bool{}, std::chrono::steady_clock::time_point{}); }) {
    /* Processor supports three-parameter version (payload, force_flush, send_time) */
    bool force_flush{};

    if constexpr (requires { msg.m_force_flush; }) {
      force_flush = msg.m_force_flush;
    }

    process_msg(payload, force_flush, send_time);

  } else if constexpr (requires { process_msg(payload, bool{}); }) {
    /* Processor supports two-parameter version - read force_flush only if envelope supports it */
    bool force_flush{};

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

/* Core messages for the actor model. You will need to define your own message types
 * for your specific use case. @see wal/service.h for an example. */
struct Poison_pill {};

/**
 * Message envelope that can hold payload or poison pill
 */
template<typename PayloadType>
struct Message_envelope {
  Pid m_sender;
  std::variant<PayloadType, Poison_pill> m_payload;
};

/**
 * Per-process mailbox with tracking for efficient consumer scanning
 */
template<typename PayloadType>
struct Process_mailbox {
  explicit Process_mailbox(std::size_t capacity) : m_queue(capacity), m_has_messages(false) {}

  using Queue_type = Spsc_bounded_queue<Process<PayloadType>*>;

  Queue_type& queue() { return m_queue; }
  const Queue_type& queue() const { return m_queue; }
  bool has_messages() const { return m_has_messages.load(std::memory_order_acquire); }
  void set_has_messages(bool value) { m_has_messages.store(value, std::memory_order_release); }

  Queue_type m_queue;

  /* Atomic flag to indicate if this mailbox has messages (reduces scanning overhead) */
  alignas(64) std::atomic<bool> m_has_messages{false};
};

/**
 * Global array of per-process mailboxes (indexed by process ID)
 * Also includes a notification queue to track which mailboxes have messages
 */
template<typename PayloadType>
struct Process_mailboxes {
  using Mailbox_type = Process_mailbox<PayloadType>;
  
  void initialize(std::size_t n_processes, std::size_t capacity, std::size_t n_producers = 0) {
    m_mailboxes.clear();

    for (std::size_t i = 0; i < n_processes; ++i) {
      m_mailboxes.push_back(std::make_unique<Mailbox_type>(capacity));
    }

    /* Notification queue capacity: scale dynamically with number of producers
     * For high producer counts, we need more capacity to handle burst notifications
     * Base capacity on processes, but scale up significantly for many producers
     * Use power of 2 for efficient masking */
    std::size_t notify_capacity;
    if (n_producers > 0) {
      /* Scale with producers: allow for bursts where many producers schedule simultaneously
       * For 8K producers, we need significant capacity to handle all notifications
       * Cap at reasonable limit to avoid excessive memory usage */
      notify_capacity = std::min<std::size_t>(131072, std::max<std::size_t>(n_processes * 16, n_producers / 2));
    } else {
      /* Fallback: scale with processes only */
      notify_capacity = std::max<std::size_t>(n_processes * 8, 1024);
    }
    notify_capacity = std::bit_ceil(notify_capacity);
    m_notify_queue = std::make_unique<Spsc_bounded_queue<std::size_t>>(notify_capacity);
  }

  Mailbox_type* get_for_process(std::size_t process_id) {
    if (process_id < m_mailboxes.size()) {
      return m_mailboxes[process_id].get();
    }
    return nullptr;
  }

  std::size_t size() const { return m_mailboxes.size(); }

  /* Notify consumer that a mailbox has messages (level-triggered)
   * Only notifies when transitioning from empty (consumer finished) to non-empty (new messages)
   * Returns true if notification was sent, false if already notified */
  bool notify_mailbox_has_messages(std::size_t process_id) {
    if (process_id >= m_mailboxes.size()) {
      return false;
    }
    
    auto mbox = m_mailboxes[process_id].get();

    /* Level-triggered: only notify when flag transitions from false (consumer finished) to true (new messages)
     * This ensures we only notify when consumer has read all entries from the SPSC queue */
    bool expected{};

    if (mbox->m_has_messages.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
      /* Try to enqueue notification (non-blocking - if queue is full, retry a few times)
       * If still full, the consumer will find it via the m_has_messages flag during scanning */
      bool notified{};
      for (int retry = 0; retry < 3 && !notified; ++retry) {
        if (m_notify_queue->enqueue(process_id)) {
          notified = true;
        } else if (retry < 2) {
          /* Brief pause before retry to let consumer drain */
          cpu_pause();
        }
      }
      /* If notification queue is full after retries, consumer will find via flag during scan */
      return true;
    }

    /* Already notified (consumer hasn't finished processing yet) */
    return false;
  }

  std::vector<std::unique_ptr<Mailbox_type>> m_mailboxes;
  std::unique_ptr<Spsc_bounded_queue<std::size_t>> m_notify_queue;
};

/**
 * Process - aligned to cache line boundary to avoid false sharing between processes
 * This is critical when multiple processes access different processes
 */
template<typename PayloadType>
struct alignas(64) Process {
  explicit Process(Pid p, std::size_t mailbox_size)
    : m_pid(p), m_mailbox(mailbox_size) {}

  /* Self-schedule into a process mailbox for better work distribution
   * Uses hash-based distribution instead of current process ID to balance load
   * Also notifies consumer via notification queue
   * Returns true if successfully scheduled, false if already scheduled or needs retry */
  bool schedule_self(Process_mailboxes<PayloadType>* mailboxes, std::size_t n_processes) {
    if (n_processes == 0){
      return false;
    }
    
    /* Hash-based distribution: use process ID to consistently map to a process mailbox
     * This distributes work evenly across processes regardless of where producer is running
     * Use a better hash function (multiply by prime) to improve distribution quality
     * This reduces clustering when many producers hash to the same process */
    std::size_t target_process_id = (m_pid * 2654435761ULL) % n_processes;

    auto process_mbox = mailboxes->get_for_process(target_process_id);

    if (process_mbox == nullptr) {
      return false;
    }

    /* Only enqueue if not already in this process's mailbox */
    if (!m_in_process_mailbox.exchange(true, std::memory_order_acq_rel)) {
      /* Try with limited yields for back-pressure */
      constexpr int max_yields = 10;  /* More persistent but still limited */

      /* Use CPU pause for retries - avoid syscalls */
      for (int i = 0; i < max_yields; ++i) {
        if (process_mbox->m_queue.enqueue(this)) {
          /* Notify consumer that this mailbox has messages (only first time) */
          mailboxes->notify_mailbox_has_messages(target_process_id);
          return true;
        }
        /* Use minimal CPU pause to avoid syscalls */
        cpu_pause();
      }

      /* Failed after retries - reset flag and let caller co_await */
      m_in_process_mailbox.store(false, std::memory_order_release);
      return false;
    }
    /* Already scheduled */
    return false;
  }

  Pid m_pid;
  Spsc_bounded_queue<Message_envelope<PayloadType>> m_mailbox;
  /* Align atomic flag to separate cache line to avoid false sharing */
  alignas(64) std::atomic<bool> m_in_process_mailbox{false};
};

/**
 * Base scheduler structure - can be extended by specific tests
 */
template<typename PayloadType>
struct Scheduler_base {
  using Process_type = Process<PayloadType>;
  using Mailboxes_type = Process_mailboxes<PayloadType>;
  
  Process_type* get_process(Pid pid) noexcept{
    if (pid >= m_processes.size()) {
      return nullptr;
    }

    return m_processes[pid].get();
  }

  Pid spawn(std::size_t mailbox_size) noexcept {
    auto pid = m_processes.size();
    auto proc = std::make_unique<Process_type>(pid, mailbox_size);

    m_processes.push_back(std::move(proc));

    return pid;
  }

  Thread_pool* m_producer_pool;
  Thread_pool* m_consumer_pool;
  Mailboxes_type* m_mailboxes{nullptr};
  std::vector<std::unique_ptr<Process_type>> m_processes;
};

}  /* namespace util */
