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
  
  void initialize(std::size_t n_processes, std::size_t capacity, std::size_t n_producers = 0, std::size_t n_workers = 0) {
    m_mailboxes.clear();
    m_pending_flags.reset();
    m_pending_flags_size = n_processes;
    m_active_queues.clear();
    m_worker_index.clear();
    m_active_count.store(0, std::memory_order_relaxed);
    m_track_active_count = (n_processes <= 1024);
    m_direct_fast_path = (n_processes <= 8);

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
    /* Build per-worker queues to reduce contention; default to mailboxes if workers not provided */
    const std::size_t workers = (n_workers > 0) ? n_workers : n_processes;
    m_active_queues.reserve(workers);
    for (std::size_t i = 0; i < workers; ++i) {
      m_active_queues.push_back(std::make_unique<Bounded_queue<std::size_t>>(notify_capacity));
    }
    m_pending_flags = std::make_unique<std::atomic<bool>[]>(m_pending_flags_size);
    for (std::size_t i = 0; i < m_pending_flags_size; ++i) {
      m_pending_flags[i].store(false, std::memory_order_relaxed);
    }
    m_worker_index.resize(n_processes);
    for (std::size_t pid = 0; pid < n_processes; ++pid) {
      m_worker_index[pid] = (workers == 0) ? 0 : (pid % workers);
    }
  }

  Mailbox_type* get_for_process(std::size_t process_id) {
    if (process_id < m_mailboxes.size()) {
      return m_mailboxes[process_id].get();
    }
    return nullptr;
  }

  std::size_t size() const { return m_mailboxes.size(); }

  /* Notify consumer that a mailbox has messages (level-triggered + pending dedupe)
   * Enqueues process_id onto its home active queue when transitioning pending=false->true */
  bool notify_mailbox_has_messages(std::size_t process_id, std::size_t n_workers) {
    if (process_id >= m_mailboxes.size() || m_active_queues.empty()) {
      return false;
    }
    if (m_direct_fast_path) {
      m_mailboxes[process_id]->m_has_messages.store(true, std::memory_order_release);
      return true;
    }
    
    auto mbox = m_mailboxes[process_id].get();

    /* Only notify if we successfully transition pending from false to true */
    bool expected_pending = false;
    if (!m_pending_flags[process_id].compare_exchange_strong(expected_pending, true, std::memory_order_acq_rel)) {
      return false;  /* already pending */
    }
    m_active_count.fetch_add(1, std::memory_order_acq_rel);

    mbox->m_has_messages.store(true, std::memory_order_release);

    const std::size_t qid = (process_id < m_worker_index.size()) ? m_worker_index[process_id]
                             : ((n_workers == 0) ? 0 : (process_id % n_workers));
    auto& act_q = *m_active_queues[qid];
    [[maybe_unused]] auto ok = act_q.enqueue(process_id);
    (void)ok;
    return true;
  }

  void clear_pending_flag(std::size_t process_id) {
    if (process_id < m_pending_flags_size && m_pending_flags) {
      bool was_pending = m_pending_flags[process_id].exchange(false, std::memory_order_release);
      if (was_pending) {
        m_active_count.fetch_sub(1, std::memory_order_acq_rel);
      }
    }
  }

  bool dequeue_active(std::size_t queue_id, std::size_t& pid_out) {
    if (queue_id >= m_active_queues.size()) {
      return false;
    }
    return m_active_queues[queue_id]->dequeue(pid_out);
  }

  std::vector<std::unique_ptr<Mailbox_type>> m_mailboxes;
  std::vector<std::unique_ptr<Bounded_queue<std::size_t>>> m_active_queues;
  std::unique_ptr<std::atomic<bool>[]> m_pending_flags;
  std::size_t m_pending_flags_size{0};
  std::vector<std::size_t> m_worker_index;
  std::atomic<std::size_t> m_active_count{0};
  bool m_track_active_count{false};
  bool m_direct_fast_path{false};
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
   * Deterministic mapping to keep SPSC semantics and avoid global contention
   * Returns true if successfully scheduled, false if already scheduled or needs retry */
  bool schedule_self(Process_mailboxes<PayloadType>* mailboxes, std::size_t n_processes) {
    if (n_processes == 0){
      return false;
    }
    
    /* Deterministic mapping: each process uses its own mailbox to preserve SPSC semantics */
    std::size_t target_process_id = m_pid % n_processes;

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
          mailboxes->notify_mailbox_has_messages(target_process_id, n_processes);
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
  /* Optional: force consumer to drain all mailboxes round-robin each loop */
  bool m_round_robin_drain{false};
  std::vector<std::unique_ptr<Process_type>> m_processes;
};

}  /* namespace util */
