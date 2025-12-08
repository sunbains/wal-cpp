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

#include "util/actor_model.h"

namespace wal {

/* Messages for the log service */
struct Fsync {};
struct Fdatasync {};

template<typename PayloadType, typename SchedulerType>
struct Producer_context_base {
  void send(const PayloadType& payload) {
    util::Message_envelope<PayloadType> env{
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

  util::Pid m_self;
  util::Process<PayloadType>* m_proc;
  SchedulerType* m_sched;
  /** Consumer's mailbox for poison pills */
  util::Process<PayloadType>* m_consumer_proc;
  std::size_t m_fdatasync_interval{0};
};

/**
 * Common test setup - creates thread pool, mailboxes, and scheduler
 */
template<typename PayloadType, typename SchedulerType>
struct Log_service_setup {
  Log_service_setup(std::size_t num_producers, std::size_t io_threads = 1)
    : m_producer_pool(create_producer_pool_config(num_producers)),
      m_consumer_pool(create_consumer_pool_config()),
      m_io_pool(create_io_pool_config(io_threads)) {
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
     producer_pool_config.m_queue_capacity = std::min<std::size_t>(1024, std::max<std::size_t>(4096, num_producers * 4));
 
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
 
     consumer_pool_config.m_num_threads = 1;
     consumer_pool_config.m_queue_capacity = 32;
 
     if (!std::has_single_bit(consumer_pool_config.m_queue_capacity)) {
       consumer_pool_config.m_queue_capacity = std::bit_ceil(consumer_pool_config.m_queue_capacity);
     }
 
     return consumer_pool_config;
   }
 
  static util::Thread_pool::Config create_io_pool_config(std::size_t io_threads = 1) {
    util::Thread_pool::Config io_pool_config;
 
     std::size_t hw_threads = std::thread::hardware_concurrency();
 
     if (hw_threads == 0) {
       hw_threads = 4;
     }
 
    io_pool_config.m_num_threads = std::max<std::size_t>(std::size_t(1), io_threads);
     io_pool_config.m_queue_capacity = 64;
 
     if (!std::has_single_bit(io_pool_config.m_queue_capacity)) {
       io_pool_config.m_queue_capacity = std::bit_ceil(io_pool_config.m_queue_capacity);
     }
 
     return io_pool_config;
   }
 
 public:
   util::Process_mailboxes<PayloadType> m_thread_mailboxes;
   util::Thread_pool m_producer_pool;
   util::Thread_pool m_consumer_pool;
   util::Thread_pool m_io_pool;
   SchedulerType m_sched;
 };


} // namespace wal
