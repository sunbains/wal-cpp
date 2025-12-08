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

#include <thread>
#include <atomic>
#include <chrono>
#include <print>
#include <vector>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <variant>
#include <array>

#include "util/actor_model.h"
#include "util/bounded_channel.h"
#include "util/spsc.h"
#include "util/thread_pool.h"
#include "util/util.h"
#include "coro/task.h"
#include "wal/service.h"

/**
 * Erlang-style Actor Model (SPSC architecture)
 * - Each process has its own SPSC mailbox
 * - Processes self-schedule into MPMC queue when they have messages
 * - Consumer drains from MPMC queue to find which mailboxes to process
 * - Run-to-completion: schedule once, then tight loops
 * - No per-message suspend/resume
 */

using Payload_size_t = std::size_t;
using Process_t = util::Process<Payload_size_t>;
using Process_mailbox_t = util::Process_mailbox<Payload_size_t>;
using Process_mailboxes_t = util::Process_mailboxes<Payload_size_t>;
using Message_envelope_t = util::Message_envelope<Payload_size_t>;

/**
 * Extended scheduler with consumer handle support (for test_actor_model)
 */
template<typename PayloadType>
struct Scheduler_with_consumer : public util::Scheduler_base<PayloadType> {
  using Process_type = util::Process<PayloadType>;
  
  void schedule_consumer() {
    if (m_consumer_handle && !m_consumer_handle.done()) {
      /* Convert generic handle to typed handle for posting
       * This is a workaround to avoid the need to use a typed handle in the consumer coroutine */
      auto typed_handle = std::coroutine_handle<Task<void>::promise_type>::from_address(m_consumer_handle.address());

      this->m_consumer_pool->try_post_coroutine_handle(typed_handle);
    }
  }

  std::coroutine_handle<> m_consumer_handle{};
};

using Scheduler_t = Scheduler_with_consumer<Payload_size_t>;

/* Actor_context - inherits from wal::Producer_context_base for common send() implementation */
struct Actor_context : public wal::Producer_context_base<Payload_size_t, Scheduler_t> {
  /* Constructor to initialize base class members */
  Actor_context(util::Pid self, Process_t* proc, Scheduler_t* sched, Process_t* consumer_proc = nullptr)
    : wal::Producer_context_base<Payload_size_t, Scheduler_t>{.m_self = self, .m_proc = proc, .m_sched = sched, .m_consumer_proc = consumer_proc} {}
};

/**
 * Producer actor - runs to completion sending all messages to OWN mailbox
 */
Task<void> producer_actor(Actor_context ctx, std::size_t num_items, std::atomic<bool>& start_flag) {
  co_await ctx.m_sched->m_producer_pool->schedule();

  while (!start_flag.load(std::memory_order_acquire)) {
    util::cpu_pause();
  }

  ctx.m_proc->schedule_self(ctx.m_sched->m_mailboxes, ctx.m_sched->m_mailboxes->size());

  /* Tight loop: write all items to own mailbox
   * Optimize fast path - queue is rarely full */
  for (std::size_t i = 0; i < num_items; ++i) {
    /* Directly construct envelope with payload - identity case (PayloadType == std::size_t) */
    util::Message_envelope<Payload_size_t> env{.m_sender = ctx.m_self, .m_payload = i};

    /* Fast path: try enqueue first without any overhead */
    if (ctx.m_proc->m_mailbox.enqueue(std::move(env))) [[likely]] {
      continue;
    }

    /* Slow path: queue full
     * Reschedule so consumer can find us and drain the queue */
    while (!ctx.m_proc->m_mailbox.enqueue(std::move(env))) {
      /* Reschedule so consumer can find us and drain the queue
       * Use hash-based distribution for better load balancing */
      ctx.m_proc->schedule_self(ctx.m_sched->m_mailboxes, ctx.m_sched->m_mailboxes->size());
      /* Minimal pause - consumer needs CPU time to drain */
      util::cpu_pause();
    }
  }

  util::Message_envelope<Payload_size_t> term_env{.m_sender = ctx.m_self, .m_payload = util::Poison_pill{}};

  if (!ctx.m_proc->m_mailbox.enqueue(std::move(term_env))) {
    while (!ctx.m_proc->m_mailbox.enqueue(std::move(term_env))) {
      ctx.m_proc->schedule_self(ctx.m_sched->m_mailboxes, ctx.m_sched->m_mailboxes->size());
      util::cpu_pause();
    }
  }
  
  /* Schedule self after enqueueing poison pill so consumer can detect it */
  ctx.m_proc->schedule_self(ctx.m_sched->m_mailboxes, ctx.m_sched->m_mailboxes->size());

  co_return;
}

/**
 * Consumer actor - uses common implementation
 */
Task<void> consumer_actor(
  Scheduler_t* sched,
  Process_t* consumer_proc,
  std::size_t num_producers,
  std::atomic<bool>& start_flag,
  std::atomic<bool>& consumer_done
) {
  /* No-op message processor */
  auto process_msg = [](const Payload_size_t&) { /* No-op for basic test */ };
  
  co_await sched->m_consumer_pool->schedule();

  while (!start_flag.load(std::memory_order_acquire)) {
    util::cpu_pause();
  }

  std::size_t completed_producers = 0;
  util::Message_envelope<Payload_size_t> msg;
  std::size_t no_work_iterations = 0;
  std::size_t next_active_queue{0};

  constexpr std::size_t bulk_read_size = 64;
  std::array<std::size_t, bulk_read_size> notification_buffer;

  while (completed_producers < num_producers) {
    /* Check for poison pills in consumer mailbox (if provided) */
    if (consumer_proc != nullptr) {
      while (consumer_proc->m_mailbox.dequeue(msg)) {
        if (std::holds_alternative<util::Poison_pill>(msg.m_payload)) {
          ++completed_producers;
          if (completed_producers >= num_producers) {
            break;
          }
        }
      }
    }

    if (completed_producers >= num_producers) {
      break;
    }

    util::Process<Payload_size_t>* proc = nullptr;
    bool found_work = false;

    std::size_t num_notifications = 0;
    std::size_t notified_mailbox_idx = 0;
    
    constexpr std::size_t fast_scan_limit = 8;
    const bool use_fast_scan = (sched->m_mailboxes->size() <= fast_scan_limit &&
                                sched->m_mailboxes->m_active_count.load(std::memory_order_acquire) <= fast_scan_limit);

    if (!use_fast_scan) {
      const std::size_t n_workers = sched->m_mailboxes->m_active_queues.size();
      for (std::size_t q = 0; q < n_workers && num_notifications < bulk_read_size; ++q) {
        const std::size_t qid = (next_active_queue + q) % n_workers;
        while (num_notifications < bulk_read_size &&
               sched->m_mailboxes->dequeue_active(qid, notified_mailbox_idx)) {
          notification_buffer[num_notifications++] = notified_mailbox_idx;
        }
      }
      if (n_workers > 0) {
        next_active_queue = (next_active_queue + 1) % n_workers;
      }
      if (num_notifications > 1) {
        std::sort(notification_buffer.begin(), notification_buffer.begin() + num_notifications);
      }
    } else {
      for (std::size_t pid = 0; pid < sched->m_mailboxes->size() && num_notifications < bulk_read_size; ++pid) {
        auto mbox = sched->m_mailboxes->get_for_process(pid);
        if (mbox && mbox->m_has_messages.load(std::memory_order_acquire)) {
          notification_buffer[num_notifications++] = pid;
        }
      }
    }

    for (std::size_t i = 0; i < num_notifications; ++i) {
      if (i > 0 && notification_buffer[i] == notification_buffer[i - 1]) {
        continue;
      }
      std::size_t mailbox_idx = notification_buffer[i];

      auto process_mbox = sched->m_mailboxes->get_for_process(mailbox_idx);

      if (process_mbox == nullptr) {
        continue;
      }

      if (!process_mbox->m_has_messages.load(std::memory_order_acquire)) {
        continue;
      }

      bool loop_exited_normally = true;

      while (process_mbox->m_queue.dequeue(proc)) {
        found_work = true;

        util::prefetch_for_read<3>(proc);

        proc->m_in_process_mailbox.store(false, std::memory_order_release);

        while (proc->m_mailbox.dequeue(msg)) {
          if (std::holds_alternative<util::Poison_pill>(msg.m_payload)) [[unlikely]] {
            ++completed_producers;

            if (completed_producers >= num_producers) {
              loop_exited_normally = false;
              break;
            }
          } else {
            /* Regular payload message */
            util::process_payload_message(process_msg, std::get<Payload_size_t>(msg.m_payload), msg);
          }
        }
        
        if (completed_producers >= num_producers) {
          loop_exited_normally = false;
          break;
        }
      }

      if (loop_exited_normally) {
        process_mbox->m_has_messages.store(false, std::memory_order_release);
        sched->m_mailboxes->clear_pending_flag(mailbox_idx);
      }
      
      if (completed_producers >= num_producers) {
        break;
      }
    }

    const std::size_t scan_interval = (num_producers == 1) ? 1000 : 20;

    if (!found_work) {
      ++no_work_iterations;
    } else {
      no_work_iterations = 0;
    }

    if (no_work_iterations % scan_interval == 0) {
      /* Scan all processes to find poison pills in producer mailboxes.
       * Producers send poison pills to their own mailboxes, so we need to scan all processes.
       * Skip the consumer process (if provided) since producers don't send poison pills there.
       */
      for (std::size_t pid = 0; pid < sched->m_processes.size() && completed_producers < num_producers; ++pid) {
        auto* p = sched->get_process(pid);
        if (p == nullptr) {
          continue;
        }
        
        /* Skip consumer process mailbox - producers don't send poison pills there */
        if (consumer_proc && p == consumer_proc) {
          continue;
        }

        while (p->m_mailbox.dequeue(msg)) {
          if (std::holds_alternative<util::Poison_pill>(msg.m_payload)) [[unlikely]] {
            ++completed_producers;
            if (completed_producers >= num_producers) {
              break;
            }
          } else {
            /* Regular payload message */
            util::process_payload_message(process_msg, std::get<Payload_size_t>(msg.m_payload), msg);
          }
        }
        
        if (completed_producers >= num_producers) {
          break;
        }
      }
    }

    if (!found_work) [[unlikely]] {
      util::cpu_pause();
    }
  }

  consumer_done.store(true, std::memory_order_release);
  co_return;
}

/**
 * Benchmark configuration
 */
struct Config {
  std::size_t m_num_producers = 1024;
  std::size_t m_total_items = 20'000'000;
  std::size_t m_mailbox_size = 256;
  std::size_t m_drain_batch_size = 512;
  bool m_run_suite = false;
};

struct Benchmark_result {
  std::size_t m_num_producers;
  double m_ops_per_sec;
  double m_time_s;
};

Benchmark_result test_actor_model(const Config& config) {
  wal::Log_service_setup<Payload_size_t, Scheduler_t> service_setup(config.m_num_producers);

  /* Spawn consumer process for poison pills */
  util::Pid consumer_pid = service_setup.m_sched.spawn(config.m_mailbox_size);
  util::Process<Payload_size_t>* consumer_proc = service_setup.m_sched.get_process(consumer_pid);

  /* Spawn producers - spawn one process per producer */
  std::vector<util::Pid> producer_pids;

  for (std::size_t i = 0; i < config.m_num_producers; ++i) {
    producer_pids.push_back(service_setup.m_sched.spawn(config.m_mailbox_size));
  }

  std::atomic<bool> start_flag{false};
  std::atomic<bool> consumer_done{false};
  std::size_t items_per_producer = config.m_total_items / config.m_num_producers;

  /* Launch consumer (scans thread-local mailboxes) */
  detach(consumer_actor(&service_setup.m_sched, consumer_proc, config.m_num_producers, start_flag, consumer_done));

  /* Launch producers */
  for (std::size_t p = 0; p < config.m_num_producers; ++p) {
    std::size_t items = (p == config.m_num_producers - 1)
      ? config.m_total_items - (p * items_per_producer)
      : items_per_producer;

    Actor_context producer_ctx(
      producer_pids[p],
      service_setup.m_sched.get_process(producer_pids[p]),
      &service_setup.m_sched,
      consumer_proc
    );

    auto producer_task = producer_actor(producer_ctx, items, start_flag);
    producer_task.start_on_pool(service_setup.m_producer_pool);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  start_flag.store(true, std::memory_order_release);

  auto start_time = std::chrono::steady_clock::now();
  constexpr auto max_wait_time = std::chrono::seconds(30);

  /* Wait for consumer to complete (event-based) */
  auto wait_start = std::chrono::steady_clock::now();

  while (!consumer_done.load(std::memory_order_acquire)) {
    auto elapsed = std::chrono::steady_clock::now() - wait_start;

    if (elapsed > max_wait_time) {
      std::println(stderr, "ERROR: Timeout waiting for consumer to finish");
      break;
    }
    /* Use CPU pause instead of yield to avoid syscalls */
    for (int j = 0; j < 10; ++j) {
      util::cpu_pause();
    }
  }

  /* Give a delay to let all producer coroutines finish cleanup */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto end_time = std::chrono::steady_clock::now();

  auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time);
  double elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;
  /* All items were processed when consumer_done is true */
  double ops_per_sec = static_cast<double>(config.m_total_items) / elapsed_s;

  return Benchmark_result{config.m_num_producers, ops_per_sec, elapsed_s};
}

void run_benchmark_suite(const Config& base_config) {
  std::vector<std::size_t> producer_counts = {
    1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536
  };

  std::println("=== Erlang Actor Model Benchmark Suite ===");
  std::println("Items: {}, Mailbox: {}, Drain Batch: {}\n",
               base_config.m_total_items, base_config.m_mailbox_size, base_config.m_drain_batch_size);
  std::println("{:>5} | {:>12} {:>8}", "Actors", "Throughput", "Time(s)");
  std::println("{}", std::string(40, '-'));

  for (std::size_t num_prod : producer_counts) {
    Config config = base_config;
    config.m_num_producers = num_prod;

    auto result = test_actor_model(config);

    std::println("{:5}P | {:9.2f} M/s {:8.3f}", num_prod, result.m_ops_per_sec / 1'000'000.0, result.m_time_s);
  }

  std::println("\n=== Done ===");
}

void print_usage(const char* prog) {
  std::println("Usage: {} [OPTIONS]", prog);
  std::println("\nOptions:");
  std::println("  -p, --producers <N>      Number of producer actors (default: 1024)");
  std::println("  -n, --items <N>          Total items to process (default: 10000000)");
  std::println("  -m, --mailbox-size <N>   Mailbox size (default: 256)");
  std::println("  -d, --drain-batch <N>    Consumer drain batch size (default: 512)");
  std::println("  -s, --suite              Run full benchmark suite");
  std::println("  -h, --help               Show this help message");
}

int main(int argc, char* argv[]) {
  Config config;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      return 0;
    } else if (arg == "-s" || arg == "--suite") {
      config.m_run_suite = true;
    } else if ((arg == "-p" || arg == "--producers") && i + 1 < argc) {
      config.m_num_producers = std::stoull(argv[++i]);
    } else if ((arg == "-n" || arg == "--items") && i + 1 < argc) {
      config.m_total_items = std::stoull(argv[++i]);
    } else if ((arg == "-m" || arg == "--mailbox-size") && i + 1 < argc) {
      config.m_mailbox_size = std::stoull(argv[++i]);
    } else if ((arg == "-d" || arg == "--drain-batch") && i + 1 < argc) {
      config.m_drain_batch_size = std::stoull(argv[++i]);
    } else {
      std::println(stderr, "Unknown option: {}", arg);
      print_usage(argv[0]);
      return 1;
    }
  }

  if (config.m_run_suite) {
    run_benchmark_suite(config);
  } else {
    std::println("=== Erlang Actor Model Benchmark ===");
    std::println("Producers: {}, Items: {}, Mailbox: {}, Drain Batch: {}\n",
                 config.m_num_producers, config.m_total_items,
                 config.m_mailbox_size, config.m_drain_batch_size);

    auto result = test_actor_model(config);

    std::println("Throughput: {:.2f} M/s", result.m_ops_per_sec / 1'000'000.0);
    std::println("Time: {:.3f}s", result.m_time_s);
  }

  return 0;
}
