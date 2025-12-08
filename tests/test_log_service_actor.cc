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

#include <cassert>
#include <cstring>
#include <cerrno>
#include <vector>
#include <thread>
#include <atomic>
#include <cstdint>
#include <chrono>
#include <getopt.h>
#include <unistd.h>
#include <sys/uio.h>
#include <fcntl.h>
#include <span>
#include <print>
#include <memory>
#include <algorithm>
#include <variant>
#include <array>
#include <limits>
#include <random>
#include <cmath>

#include "coro/task.h"
#include "util/actor_model.h"
#include "util/bounded_channel.h"
#include "util/logger.h"
#include "util/spsc.h"
#include "util/util.h"
#include "util/thread_pool.h"
#include "util/metrics.h"
#include "util/checksum.h"
#include "wal/buffer_pool.h"
#include "wal/io.h"
#include "wal/service.h"
#include "wal/wal.h"

namespace {

inline int portable_posix_fallocate(int fd, off_t offset, off_t len) {
#if defined(__APPLE__)
  const off_t end = offset + len;
  return (::ftruncate(fd, end) == 0) ? 0 : errno;
#else
  return ::posix_fallocate(fd, offset, len);
#endif
}

inline int portable_fdatasync(int fd) {
#if defined(__APPLE__)
  return ::fsync(fd);
#else
  return ::fdatasync(fd);
#endif
}

inline int portable_posix_fadvise(int fd, off_t offset, off_t len) {
#if defined(POSIX_FADV_DONTNEED) && !defined(__APPLE__)
  return ::posix_fadvise(fd, offset, len, POSIX_FADV_DONTNEED);
#else
  (void)fd;
  (void)offset;
  (void)len;
  return 0;
#endif
}

} // namespace

util::Logger<util::MT_logger_writer> g_logger(util::MT_logger_writer{std::cerr}, util::Log_level::Trace);

static constexpr util::ChecksumAlgorithm kChecksumAlgorithm = util::ChecksumAlgorithm::CRC32C;

using Log_writer = wal::Log_writer;
using Clock = std::chrono::steady_clock;

struct Metrics_config {
  bool m_enable_producer_latency{true};  /* Most expensive - track per-message latency */
  
  /* Returns true if any metrics are enabled */
  [[nodiscard]] bool any_enabled() const noexcept {
    return m_enable_producer_latency;
  }
};

enum class Sync_message_type { Fdatasync, Fsync };

struct Test_config {
  std::size_t m_mailbox_size{256};
  std::size_t m_num_producers{1};
  std::size_t m_num_consumers{1};
  std::size_t m_num_messages{10000000};
  /** Number of runs to average for stability */
  std::size_t m_num_iterations{1};
  bool m_disable_writes{false};
  bool m_disable_log_writes{false};
  bool m_skip_memcpy{false};
  std::size_t m_log_block_size{4096};
  std::size_t m_log_buffer_size_blocks{16384};
  std::size_t m_pool_size{32};
  std::size_t m_io_queue_size{0};  // 0 means use pool_size * 2
  std::size_t m_timeout_ms{5000};

  /** Send fdatasync/fsync with probability 1/N (0 = disabled, calculated from probability) */
  std::size_t m_fdatasync_interval{0};

  /** Type of sync message to send (default: Fdatasync) */
  Sync_message_type m_sync_type{Sync_message_type::Fdatasync};

  /** Disable metrics collection entirely (overrides Metrics_config) */
  bool m_disable_metrics{false};

  /** Fine-grained control over which metrics to collect */
  Metrics_config m_metrics_config{};

  /** Enable batch dequeue (default: false - single dequeue) */
  bool m_enable_batch_dequeue{false};

  /** Batch size for batch dequeue (default: 32) */
  std::size_t m_batch_size{32};

  /** Verbosity level for metrics output (0=concise, 1=summary, 2=full histograms) */
  int m_verbosity{0};
};

/**
 * Erlang-style Actor Model for Log Service
 * - Each producer/consumer has its own SPSC mailbox
 * - Processes self-schedule into thread mailboxes when they have messages
 * - Consumer is a coroutine that writes to WAL (like test_log_io_simple producer)
 * - Consumer drains from thread mailboxes and writes to log
 */

/* Specialize Message_envelope to use Poison_pill, Fdatasync, and Fsync instead of monostate */
namespace util {
template<>
struct Message_envelope<wal::Test_message> {
  std::variant<wal::Test_message, Poison_pill, wal::Fdatasync, wal::Fsync> m_payload;
  Pid m_sender;
  Clock::time_point m_send_time{};  /* Timestamp when message was sent (for latency tracking) */
};
} // namespace util

using Payload_type = wal::Test_message;
using Message_envelope_type = util::Message_envelope<Payload_type>;
using Process_type = util::Process<Payload_type>;
using Process_mailbox_type = util::Process_mailbox<Payload_type>;
using Process_mailboxes_type = util::Process_mailboxes<Payload_type>;
using Scheduler_type = util::Scheduler_base<Payload_type>;

struct Producer_context : public wal::Producer_context_base<Payload_type, Scheduler_type> {
  Sync_message_type m_sync_type{Sync_message_type::Fdatasync};
  bool m_track_latency{false};
  bool m_disable_log_writes{false};
};

/* Forward declaration */
struct Log_message_processor;

struct Log_service_context {
  std::shared_ptr<wal::Log> m_log;
  Log_writer m_log_writer;
  std::atomic<bool>* m_service_done;
  bool m_disable_log_writes;
  util::Thread_pool* m_service_pool;
  util::Thread_pool* m_io_pool;
  /** Service process for poison pills */
  Process_type* m_service_process;
  Scheduler_type* m_sched;
  /** Pass by pointer so we can call write_and_fdatasync/fsync */
  Log_message_processor* m_processor;
  std::size_t m_num_producers;
  bool m_enable_batch_dequeue{false};
  /** Batch size for batch dequeue, only used if m_enable_batch_dequeue is true */
  std::size_t m_batch_size{32};
};

/* Forward declaration */
struct Log_message_processor;

/* Callable for sync operations (fdatasync/fsync) - defined before Log_message_processor
 * Carries fd, offset, and metrics */
struct Sync_callable {
  [[nodiscard]] wal::Result<bool> operator()() const noexcept;
  int m_fd{-1};
  wal::Sync_type m_sync_type;
  wal::lsn_t m_sync_offset{0};
  util::Metrics* m_metrics{nullptr};
};

/* Function object wrapper that stores values as members and calls static function */
struct Log_message_processor {
  Log_message_processor() = default;

  Log_message_processor(
    std::shared_ptr<wal::Log> log,
    util::Thread_pool* io_pool,
    bool disable_log_writes,
    util::Metrics* metrics = nullptr,
    const Metrics_config* metrics_config = nullptr,
    std::shared_ptr<wal::Log_file> log_file = nullptr
  ) : m_log(std::move(log)),
      m_io_pool(io_pool),
      m_disable_log_writes(disable_log_writes),
      m_metrics(metrics),
      m_metrics_config(metrics_config),
      m_log_file(log_file),
      m_fdatasync_callable{.m_fd = log_file ? log_file->m_fd : -1, .m_sync_type = wal::Sync_type::Fdatasync, .m_metrics = metrics},
      m_fsync_callable{.m_fd = log_file ? log_file->m_fd : -1, .m_sync_type = wal::Sync_type::Fsync, .m_metrics = metrics} {
  }

  void operator()(const Payload_type& payload, Clock::time_point send_time = Clock::time_point{}) noexcept{
    if (m_disable_log_writes) {
      return;
    }

    /* Write directly to log - no batching */
    [[maybe_unused]] auto result = m_log->append(payload.get_span(), m_io_pool);
    WAL_ASSERT(result.has_value());

    /* Track producer latency AFTER successful write to measure true end-to-end latency */
    if (m_metrics != nullptr && m_metrics_config != nullptr &&
        m_metrics_config->m_enable_producer_latency &&
        send_time != Clock::time_point{}) [[unlikely]] {
      const auto write_time = Clock::now();
      auto latency = std::chrono::duration_cast<std::chrono::nanoseconds>(write_time - send_time);
      m_metrics->add_timing(util::MetricType::ProducerLatency, latency);
    }
  }

  void write_and_fdatasync(int) {
    if (m_log != nullptr && m_log->m_pool != nullptr && m_log->m_pool->m_active != nullptr) {
      m_fdatasync_callable.m_sync_offset = m_log->m_pool->m_active->m_buffer.m_hwm;
    }
    m_log->request_sync(wal::Sync_type::Fdatasync, &m_fdatasync_callable);
  }

  void write_and_fsync(int) {
    if (m_log != nullptr && m_log->m_pool != nullptr && m_log->m_pool->m_active != nullptr) {
      m_fsync_callable.m_sync_offset = m_log->m_pool->m_active->m_buffer.m_hwm;
    }
    m_log->request_sync(wal::Sync_type::Fsync, &m_fsync_callable);
  }

public:
  std::shared_ptr<wal::Log> m_log;
  util::Thread_pool* m_io_pool;
  bool m_disable_log_writes;

  /** Metrics collector for latency tracking */
  util::Metrics* m_metrics{nullptr};

  /** Metrics configuration - which metrics to collect */
  const Metrics_config* m_metrics_config{nullptr};
  
  /** Log file for direct sync operations */
  std::shared_ptr<wal::Log_file> m_log_file{nullptr};
  
  /** Fdatasync callable - member to avoid lifetime issues */
  Sync_callable m_fdatasync_callable;
  
  /** Fsync callable - member to avoid lifetime issues */
  Sync_callable m_fsync_callable;
};

/* Implementation of Sync_callable::operator() - defined after Log_message_processor is complete */
inline wal::Result<bool> Sync_callable::operator()() const noexcept {
  Clock::time_point sync_start;
  if (m_metrics != nullptr) [[likely]] {
    sync_start = Clock::now();
  }
  
  if (m_fd != -1) {
    int result = -1;
    if (m_sync_type == wal::Sync_type::Fdatasync) [[likely]] {
      result = portable_fdatasync(m_fd);
    } else if (m_sync_type == wal::Sync_type::Fsync) {
      result = ::fsync(m_fd);
    }
    
    if (result == 0) [[likely]] {
      if (m_metrics != nullptr) [[likely]] {
        auto sync_end = Clock::now();
        auto sync_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(sync_end - sync_start);
        if (m_sync_type == wal::Sync_type::Fdatasync) {
          m_metrics->inc(util::MetricType::FdatasyncCount);
          m_metrics->add_timing(util::MetricType::FdatasyncTiming, sync_duration);
        } else {
          m_metrics->inc(util::MetricType::FsyncCount);
          m_metrics->add_timing(util::MetricType::FsyncTiming, sync_duration);
        }
      }

      /* Drop cache for all data written up to current HWM (best effort) */
      if (m_sync_offset > 0) {
        portable_posix_fadvise(m_fd, 0, static_cast<off_t>(m_sync_offset));
      }
      return wal::Result<bool>(true);
    }
    const char* op_name = (m_sync_type == wal::Sync_type::Fdatasync) ? "fdatasync" : "fsync";
    log_err("Failed to {} log file: {}", op_name, std::strerror(errno));
    return wal::Result<bool>(false);
  }
  return wal::Result<bool>(true);
}

/** Producer actor - sends log messages to consumer
 * @param[in] ctx The producer context (contains fdatasync_interval, sync_type, and track_latency)
 * @param[in] num_items The number of items to send
 * @param[in] start_flag The start flag
 */
Task<void> producer_actor(Producer_context ctx, std::size_t num_items, std::atomic<bool>& start_flag) {
  co_await ctx.m_sched->m_producer_pool->schedule();

  while (!start_flag.load(std::memory_order_acquire)) {
    util::cpu_pause();
  }

  /* Cache mailbox count - it never changes during execution (6.39% hotspot) */
  const std::size_t n_mailboxes = ctx.m_sched->m_mailboxes->size();

  ctx.m_proc->schedule_self(ctx.m_sched->m_mailboxes, n_mailboxes);

  auto enqueue_with_retry = [&](auto&& env_factory) {
    while (true) {
      auto env = env_factory();
      if (ctx.m_proc->m_mailbox.enqueue(std::move(env))) [[likely]] {
        /* Notify consumer only if not already scheduled. */
        if (!ctx.m_proc->m_in_process_mailbox.load(std::memory_order_acquire)) {
          ctx.m_proc->schedule_self(ctx.m_sched->m_mailboxes, n_mailboxes);
        }
        break;
      }
      /* Failed to enqueue (mailbox full) - notify consumer and retry. */
      ctx.m_proc->schedule_self(ctx.m_sched->m_mailboxes, n_mailboxes);
      util::cpu_pause();
    }
  };

  /* Send messages, randomly sending sync messages if interval is set */
  for (std::size_t i = 0; i < num_items; ++i) {
    /* Always send the payload message */
    const auto send_time = ctx.m_track_latency ? Clock::now() : Clock::time_point{};
    enqueue_with_retry([&]() {
      util::Message_envelope<Payload_type> env;
      env.m_sender = ctx.m_self;
      env.m_send_time = send_time;
      env.m_payload = wal::Test_message{};
      return env;
    });

    /* Optionally send an additional sync message */
    if (ctx.m_fdatasync_interval > 0 && !ctx.m_disable_log_writes && ((i + 1) % ctx.m_fdatasync_interval == 0)) {
      enqueue_with_retry([&]() {
        util::Message_envelope<Payload_type> env;
        env.m_sender = ctx.m_self;
        if (ctx.m_sync_type == Sync_message_type::Fdatasync) {
          env.m_payload = wal::Fdatasync{};
        } else {
          env.m_payload = wal::Fsync{};
        }
        return env;
      });
    }
  }

  enqueue_with_retry([&]() {
    util::Message_envelope<Payload_type> env;
    env.m_sender = ctx.m_self;
    env.m_payload = util::Poison_pill{};
    return env;
  });

  /* Schedule self after enqueueing poison pill so consumer can detect it */
  ctx.m_proc->schedule_self(ctx.m_sched->m_mailboxes, n_mailboxes);

  co_return;
}

/* Process a single message and return true if processing should continue, false to stop */
[[nodiscard]] inline bool process_single_message(
  const util::Message_envelope<Payload_type>& msg,
  Log_service_context& ctx,
  std::size_t& completed_producers,
  bool& loop_exited_normally
) noexcept {
  if (std::holds_alternative<util::Poison_pill>(msg.m_payload)) [[unlikely]] {
    ++completed_producers;
    if (completed_producers >= ctx.m_num_producers) {
      loop_exited_normally = false;
      return false;
    }
    return true;
  }
  
  if (std::holds_alternative<wal::Fdatasync>(msg.m_payload)) {
    if constexpr (requires { ctx.m_processor->write_and_fdatasync(0); }) {
      if (ctx.m_processor->m_log_file != nullptr) {
        ctx.m_processor->write_and_fdatasync(ctx.m_processor->m_log_file->m_fd);
      }
    }
    return true;
  }
  
  if (std::holds_alternative<wal::Fsync>(msg.m_payload)) {
    if constexpr (requires { ctx.m_processor->write_and_fsync(0); }) {
      if (ctx.m_processor->m_log_file != nullptr) {
        ctx.m_processor->write_and_fsync(ctx.m_processor->m_log_file->m_fd);
      }
    }
    return true;
  }
  
  if (std::holds_alternative<Payload_type>(msg.m_payload)) {
    process_payload_message(*ctx.m_processor, std::get<Payload_type>(msg.m_payload), msg);
  }
  
  return true;
}

/* Process messages from a process mailbox until it's empty or all producers are done.
 * Returns true if processing should continue, false if all producers are done.
 *  
 * @param[in] process The process to process
 * @param[in] ctx The consumer context
 * @param[in] completed_producers The number of completed producers
 * @param[in] msg The message envelope
 * 
 * @return True if processing should continue, false if all producers are done
 */
[[nodiscard]] inline bool process_mailbox(
  util::Process<Payload_type>* process,
  Log_service_context& ctx,
  std::size_t& completed_producers,
  util::Message_envelope<Payload_type>& msg
) noexcept {
  if (process == nullptr) [[unlikely]] {
    return true;
  }
  
  /* Skip service process mailbox - producers don't send poison pills there */
  if (ctx.m_service_process && process == ctx.m_service_process) {
    return true;
  }
  
  bool loop_exited_normally{true};
  while (process->m_mailbox.dequeue(msg)) [[likely]] {
    if (!process_single_message(msg, ctx, completed_producers, loop_exited_normally)) [[unlikely]] {
      break;
    }
  }
  
  return completed_producers < ctx.m_num_producers;
}

/* Log service coroutine - runs on thread pool and writes to log */
Task<void> log_service_actor(
  Log_service_context ctx,
  std::atomic<bool>& start_flag
) noexcept {
  co_await ctx.m_service_pool->schedule();
 
  while (!start_flag.load(std::memory_order_acquire)) {
    util::cpu_pause();
  }
 
  std::size_t completed_producers = 0;
  std::size_t no_work_iterations = 0;
  util::Message_envelope<Payload_type> msg;
  constexpr std::size_t bulk_read_size = 64;
  std::array<std::size_t, bulk_read_size> notification_buffer;
 
  while (completed_producers < ctx.m_num_producers) {
 
    /* Check for poison pills in service mailbox (if provided) */
    if (ctx.m_service_process != nullptr) {
      while (ctx.m_service_process->m_mailbox.dequeue(msg)) {
        if (std::holds_alternative<util::Poison_pill>(msg.m_payload)) {
          ++completed_producers;
          if (completed_producers >= ctx.m_num_producers) {
            break;
          }
        }
      }
    }
 
    if (completed_producers >= ctx.m_num_producers) {
      break;
    }
 
    bool found_work{false};
    util::Process<Payload_type>* proc{nullptr};
 
    std::size_t n_notifications{0};
    std::size_t notified_mailbox_idx{0};
    
    while (n_notifications < bulk_read_size && ctx.m_sched->m_mailboxes->m_notify_queue->dequeue(notified_mailbox_idx)) {
      notification_buffer[n_notifications++] = notified_mailbox_idx;
    }
 
    for (std::size_t i = 0; i < n_notifications; ++i) {
      std::size_t mailbox_idx = notification_buffer[i];

      auto process_mbox = ctx.m_sched->m_mailboxes->get_for_process(mailbox_idx);

      if (process_mbox == nullptr) [[unlikely]] {
        continue;
      }

      if (!process_mbox->m_has_messages.load(std::memory_order_acquire)) [[unlikely]] {
        continue;
      }

      bool loop_exited_normally{true};

      while (process_mbox->m_queue.dequeue(proc)) [[likely]] {
        found_work = true;

        util::prefetch_for_read<3>(proc);

        proc->m_in_process_mailbox.store(false, std::memory_order_release);

        if (ctx.m_enable_batch_dequeue) [[likely]] {
          while (proc->m_mailbox.dequeue_batch(ctx.m_batch_size, [&](auto& batch_msg) {
            return process_single_message(batch_msg, ctx, completed_producers, loop_exited_normally);
          }) > 0) [[likely]] {
            if (completed_producers >= ctx.m_num_producers) [[unlikely]] {
              break;
            }
          }
        } else {
          while (proc->m_mailbox.dequeue(msg)) [[likely]] {
            if (!process_single_message(msg, ctx, completed_producers, loop_exited_normally)) [[unlikely]] {
              break;
            }
          }
        }
        
        if (completed_producers >= ctx.m_num_producers) [[unlikely]] {
          loop_exited_normally = false;
          break;
        }
      }
 
      if (loop_exited_normally) {
        process_mbox->m_has_messages.store(false, std::memory_order_release);
      }
      
      if (completed_producers >= ctx.m_num_producers) {
        break;
      }
    }
 
    const std::size_t scan_interval = (ctx.m_num_producers == 1) ? 1000 : 20;
 
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
      for (std::size_t pid = 0; pid < ctx.m_sched->m_processes.size() && completed_producers < ctx.m_num_producers; ++pid) {
        auto* p = ctx.m_sched->get_process(pid);
        if (!process_mailbox(p, ctx, completed_producers, msg)) {
          break;
        }
      }
    }
 
    if (!found_work) [[unlikely]] {
      util::cpu_pause();
    }
  }

  if (ctx.m_service_done) {
    ctx.m_service_done->store(true, std::memory_order_release);
  }
  co_return;
}

struct Test_run_result {
  double m_ops_per_sec;
  std::size_t m_sync_write_count;
};

static Test_run_result test_throughput_actor_model_single_run(const Test_config& config) {
  const std::size_t total_messages = config.m_num_messages;
  const std::size_t producers = config.m_num_producers;
  const std::size_t base_per_producer = producers == 0 ? 0 : total_messages / producers;
  const std::size_t producer_remainder = producers == 0 ? 0 : total_messages % producers;

  std::string path = config.m_disable_writes ? "/dev/null" : ("/tmp/test_actor.log");
  auto log_file = std::make_shared<wal::Log_file>(path, 512 * 1024 * 1024, config.m_log_block_size, config.m_disable_writes);

  WAL_ASSERT(log_file->m_fd != -1);

  if (!config.m_disable_writes) {
    auto ret = portable_posix_fallocate(log_file->m_fd, 0, log_file->m_max_file_size);
    if (ret != 0) {
      log_fatal("Failed to preallocate log file: {}", std::strerror(errno));
    }
  }

  /* Use smaller buffers but more of them for better parallelism, when we have small writes and frequent syncs.
   * If buffer size is large (e.g., 16384 blocks), use more smaller buffers.
   * Aim for ~4-8MB per buffer instead of 64MB */
  const std::size_t target_buffer_size_blocks = std::min(config.m_log_buffer_size_blocks, std::size_t(2048));
  auto wal_buffer_config = wal::Buffer::Config(target_buffer_size_blocks, config.m_log_block_size, kChecksumAlgorithm);
  
  /* Ensure pool size is a power of 2 */
  const std::size_t log2_pool_size = static_cast<std::size_t>(std::ceil(std::log2(static_cast<double>(config.m_pool_size))));
  const std::size_t pool_size_pow2 = std::size_t(1) << log2_pool_size;
  
  /* Ensure IO queue size is a power of 2 */
  std::size_t io_queue_size_pow2 = 0;
  if (config.m_io_queue_size > 0) {
    const std::size_t log2_io_queue_size = static_cast<std::size_t>(std::ceil(std::log2(static_cast<double>(config.m_io_queue_size))));
    io_queue_size_pow2 = std::size_t(1) << log2_io_queue_size;
  }
  
  wal::Pool::Config pool_config;
  pool_config.m_pool_size = pool_size_pow2;
  pool_config.m_io_queue_size = io_queue_size_pow2;
  
  wal::Log::Config log_config{
    .m_pool_config = pool_config,
    .m_buffer_config = wal_buffer_config
  };
  
  auto log = std::make_shared<wal::Log>(0, log_config);

  /* Create metrics collector for write_to_store operations - always enabled unless explicitly disabled.
   * Write-to-store metrics are always collected; producer latency is controlled by Metrics_config */
  util::Metrics metrics;
  const Metrics_config* metrics_config_ptr = nullptr;

  if (!config.m_disable_metrics) {
    log->set_metrics(&metrics);
    metrics_config_ptr = &config.m_metrics_config;
  }

  /* Capture metrics pointer directly to avoid circular reference with log shared_ptr.
   * Use raw pointer since metrics lifetime is managed by this function scope */
  util::Metrics* metrics_ptr = (!config.m_disable_metrics) ? &metrics : nullptr;

  struct Log_file_wrapper {
    std::shared_ptr<wal::Log_file> m_log_file;
    util::Metrics* m_metrics_ptr;
    bool m_disable_writes;
  };
  
  auto log_file_wrapper = std::make_shared<Log_file_wrapper>(Log_file_wrapper{
    .m_log_file = log_file,
    .m_metrics_ptr = metrics_ptr,
    .m_disable_writes = config.m_disable_writes
  });

  Log_writer log_writer = [log_file_wrapper]
    (std::span<struct iovec> span) -> wal::Result<std::size_t> {
    
    auto result = log_file_wrapper->m_log_file->write(span);
    return result;
  };

  wal::Log_service_setup<Payload_type, Scheduler_type> service_setup(producers);

  std::vector<util::Pid> producer_pids;
  for (std::size_t i = 0; i < producers; ++i) {
    producer_pids.push_back(service_setup.m_sched.spawn(config.m_mailbox_size));
  }

  /* Create log service process for poison pills */
  util::Pid service_pid = service_setup.m_sched.spawn(config.m_mailbox_size);
  auto service_process = service_setup.m_sched.get_process(service_pid);

  std::atomic<bool> start_flag{false};
  std::atomic<bool> service_done{false};

  Log_service_context service_ctx{
    .m_log = log,
    .m_log_writer = log_writer,
    .m_service_done = &service_done,
    .m_disable_log_writes = config.m_disable_log_writes,
    .m_service_pool = &service_setup.m_consumer_pool,
    .m_io_pool = &service_setup.m_io_pool,
    .m_service_process = service_process,
    .m_sched = nullptr,  /* Will be set after processor is created */
    .m_processor = nullptr,  /* Will be set after processor is created */
    .m_num_producers = 0,  /* Will be set after processor is created */
    .m_enable_batch_dequeue = config.m_enable_batch_dequeue,
    .m_batch_size = config.m_batch_size
  };

  /* Start the background I/O coroutine to process buffers on dedicated I/O pool */
  if (!config.m_disable_log_writes) {
    log->start_io(log_writer, &service_setup.m_io_pool);
    /* Brief pause to ensure I/O coroutine is scheduled and ready */
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  Log_message_processor processor(
    service_ctx.m_log,
    service_ctx.m_io_pool,
    service_ctx.m_disable_log_writes,
    config.m_disable_metrics ? nullptr : &metrics,  /* Pass metrics only if not disabled */
    metrics_config_ptr,  /* Pass metrics config for fine-grained control */
    log_file  /* Pass log_file for direct sync operations */
  );

  Log_message_processor msg_processor(
    service_ctx.m_log,
    service_ctx.m_io_pool,
    service_ctx.m_disable_log_writes,
    config.m_disable_metrics ? nullptr : &metrics,  /* Pass metrics only if not disabled */
    metrics_config_ptr,  /* Pass metrics config for fine-grained control */
    log_file  /* Pass log_file for direct sync operations */
  );

  /* Set additional fields in consumer context */
  service_ctx.m_sched = &service_setup.m_sched;
  service_ctx.m_processor = &msg_processor;
  service_ctx.m_num_producers = producers;

  /* Use simple lambda when writes are disabled, matching test_actor_model exactly */
  detach(log_service_actor(service_ctx, std::ref(start_flag)));

  /* Launch producer actors (coroutines) */
  for (std::size_t p = 0; p < producers; ++p) {
    const std::size_t messages_for_this_producer = base_per_producer + (p < producer_remainder ? 1 : 0);

    Producer_context producer_ctx;
    producer_ctx.m_self = producer_pids[p];
    producer_ctx.m_proc = service_setup.m_sched.get_process(producer_pids[p]);
    producer_ctx.m_sched = &service_setup.m_sched;
    producer_ctx.m_fdatasync_interval = config.m_fdatasync_interval;
    producer_ctx.m_sync_type = config.m_sync_type;
    producer_ctx.m_disable_log_writes = config.m_disable_log_writes;
    /* Enable latency tracking if metrics are enabled, producer latency is enabled, and log writes are not disabled */
    producer_ctx.m_track_latency = (!config.m_disable_metrics && 
                                    config.m_metrics_config.m_enable_producer_latency &&
                                    log->get_metrics() != nullptr && 
                                    !config.m_disable_log_writes);
    auto producer_task = producer_actor(producer_ctx, messages_for_this_producer, start_flag); 
    producer_task.start_on_pool(service_setup.m_producer_pool);
  }

  /* Brief warmup to ensure all coroutines are scheduled and ready */
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  
  /* Start timing immediately before releasing start flag */
  auto start = Clock::now();
  start_flag.store(true, std::memory_order_release);

  /* Wait for service to complete (event-based) */
  constexpr auto max_wait_time = std::chrono::seconds(600);
  auto wait_start = Clock::now();

  while (!service_done.load(std::memory_order_acquire)) {
    auto elapsed = Clock::now() - wait_start;
    if (elapsed > max_wait_time) {
      std::println(stderr, "ERROR: Timeout waiting for consumer to complete");
      break;
    }
    for (int j = 0; j < 10; ++j) {
      util::cpu_pause();
    }
  }

  /* Service coroutine will complete and signal via service_done flag */

  auto end = Clock::now();

  /* Brief delay to ensure all I/O is complete before shutdown */
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  if (!config.m_disable_log_writes) {
    /* Flush any remaining data in the active buffer before shutdown.
     * The actor test typically issues fdatasync/fsync operations to drive writes,
     * but when no sync messages are sent we may finish with a partially filled
     * buffer that never got submitted to the I/O queue. Preparing it here ensures
     * write_to_store processes the final partial buffer and metrics reflect all
     * bytes produced. */
    if (log->m_pool != nullptr &&
        log->m_pool->m_active != nullptr &&
        !log->m_pool->m_active->m_buffer.is_empty()) {
      log->m_pool->prepare_buffer_for_io(log->m_pool->m_active, service_setup.m_io_pool, log->m_write_callback);
    }

    /* Ensure all pending background I/O is complete before shutdown to avoid races. */
    log->wait_for_io_idle();

    /* Use fdatasync callable for shutdown sync */
    WAL_ASSERT(log->shutdown(log_writer, &msg_processor.m_fdatasync_callable).has_value());
    
    /* Clear the I/O callback to break circular reference before log goes out of scope */
    /* The callback lambda captures metrics_ptr, but Log also stores it, creating a cycle */
    /* Wait a bit more to ensure all I/O coroutines have finished */
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  
  /* Get sync write count before log goes out of scope */
  const std::size_t sync_write_count = log->get_sync_write_count();

  /* Explicitly clear the log_writer lambda to break any remaining references */
  /* This ensures the lambda (which may capture log_file and metrics_ptr) is destroyed */
  log_writer = {};

  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  double elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;

  /* All messages were processed when service_done is true */
  const auto processed = total_messages;
  const auto ops_per_sec = elapsed_s == 0.0 ? 0.0 : static_cast<double>(processed) / elapsed_s;

  /* Print metrics with verbosity control */
  if (!config.m_disable_metrics && log->get_metrics() != nullptr) {
    auto* m = log->get_metrics();
    const auto write_calls = m->get_counter(util::MetricType::WriteToStoreCalls);
    const auto blocks_written = m->get_counter(util::MetricType::WriteToStoreBlocksWritten);
    const auto bytes_written = m->get_counter(util::MetricType::WriteToStoreBytesWritten);
    const auto wts_total = m->get_timing_stats(util::MetricType::WriteToStoreTotal);
    const auto io_cb = m->get_timing_stats(util::MetricType::WriteToStoreIoCallback);
    const auto prep = m->get_timing_stats(util::MetricType::WriteToStorePrepareBatch);

    auto fdatasync_count = m->get_counter(util::MetricType::FdatasyncCount);
    auto fsync_count = m->get_counter(util::MetricType::FsyncCount);
    auto fdatasync_stats = m->get_timing_stats(util::MetricType::FdatasyncTiming);
    auto fsync_stats = m->get_timing_stats(util::MetricType::FsyncTiming);

    auto producer_lat = m->get_timing_stats(util::MetricType::ProducerLatency);

    std::println("Write-to-Store Metrics:");
    std::println("  calls={} blocks={} bytes={} avg_total={:.2f}us avg_io={:.2f}us avg_prep={:.2f}us",
                 write_calls, blocks_written, bytes_written,
                 wts_total.m_count ? static_cast<double>(wts_total.m_avg_ns) / 1000.0 : 0.0,
                 io_cb.m_count ? static_cast<double>(io_cb.m_avg_ns) / 1000.0 : 0.0,
                 prep.m_count ? static_cast<double>(prep.m_avg_ns) / 1000.0 : 0.0);

    std::println("Sync Metrics: fdatasync.count={} avg={:.2f}us fsync.count={} avg={:.2f}us",
                 fdatasync_count,
                 fdatasync_stats.m_count ? static_cast<double>(fdatasync_stats.m_avg_ns) / 1000.0 : 0.0,
                 fsync_count,
                 fsync_stats.m_count ? static_cast<double>(fsync_stats.m_avg_ns) / 1000.0 : 0.0);

    if (producer_lat.m_count > 0) {
      std::println("Producer Latency: count={} avg={:.2f}us min={:.2f}us max={:.2f}us",
                   producer_lat.m_count,
                   static_cast<double>(producer_lat.m_avg_ns) / 1000.0,
                   static_cast<double>(producer_lat.m_min_ns) / 1000.0,
                   static_cast<double>(producer_lat.m_max_ns) / 1000.0);
    }

    if (config.m_verbosity >= 1) {
      const auto print_hist = [&](util::MetricType t, std::string_view label) {
        const auto& timings = m->get_timings(t);
        if (timings.empty()) {
          std::println("  {}: no samples", label);
          return;
        }
        auto stats = m->get_timing_stats(t);
        std::println("  {}: count={} min={}ns max={}ns avg={:.2f}ns", label, stats.m_count,
                     stats.m_min_ns, stats.m_max_ns, static_cast<double>(stats.m_avg_ns));
        if (config.m_verbosity >= 2) {
          auto hist = util::Metrics::generate_timing_histogram(timings, 20);
          std::println("    histogram:");
          for (const auto& [range, count] : hist) {
            double pct = (100.0 * static_cast<double>(count) / static_cast<double>(stats.m_count));
            std::println("      {}: {} ({:.2f}%)", range, count, pct);
          }
        }
      };

      std::println("Detailed Timings:");
      print_hist(util::MetricType::WriteToStoreTotal, "write_to_store.total");
      print_hist(util::MetricType::WriteToStoreIoCallback, "write_to_store.io_callback");
      print_hist(util::MetricType::WriteToStorePrepareBatch, "write_to_store.prepare_batch");

      if (fdatasync_count > 0) {
        print_hist(util::MetricType::FdatasyncTiming, "fdatasync.timing");
      }
      if (fsync_count > 0) {
        print_hist(util::MetricType::FsyncTiming, "fsync.timing");
      }
      if (producer_lat.m_count > 0) {
        print_hist(util::MetricType::ProducerLatency, "producer.latency");
      }
    }
  }

  return Test_run_result{.m_ops_per_sec = ops_per_sec, .m_sync_write_count = sync_write_count};
}

static void test_throughput_actor_model(const Test_config& config) {
  std::println("[test_throughput_actor_model] producers={}, consumers={}, messages={}, iterations={}",
               config.m_num_producers, config.m_num_consumers, config.m_num_messages, config.m_num_iterations);

  double total_ops_per_sec = 0.0;
  double min_ops_per_sec = std::numeric_limits<double>::max();
  double max_ops_per_sec = 0.0;
  std::size_t total_sync_write_count = 0;

  for (std::size_t run = 0; run < config.m_num_iterations; ++run) {
    auto result = test_throughput_actor_model_single_run(config);
    total_ops_per_sec += result.m_ops_per_sec;
    min_ops_per_sec = std::min(min_ops_per_sec, result.m_ops_per_sec);
    max_ops_per_sec = std::max(max_ops_per_sec, result.m_ops_per_sec);
    total_sync_write_count += result.m_sync_write_count;
  }

  const double avg_ops_per_sec = total_ops_per_sec / static_cast<double>(config.m_num_iterations);
  const std::size_t total_messages = config.m_num_messages;
  const double avg_elapsed_s = static_cast<double>(total_messages) / avg_ops_per_sec;
  const double throughput_bytes_s = avg_ops_per_sec * wal::Test_message::SIZE;

  /* Format throughput with appropriate units (KiB/s, MiB/s, GiB/s) */
  const char* throughput_unit = "B/s";
  double throughput_value = throughput_bytes_s;
  if (throughput_bytes_s >= 1024.0 * 1024.0 * 1024.0) {
    throughput_value = throughput_bytes_s / (1024.0 * 1024.0 * 1024.0);
    throughput_unit = "GiB/s";
  } else if (throughput_bytes_s >= 1024.0 * 1024.0) {
    throughput_value = throughput_bytes_s / (1024.0 * 1024.0);
    throughput_unit = "MiB/s";
  } else if (throughput_bytes_s >= 1024.0) {
    throughput_value = throughput_bytes_s / 1024.0;
    throughput_unit = "KiB/s";
  }

  /* Format ops with appropriate units (K/s, M/s, G/s) */
  const char* ops_unit = "ops/s";
  double ops_value = avg_ops_per_sec;
  if (avg_ops_per_sec >= 1'000'000'000.0) {
    ops_value = avg_ops_per_sec / 1'000'000'000.0;
    ops_unit = "G ops/s";
  } else if (avg_ops_per_sec >= 1'000'000.0) {
    ops_value = avg_ops_per_sec / 1'000'000.0;
    ops_unit = "M ops/s";
  } else if (avg_ops_per_sec >= 1'000.0) {
    ops_value = avg_ops_per_sec / 1'000.0;
    ops_unit = "K ops/s";
  }

  std::println("[test_throughput_actor_model] total_messages={}, iterations={}, elapsed={:.3f}s, throughput={:.2f} {}, ops={:.2f} {} (min={:.2f}, max={:.2f})",
               total_messages, config.m_num_iterations, avg_elapsed_s, throughput_value, throughput_unit,
               ops_value, ops_unit, min_ops_per_sec / 1'000'000.0, max_ops_per_sec / 1'000'000.0);
  
  /* Print sync write count */
  if (total_sync_write_count > 0) {
    std::println("[test_throughput_actor_model] sync_writes={} (no free buffer available)", total_sync_write_count);
  }
}

static void print_usage(const char* program_name) noexcept {
  std::println(stderr,
               "Usage: {} [OPTIONS]\n"
               "\n"
               "Options:\n"
               "  -M, --mailbox-size NUM   Mailbox size (default: 256)\n"
               "  -m, --messages NUM       Number of messages per iteration (default: 1000000)\n"
               "  -i, --iterations NUM     Number of iterations to average (default: 1)\n"
               "  -p, --producers NUM      Number of producer actors (default: 1)\n"
               "  -d, --disable-writes     Disable actual disk writes (default: off)\n"
               "  -w, --disable-log-writes Disable log->append() calls entirely (default: off)\n"
               "  -S, --skip-memcpy        Skip memcpy in write operation (default: off)\n"
               "  -X, --disable-metrics    Disable metrics collection entirely (default: off)\n"
               "      --no-producer-latency Disable producer latency tracking (most expensive metric)\n"
               "      --log-block-size NUM Size of each log block in bytes (default: 4096)\n"
               "      --log-buffer-blocks NUM Number of blocks in log buffer (default: 16384)\n"
               "      --pool-size NUM        Number of buffers in pool (default: 32, must be power of 2)\n"
               "      --io-queue-size NUM     Size of IO operations queue (default: pool_size * 2, must be power of 2)\n"
               "      --timeout-ms NUM     Timeout in milliseconds (0 disables, default: 3000)\n"
               "  -f, --fdatasync-interval NUM Send sync messages with probability NUM (0.0-1.0, e.g., 0.3 = 30%%, default: 0)\n"
               "      --use-fsync          Use fsync messages instead of fdatasync (default: fdatasync)\n"
               "  -b, --batch-dequeue      Enable batch dequeue mode (default: off)\n"
               "  -B, --batch-size NUM     Batch size for batch dequeue (default: 32)\n"
               "  -v, --verbose            Increase verbosity (metrics): repeat for more detail (e.g., -vv)\n"
               "  -h, --help                Show this help message\n",
               program_name);
}

int main(int argc, char** argv) {
  Test_config config;
  bool show_help = false;

  static const struct option long_options[] = {
    {"mailbox-size", required_argument, nullptr, 'M'},
    {"messages", required_argument, nullptr, 'm'},
    {"iterations", required_argument, nullptr, 'i'},
    {"producers", required_argument, nullptr, 'p'},
    {"disable-writes", no_argument, nullptr, 'd'},
    {"disable-log-writes", no_argument, nullptr, 'w'},
    {"skip-memcpy", no_argument, nullptr, 'S'},
    {"disable-metrics", no_argument, nullptr, 'X'},
    {"no-producer-latency", no_argument, nullptr, 1000},
    {"log-block-size", required_argument, nullptr, 'L'},
    {"log-buffer-blocks", required_argument, nullptr, 'R'},
    {"timeout-ms", required_argument, nullptr, 'T'},
    {"fdatasync-interval", required_argument, nullptr, 'f'},
    {"use-fsync", no_argument, nullptr, 1005},
    {"batch-dequeue", no_argument, nullptr, 'b'},
    {"batch-size", required_argument, nullptr, 'B'},
    {"help", no_argument, nullptr, 'h'},
    {nullptr, 0, nullptr, 0}
  };

  int opt;
  int option_index = 0;
  while ((opt = getopt_long(argc, argv, "M:m:i:p:hwdSXL:R:T:f:bB:v", long_options, &option_index)) != -1) {
    switch (opt) {
      case 'M':
        config.m_mailbox_size = std::stoull(optarg);
        break;
      case 'm':
        config.m_num_messages = std::stoull(optarg);
        break;
      case 'i':
        config.m_num_iterations = std::stoull(optarg);
        break;
      case 'p':
        config.m_num_producers = std::stoull(optarg);
        break;
      case 'd':
        config.m_disable_writes = true;
        break;
      case 'w':
        config.m_disable_log_writes = true;
        break;
      case 'S':
        config.m_skip_memcpy = true;
        break;
      case 'X':
        config.m_disable_metrics = true;
        break;
      case 1000:
        config.m_metrics_config.m_enable_producer_latency = false;
        break;
      case 'L':
        config.m_log_block_size = std::stoull(optarg);
        break;
      case 'R':
        config.m_log_buffer_size_blocks = std::stoull(optarg);
        break;
      case 1007:
        config.m_pool_size = std::stoull(optarg);
        break;
      case 1008:
        config.m_io_queue_size = std::stoull(optarg);
        break;
      case 'T':
        config.m_timeout_ms = std::stoull(optarg);
        break;
      case 'f': {
        /* Parse as double to support fractional values like 0.3 (30% probability) */
        double prob = std::stod(optarg);
        if (prob < 0.0 || prob > 1.0) {
          std::println(stderr, "Error: fdatasync-interval must be between 0 and 1 (got {})", prob);
          return EXIT_FAILURE;
        }
        if (prob <= 0.0) {
          /* 0 or negative means disabled */
          config.m_fdatasync_interval = 0;
        } else {
          /* Convert probability to interval: if prob = 0.3, we want 1/interval = 0.3, so interval = 1/0.3 ≈ 3.33 */
          /* Use ceiling to ensure we get at least the desired probability */
          config.m_fdatasync_interval = static_cast<std::size_t>(std::ceil(1.0 / prob));
        }
        break;
      }
      case 1005:
        config.m_sync_type = Sync_message_type::Fsync;
        break;
      case 'b':
        config.m_enable_batch_dequeue = true;
        break;
      case 'B':
        config.m_batch_size = std::stoull(optarg);
        break;
      case 'v':
        ++config.m_verbosity;
        break;
      case 'h':
        show_help = true;
        break;
      default:
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }
  }

  if (show_help) {
    print_usage(argv[0]);
    return EXIT_SUCCESS;
  }

  test_throughput_actor_model(config);
  return EXIT_SUCCESS;
}
