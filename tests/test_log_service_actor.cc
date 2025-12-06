#include <cassert>
#include <cstring>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <getopt.h>
#include <unistd.h>
#include <sys/uio.h>
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
  std::size_t m_timeout_ms{5000};

  /** Send fdatasync/fsync with probability 1/N (0 = disabled, calculated from probability) */
  std::size_t m_fdatasync_interval{0};

  /** Type of sync message to send (default: Fdatasync) */
  Sync_message_type m_sync_type{Sync_message_type::Fdatasync};

  /** Disable metrics collection entirely (overrides Metrics_config) */
  bool m_disable_metrics{false};

  /** Fine-grained control over which metrics to collect */
  Metrics_config m_metrics_config{};
};

/**
 * Erlang-style Actor Model for Log Service
 * - Each producer/consumer has its own SPSC mailbox
 * - Processes self-schedule into thread mailboxes when they have messages
 * - Consumer is a regular thread (not coroutine) that writes to WAL (like test_log_io_simple producer)
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
};

/* Forward declaration */
struct Log_message_processor;

struct Consumer_context {
  std::shared_ptr<wal::Log> m_log;
  Log_writer m_log_writer;
  std::atomic<bool>* m_consumer_done;
  bool m_disable_log_writes;
  util::Thread_pool* m_consumer_pool;
  util::Thread_pool* m_io_pool;
  Process_type* m_consumer_process;  /* Consumer's mailbox for poison pills */
  Scheduler_type* m_sched;
  Log_message_processor* m_processor;  /* Pass by pointer so we can call write_and_fdatasync/fsync */
  std::size_t m_num_producers;
  std::chrono::milliseconds m_batch_timeout{std::chrono::milliseconds(10)};
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
      m_log_file(log_file) {
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

  void write_and_fdatasync() {
    if (m_log && m_io_pool && m_log->m_write_callback) {
      if (m_log->m_pool && m_log->m_pool->m_active && !m_log->m_pool->m_active->m_buffer.is_empty()) {
        m_log->m_pool->prepare_buffer_for_io(m_log->m_pool->m_active, *m_io_pool, m_log->m_write_callback);
      }
      
      auto sync_callback = [this](std::span<struct iovec> span, wal::Log::Sync_type) -> wal::Result<std::size_t> {
        return m_log->m_write_callback(span, wal::Log::Sync_type::Fdatasync);
      };

      [[maybe_unused]] auto result = m_log->write_to_store(sync_callback);
      WAL_ASSERT(result.has_value());
      
      if (m_log->m_pool && m_log->m_pool->m_active && !m_log->m_pool->m_active->m_buffer.is_empty() && m_log->m_pool->m_active->m_buffer.is_write_pending()) {
        auto active_sync_callback = [this](std::span<struct iovec> span, wal::Log::Sync_type) -> wal::Result<std::size_t> {
          return m_log->m_write_callback(span, wal::Log::Sync_type::Fdatasync);
        };

        [[maybe_unused]] auto active_result = m_log->m_pool->m_active->m_buffer.write_to_store(active_sync_callback, 0, wal::Log::Sync_type::Fdatasync);
        WAL_ASSERT(active_result.has_value());
      }
      
      if (m_log_file && m_log_file->m_fd != -1 && m_metrics) {
        auto sync_start = Clock::now();
        if (::fdatasync(m_log_file->m_fd) == 0) {
          auto sync_end = Clock::now();
          auto sync_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(sync_end - sync_start);
          m_metrics->inc(util::MetricType::FdatasyncCount);
          m_metrics->add_timing(util::MetricType::FdatasyncTiming, sync_duration);
        }
      }
    }
  }

  void write_and_fsync() {
    if (m_log && m_io_pool && m_log->m_write_callback) {
      if (m_log->m_pool && m_log->m_pool->m_active && !m_log->m_pool->m_active->m_buffer.is_empty()) {
        m_log->m_pool->prepare_buffer_for_io(m_log->m_pool->m_active, *m_io_pool, m_log->m_write_callback);
      }
      
      auto sync_callback = [this](std::span<struct iovec> span, wal::Log::Sync_type) -> wal::Result<std::size_t> {
        return m_log->m_write_callback(span, wal::Log::Sync_type::Fsync);
      };

      [[maybe_unused]] auto result = m_log->write_to_store(sync_callback);
      WAL_ASSERT(result.has_value());
      
      if (m_log->m_pool && m_log->m_pool->m_active && !m_log->m_pool->m_active->m_buffer.is_empty() && m_log->m_pool->m_active->m_buffer.is_write_pending()) {
        auto active_sync_callback = [this](std::span<struct iovec> span, wal::Log::Sync_type) -> wal::Result<std::size_t> {
          return m_log->m_write_callback(span, wal::Log::Sync_type::Fsync);
        };

        [[maybe_unused]] auto active_result = m_log->m_pool->m_active->m_buffer.write_to_store(active_sync_callback, 0, wal::Log::Sync_type::Fsync);
        WAL_ASSERT(active_result.has_value());
      }
      
      if (m_log_file && m_log_file->m_fd != -1 && m_metrics) {
        auto sync_start = Clock::now();
        if (::fsync(m_log_file->m_fd) == 0) {
          auto sync_end = Clock::now();
          auto sync_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(sync_end - sync_start);
          m_metrics->inc(util::MetricType::FsyncCount);
          m_metrics->add_timing(util::MetricType::FsyncTiming, sync_duration);
        }
      }
    }
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
};

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

  /* Use cheaper RNG seed - std::random_device is expensive */
  std::mt19937 rng(static_cast<uint32_t>(reinterpret_cast<uintptr_t>(ctx.m_proc) ^ static_cast<uintptr_t>(std::chrono::steady_clock::now().time_since_epoch().count())));
  std::uniform_int_distribution<std::size_t> dist(1, ctx.m_fdatasync_interval > 0 ? ctx.m_fdatasync_interval : 1);

  /* Send messages, randomly sending sync messages if interval is set */
  for (std::size_t i = 0; i < num_items; ++i) {
    util::Message_envelope<Payload_type> env;
    env.m_sender = ctx.m_self;
    /* Only record send time if latency tracking is enabled - avoids expensive clock call */
    if (ctx.m_track_latency) [[unlikely]] {
      env.m_send_time = Clock::now();
    }

    /* Send sync message randomly with 1/fdatasync_interval probability if interval is set */
    if (ctx.m_fdatasync_interval > 0 && dist(rng) == 1) {
      if (ctx.m_sync_type == Sync_message_type::Fdatasync) {
        env.m_payload = wal::Fdatasync{};
      } else {
        env.m_payload = wal::Fsync{};
      }
    } else {
      /* Direct construct in variant to avoid extra copy */
      env.m_payload = wal::Test_message{};
    }

    if (ctx.m_proc->m_mailbox.enqueue(std::move(env))) [[likely]] {
      continue;
    }

    /* Slow path: queue full - recreate env since it was moved */
    do {
      ctx.m_proc->schedule_self(ctx.m_sched->m_mailboxes, n_mailboxes);
      util::cpu_pause();
    } while (!ctx.m_proc->m_mailbox.enqueue(std::move(env)));
  }

  util::Message_envelope<Payload_type> term_env{.m_payload = util::Poison_pill{}, .m_sender = ctx.m_self};

  while (!ctx.m_proc->m_mailbox.enqueue(std::move(term_env))) {
    ctx.m_proc->schedule_self(ctx.m_sched->m_mailboxes, n_mailboxes);
    util::cpu_pause();
  }

  /* Schedule self after enqueueing poison pill so consumer can detect it */
  ctx.m_proc->schedule_self(ctx.m_sched->m_mailboxes, n_mailboxes);

  co_return;
}

/* Consumer coroutine - runs on thread pool and writes to log */
Task<void> consumer_actor(
  Consumer_context ctx,
  std::atomic<bool>& start_flag
) noexcept {
  co_await ctx.m_sched->m_consumer_pool->schedule();
 
  while (!start_flag.load(std::memory_order_acquire)) {
    util::cpu_pause();
  }
 
  std::size_t completed_producers = 0;
  util::Message_envelope<Payload_type> msg;
  std::size_t no_work_iterations = 0;
 
  constexpr std::size_t bulk_read_size = 64;
  std::array<std::size_t, bulk_read_size> notification_buffer;
 
  while (completed_producers < ctx.m_num_producers) {
 
    /* Check for poison pills in consumer mailbox (if provided) */
    if (ctx.m_consumer_process != nullptr) {
      while (ctx.m_consumer_process->m_mailbox.dequeue(msg)) {
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
 
    util::Process<Payload_type>* proc = nullptr;
    bool found_work = false;
 
    std::size_t num_notifications = 0;
    std::size_t notified_mailbox_idx = 0;
    
    while (num_notifications < bulk_read_size && ctx.m_sched->m_mailboxes->m_notify_queue->dequeue(notified_mailbox_idx)) {
      notification_buffer[num_notifications++] = notified_mailbox_idx;
    }
 
    for (std::size_t i = 0; i < num_notifications; ++i) {
      std::size_t mailbox_idx = notification_buffer[i];
 
      auto process_mbox = ctx.m_sched->m_mailboxes->get_for_process(mailbox_idx);

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
 
            if (completed_producers >= ctx.m_num_producers) {
              loop_exited_normally = false;
              break;
            }
          } else {
            /* Check for Fdatasync */
            if (std::holds_alternative<wal::Fdatasync>(msg.m_payload)) {
              /* Flush batch and call fdatasync */
              if constexpr (requires { ctx.m_processor->write_and_fdatasync(); }) {
                ctx.m_processor->write_and_fdatasync();
              }
              continue;
            }
            /* Check for Fsync */
            if (std::holds_alternative<wal::Fsync>(msg.m_payload)) {
              /* Flush batch and call fsync */
              if constexpr (requires { ctx.m_processor->write_and_fsync(); }) {
                ctx.m_processor->write_and_fsync();
              }
              continue;
            }
            /* Regular payload message */
            if (std::holds_alternative<Payload_type>(msg.m_payload)) {
              process_payload_message(*ctx.m_processor, std::get<Payload_type>(msg.m_payload), msg);
            }
          }
        }
        
        if (completed_producers >= ctx.m_num_producers) {
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
        if (p == nullptr) {
          continue;
        }
        
        /* Skip consumer process mailbox - producers don't send poison pills there */
        if (ctx.m_consumer_process && p == ctx.m_consumer_process) {
          continue;
        }
 
        while (p->m_mailbox.dequeue(msg)) {
          if (std::holds_alternative<util::Poison_pill>(msg.m_payload)) [[unlikely]] {
            ++completed_producers;
            if (completed_producers >= ctx.m_num_producers) {
              break;
            }
          } else {
            /* Check for Fdatasync */
            if (std::holds_alternative<wal::Fdatasync>(msg.m_payload)) {
              if constexpr (requires { ctx.m_processor->write_and_fdatasync(); }) {
                ctx.m_processor->write_and_fdatasync();
              }
              continue;
            }
            /* Regular payload message */
            if (std::holds_alternative<Payload_type>(msg.m_payload)) {
              process_payload_message(*ctx.m_processor, std::get<Payload_type>(msg.m_payload), msg);
            }
          }
        }
        
        if (completed_producers >= ctx.m_num_producers) {
          break;
        }
      }
    }
 
    if (!found_work) [[unlikely]] {
      util::cpu_pause();
    }
  }

  if (ctx.m_consumer_done) {
    ctx.m_consumer_done->store(true, std::memory_order_release);
  }
  co_return;
}

static double test_throughput_actor_model_single_run(const Test_config& config) {
  const std::size_t total_messages = config.m_num_messages;
  const std::size_t producers = config.m_num_producers;
  const std::size_t base_per_producer = producers == 0 ? 0 : total_messages / producers;
  const std::size_t producer_remainder = producers == 0 ? 0 : total_messages % producers;

  std::string path = config.m_disable_writes ? "/dev/null" : ("/local/tmp/test_actor.log");
  auto log_file = std::make_shared<wal::Log_file>(path, 512 * 1024 * 1024, config.m_log_block_size, config.m_disable_writes);

  WAL_ASSERT(log_file->m_fd != -1);

  if (!config.m_disable_writes) {
    auto ret = ::posix_fallocate(log_file->m_fd, 0, log_file->m_max_file_size);
    if (ret != 0) {
      log_fatal("Failed to preallocate log file: {}", std::strerror(errno));
    }
  }

  /* Use smaller buffers but more of them for better parallelism, when we have small writes and frequent syncs.
   * If buffer size is large (e.g., 16384 blocks), use more smaller buffers.
   * Aim for ~4-8MB per buffer instead of 64MB */
  const std::size_t target_buffer_size_blocks = std::min(config.m_log_buffer_size_blocks, std::size_t(2048));
  const std::size_t buffer_pool_size = std::max(std::size_t(8), (config.m_log_buffer_size_blocks + target_buffer_size_blocks - 1) / target_buffer_size_blocks);
  /* Ensure pool size is a power of 2 */
  const std::size_t log2_pool_size = static_cast<std::size_t>(std::ceil(std::log2(static_cast<double>(buffer_pool_size))));
  const std::size_t buffer_pool_size_pow2 = std::size_t(1) << log2_pool_size;
  auto wal_config = wal::Config(target_buffer_size_blocks, config.m_log_block_size, kChecksumAlgorithm);
  auto log = std::make_shared<wal::Log>(0, buffer_pool_size_pow2, wal_config);

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

  /* Store log_file in a way that write_and_fdatasync/fsync can access it.
   * We'll pass it through the Log_message_processor context */
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
    (std::span<struct iovec> span, wal::Log::Sync_type sync_type) -> wal::Result<std::size_t> {
    
    auto result = log_file_wrapper->m_log_file->write(span);
    
    if (result.has_value() && !log_file_wrapper->m_disable_writes) {
      bool sync_succeeded = false;
      
      if (sync_type == wal::Log::Sync_type::Fdatasync) [[likely]] {
        auto sync_start = Clock::now();
        if (::fdatasync(log_file_wrapper->m_log_file->m_fd) == 0) {
          sync_succeeded = true;
        } else {
          log_err("fdatasync failed: {}", std::strerror(errno));
          return std::unexpected(wal::Status::IO_error);
        }
        auto sync_end = Clock::now();
        auto sync_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(sync_end - sync_start);
        
        if (log_file_wrapper->m_metrics_ptr != nullptr) {
          log_file_wrapper->m_metrics_ptr->inc(util::MetricType::FdatasyncCount);
          log_file_wrapper->m_metrics_ptr->add_timing(util::MetricType::FdatasyncTiming, sync_duration);
        }
      } else if (sync_type == wal::Log::Sync_type::Fsync) {
        auto sync_start = Clock::now();
        if (::fsync(log_file_wrapper->m_log_file->m_fd) == 0) {
          sync_succeeded = true;
        } else {
          log_err("fsync failed: {}", std::strerror(errno));
          return std::unexpected(wal::Status::IO_error);
        }
        auto sync_end = Clock::now();
        auto sync_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(sync_end - sync_start);
        
        if (log_file_wrapper->m_metrics_ptr != nullptr) {
          log_file_wrapper->m_metrics_ptr->inc(util::MetricType::FsyncCount);
          log_file_wrapper->m_metrics_ptr->add_timing(util::MetricType::FsyncTiming, sync_duration);
        }
      }
      
      if (sync_succeeded && !span.empty()) {
        const auto n_blocks = span.size() / 3;
        if (n_blocks > 0) {
          const auto block_header = reinterpret_cast<const wal::Block_header*>(span[0].iov_base);
          const auto block_no = block_header->get_block_no();
          const off_t length = static_cast<off_t>(n_blocks * log_file_wrapper->m_log_file->m_block_size);
          const off_t phy_off = static_cast<off_t>(block_no * log_file_wrapper->m_log_file->m_block_size % static_cast<std::size_t>(log_file_wrapper->m_log_file->m_max_file_size));
          
          if (::posix_fadvise(log_file_wrapper->m_log_file->m_fd, phy_off, length, POSIX_FADV_DONTNEED) != 0) {
            log_warn("posix_fadvise(DONTNEED) failed: {}", std::strerror(errno));
          }
        }
      }
    }
    
    return result;
  };

  wal::Log_service_setup<Payload_type, Scheduler_type> service_setup(producers);

  std::vector<util::Pid> producer_pids;
  for (std::size_t i = 0; i < producers; ++i) {
    producer_pids.push_back(service_setup.m_sched.spawn(config.m_mailbox_size));
  }

  /* Create consumer process for poison pills */
  util::Pid consumer_pid = service_setup.m_sched.spawn(config.m_mailbox_size);
  auto consumer_process = service_setup.m_sched.get_process(consumer_pid);

  std::atomic<bool> start_flag{false};
  std::atomic<bool> consumer_done{false};

  Consumer_context consumer_ctx{
    .m_log = log,
    .m_log_writer = log_writer,
    .m_consumer_done = &consumer_done,
    .m_disable_log_writes = config.m_disable_log_writes,
    .m_consumer_pool = &service_setup.m_consumer_pool,
    .m_io_pool = &service_setup.m_io_pool,
    .m_consumer_process = consumer_process,
    .m_sched = nullptr,  /* Will be set after processor is created */
    .m_processor = nullptr,  /* Will be set after processor is created */
    .m_num_producers = 0,  /* Will be set after processor is created */
    .m_batch_timeout = std::chrono::milliseconds(10)
  };

  /* Start the background I/O coroutine to process buffers on dedicated I/O pool */
  if (!config.m_disable_log_writes) {
    log->start_io(log_writer, &service_setup.m_io_pool);
    /* Brief pause to ensure I/O coroutine is scheduled and ready */
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  Log_message_processor processor(
    consumer_ctx.m_log,
    consumer_ctx.m_io_pool,
    consumer_ctx.m_disable_log_writes,
    config.m_disable_metrics ? nullptr : &metrics,  /* Pass metrics only if not disabled */
    metrics_config_ptr,  /* Pass metrics config for fine-grained control */
    log_file  /* Pass log_file for direct sync operations */
  );

  assert(config.m_num_consumers == 1);

  Log_message_processor msg_processor(
    consumer_ctx.m_log,
    consumer_ctx.m_io_pool,
    consumer_ctx.m_disable_log_writes,
    config.m_disable_metrics ? nullptr : &metrics,  /* Pass metrics only if not disabled */
    metrics_config_ptr,  /* Pass metrics config for fine-grained control */
    log_file  /* Pass log_file for direct sync operations */
  );

  /* Set additional fields in consumer context */
  consumer_ctx.m_sched = &service_setup.m_sched;
  consumer_ctx.m_processor = &msg_processor;
  consumer_ctx.m_num_producers = producers;
  consumer_ctx.m_batch_timeout = std::chrono::milliseconds(10);  /* Flush batch every 10ms */

  /* Use simple lambda when writes are disabled, matching test_actor_model exactly */
  /* Pass processor by reference so timer can call write_and_fdatasync/fsync */
  detach(consumer_actor(consumer_ctx, std::ref(start_flag)));

  /* Launch producer actors (coroutines) */
  for (std::size_t p = 0; p < producers; ++p) {
    const std::size_t messages_for_this_producer = base_per_producer + (p < producer_remainder ? 1 : 0);

    Producer_context producer_ctx;
    producer_ctx.m_self = producer_pids[p];
    producer_ctx.m_proc = service_setup.m_sched.get_process(producer_pids[p]);
    producer_ctx.m_sched = &service_setup.m_sched;
    producer_ctx.m_fdatasync_interval = config.m_fdatasync_interval;
    producer_ctx.m_sync_type = config.m_sync_type;
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

  /* Wait for consumer to complete (event-based) */
  constexpr auto max_wait_time = std::chrono::seconds(600);
  auto wait_start = Clock::now();

  while (!consumer_done.load(std::memory_order_acquire)) {
    auto elapsed = Clock::now() - wait_start;
    if (elapsed > max_wait_time) {
      std::println(stderr, "ERROR: Timeout waiting for consumer to complete");
      break;
    }
    for (int j = 0; j < 10; ++j) {
      util::cpu_pause();
    }
  }

  /* Consumer coroutines will complete and signal via consumer_done flag */

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

    WAL_ASSERT(log->shutdown(log_writer).has_value());
    
    /* Clear the I/O callback to break circular reference before log goes out of scope */
    /* The callback lambda captures metrics_ptr, but Log also stores it, creating a cycle */
    /* Wait a bit more to ensure all I/O coroutines have finished */
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  
  /* Explicitly clear the log_writer lambda to break any remaining references */
  /* This ensures the lambda (which may capture log_file and metrics_ptr) is destroyed */
  log_writer = {};

  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  double elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;

  /* All messages were processed when consumer_done is true */
  const auto processed = total_messages;
  const auto ops_per_sec = elapsed_s == 0.0 ? 0.0 : static_cast<double>(processed) / elapsed_s;

  /* Print all metrics if enabled */
  if (!config.m_disable_metrics && log->get_metrics() != nullptr) {
    /* Print write-to-store metrics */
    log->get_metrics()->print("Write-to-Store Metrics:");
    
    /* Explicitly print sync metrics even if count is 0 for visibility */
    auto fdatasync_count = log->get_metrics()->get_counter(util::MetricType::FdatasyncCount);
    auto fsync_count = log->get_metrics()->get_counter(util::MetricType::FsyncCount);
    auto consumer_sync_count = log->get_metrics()->get_counter(util::MetricType::ConsumerSyncCount);
    std::println("\nSync Operations:");
    std::println("  fdatasync.count: {}", fdatasync_count);
    std::println("  fsync.count: {}", fsync_count);
    std::println("  consumer_sync.count: {}", consumer_sync_count);
    
    if (fdatasync_count > 0) {
      auto fdatasync_stats = log->get_metrics()->get_timing_stats(util::MetricType::FdatasyncTiming);
      if (fdatasync_stats.m_count > 0) {
        std::println("  fdatasync.timing: count: {} min: {}ns max: {}ns avg: {:.2f}ns total: {}ns",
                     fdatasync_stats.m_count, fdatasync_stats.m_min_ns, fdatasync_stats.m_max_ns,
                     static_cast<double>(fdatasync_stats.m_avg_ns), fdatasync_stats.m_sum_ns);
        const auto& timings = log->get_metrics()->get_timings(util::MetricType::FdatasyncTiming);
        if (!timings.empty()) {
          auto histogram = util::Metrics::generate_timing_histogram(timings, 20);
          std::println("    histogram:");
          for (const auto& [range, count] : histogram) {
            double percentage = (100.0 * static_cast<double>(count) / static_cast<double>(fdatasync_stats.m_count));
            std::println("      {}: {} ({:.2f}%)", range, count, percentage);
          }
        }
      } else {
        std::println("  fdatasync.timing: no timing data collected");
      }
    }
    
    if (fsync_count > 0) {
      auto fsync_stats = log->get_metrics()->get_timing_stats(util::MetricType::FsyncTiming);
      if (fsync_stats.m_count > 0) {
        std::println("  fsync.timing: count: {} min: {}ns max: {}ns avg: {:.2f}ns total: {}ns",
                     fsync_stats.m_count, fsync_stats.m_min_ns, fsync_stats.m_max_ns,
                     static_cast<double>(fsync_stats.m_avg_ns), fsync_stats.m_sum_ns);
        const auto& timings = log->get_metrics()->get_timings(util::MetricType::FsyncTiming);
        if (!timings.empty()) {
          auto histogram = util::Metrics::generate_timing_histogram(timings, 20);
          std::println("    histogram:");
          for (const auto& [range, count] : histogram) {
            double percentage = (100.0 * static_cast<double>(count) / static_cast<double>(fsync_stats.m_count));
            std::println("      {}: {} ({:.2f}%)", range, count, percentage);
          }
        }
      } else {
        std::println("  fsync.timing: no timing data collected");
      }
    }
    
    /* Print producer latency histogram */
    auto latency_stats = log->get_metrics()->get_timing_stats(util::MetricType::ProducerLatency);
    if (latency_stats.m_count > 0) {
      std::println("\nProducer Latency Statistics:");
      std::println("  Total requests: {}", latency_stats.m_count);
      std::println("  Min latency: {} ns ({:.3f} us)", latency_stats.m_min_ns, static_cast<double>(latency_stats.m_min_ns) / 1000.0);
      std::println("  Max latency: {} ns ({:.3f} us)", latency_stats.m_max_ns, static_cast<double>(latency_stats.m_max_ns) / 1000.0);
      std::println("  Avg latency: {:.2f} ns ({:.3f} us)", static_cast<double>(latency_stats.m_avg_ns), static_cast<double>(latency_stats.m_avg_ns) / 1000.0);
      
      /* Print latency histogram */
      const auto& timings = log->get_metrics()->get_timings(util::MetricType::ProducerLatency);
      if (!timings.empty()) {
        auto histogram = util::Metrics::generate_timing_histogram(timings, 20);
        std::println("\n  Producer Latency Histogram:");
        for (const auto& [range, count] : histogram) {
          double percentage = (100.0 * static_cast<double>(count) / static_cast<double>(latency_stats.m_count));
          std::println("    {}: {} ({:.2f}%)", range, count, percentage);
        }
      }
    }
  }

  return ops_per_sec;
}

static void test_throughput_actor_model(const Test_config& config) {
  std::println("[test_throughput_actor_model] producers={}, consumers={}, messages={}, iterations={}",
               config.m_num_producers, config.m_num_consumers, config.m_num_messages, config.m_num_iterations);

  double total_ops_per_sec = 0.0;
  double min_ops_per_sec = std::numeric_limits<double>::max();
  double max_ops_per_sec = 0.0;

  for (std::size_t run = 0; run < config.m_num_iterations; ++run) {
    double ops_per_sec = test_throughput_actor_model_single_run(config);
    total_ops_per_sec += ops_per_sec;
    min_ops_per_sec = std::min(min_ops_per_sec, ops_per_sec);
    max_ops_per_sec = std::max(max_ops_per_sec, ops_per_sec);
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
               "  -c, --consumers NUM       Number of consumer actors (default: 1)\n"
               "  -d, --disable-writes     Disable actual disk writes (default: off)\n"
               "  -w, --disable-log-writes Disable log->append() calls entirely (default: off)\n"
               "  -S, --skip-memcpy        Skip memcpy in write operation (default: off)\n"
               "  -X, --disable-metrics    Disable metrics collection entirely (default: off)\n"
               "      --no-producer-latency Disable producer latency tracking (most expensive metric)\n"
               "      --log-block-size NUM Size of each log block in bytes (default: 4096)\n"
               "      --log-buffer-blocks NUM Number of blocks in log buffer (default: 16384)\n"
               "      --timeout-ms NUM     Timeout in milliseconds (0 disables, default: 3000)\n"
               "  -f, --fdatasync-interval NUM Send sync messages with probability NUM (0.0-1.0, e.g., 0.3 = 30%%, default: 0)\n"
               "      --use-fsync          Use fsync messages instead of fdatasync (default: fdatasync)\n"
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
    {"consumers", required_argument, nullptr, 'c'},
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
    {"help", no_argument, nullptr, 'h'},
    {nullptr, 0, nullptr, 0}
  };

  int opt;
  int option_index = 0;
  while ((opt = getopt_long(argc, argv, "M:m:i:p:c:hwdSXL:R:T:f:", long_options, &option_index)) != -1) {
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
      case 'c':
        config.m_num_consumers = std::stoull(optarg);
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
