#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <random>
#include <span>

#include "wal/wal.h"

namespace {

struct Null_writer {
  [[nodiscard]] wal::Result<wal::lsn_t>
  operator()(wal::lsn_t lsn, const wal::Log::IO_vecs &iovecs) const noexcept {
    std::size_t data_bytes{};

    for (std::size_t i = 1; i < iovecs.size(); i += 3) {
      data_bytes += iovecs[i].iov_len;
    }

    return wal::Result<wal::lsn_t>(lsn + data_bytes);
  }
};

} // namespace

int main() {
  std::fprintf(stderr, "[wal_perf] start\n");

  constexpr std::size_t kBlocks = 512;
  constexpr std::size_t kBlockSize = 4096;
  wal::Circular_buffer::Config config(kBlocks, kBlockSize);

  wal::Log log(0, config);
  wal::Log::Write_callback null_writer = Null_writer{};

  constexpr std::size_t kIterationsSingle = 100'000;
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> len_dist(12, 256);
  std::uniform_int_distribution<int> byte_dist(0, 255);
  std::array<std::byte, 256> buffer{};

  const auto start_single = std::chrono::steady_clock::now();

  std::size_t total_bytes_single{};

  for (std::size_t i = 0; i < kIterationsSingle; ++i) {
    const auto len = static_cast<std::uint16_t>(len_dist(rng));

    for (int j = 0; j < len; ++j) {
      buffer[static_cast<std::size_t>(j)] = static_cast<std::byte>(byte_dist(rng));
    }

    auto slot = log.reserve(len);
    auto data_span = std::span<const std::byte>(buffer.data(), static_cast<std::size_t>(len));

    do {
      auto result = log.write(slot, data_span);

      if (!result.has_value()) {
        assert(result.error() == wal::Status::Not_enough_space);

        data_span = data_span.subspan(len - slot.m_len, slot.m_len);

        auto flush_result = log.m_buffer.write_to_store(null_writer);
        assert(flush_result.has_value());
      }
    } while (slot.m_len > 0);

    total_bytes_single += len;
  }

  assert(log.m_buffer.m_n_pending_writes.load(std::memory_order_relaxed) == 0);
  assert(log.m_buffer.m_written_lsn.load(std::memory_order_relaxed) == log.m_buffer.m_hwm.load(std::memory_order_relaxed));

  if (!log.m_buffer.is_empty()) {
    auto flush_result = log.m_buffer.write_to_store(null_writer);
    assert(flush_result.has_value());
  }

  const auto stop_single = std::chrono::steady_clock::now();
  const auto total_ns_single = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_single - start_single).count();
  const double elapsed_single_s = static_cast<double>(total_ns_single) / 1'000'000'000.0;
  const double mib_single = total_bytes_single / (1024.0 * 1024.0);
  const double throughput_single = (elapsed_single_s > 0.0) ? (mib_single / elapsed_single_s) : 0.0;

  std::fprintf(stderr,
               "[wal_perf.single] iterations=%zu bytes=%zu elapsed=%.3fs throughput=%.2f MiB/s\n",
               kIterationsSingle,
               total_bytes_single,
               elapsed_single_s,
               throughput_single);

  /*
  const std::size_t n_threads = std::max<std::size_t>(
    std::size_t{2},
    static_cast<std::size_t>(std::thread::hardware_concurrency()));
  util::Thread_pool pool(n_threads);

  wal::Circular_buffer::Config config(kBlocks, kBlockSize);

  wal::Log log(0, config);

  constexpr std::size_t kWriters = 8;
  constexpr std::size_t kIterationsPerWriter = 10'000;

  std::atomic<std::size_t> total_bytes{};
  std::atomic<std::size_t> total_writes{};
  std::mutex flush_mutex;
  std::latch done(kWriters);

  wal::Log::Write_callback null_writer = Null_writer{};

  const auto start = std::chrono::steady_clock::now();

  for (std::size_t i = 0; i < kWriters; ++i) {
    detach(writer_task(pool,
                       log,
                       total_bytes,
                       total_writes,
                       done,
                       flush_mutex,
                       null_writer,
                       kIterationsPerWriter,
                       static_cast<std::uint32_t>(i + 1U)));
  }

  done.wait();

  {
    std::lock_guard lk(flush_mutex);
    if (!log.m_buffer.is_empty()) {
      auto flush_result = log.m_buffer.write_to_store(null_writer);
      if (!flush_result.has_value()) {
        std::fprintf(stderr,
                     "[wal_perf] final flush error: %s\n",
                     wal::to_string(flush_result.error()).c_str());
        return EXIT_FAILURE;
      }
    }
    assert(!log.m_buffer.pending_writes());
  }

  const auto stop = std::chrono::steady_clock::now();

  const auto total_ns =
    std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
  const double elapsed_s = static_cast<double>(total_ns) / 1'000'000'000.0;

  const auto bytes = total_bytes.load(std::memory_order_relaxed);
  const auto writes = total_writes.load(std::memory_order_relaxed);
  const double mib = bytes / (1024.0 * 1024.0);
  const double throughput = (elapsed_s > 0.0) ? (mib / elapsed_s) : 0.0;

  std::fprintf(stderr,
               "[wal_perf] threads=%zu writers=%zu ops_per_writer=%zu "
               "bytes=%zu elapsed=%.3fs throughput=%.2f MiB/s\n",
               n_threads,
               kWriters,
               kIterationsPerWriter,
               bytes,
               elapsed_s,
               throughput);

  assert(writes == kWriters * kIterationsPerWriter);
  */

  return EXIT_SUCCESS;
}
