#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <string>
#include <span>
#include <algorithm>
#include <iterator>
#include <chrono>
#include <print>

#include "wal/wal.h"
#include "util/logger.h"
#include "util/thread_pool.h"

using wal::lsn_t;
using wal::Log;
using wal::Status;
using wal::block_no_t;
using wal::Buffer;
using Slot = Buffer::Slot;
using Record = std::vector<std::byte>;

util::Logger<util::MT_logger_writer> g_logger(util::MT_logger_writer{std::cerr}, util::Log_level::Trace);

/* Physical write function that simulates writing to storage without actually doing I/O.
 * This is called by Buffer::write_to_store as the Write_callback.
 * Returns the number of bytes that would have been written. */
static wal::Result<std::size_t> null_writer(std::span<struct iovec> span, wal::Log::Sync_type) {
  WAL_ASSERT(span.size() > 0);
  WAL_ASSERT(span.size() % 3 == 0);

  return span[1].iov_len * ((span.size() / 3));
}

/* Helper to call write_to_store synchronously in tests */
/* Pass nullptr to pool to execute I/O synchronously on the same thread */
static wal::Result<wal::lsn_t> write_to_store(Buffer& buffer, lsn_t max_write_lsn = 0) {
  return buffer.write_to_store([](std::span<struct iovec> span, wal::Log::Sync_type) -> wal::Result<wal::lsn_t> {
    return null_writer(span, wal::Log::Sync_type::None);
  }, max_write_lsn);
}

/* Helper to call shutdown synchronously in tests - unused but kept for API compatibility */
[[maybe_unused]] static wal::Result<bool> shutdown(Log& log, Buffer::Write_callback callback) {
  return log.shutdown(callback);
}

static void test_circular_buffer_basic() {
  std::println("[test_circular_buffer_basic] start");
  
  constexpr lsn_t initial_lsn = 0;
  Buffer::Config config(2, 512);
  Buffer buffer(initial_lsn, config);
  
  /* Verify initial state */
  assert(buffer.is_empty());
  assert(!buffer.is_full());
  assert(buffer.margin() == buffer.get_total_data_size());
  
  /* Test copy */
  Record data(100, std::byte{'A'});
  auto copy_result = buffer.append(std::as_bytes(std::span{data}));
  auto slot = copy_result.value();
  assert(slot.m_lsn == initial_lsn);
  assert(slot.m_len == 100);

  assert(buffer.m_lwm == initial_lsn);
  assert(buffer.m_hwm == initial_lsn + 100);
  assert(buffer.margin() == buffer.get_total_data_size() - 100);
  
  std::println("[test_circular_buffer_basic] done");
}

static void test_circular_buffer_multiple_writes() {
  std::println("[test_circular_buffer_multiple_writes] start");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t record_size = 64;  
  Buffer::Config config(2, 512);
  Buffer buffer(initial_lsn, config);
  const size_t n_records = (buffer.get_total_data_size() / record_size) + 1;
  
  for (size_t i = 0; i < n_records; ++i) {
    Record record(record_size, std::byte{static_cast<unsigned char>('A' + (i % 26))});
    [[maybe_unused]] auto copy_result = buffer.append(record);

    WAL_ASSERT(copy_result.has_value());
  }

  WAL_ASSERT(buffer.is_full());
  /* The last record will be partially written to the buffer (only half of the record size). */
  WAL_ASSERT(buffer.margin() == buffer.get_total_data_size() - (n_records * record_size - record_size / 2));
  
  auto ptr = buffer.data().data();;

  /* Verify the data written matches the contents of the data array */
  for (size_t i = 0; i < n_records - 1; ++i, ptr += record_size) {
    Record record(record_size, std::byte{static_cast<unsigned char>('A' + (i % 26))});
    WAL_ASSERT(::memcmp(ptr, record.data(), record.size()) == 0);
  }
  Record record(record_size / 2, std::byte{static_cast<unsigned char>('A' + ((n_records - 1) % 26))});
  WAL_ASSERT(::memcmp(ptr, record.data(), record.size()) == 0);

  std::println("[test_circular_buffer_multiple_writes] done");
}

static void test_circular_buffer_block_boundary() {
  std::println("[test_circular_buffer_block_boundary] start");
  
  constexpr lsn_t initial_lsn = 0;
  Buffer::Config config(2, 512);
  Buffer buffer(initial_lsn, config);
  const auto data_size = buffer.get_data_size_in_block();

  {
    /* Write exactly one data block worth of data */
    auto result = buffer.append(Record(data_size, std::byte{'X'}));
    auto slot = result.value();
    WAL_ASSERT(slot.m_lsn == initial_lsn);
    WAL_ASSERT(slot.m_len == data_size);
  }

  /* Check if buffer reports not full */
  WAL_ASSERT(!buffer.is_full());
  WAL_ASSERT(buffer.margin() == buffer.get_total_data_size() - data_size);

  {
    auto result = buffer.append(Record(data_size / 2, std::byte{'Y'}));
    auto slot = result.value();
    WAL_ASSERT(slot.m_lsn == initial_lsn + data_size);
    WAL_ASSERT(slot.m_len == data_size / 2);
  }

  /* Check if buffer reports full */
  WAL_ASSERT(!buffer.is_full());
  WAL_ASSERT(buffer.margin() == buffer.get_total_data_size() - data_size - data_size / 2);

  {
    auto result = buffer.append(Record(data_size / 2, std::byte{'Z'}));
    auto slot = result.value();
    WAL_ASSERT(slot.m_lsn == initial_lsn + data_size + data_size / 2);
    WAL_ASSERT(slot.m_len == data_size / 2);
  }

  WAL_ASSERT(buffer.is_full());
  WAL_ASSERT(buffer.margin() == 0);

  std::println("[test_circular_buffer_block_boundary] done");
}

static void test_circular_buffer_full() {
  std::println("[test_circular_buffer_full] start");
  
  constexpr lsn_t initial_lsn = 0;
  Buffer::Config config(2, 512);
  Buffer buffer(initial_lsn, config);
  const auto total_size = buffer.get_total_data_size();
  
  size_t written = 0;
  while (!buffer.is_full()) {
    auto copy_result = buffer.append(Record(100, std::byte{'F'}));
    auto slot = copy_result.value();
    WAL_ASSERT(slot.m_lsn == initial_lsn + written);
    WAL_ASSERT(slot.m_len == 100 || slot.m_len == total_size - written);
    written += slot.m_len;
  }

  WAL_ASSERT(buffer.is_full());
  WAL_ASSERT(buffer.margin() == 0);

  WAL_ASSERT(buffer.m_hwm == total_size);
  WAL_ASSERT(buffer.m_lwm == initial_lsn);
  WAL_ASSERT(buffer.m_hwm == initial_lsn + written);
  
  std::println("[test_circular_buffer_full] done (wrote {} bytes)", written);
}

static void test_circular_buffer_write_to_store() {
  std::println("[test_circular_buffer_write_to_store] start");
  
  const lsn_t initial_lsn = 0;
  Buffer::Config config(2, 512);
  Buffer buffer(initial_lsn, config);
  Record record(config.m_block_size, std::byte{'F'});

  std::size_t n_bytes_copied = 0;
  for (size_t i = 0; i < config.m_n_blocks; ++i) {
    auto span = std::as_bytes(std::span{record});

    for (;;) {
      auto result = buffer.append(span);

      if (!result.has_value() || result.value().m_len < span.size()) [[unlikely]] {
        WAL_ASSERT(result.has_value() || result.error() == Status::Not_enough_space);
        Slot slot;

        if (result.has_value()) {
          slot = result.value();
        } else {
          slot.m_len = 0;
          slot.m_lsn = buffer.m_hwm;
        }

        auto flush_result = write_to_store(buffer);

        WAL_ASSERT(buffer.is_empty());

        n_bytes_copied += slot.m_len;

        WAL_ASSERT(flush_result.has_value());
        WAL_ASSERT(flush_result.value() == n_bytes_copied);

        span = span.subspan(slot.m_len);
        continue;
      }

      n_bytes_copied += result.value().m_len;
      break;
    }
  }
  WAL_ASSERT(!buffer.is_full());
  WAL_ASSERT(buffer.is_write_pending());
  WAL_ASSERT(buffer.m_lwm == buffer.get_total_data_size());

  const auto spillover_data_size = n_bytes_copied % buffer.get_total_data_size();

  WAL_ASSERT(buffer.m_hwm - buffer.m_lwm == spillover_data_size);

  /* Write the remaining data to the store. */
  auto flush_result = write_to_store(buffer, buffer.m_hwm);

  WAL_ASSERT(flush_result.has_value());
  WAL_ASSERT(flush_result.value() == buffer.m_lwm);
  WAL_ASSERT(buffer.is_empty());
  WAL_ASSERT(!buffer.is_write_pending());
  WAL_ASSERT(buffer.m_lwm == buffer.m_hwm);

  std::println("[test_circular_buffer_write_to_store] done");
}

static void test_circular_buffer_nonzero_initial_lsn() {
  std::println("[test_circular_buffer_nonzero_initial_lsn] start");

  constexpr lsn_t initial_lsn = 1000000;
  Buffer::Config config(4, 512);
  using Record = std::vector<std::byte>;
  Buffer buffer(initial_lsn, config);
  const auto data_size = buffer.get_data_size_in_block();
  const auto expected_start_block = initial_lsn / data_size;
  auto block_header = buffer.get_block_header(expected_start_block % config.m_n_blocks);

  WAL_ASSERT(block_header.get_block_no() == expected_start_block);

  /* Write some data */
  auto reserve_result = buffer.append(Record(500, std::byte{'Z'}));
  auto slot = reserve_result.value();
  WAL_ASSERT(slot.m_lsn == initial_lsn);
  WAL_ASSERT(slot.m_len == 500);

  std::println("[test_circular_buffer_nonzero_initial_lsn] done");
}

static void test_log_basic() {
  std::println("[test_log_basic] start");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t pool_size = 4;
  Log::Config config(4, 512);
  Log log(initial_lsn, pool_size, config);
  
  log.start_io(null_writer, nullptr);

  /* Verify initial state */
  WAL_ASSERT(log.is_empty());
  WAL_ASSERT(!log.is_full());
  WAL_ASSERT(log.margin() > 0);
  
  /* Pass nullptr to pool for synchronous execution */
  auto done = shutdown(log, null_writer);
  WAL_ASSERT(done.has_value() && done.value());

  std::println("[test_log_basic] done");
}

static void test_log_write() {
  std::println("[test_log_write] start");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t pool_size = 4;
  Log::Config config(4, 512);
  Log log(initial_lsn, pool_size, config);
  
  log.start_io(null_writer, nullptr);

  /* Write data */
  Record data(100, std::byte{'A'});
  auto null_writer = [](std::span<struct iovec>, wal::Log::Sync_type) -> wal::Result<size_t> {
    return wal::Result<size_t>(0);
  };
  auto write_result = log.append(std::as_bytes(std::span{data}));
  WAL_ASSERT(write_result.has_value());
  auto slot = write_result.value();
  WAL_ASSERT(slot.m_len == 100);
  
  /* Verify state */
  WAL_ASSERT(!log.is_empty());

  auto done = shutdown(log, null_writer);
  WAL_ASSERT(done.has_value() && done.value());

  std::println("[test_log_write] done");
}

static void test_log_multiple_writes() {
  std::println("[test_log_multiple_writes] start");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t pool_size = 4;
  constexpr size_t record_size = 26;
  Log::Config config(4, 512);
  Log log(initial_lsn, pool_size, config);
  
  log.start_io(null_writer, nullptr);

  auto null_writer = [](std::span<struct iovec>, wal::Log::Sync_type) -> wal::Result<size_t> {
    return wal::Result<size_t>(0);
  };
  
  constexpr int num_writes = 10;
  for (int i = 0; i < num_writes; ++i) {
    Record data(record_size, std::byte{static_cast<unsigned char>('A' + i)});
    auto write_result = log.append(data);
    WAL_ASSERT(write_result.has_value());
  }
  
  /* Verify buffer state */
  WAL_ASSERT(!log.is_empty());

  auto done = shutdown(log, null_writer);
  WAL_ASSERT(done.has_value() && done.value());

  std::println("[test_log_multiple_writes] done");
}

static void test_circular_buffer_margin_edge_cases() {
  std::println("[test_circular_buffer_margin_edge_cases] start");

  constexpr lsn_t initial_lsn = 0;
  Buffer::Config config(4, 512);
  Buffer buffer(initial_lsn, config);
  const auto total_size = buffer.get_total_data_size();

  WAL_ASSERT(buffer.margin() == total_size);

  {
  /* Test 2: Single byte write */
    auto result = buffer.append(Record(1, std::byte{'X'}));
    auto slot = result.value();
    WAL_ASSERT(slot.m_len == 1);
    WAL_ASSERT(buffer.margin() == total_size - slot.m_len);
  }

  {
    const auto len = buffer.margin() - 1;
    auto result = buffer.append(Record(len, std::byte{'Y'}));
    auto slot = result.value();
    WAL_ASSERT(slot.m_len == len);
    WAL_ASSERT(buffer.margin() == 1);
  }

  {
    auto result = buffer.append(Record(buffer.margin(), std::byte{'Z'}));
    auto slot= result.value();
    WAL_ASSERT(slot.m_len == 1);
    WAL_ASSERT(buffer.is_full());
    WAL_ASSERT(buffer.margin() == 0);
  }

  {
    auto result = write_to_store(buffer);
    WAL_ASSERT(result.has_value());
    WAL_ASSERT(buffer.margin() == total_size);
  }

  std::println("[test_circular_buffer_margin_edge_cases] done");
}

static void test_circular_buffer_write_performance() {
  std::println("[test_circular_buffer_write_performance] start");

  constexpr std::size_t message_size = 32;
  constexpr std::size_t block_size = 4096;
  constexpr std::size_t buffer_blocks = 16384;

  Buffer::Config config(buffer_blocks, block_size);

  const auto data_size = config.get_data_size_in_block();
  const std::size_t num_messages = ((data_size * buffer_blocks) / (100'000 * message_size)) * 100'000;

  Buffer buffer(0, config);

  std::vector<std::byte> msg(message_size, std::byte{0x42});
  auto start = std::chrono::steady_clock::now();

  for (std::size_t i = 0; i < num_messages; ++i) {
    auto result = buffer.append(Record(message_size, std::byte{'A'}));
    WAL_ASSERT(result.has_value());
    WAL_ASSERT(result.value().m_len == message_size);
  }

  auto end = std::chrono::steady_clock::now();

  auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  double elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;

  const auto total_bytes = num_messages * message_size;
  const auto throughput_mib_s = (static_cast<double>(total_bytes) / elapsed_s) / (1024.0 * 1024.0);
  const auto ops_per_sec = static_cast<double>(num_messages) / elapsed_s;

  std::println("[test_circular_buffer_write_performance] messages={}, elapsed={:.3f}s, throughput={:.2f} MiB/s, ops={:.0f} ops/s",
               num_messages, elapsed_s, throughput_mib_s, ops_per_sec);
  std::println("[test_circular_buffer_write_performance] done");
}

static void test_log_write_performance() {
  std::println("[test_log_write_performance] start");

  constexpr std::size_t num_messages = 100'000;
  constexpr std::size_t message_size = 32;
  constexpr std::size_t buffer_blocks = 16 * 1024;
  constexpr std::size_t block_size = 4096;

  using Clock = std::chrono::steady_clock;

  wal::Config config(buffer_blocks, block_size, util::ChecksumAlgorithm::NONE);

  Log log(0, 1, config);

  log.start_io(null_writer, nullptr);

  std::vector<std::byte> msg(message_size, std::byte{0x42});
  auto span = std::span<const std::byte>(msg);
  auto start = Clock::now();

  std::size_t successful_writes{};
  std::size_t flush_count{};
  std::chrono::nanoseconds total_flush_time{};

  for (std::size_t i = 0; i < num_messages; ++i) {
    auto result = log.append(span);

    if (result.has_value()) {
      ++successful_writes;
      continue;
    }

    ++flush_count;
    auto flush_start = Clock::now();
    auto flush_result = log.write_to_store(null_writer);
    WAL_ASSERT(flush_result.has_value());
    auto flush_end = Clock::now();
    total_flush_time += (flush_end - flush_start);

      if (!flush_result.has_value()) {
        std::println("Flush failed at message {}", i);
        assert(false);
      }

      result = log.append(span);

      if (!result.has_value()) {
        std::println("Write failed after flush at message {}", i);
        assert(false);
      }

    ++successful_writes;
  }

  auto end = Clock::now();

  auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  double elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;

  const auto total_bytes = successful_writes * message_size;
  const auto throughput_mib_s = (static_cast<double>(total_bytes) / elapsed_s) / (1024.0 * 1024.0);
  const auto ops_per_sec = static_cast<double>(successful_writes) / elapsed_s;

  double write_s = elapsed_s;
  double flush_s = static_cast<double>(total_flush_time.count()) / 1'000'000'000.0;
  write_s = std::max(0.0, write_s - flush_s);
  double other_s = 0.0;

  std::println("[test_log_write_performance] messages={}, flushes={}, elapsed={:.3f}s, throughput={:.2f} MiB/s, ops={:.0f} ops/s",
               successful_writes, flush_count, elapsed_s, throughput_mib_s, ops_per_sec);

  std::println("[test_log_write_performance] time breakdown: write={:.3f}s ({:.1f}%), flush={:.3f}s ({:.1f}%), other={:.3f}s ({:.1f}%)",
               write_s, (write_s / elapsed_s) * 100.0,
               flush_s, (flush_s / elapsed_s) * 100.0,
               other_s, (other_s / elapsed_s) * 100.0);

  /* Shutdown */
  auto shutdown_result = shutdown(log, null_writer);
  assert(shutdown_result.has_value() && shutdown_result.value());

  std::println("[test_log_write_performance] done");
}

int main() {
  test_circular_buffer_basic();
  test_circular_buffer_multiple_writes();
  test_circular_buffer_block_boundary();
  test_circular_buffer_full();
  test_circular_buffer_write_to_store();
  test_circular_buffer_nonzero_initial_lsn();
  test_circular_buffer_margin_edge_cases();

  test_log_basic();
  test_log_write();
  test_log_multiple_writes();

  test_circular_buffer_write_performance();
  test_log_write_performance();

  std::println("\n=== All WAL tests passed! ===");
  return 0;
}
