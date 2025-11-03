#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <string>
#include <span>
#include <algorithm>
#include <iterator>
#include <chrono>

#include "wal/wal.h"
#include "util/logger.h"
#include "util/thread_pool.h"

using wal::lsn_t;
using wal::Log;
using wal::Status;
using wal::block_no_t;
using wal::Circular_buffer;
using Slot = Circular_buffer::Slot;
using Record = std::vector<std::byte>;

util::Logger<util::MT_logger_writer> g_logger(util::MT_logger_writer{std::cerr}, util::Log_level::Trace);

/* Physical write function that simulates writing to storage without actually doing I/O.
 * This is called by Circular_buffer::write_to_store as the Write_callback.
 * Returns the number of bytes that would have been written. */
static wal::Result<std::size_t> null_writer(std::span<struct iovec> span, wal::Log::Sync_type) {
  WAL_ASSERT(span.size() > 0);
  WAL_ASSERT(span.size() % 3 == 0);

  return span[1].iov_len * ((span.size() / 3));
}

/* Helper to call write_to_store synchronously in tests */
/* Pass nullptr to pool to execute I/O synchronously on the same thread */
static wal::Result<wal::lsn_t> write_to_store(Circular_buffer& buffer) {
  return buffer.write_to_store([](std::span<struct iovec> span, wal::Log::Sync_type) -> wal::Result<wal::lsn_t> {
    return null_writer(span, wal::Log::Sync_type::None);
  });
}

/* Helper to call shutdown synchronously in tests - unused but kept for API compatibility */
[[maybe_unused]] static wal::Result<bool> shutdown(Log& log, Circular_buffer::Write_callback callback) {
  return log.shutdown(callback);
}

static void test_circular_buffer_basic() {
  std::fprintf(stderr, "[test_circular_buffer_basic] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(2, 512);
  Circular_buffer buffer(initial_lsn, config);
  
  /* Verify initial state */
  assert(buffer.is_empty());
  assert(!buffer.is_full());
  assert(buffer.margin() == buffer.get_total_data_size());
  
  /* Test reserve */
  auto reserve_result = buffer.reserve(100);
  auto slot = reserve_result.value();
  assert(slot.m_lsn == initial_lsn);
  assert(slot.m_len == 100);

  auto &block_header = buffer.get_block_header(slot.m_lsn / config.m_block_size);
  assert(block_header.get_data_len() == 100);
  assert(!block_header.get_flush_bit());
  assert(block_header.get_first_rec_group() == 0);
  assert(block_header.get_block_no() == slot.m_lsn / config.m_block_size);
  
  /* After reserve, HWM should be incremented but still empty until write */
  assert(buffer.m_lsn_counters.m_lwm == initial_lsn);
  assert(buffer.m_reserve_counters.m_hwm == initial_lsn + 100);
  assert(buffer.m_lsn_counters.m_written_lsn == initial_lsn);
  
  std::fprintf(stderr, "[test_circular_buffer_basic] done\n");
}

static void test_circular_buffer_reserve_and_write() {
  std::fprintf(stderr, "[test_circular_buffer_reserve_and_write] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);
  Circular_buffer buffer(initial_lsn, config);
  
  /* Reserve and write a small message */
  auto reserve_result = buffer.reserve(50);
  Record data(50, std::byte{'A'});
  
  assert(buffer.m_reserve_counters.m_n_pending_writes == 1);
  buffer.write(reserve_result.value(), data);
  assert(buffer.m_reserve_counters.m_n_pending_writes == 0);

  /* Buffer should no longer be empty */
  assert(!buffer.is_empty());
  assert(buffer.margin() == buffer.get_total_data_size() - 50);
  assert(buffer.m_lsn_counters.m_lwm == initial_lsn);
  assert(buffer.m_reserve_counters.m_hwm == initial_lsn + 50);
  
  std::fprintf(stderr, "[test_circular_buffer_reserve_and_write] done\n");
}

static void test_circular_buffer_multiple_writes() {
  std::fprintf(stderr, "[test_circular_buffer_multiple_writes] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t record_size = 64;  
  Circular_buffer::Config config(2, 512);
  Circular_buffer buffer(initial_lsn, config);
  const size_t n_records = (buffer.get_total_data_size() / record_size) + 1;
  
  size_t written_records = 0;
  for (size_t i = 0; /* No op */ ; ++i) {
    auto reserve_result = buffer.reserve(record_size);

    if (!reserve_result.has_value()) {
      assert(reserve_result.error() == Status::Not_enough_space);
      break;
    }

    Record record(record_size, std::byte{static_cast<unsigned char>('A' + (i % 26))});
    
    buffer.write(reserve_result.value(), record);
    ++written_records;
  }
  assert(buffer.m_reserve_counters.m_n_pending_writes == 0);

  assert(written_records == 15);

  const auto data_size = buffer.get_data_size_in_block();

  const auto &block_headers = buffer.headers();

  {
    const auto &block_data = block_headers[0].m_data;

    /* Flush bit is set in write_to_store(). */
    assert(block_data.m_len == data_size);
    assert(block_data.m_block_no == 0);
    assert(block_data.m_first_rec_group == 0);
    assert(!(block_data.m_block_no & wal::Block_header::FLUSH_BIT_MASK));
  }

  {
    const auto &block_data = block_headers[1].m_data;

    assert(block_data.m_block_no == 1);
    assert(!(block_data.m_block_no & wal::Block_header::FLUSH_BIT_MASK));
    assert(block_data.m_len == buffer.m_lsn_counters.m_written_lsn % data_size);
    assert(block_data.m_first_rec_group == (record_size * (n_records / config.m_n_blocks)) % data_size);
  }

  auto span = buffer.data();
  auto ptr = span.data();

  /* Verify the data written matches the contents of the data array */
  for (size_t i = 0; i < written_records; ++i, ptr += record_size) {
    Record record(record_size, std::byte{static_cast<unsigned char>('A' + (i % 26))});

    assert(::memcmp(ptr, record.data(), record_size) == 0);
  }

  /* Verify buffer state */
  assert(!buffer.is_empty());
  assert(buffer.margin() == buffer.get_total_data_size() - (n_records - 1) * record_size);
  
  std::fprintf(stderr, "[test_circular_buffer_multiple_writes] done\n");
}

static void test_circular_buffer_block_boundary() {
  std::fprintf(stderr, "[test_circular_buffer_block_boundary] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(2, 512);
  Circular_buffer buffer(initial_lsn, config);
  const auto data_size = buffer.get_data_size_in_block();

  /* Write exactly one data block worth of data */
  auto reserve_result = buffer.reserve(static_cast<std::uint16_t>(data_size));
  auto slot = reserve_result.value();

  {
    const auto block_header = &buffer.get_block_header(slot.m_lsn / data_size);

    assert(!block_header[0].get_flush_bit());
    assert(block_header[0].get_first_rec_group() == 0);
    assert(block_header[0].get_data_len() == data_size);
    assert(block_header[0].get_block_no() == slot.m_lsn / data_size);

    assert(!block_header[1].get_flush_bit());
    assert(block_header[1].get_block_no() == 0);
    assert(block_header[1].get_data_len() == 0);
    assert(block_header[1].get_first_rec_group() == 0);
  }

  Record data(data_size, std::byte{'X'});
  
  buffer.write(slot, data);

  auto reserve_result2 = buffer.reserve(static_cast<std::uint16_t>(data_size / 2));
  auto slot2 = reserve_result2.value();

  {
    const auto &block_header = buffer.get_block_header(slot2.m_lsn / data_size);

    assert(!block_header.get_flush_bit());
    assert(block_header.get_first_rec_group() == 0);
    assert(block_header.get_data_len() == data_size / 2);
    assert(block_header.get_block_no() == slot2.m_lsn / data_size);
  }

  Record data2(data_size / 2, std::byte{'Y'});
  
  buffer.write(slot2, data2);

  auto reserve_result3 = buffer.reserve(static_cast<std::uint16_t>(data_size / 2));
  auto slot3 = reserve_result3.value();

  {
    auto &block_header = buffer.get_block_header(slot3.m_lsn / data_size);

    assert(!block_header.get_flush_bit());
    assert(block_header.get_first_rec_group() == 0);
    assert(block_header.get_data_len() == data_size);
    assert(block_header.get_block_no() == slot2.m_lsn / data_size);
  }

  Record data3(data_size / 2, std::byte{'Z'});
  
  buffer.write(slot3, data3);

  assert(buffer.m_lsn_counters.m_lwm == initial_lsn);
  assert(buffer.m_reserve_counters.m_hwm == initial_lsn + data_size * 2);
  assert(buffer.is_full());
  assert(buffer.margin() == 0);

  std::fprintf(stderr, "[test_circular_buffer_block_boundary] done\n");
}

static void test_circular_buffer_full() {
  std::fprintf(stderr, "[test_circular_buffer_full] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(2, 512);
  Circular_buffer buffer(initial_lsn, config);
  const auto total_size = buffer.get_total_data_size();
  
  /* Fill the buffer */
  size_t written = 0;
  while (!buffer.is_full() && written < total_size) {
    const auto remaining = buffer.margin();
    const auto chunk_size = std::min(remaining, size_t(100));
    
    auto reserve_result = buffer.reserve(static_cast<std::uint16_t>(chunk_size));
    auto slot = reserve_result.value();
    Record data(chunk_size, std::byte{'F'});

    /* Reserve doesn't over commit, write must succeed. */
    buffer.write(slot, data);
    written += slot.m_len;
  }
  assert(buffer.m_reserve_counters.m_n_pending_writes == 0);
  
  /* Check if buffer reports full */
  WAL_ASSERT(buffer.is_full());
  WAL_ASSERT(buffer.margin() == 0);

  assert(buffer.m_lsn_counters.m_lwm == initial_lsn);
  assert(buffer.m_reserve_counters.m_hwm == initial_lsn + written);
  
  std::fprintf(stderr, "[test_circular_buffer_full] done (wrote %zu bytes)\n", written);
}

static void test_circular_buffer_write_to_store() {
  std::fprintf(stderr, "[test_circular_buffer_write_to_store] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(2, 512);
  Circular_buffer buffer(initial_lsn, config);
  Record record(config.m_block_size, std::byte{'F'});

  /* This will trigger a flush by persisting the first block. */
  for (size_t i = 0; i < config.m_n_blocks; ++i) {
    Slot slot;
    wal::Result<Slot> slot_result;

    for (;;) {
      slot_result = buffer.reserve(static_cast<std::uint16_t>(config.m_block_size));

      if (slot_result.has_value()) {
        slot = slot_result.value();
        break;
      }

      WAL_ASSERT(slot_result.error() == Status::Not_enough_space);
      auto flush_result = write_to_store(buffer);
      assert(flush_result.has_value());
    }

    buffer.write(slot, record);
  }
  assert(buffer.m_reserve_counters.m_n_pending_writes == 0);

  const auto data_size = buffer.get_data_size_in_block();
  /* Note: Free space has to compensate for the LWM block boundary. */
  assert(buffer.margin() == data_size - (buffer.m_lsn_counters.m_written_lsn % data_size));
  assert(!buffer.is_full());

  assert(buffer.m_lsn_counters.m_lwm == initial_lsn + config.m_block_size);
  assert(buffer.m_reserve_counters.m_hwm == buffer.m_lsn_counters.m_written_lsn);
  assert(buffer.m_lsn_counters.m_written_lsn == config.m_n_blocks * config.m_block_size);

  auto flush_result = write_to_store(buffer);

  assert(flush_result.has_value());
  assert(flush_result.value() == initial_lsn + record.size() * config.m_n_blocks);
  /* The buffer should be empty now, except for any prefix bytes from a previous write.. */
  assert(buffer.margin() == buffer.get_total_data_size() - (buffer.m_lsn_counters.m_written_lsn % data_size));
  assert(buffer.is_empty());
  assert(!buffer.pending_writes());

  assert(buffer.m_lsn_counters.m_lwm == buffer.m_reserve_counters.m_hwm);
  assert(buffer.m_reserve_counters.m_hwm == buffer.m_lsn_counters.m_written_lsn);

  std::fprintf(stderr, "[test_circular_buffer_write_to_store] done\n");
}

static void test_circular_buffer_wrap_around_basic() {
  std::fprintf(stderr, "[test_circular_buffer_wrap_around_basic] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);  // 4 blocks
  Circular_buffer buffer(initial_lsn, config);
  const auto data_size = static_cast<std::uint16_t>(buffer.get_data_size_in_block());

  /* Fill the buffer completely */
  for (size_t i = 0; i < config.m_n_blocks; ++i) {
    auto reserve_result = buffer.reserve(data_size);
    auto slot = reserve_result.value();
    Record data(data_size, std::byte{static_cast<unsigned char>('A' + (i % 26))});

    buffer.write(slot, data);
  }
  assert(buffer.m_reserve_counters.m_n_pending_writes == 0);

  assert(buffer.is_full());
  assert(buffer.m_lsn_counters.m_lwm == initial_lsn);
  const auto lwm_before_flush = buffer.m_lsn_counters.m_lwm;

  /* Flush to store - this should advance LWM */
  auto flush_result = write_to_store(buffer);
  assert(flush_result.has_value());

  const auto lwm_after_flush = buffer.m_lsn_counters.m_lwm;
  assert(lwm_after_flush > lwm_before_flush);
  assert(buffer.margin() == buffer.get_total_data_size());

  /* Now write more data - this tests wrap-around */
  for (size_t i = 0; i < config.m_n_blocks; ++i) {
    auto reserve_result = buffer.reserve(data_size);
    auto slot = reserve_result.value();
    Record data(data_size, std::byte{static_cast<unsigned char>('W' + (i % 4))});

    buffer.write(slot, data);
  }

  /* Verify the data wrapped around correctly */
  auto span = buffer.data();
  for (size_t i = 0; i < config.m_n_blocks; ++i) {
    const auto offset = i * data_size;
    Record expected(data_size, std::byte{static_cast<unsigned char>('W' + (i % 4))});
    assert(::memcmp(span.data() + offset, expected.data(), data_size) == 0);
  }

  assert(buffer.is_full());

  std::fprintf(stderr, "[test_circular_buffer_wrap_around_basic] done\n");
}

static void test_circular_buffer_multiple_cycles() {
  std::fprintf(stderr, "[test_circular_buffer_multiple_cycles] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(8, 512);
  Circular_buffer buffer(initial_lsn, config);
  const std::uint16_t record_size = 128;

  constexpr size_t num_cycles = 10;
  lsn_t last_lwm = initial_lsn;

  for (size_t cycle = 0; cycle < num_cycles; ++cycle) {
    /* Fill buffer */
    size_t writes = 0;
    while (buffer.margin() >= record_size) {
      auto reserve_result = buffer.reserve(record_size);
      auto slot = reserve_result.value();
      Record data(record_size, std::byte{static_cast<unsigned char>('A' + (cycle % 26))});

      buffer.write(slot, data);
      ++writes;
    }
    assert(buffer.m_reserve_counters.m_n_pending_writes == 0);


    /* Flush */
    auto flush_result = write_to_store(buffer);
    assert(flush_result.has_value());

    /* Verify LWM is advancing */
    const auto current_lwm = buffer.m_lsn_counters.m_lwm;
    assert(current_lwm > last_lwm);
    last_lwm = current_lwm;

    /* Buffer should be empty after flush */
    assert(buffer.margin() == buffer.get_total_data_size());
  }

  /* Verify we've advanced significantly through the LSN space */
  assert(buffer.m_lsn_counters.m_lwm > initial_lsn + buffer.get_total_data_size() * (num_cycles - 1));

  std::fprintf(stderr, "[test_circular_buffer_multiple_cycles] done (final LWM: %lu)\n",
               buffer.m_lsn_counters.m_lwm);
}

static void test_circular_buffer_large_dataset() {
  std::fprintf(stderr, "[test_circular_buffer_large_dataset] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(16, 1024);

  Circular_buffer buffer(initial_lsn, config);

  const std::uint16_t record_size = 256;
  constexpr size_t target_records = 1000;

  size_t flush_count = 0;
  size_t total_written = 0;

  for (size_t i = 0; i < target_records; ++i) {
    /* Check if we need to flush */
    if (buffer.margin() < record_size) {
      auto flush_result = write_to_store(buffer);
      assert(flush_result.has_value());
      ++flush_count;
    }

    auto reserve_result = buffer.reserve(record_size);
    auto slot = reserve_result.value();
    Record data(record_size);

    /* Fill with pattern based on record number */
    for (size_t j = 0; j < record_size; ++j) {
      data[j] = std::byte{static_cast<unsigned char>((i + j) % 256)};
    }

    buffer.write(slot, data);
    ++total_written;
  }
  assert(buffer.m_reserve_counters.m_n_pending_writes == 0);

  /* Final flush */
  if (!buffer.is_empty()) {
    auto flush_result = write_to_store(buffer);
    assert(flush_result.has_value());
    ++flush_count;
  }

  assert(total_written == target_records);
  assert(flush_count > 0);

  std::fprintf(stderr, "[test_circular_buffer_large_dataset] done\n");
}

static void test_circular_buffer_wrap_around_partial_block() {
  std::fprintf(stderr, "[test_circular_buffer_wrap_around_partial_block] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);
  Circular_buffer buffer(initial_lsn, config);
  const auto data_size = buffer.get_data_size_in_block();

  /* Fill buffer mostly, leaving a partial block */
  for (size_t i = 0; i < config.m_n_blocks - 1; ++i) {
    auto reserve_result = buffer.reserve(static_cast<std::uint16_t>(data_size));
    auto slot = reserve_result.value();
    Record data(data_size, std::byte{static_cast<unsigned char>('F')});

    buffer.write(slot, data);
  }
  assert(buffer.m_reserve_counters.m_n_pending_writes == 0);

  /* Write partial block */
  const std::uint16_t partial_size = static_cast<std::uint16_t>(data_size / 2);
  auto reserve_result = buffer.reserve(partial_size);
  auto partial_slot = reserve_result.value();
  Record partial_data(partial_size, std::byte{static_cast<unsigned char>('P')});

  buffer.write(partial_slot, partial_data);

  {
    /* Flush - partial block should be handled correctly */
    auto flush_result = write_to_store(buffer);
    assert(flush_result.has_value());
  }

  /* Write more data to test wrap-around with partial block */
  for (size_t i = 0; i < config.m_n_blocks &&buffer.margin() >= data_size; ++i) {
    auto reserve_result = buffer.reserve(static_cast<std::uint16_t>(data_size));
    auto slot = reserve_result.value();
    Record data(data_size, std::byte{static_cast<unsigned char>('N')});

    buffer.write(slot, data);
  }
  assert(buffer.m_reserve_counters.m_n_pending_writes == 0);

  {
    auto flush_result = write_to_store(buffer);
    assert(flush_result.has_value());
    assert(flush_result.value() == buffer.m_lsn_counters.m_lwm);
  }

  auto block_no = buffer.m_lsn_counters.m_lwm / data_size;
  assert(block_no == 6);

  auto block_index = (block_no % config.m_n_blocks);
  assert(block_index == 2);
  assert(buffer.get_block_header(block_index).get_data_len() == data_size / 2);

  /* We should have 3 empty blocks + the overflow written to the 2nd block. */
  assert((buffer.m_lsn_counters.m_lwm / data_size) % config.m_n_blocks == 2);
  const auto partial_filled = buffer.m_lsn_counters.m_written_lsn - buffer.m_lsn_counters.m_lwm;
  assert(partial_filled == data_size / 2);
  assert(buffer.margin() == (data_size * 3 + buffer.m_lsn_counters.m_lwm % data_size) + partial_filled);

  std::fprintf(stderr, "[test_circular_buffer_wrap_around_partial_block] done\n");
}

static void test_circular_buffer_reserve_write_gap() {
  std::fprintf(stderr, "[test_circular_buffer_reserve_write_gap] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);
  Circular_buffer buffer(initial_lsn, config);

  /* Test the gap between HWM and committed_lsn */
  auto reserve_result1 = buffer.reserve(500);
  auto slot1 = reserve_result1.value();
  auto reserve_result2 = buffer.reserve(500);
  auto slot2 = reserve_result2.value();
  auto reserve_result3 = buffer.reserve(500);
  auto slot3 = reserve_result3.value();

  assert(buffer.m_reserve_counters.m_n_pending_writes == 3);

  /* HWM has advanced by 1500, but nothing committed yet */
  assert(buffer.m_reserve_counters.m_hwm == 1500);
  assert(buffer.m_lsn_counters.m_written_lsn == 0);
  assert(buffer.pending_writes());

  /* margin() should reflect committed data, not reserved */
  assert(buffer.margin() == buffer.get_total_data_size() - slot1.m_len - slot2.m_len - slot3.m_len);

  /* Write in order - write slot1, slot2, slot3 */
  Record data1(500, std::byte{'A'});
  buffer.write(slot1, data1);
  assert(buffer.m_lsn_counters.m_written_lsn == 500);

  Record data2(500, std::byte{'B'});
  buffer.write(slot2, data2);
  assert(buffer.m_lsn_counters.m_written_lsn == 1000);

  Record data3(500, std::byte{'C'});
  buffer.write(slot3, data3);
  assert(buffer.m_lsn_counters.m_written_lsn == 1500);
  assert(!buffer.pending_writes());

  {
    auto flush_result = write_to_store(buffer);
    assert(flush_result.has_value());
    assert(flush_result.value() == 1500);
  }

  std::fprintf(stderr, "[test_circular_buffer_reserve_write_gap] done\n");
}

static void test_circular_buffer_incremental_partial_block() {
  std::fprintf(stderr, "[test_circular_buffer_incremental_partial_block] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);

  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);


  const auto data_size_per_block = static_cast<std::uint16_t>(buffer.get_data_size_in_block());

  /* Write 3 full blocks + partial block */
  for (size_t i = 0; i < 3; ++i) {
    auto reserve_result = buffer.reserve(data_size_per_block);
    auto slot = reserve_result.value();
    Record data(data_size_per_block, std::byte{static_cast<unsigned char>('A' + i)});

    buffer.write(slot, data);
  }

  /* Write partial block (248 bytes) */
  const std::uint16_t partial_size = data_size_per_block / 2;
  auto reserve_partial_result = buffer.reserve(partial_size);
  auto partial_slot = reserve_partial_result.value();
  Record partial_data(partial_size, std::byte{'X'});

  buffer.write(partial_slot, partial_data);

  const auto committed_before_flush = buffer.m_lsn_counters.m_written_lsn;
  const auto block_3_index = 3;
  const auto block_3_len_before = buffer.get_block_header(block_3_index).get_data_len();
  assert(block_3_len_before == partial_size);

  /* Flush - should preserve the partial block */
  auto flush_result = write_to_store(buffer);
  assert(flush_result.has_value());

  /* Write another 248 bytes to complete block 3 */
  auto reserve_continue_result = buffer.reserve(partial_size);
  auto continue_slot = reserve_continue_result.value();
  Record continue_data(partial_size, std::byte{'Y'});

  buffer.write(continue_slot, continue_data);

  /* Verify block 3 now has full data */
  const auto block_3_len_after = buffer.get_block_header(block_3_index).get_data_len();
  assert(block_3_len_after == data_size_per_block);

  /* Verify block number stayed the same */
  const auto expected_block_no = committed_before_flush / data_size_per_block;
  assert(buffer.get_block_header(block_3_index).get_block_no() == expected_block_no);

  std::fprintf(stderr, "[test_circular_buffer_incremental_partial_block] done\n");
}

static void test_circular_buffer_write_exact_margin() {
  std::fprintf(stderr, "[test_circular_buffer_write_exact_margin] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);

  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);

  const auto total_size = buffer.get_total_data_size();

  /* Fill buffer to leave exactly 184 bytes */
  const std::uint16_t fill_size = 1800;
  auto reserve_result1 = buffer.reserve(fill_size);
  auto slot1 = reserve_result1.value();
  Record data1(fill_size, std::byte{'A'});

  buffer.write(slot1, data1);

  /* Verify margin */
  const auto margin = buffer.margin();
  assert(margin == total_size - fill_size);
  assert(margin == 184);

  /* Write exactly the remaining space */
  const std::uint16_t exact_size = static_cast<std::uint16_t>(margin);
  auto reserve_result2 = buffer.reserve(exact_size);
  auto slot2 = reserve_result2.value();
  Record data2(exact_size, std::byte{'B'});

  buffer.write(slot2, data2);

  /* Buffer should now be exactly full */
  assert(buffer.is_full());
  assert(buffer.margin() == 0);

  std::fprintf(stderr, "[test_circular_buffer_write_exact_margin] done\n");
}

static void test_circular_buffer_nonzero_initial_lsn() {
  std::fprintf(stderr, "[test_circular_buffer_nonzero_initial_lsn] start\n");

  constexpr lsn_t initial_lsn = 1000000;
  Circular_buffer::Config config(4, 512);
  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);
  const auto data_size = buffer.get_data_size_in_block();
  const auto expected_start_block = initial_lsn / data_size;
  auto block_header = buffer.get_block_header(expected_start_block % config.m_n_blocks);

  assert(block_header.get_block_no() == expected_start_block);

  /* Write some data */
  auto reserve_result = buffer.reserve(500);
  auto slot = reserve_result.value();

  assert(slot.m_lsn == initial_lsn);

  Record data(500, std::byte{'Z'});

  buffer.write(slot, data);

  assert(buffer.m_lsn_counters.m_written_lsn == initial_lsn + 500);

  std::fprintf(stderr, "[test_circular_buffer_nonzero_initial_lsn] done\n");
}

static void test_circular_buffer_block_number_continuity() {
  std::fprintf(stderr, "[test_circular_buffer_block_number_continuity] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);

  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);


  const auto data_size_per_block = static_cast<std::uint16_t>(buffer.get_data_size_in_block());

  /* Track all block numbers written */
  std::vector<block_no_t> block_numbers;

  constexpr size_t num_cycles = 20;
  for (size_t cycle = 0; cycle < num_cycles; ++cycle) {
    /* Fill buffer */
    for (size_t i = 0; i < config.m_n_blocks; ++i) {
      auto reserve_result = buffer.reserve(data_size_per_block);
      auto slot = reserve_result.value();
      Record data(data_size_per_block, std::byte{static_cast<unsigned char>('A' + (cycle % 26))});

      buffer.write(slot, data);

      /* Record the block number */
      const auto block_index = i;
      block_numbers.push_back(buffer.get_block_header(block_index).get_block_no());
    }

    /* Flush */
    auto flush_result = write_to_store(buffer);
    assert(flush_result.has_value());
  }

  /* Verify block numbers are sequential */
  for (size_t i = 1; i < block_numbers.size(); ++i) {
    assert(block_numbers[i] == block_numbers[i - 1] + 1);
  }

  /* Verify total blocks written */
  assert(block_numbers.size() == num_cycles * config.m_n_blocks);

  std::fprintf(stderr, "[test_circular_buffer_block_number_continuity] done "
               "(verified %zu sequential block numbers)\n", block_numbers.size());
}

/**
 * Test: Off-by-One Block Boundary
 * Category: Boundary Conditions
 * Purpose: Test writes that are exactly one byte less than, equal to, and one byte more than block size
 */
static void test_circular_buffer_off_by_one_boundary() {
  std::fprintf(stderr, "[test_circular_buffer_off_by_one_boundary] start\n");

  using Record = std::vector<std::byte>;

  Circular_buffer::Config config(4, 512);
  Circular_buffer buffer(0, config);

  const auto data_size_per_block = buffer.get_data_size_in_block();

  /* Test 1: Write 495 bytes (one less than block size) */
  {
    auto reserve_result = buffer.reserve(static_cast<std::uint16_t>(data_size_per_block - 1));
    auto slot = reserve_result.value();
    Record data(data_size_per_block - 1, std::byte{0xAA});

    buffer.write(slot, data);

    /* Should still be in block 0 */
    assert(buffer.get_block_header(0).get_data_len() == data_size_per_block - 1);
  }

  /* Test 2: Write 1 more byte to complete the block (total 496) */
  {
    auto reserve_result = buffer.reserve(1);
    auto slot = reserve_result.value();
    Record data(1, std::byte{0xBB});

    buffer.write(slot, data);

    /* Block 0 should now be full */
    assert(buffer.get_block_header(0).get_data_len() == data_size_per_block);
  }

  /* Test 3: Write exactly block size (496 bytes) */
  {
    auto reserve_result = buffer.reserve(static_cast<std::uint16_t>(data_size_per_block));
    auto slot = reserve_result.value();
    Record data(data_size_per_block, std::byte{0xCC});

    buffer.write(slot, data);

    /* Block 1 should be full */
    assert(buffer.get_block_header(1).get_data_len() == data_size_per_block);
  }

  /* Test 4: Write 497 bytes (one more than block size) */
  {
    auto reserve_result = buffer.reserve(static_cast<std::uint16_t>(data_size_per_block + 1));
    auto slot = reserve_result.value();
    Record data(data_size_per_block + 1, std::byte{0xDD});

    buffer.write(slot, data);

    /* Should span blocks 2 and 3 */
    assert(buffer.get_block_header(2).get_data_len() == data_size_per_block);
    assert(buffer.get_block_header(3).get_data_len() == 1);
  }

  std::fprintf(stderr, "[test_circular_buffer_off_by_one_boundary] done\n");
}

/**
 * Test: Write Pattern Verification Across Wrap
 * Category: Data Integrity Verification
 * Purpose: Verify data doesn't get corrupted during wrap-around
 */
static void test_circular_buffer_pattern_verification() {
  std::fprintf(stderr, "[test_circular_buffer_pattern_verification] start\n");

  using Record = std::vector<std::byte>;

  Circular_buffer::Config config(4, 512);
  Circular_buffer buffer(0, config);

  const auto data_size_per_block = buffer.get_data_size_in_block();

  /* Define unique patterns for each block */
  const std::array<std::byte, 4> patterns = {
    std::byte{0xAA}, std::byte{0xBB}, std::byte{0xCC}, std::byte{0xDD}
  };

  /* Write blocks with unique patterns */
  for (size_t i = 0; i < 4; ++i) {
    auto reserve_result = buffer.reserve(static_cast<std::uint16_t>(data_size_per_block));
    auto slot = reserve_result.value();
    Record data(data_size_per_block, patterns[i]);

    buffer.write(slot, data);
  }

  /* Verify each block has correct pattern before flush */
  for (size_t i = 0; i < 4; ++i) {
    auto [header, span, crc] = buffer.get_block(i);
    assert(header->get_data_len() == data_size_per_block);

    /* Check that all bytes in the block match the expected pattern */
    for (size_t j = 0; j < data_size_per_block; ++j) {
      assert(span[j] == patterns[i]);
    }
  }

  /* Flush */

  auto flush_result = write_to_store(buffer);
  assert(flush_result.has_value());

  /* Write new blocks with different patterns after wrap */
  const std::array<std::byte, 4> new_patterns = {
    std::byte{0x11}, std::byte{0x22}, std::byte{0x33}, std::byte{0x44}
  };

  for (size_t i = 0; i < 4; ++i) {
    auto reserve_result = buffer.reserve(static_cast<std::uint16_t>(data_size_per_block));
    auto slot = reserve_result.value();
    Record data(data_size_per_block, new_patterns[i]);

    buffer.write(slot, data);
  }

  /* Verify new blocks have correct patterns (no mixing with old data) */
  for (size_t i = 0; i < 4; ++i) {
    auto [header, span, crc] = buffer.get_block(i);
    assert(header->get_data_len() == data_size_per_block);

    /* Check that all bytes match the NEW pattern, not the old one */
    for (size_t j = 0; j < data_size_per_block; ++j) {
      assert(span[j] == new_patterns[i]);
    }
  }

  std::fprintf(stderr, "[test_circular_buffer_pattern_verification] done (no data corruption)\n");
}

/**
 * Test: Minimal Write-Flush Cycle
 * Category: Flush Edge Cases
 * Purpose: Stress test with frequent flushes
 */
static void test_circular_buffer_minimal_write_flush_cycle() {
  std::fprintf(stderr, "[test_circular_buffer_minimal_write_flush_cycle] start\n");

  using Record = std::vector<std::byte>;

  Circular_buffer::Config config(4, 512);
  Circular_buffer buffer(0, config);

  /* Perform multiple iterations of small write + flush */
  /* Write at least one full block per iteration to avoid clear() issues with partial blocks */
  const auto data_size_per_block = buffer.get_data_size_in_block();
  constexpr size_t num_iterations = 20;

  for (size_t i = 0; i < num_iterations; ++i) {
    /* Write one full block (496 bytes) */
    auto reserve_result = buffer.reserve(static_cast<std::uint16_t>(data_size_per_block));
    auto slot = reserve_result.value();
    Record data(data_size_per_block, std::byte{static_cast<unsigned char>('A' + (i % 26))});

    buffer.write(slot, data);

    auto flush_result = write_to_store(buffer);
    assert(flush_result.has_value());
  }

  /* Verify total bytes written */
  lsn_t expected_lsn = num_iterations * data_size_per_block;
  assert(buffer.m_lsn_counters.m_lwm == expected_lsn);

  std::fprintf(stderr, "[test_circular_buffer_minimal_write_flush_cycle] done "
               "(%zu iterations)\n", num_iterations);
}

// FIXME: Add read tests once the writes work.

static void test_log_basic() {
  std::fprintf(stderr, "[test_log_basic] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t pool_size = 4;
  Circular_buffer::Config config(4, 512);

  Log log(initial_lsn, pool_size, config);
  
  /* Verify initial state */
  assert(log.is_empty());
  assert(!log.is_full());
  assert(log.margin() > 0);
  
  /* Pass nullptr to pool for synchronous execution */
  auto done = shutdown(log, null_writer);
  assert(done.has_value() && done.value());

  std::fprintf(stderr, "[test_log_basic] done\n");
}

static void test_log_reserve_and_write() {
  std::fprintf(stderr, "[test_log_reserve_and_write] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t pool_size = 4;
  Circular_buffer::Config config(4, 512);
  
  Log log(initial_lsn, pool_size, config);
  
  /* Write data */
  Record data(100, std::byte{'A'});
  auto null_writer = [](std::span<struct iovec>, wal::Log::Sync_type) -> wal::Result<size_t> {
    return wal::Result<size_t>(0);
  };
  auto write_result = log.write(std::as_bytes(std::span{data}));
  assert(write_result.has_value());
  auto slot = write_result.value();
  assert(slot.m_len == 100);
  
  /* Verify state */
  assert(!log.is_empty());

  auto done = shutdown(log, null_writer);
  assert(done.has_value() && done.value());

  std::fprintf(stderr, "[test_log_reserve_and_write] done\n");
}

static void test_log_write_data_verification() {
  std::fprintf(stderr, "[test_log_write_data_verification] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t pool_size = 4;
  Circular_buffer::Config config(4, 512);

  Log log(initial_lsn, pool_size, config);
  
  /* Write specific data and verify it can be read back */
  std::string test_data = "Hello, WAL!";
  auto null_writer = [](std::span<struct iovec>, wal::Log::Sync_type) -> wal::Result<size_t> {
    return wal::Result<size_t>(0);
  };
  
  auto write_result = log.write(std::as_bytes(std::span{test_data}));
  assert(write_result.has_value());
  auto slot = write_result.value();
  
  /* Verify slot was returned correctly */
  assert(slot.m_len == test_data.size());

  auto done = shutdown(log, null_writer);
  assert(done.has_value() && done.value());

  std::fprintf(stderr, "[test_log_write_data_verification] done\n");
}

static void test_log_multiple_writes() {
  std::fprintf(stderr, "[test_log_multiple_writes] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t pool_size = 4;
  constexpr size_t record_size = 26;
  Circular_buffer::Config config(4, 512);

  using Record = std::vector<std::byte>;

  Log log(initial_lsn, pool_size, config);
  
  auto null_writer = [](std::span<struct iovec>, wal::Log::Sync_type) -> wal::Result<size_t> {
    return wal::Result<size_t>(0);
  };
  
  /* Write multiple entries */
  constexpr int num_writes = 10;
  for (int i = 0; i < num_writes; ++i) {
    Record data(record_size, std::byte{static_cast<unsigned char>('A' + i)});
    auto write_result = log.write(data);
    assert(write_result.has_value());
  }
  
  /* Verify buffer state */
  assert(!log.is_empty());

  auto done = shutdown(log, null_writer);
  assert(done.has_value() && done.value());

  std::fprintf(stderr, "[test_log_multiple_writes] done\n");
}

static void test_circular_buffer_margin_edge_cases() {
  std::fprintf(stderr, "[test_circular_buffer_margin_edge_cases] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);
  Circular_buffer buffer(initial_lsn, config);
  const auto total_size = buffer.get_total_data_size();

  /* Test 1: Empty buffer */
  assert(buffer.margin() == total_size);

  /* Test 2: Single byte write */
  auto reserve_result1 = buffer.reserve(1);
  auto slot1 = reserve_result1.value();
  Record data1(1, std::byte{'X'});
  buffer.write(slot1, data1);
  assert(buffer.margin() == total_size - 1);

  /* Test 3: Fill to one byte before full */
  const auto remaining = buffer.margin();
  if (remaining > 1) {
    auto reserve_result2 = buffer.reserve(static_cast<std::uint16_t>(remaining - 1));
    auto slot2 = reserve_result2.value();
    Record data2(remaining - 1, std::byte{'Y'});
    buffer.write(slot2, data2);
    assert(buffer.margin() == 1);
  }

  /* Test 4: Fill exactly to full */
  if (buffer.margin() > 0) {
    auto len = buffer.margin();
    auto reserve_result3 = buffer.reserve(static_cast<std::uint16_t>(len));
    assert(buffer.margin() == 0);
    auto slot3 = reserve_result3.value();
    Record data3(len, std::byte{'Z'});
    buffer.write(slot3, data3);
    assert(buffer.margin() == 0);
    assert(buffer.is_full());
  }

  /* Test 5: After flush, margin should be total_size */
  auto flush_result = write_to_store(buffer);
  assert(flush_result.has_value());
  assert(buffer.margin() == total_size);

  std::fprintf(stderr, "[test_circular_buffer_margin_edge_cases] done\n");
}

static void test_circular_buffer_write_performance() {
  std::fprintf(stderr, "[test_circular_buffer_write_performance] start\n");

  constexpr std::size_t num_messages = 100'000;
  /* 32-byte payload matches upstream benchmarks and stresses metadata overhead. */
  constexpr std::size_t message_size = 32;
  constexpr std::size_t buffer_blocks = 16384;
  constexpr std::size_t block_size = 4096;

  Circular_buffer::Config config(buffer_blocks, block_size);
  Circular_buffer buffer(0, config);

  std::vector<std::byte> msg_data(message_size, std::byte{0x42});
  auto msg_span = std::span<const std::byte>(msg_data);

  auto start = std::chrono::steady_clock::now();

  for (std::size_t i = 0; i < num_messages; ++i) {
    auto slot_result = buffer.reserve(message_size);
    WAL_ASSERT(slot_result.has_value());
    buffer.write(slot_result.value(), msg_span);
  }

  auto end = std::chrono::steady_clock::now();

  auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start);
  double elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;

  const auto total_bytes = num_messages * message_size;
  const auto throughput_mib_s = (static_cast<double>(total_bytes) / elapsed_s) / (1024.0 * 1024.0);
  const auto ops_per_sec = static_cast<double>(num_messages) / elapsed_s;

  std::fprintf(stderr, "[test_circular_buffer_write_performance] messages=%zu, elapsed=%.3fs, throughput=%.2f MiB/s, ops=%.0f ops/s\n",
               num_messages, elapsed_s, throughput_mib_s, ops_per_sec);
  std::fprintf(stderr, "[test_circular_buffer_write_performance] done\n");
}

static void test_log_write_performance() {
  std::fprintf(stderr, "[test_log_write_performance] start\n");

  constexpr std::size_t num_messages = 100'000;
  /* 32-byte payload matches upstream benchmarks and stresses metadata overhead. */
  constexpr std::size_t message_size = 32;
  constexpr std::size_t buffer_blocks = 16384;  /* 64MB with 4KB blocks */
  constexpr std::size_t block_size = 4096;

  /* Create log with large buffer to avoid flushes */
  wal::Config config(buffer_blocks, block_size, util::ChecksumAlgorithm::NONE);
  Log log(0, 1, config);  /* buffer_pool_size = 1 */

  /* Prepare message data */
  std::vector<std::byte> msg_data(message_size, std::byte{0x42});
  auto msg_span = std::span<const std::byte>(msg_data);

  using Clock = std::chrono::steady_clock;
  auto start = Clock::now();

  /* Write messages in a loop */
  std::size_t successful_writes = 0;
  std::size_t flush_count = 0;
  std::chrono::nanoseconds total_flush_time{0};

  for (std::size_t i = 0; i < num_messages; ++i) {
    auto result = log.write(msg_span);

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
        std::fprintf(stderr, "Flush failed at message %zu\n", i);
        assert(false);
      }

      result = log.write(msg_span);

      if (!result.has_value()) {
        std::fprintf(stderr, "Write failed after flush at message %zu\n", i);
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

  // constexpr double target_throughput_mib_s = 450.0;
  std::fprintf(stderr, "[test_log_write_performance] messages=%zu, flushes=%zu, elapsed=%.3fs, throughput=%.2f MiB/s, ops=%.0f ops/s\n",
               successful_writes, flush_count, elapsed_s, throughput_mib_s, ops_per_sec);
  std::fprintf(stderr, "[test_log_write_performance] time breakdown: write=%.3fs (%.1f%%), flush=%.3fs (%.1f%%), other=%.3fs (%.1f%%)\n",
               write_s, (write_s / elapsed_s) * 100.0,
               flush_s, (flush_s / elapsed_s) * 100.0,
               other_s, (other_s / elapsed_s) * 100.0);
  // assert(throughput_mib_s >= target_throughput_mib_s);

  /* Shutdown */
  auto shutdown_result = shutdown(log, null_writer);
  assert(shutdown_result.has_value() && shutdown_result.value());

  std::fprintf(stderr, "[test_log_write_performance] done\n");
}

int main() {
  /* Basic circular buffer tests */
  test_circular_buffer_basic();
  test_circular_buffer_reserve_and_write();
  test_circular_buffer_multiple_writes();
  test_circular_buffer_block_boundary();
  test_circular_buffer_full();
  test_circular_buffer_write_to_store();

  /* Wrap-around and edge case tests */
  test_circular_buffer_wrap_around_basic();
  test_circular_buffer_multiple_cycles();
  test_circular_buffer_large_dataset();
  test_circular_buffer_wrap_around_partial_block();

  /* Additional edge case tests */
  test_circular_buffer_reserve_write_gap();
  test_circular_buffer_incremental_partial_block();
  test_circular_buffer_write_exact_margin();
  test_circular_buffer_nonzero_initial_lsn();
  test_circular_buffer_block_number_continuity();
  test_circular_buffer_off_by_one_boundary();
  test_circular_buffer_pattern_verification();
  test_circular_buffer_minimal_write_flush_cycle();  /* Disabled - see test comment */

  /* Margin calculation tests */
  test_circular_buffer_margin_edge_cases();

  /* Log tests */
  test_log_basic();
  test_log_reserve_and_write();
  test_log_write_data_verification();
  test_log_multiple_writes();

  /* Performance tests */
  test_circular_buffer_write_performance();
  test_log_write_performance();

  std::fprintf(stderr, "\n=== All WAL tests passed! ===\n");
  return 0;
}
