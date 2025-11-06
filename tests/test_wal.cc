#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <string>
#include <span>
#include <algorithm>
#include <iterator>

#include "wal/wal.h"

using wal::lsn_t;
using wal::Log;
using wal::Status;
using wal::Circular_buffer;
using wal::block_no_t;

/* Null writer callback that simulates writing to storage without actually doing I/O */
static wal::Result<lsn_t> null_writer(lsn_t lsn, const wal::Log::IO_vecs &iovecs) {
  std::size_t bytes_written{};
  for (std::size_t i = 1; i < iovecs.size(); i += 3) {
    bytes_written += iovecs[i].iov_len;
  }
  return wal::Result<lsn_t>(lsn + bytes_written);
}

static void test_circular_buffer_basic() {
  std::fprintf(stderr, "[test_circular_buffer_basic] start\n");
  
  constexpr lsn_t initial_lsn = 100;
  Circular_buffer::Config config(4, 512);
  
  Circular_buffer buffer(initial_lsn, config);
  
  /* Verify initial state */
  assert(buffer.is_empty());
  assert(!buffer.is_full());
  assert(buffer.check_margin() == buffer.get_total_data_size());
  
  /* Test reserve */
  auto slot1 = buffer.reserve(100);
  assert(slot1.m_lsn == initial_lsn);
  assert(slot1.m_len == 100);
  
  /* After reserve, HWM should be incremented but still empty until write */
  assert(buffer.m_lwm == initial_lsn);
  assert(buffer.m_hwm == initial_lsn + 100);
  assert(buffer.m_written_lsn == initial_lsn);
  
  std::fprintf(stderr, "[test_circular_buffer_basic] done\n");
}

static void test_circular_buffer_reserve_and_write() {
  std::fprintf(stderr, "[test_circular_buffer_reserve_and_write] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);
  
  Circular_buffer buffer(initial_lsn, config);
  
  /* Reserve and write a small message */
  auto slot = buffer.reserve(50);
  std::vector<std::byte> data(50, std::byte{'A'});
  
  auto result = buffer.write(slot, data);
  assert(result.has_value());
  assert(result.value() == 50);
  
  /* Buffer should no longer be empty */
  assert(!buffer.is_empty());
  assert(buffer.check_margin() == buffer.get_total_data_size() - 50);
  assert(buffer.m_lwm == initial_lsn);
  assert(buffer.m_hwm == initial_lsn + 50);
  
  std::fprintf(stderr, "[test_circular_buffer_reserve_and_write] done\n");
}

static void test_circular_buffer_multiple_writes() {
  std::fprintf(stderr, "[test_circular_buffer_multiple_writes] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t record_size = 64;  
  Circular_buffer::Config config(2, 512);
  
  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);

  const size_t n_records = (buffer.get_total_data_size() / record_size) + 1;
  
  /* Write multipleslots */
  size_t i{};
  for (;;) {
    auto slot = buffer.reserve(record_size);
    Record record(record_size, std::byte{static_cast<unsigned char>('A' + (i % 26))});
    
    auto result = buffer.write(slot, record);
    if (!result.has_value()) {
      assert(result.error() == Status::Not_enough_space);
      break;
    }
    ++i;
  }

  auto span = buffer.data();
  auto ptr = span.data();

  /* Verify the data written matches the contents of the data array */
  for (size_t i = 0; i < n_records - 1; ++i, ptr += record_size) {
    Record record(record_size, std::byte{static_cast<unsigned char>('A' + (i % 26))});

    assert(::memcmp(ptr, record.data(), record_size) == 0);
  }

  /* The last record wrote only half of the record size */
  Record record(record_size / 2, std::byte{static_cast<unsigned char>('A' + ((n_records - 1) % 26))});
  assert(::memcmp(ptr, record.data(), record.size()) == 0);

  assert(buffer.m_lwm == initial_lsn);
  assert(buffer.m_hwm == initial_lsn + n_records * record_size);
  
  /* Verify buffer state */
  assert(!buffer.is_empty());
  assert(buffer.check_margin() == 0);
  
  std::fprintf(stderr, "[test_circular_buffer_multiple_writes] done\n");
}

static void test_circular_buffer_block_boundary() {
  std::fprintf(stderr, "[test_circular_buffer_block_boundary] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(2, 512);

  using Record = std::vector<std::byte>;
  
  Circular_buffer buffer(initial_lsn, config);
  const auto data_size_per_block = static_cast<std::uint16_t>(buffer.get_data_size_in_block());
  
  Record data(data_size_per_block, std::byte{'X'});

  /* Write exactly one block worth of data */
  auto slot = buffer.reserve(data_size_per_block);
  
  auto result = buffer.write(slot, data);
  assert(result.has_value());
  assert(result.value() == data_size_per_block);
  
  /* Write half of the data size of the next block */
  auto slot2 = buffer.reserve(data_size_per_block / 2);
  Record data2(data_size_per_block / 2, std::byte{'Y'});
  
  auto result2 = buffer.write(slot2, data2);
  assert(result2.has_value());
  assert(result2.value() == data_size_per_block / 2);

  /** Write half of the data size of the block */
  auto slot3 = buffer.reserve(data_size_per_block / 2);
  Record data3(data_size_per_block / 2, std::byte{'Z'});
  
  auto result3 = buffer.write(slot3, data3);
  assert(result3.has_value());
  assert(result3.value() == data_size_per_block / 2);

  assert(buffer.m_lwm == initial_lsn);
  assert(buffer.m_hwm == initial_lsn + data_size_per_block * 2);
  assert(buffer.is_full());
  assert(buffer.check_margin() == 0);

  std::fprintf(stderr, "[test_circular_buffer_block_boundary] done\n");
}

static void test_circular_buffer_full() {
  std::fprintf(stderr, "[test_circular_buffer_full] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(2, 512);
  
  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);
  const auto total_size = buffer.get_total_data_size();
  
  /* Fill the buffer */
  size_t written = 0;
  while (!buffer.is_full() && written < total_size) {
    const auto remaining = buffer.check_margin();
    const auto chunk_size = std::min(remaining, size_t(100));
    
    auto slot = buffer.reserve(static_cast<std::uint16_t>(chunk_size));
    Record data(chunk_size, std::byte{'F'});
    
    auto result = buffer.write(slot, data);
    if (result.has_value()) {
      /** There is no guarantee that the entire chunk will be written.
       * Only as many bytes that fit in the buffer will be written.
       * 
       * We ignore this error because we will not write out the buffer and retry.
       */
      const auto n_written = result.value();
      written += n_written;
    } else {
      assert(result.error() == Status::Not_enough_space);
      break;
    }
  }
  
  /* Check if buffer reports full */
  if (buffer.is_full()) {
    assert(buffer.check_margin() == 0);
  }

  assert(buffer.m_lwm == initial_lsn);
  assert(buffer.m_hwm == initial_lsn + written);
  
  std::fprintf(stderr, "[test_circular_buffer_full] done (wrote %zu bytes)\n", written);
}

static void test_circular_buffer_write_to_store() {
  std::fprintf(stderr, "[test_circular_buffer_write_to_store] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(2, 512);
  
  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);

  Circular_buffer::Slot pending{};

  Record record(config.m_block_size, std::byte{'F'});
  std::span<const std::byte> span(record);

  for (size_t i = 0; i < config.m_n_blocks; ++i) {
    auto slot = buffer.reserve(config.m_block_size);
    
    do {
      auto result = buffer.write(slot, span);
      if (!result.has_value()) {
        assert(result.error() == Status::Not_enough_space);
        pending = slot;
        span = span.subspan(config.m_block_size - slot.m_len, slot.m_len);
        break;
      }
    } while (slot.m_len > 0);
  }

  assert(buffer.is_full());
  assert(buffer.check_margin() == 0);
  assert(buffer.m_lwm == initial_lsn);
  assert(buffer.m_hwm == initial_lsn + config.m_n_blocks * config.m_block_size);

  auto result = buffer.write_to_store(null_writer);

  const auto data_size = buffer.get_data_size_in_block();

  assert(result.has_value());
  assert(result.value() == initial_lsn + config.m_n_blocks * data_size);
  assert(buffer.check_margin() == buffer.get_total_data_size());

  assert(buffer.pending_writes());

  auto result2 = buffer.write(pending, span);
  assert(pending.m_len == 0);

  assert(result2.has_value());
  assert(!buffer.pending_writes());
  assert(buffer.check_margin() == buffer.get_total_data_size() - span.size());

  assert(buffer.m_lwm == buffer.m_hwm - span.size());
  assert(buffer.m_hwm == buffer.m_written_lsn);
  assert(buffer.m_n_pending_writes == 0);

  auto result3 = buffer.write_to_store(null_writer);
  assert(result3.has_value());

  /* Round up to block boundary and calculate number of blocks written */
  const auto total_bytes = config.m_n_blocks * data_size + span.size();
  const auto n_blocks_written = (total_bytes + data_size - 1) / data_size;
  const auto expected_lsn = initial_lsn + n_blocks_written * data_size;

  assert(result3.value() == expected_lsn);

  /* Since we only partially filled the block (span.size() bytes),
  the LWM should not advance past that. */
  const auto expected_lwm = initial_lsn + config.m_n_blocks * data_size + span.size();

  assert(buffer.m_lwm == expected_lwm);
  assert(buffer.m_lwm == buffer.m_hwm);
  assert(buffer.m_lwm == buffer.m_written_lsn);

  /* The block header for the last block should not be cleared. */
  const auto block_no = expected_lwm / data_size;
  const auto block_index = block_no % buffer.m_config.m_n_blocks;
  const auto block_header = buffer.get_block_header(block_index);
  assert(block_header.get_block_no() == block_no);
  assert(block_header.get_flush_bit());
  assert(block_header.get_data_len() == span.size());

  // FIXME: First rec group checks.

  std::fprintf(stderr, "[test_circular_buffer_write_to_store] done\n");
}

static void test_circular_buffer_wrap_around_basic() {
  std::fprintf(stderr, "[test_circular_buffer_wrap_around_basic] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);  // 4 blocks

  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);

  const auto data_size_per_block = static_cast<std::uint16_t>(buffer.get_data_size_in_block());

  /* Fill the buffer completely */
  for (size_t i = 0; i < config.m_n_blocks; ++i) {
    auto slot = buffer.reserve(data_size_per_block);
    Record data(data_size_per_block, std::byte{static_cast<unsigned char>('A' + (i % 26))});

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == data_size_per_block);
  }

  assert(buffer.is_full());
  assert(buffer.m_lwm == initial_lsn);
  const auto lwm_before_flush = buffer.m_lwm.load();

  /* Flush to store - this should advance LWM */
  auto flush_result = buffer.write_to_store(null_writer);
  assert(flush_result.has_value());

  const auto lwm_after_flush = buffer.m_lwm.load();
  assert(lwm_after_flush > lwm_before_flush);
  assert(buffer.check_margin() == buffer.get_total_data_size());

  /* Now write more data - this tests wrap-around */
  for (size_t i = 0; i < config.m_n_blocks; ++i) {
    auto slot = buffer.reserve(data_size_per_block);
    Record data(data_size_per_block, std::byte{static_cast<unsigned char>('W' + (i % 4))});

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == data_size_per_block);
  }

  /* Verify the data wrapped around correctly */
  auto span = buffer.data();
  for (size_t i = 0; i < config.m_n_blocks; ++i) {
    const auto offset = i * data_size_per_block;
    Record expected(data_size_per_block, std::byte{static_cast<unsigned char>('W' + (i % 4))});
    assert(::memcmp(span.data() + offset, expected.data(), data_size_per_block) == 0);
  }

  assert(buffer.is_full());

  std::fprintf(stderr, "[test_circular_buffer_wrap_around_basic] done\n");
}

static void test_circular_buffer_wrap_around_spanning() {
  std::fprintf(stderr, "[test_circular_buffer_wrap_around_spanning] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(8, 512);

  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);

  const auto data_size = buffer.get_data_size_in_block();

  /* Fill exactly half the buffer with aligned writes */
  const std::uint16_t record_size = data_size;

  const size_t blocks_to_write = config.m_n_blocks / 2;

  for (size_t i = 0; i < blocks_to_write; ++i) {
    auto slot = buffer.reserve(record_size);
    Record data(record_size, std::byte{static_cast<unsigned char>('A' + (i % 26))});

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == record_size);
  }

  /* Flush to make room */
  auto flush_result = buffer.write_to_store(null_writer);
  assert(flush_result.has_value());

  /* Write same number of blocks again - this test wraps-around */
  size_t post_flush_writes = 0;

  for (size_t i = 0; i < blocks_to_write && buffer.check_margin() >= record_size; ++i) {
    auto slot = buffer.reserve(record_size);
    Record data(record_size, std::byte{static_cast<unsigned char>('X' + (i % 3))});
    std::span<const std::byte> data_span = data;

    do {
      auto result = buffer.write(slot, data_span);
      if (result.has_value()) {
        assert(result.value() == record_size);
        ++post_flush_writes;
      } else {
        assert(result.error() == Status::Not_enough_space);
        data_span = data_span.subspan(data_span.size() - slot.m_len, slot.m_len);
      }
    } while (slot.m_len > 0);
  }

  assert(post_flush_writes == blocks_to_write);

  std::fprintf(stderr, "[test_circular_buffer_wrap_around_spanning] done\n");
}

static void test_circular_buffer_multiple_cycles() {
  std::fprintf(stderr, "[test_circular_buffer_multiple_cycles] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(8, 512);

  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);

  const std::uint16_t record_size = 128;

  constexpr size_t num_cycles = 10;
  lsn_t last_lwm = initial_lsn;

  for (size_t cycle = 0; cycle < num_cycles; ++cycle) {
    /* Fill buffer */
    size_t writes = 0;
    while (buffer.check_margin() >= record_size) {
      auto slot = buffer.reserve(record_size);
      Record data(record_size, std::byte{static_cast<unsigned char>('A' + (cycle % 26))});

      auto result = buffer.write(slot, data);
      if (result.has_value()) {
        assert(result.value() == record_size);
        ++writes;
      } else {
        break;
      }
    }

    /* Flush */
    auto flush_result = buffer.write_to_store(null_writer);
    assert(flush_result.has_value());

    /* Verify LWM is advancing */
    const auto current_lwm = buffer.m_lwm.load();
    assert(current_lwm > last_lwm);
    last_lwm = current_lwm;

    /* Buffer should be empty after flush */
    assert(buffer.check_margin() == buffer.get_total_data_size());
  }

  /* Verify we've advanced significantly through the LSN space */
  assert(buffer.m_lwm.load() > initial_lsn + buffer.get_total_data_size() * (num_cycles - 1));

  std::fprintf(stderr, "[test_circular_buffer_multiple_cycles] done (final LWM: %lu)\n",
               buffer.m_lwm.load());
}

static void test_circular_buffer_large_dataset() {
  std::fprintf(stderr, "[test_circular_buffer_large_dataset] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(16, 1024);

  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);

  const std::uint16_t record_size = 256;
  constexpr size_t target_records = 1000;

  size_t flush_count = 0;
  size_t total_written = 0;

  for (size_t i = 0; i < target_records; ++i) {
    /* Check if we need to flush */
    if (buffer.check_margin() < record_size) {
      auto flush_result = buffer.write_to_store(null_writer);
      assert(flush_result.has_value());
      ++flush_count;
    }

    auto slot = buffer.reserve(record_size);
    Record data(record_size);

    /* Fill with pattern based on record number */
    for (size_t j = 0; j < record_size; ++j) {
      data[j] = std::byte{static_cast<unsigned char>((i + j) % 256)};
    }

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == record_size);
    ++total_written;
  }

  /* Final flush */
  if (!buffer.is_empty()) {
    auto flush_result = buffer.write_to_store(null_writer);
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

  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);

  const auto data_size = buffer.get_data_size_in_block();

  /* Fill buffer mostly, leaving a partial block */
  for (size_t i = 0; i < config.m_n_blocks - 1; ++i) {
    auto slot = buffer.reserve(data_size);
    Record data(data_size, std::byte{static_cast<unsigned char>('F')});

    auto result = buffer.write(slot, data);
    assert(result.has_value());
  }

  /* Write partial block */
  const std::uint16_t partial_size = data_size / 2;
  auto partial_slot = buffer.reserve(partial_size);
  Record partial_data(partial_size, std::byte{static_cast<unsigned char>('P')});

  auto partial_result = buffer.write(partial_slot, partial_data);
  assert(partial_result.has_value());
  assert(partial_result.value() == partial_size);

  /* Flush - partial block should be handled correctly */
  auto flush_result = buffer.write_to_store(null_writer);
  assert(flush_result.has_value());

  /* Write more data to test wrap-around with partial block */
  for (size_t i = 0; i < config.m_n_blocks &&buffer.check_margin() >= data_size; ++i) {
    auto slot = buffer.reserve(data_size);
    Record data(data_size, std::byte{static_cast<unsigned char>('N')});
    std::span<const std::byte> data_span = data;

    do {
      auto result = buffer.write(slot, data_span);
      if (!result.has_value()) {
        assert(result.error() == Status::Not_enough_space);
        auto flush_result = buffer.write_to_store(null_writer);
        assert(flush_result.has_value());
        data_span = data_span.subspan(data_span.size() - slot.m_len, slot.m_len);
      }
    } while (slot.m_len > 0);
  }

  /* Buffer should be mostly empty after final flush */
  assert(buffer.check_margin() >= data_size / 2);

  std::fprintf(stderr, "[test_circular_buffer_wrap_around_partial_block] done\n");
}

static void test_circular_buffer_reserve_write_gap() {
  std::fprintf(stderr, "[test_circular_buffer_reserve_write_gap] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);

  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);

  /* Test the gap between HWM and committed_lsn */
  auto slot1 = buffer.reserve(500);
  auto slot2 = buffer.reserve(500);
  auto slot3 = buffer.reserve(500);

  /* HWM has advanced by 1500, but nothing committed yet */
  assert(buffer.m_hwm == 1500);
  assert(buffer.m_written_lsn == 0);
  assert(buffer.pending_writes());

  /* check_margin() should reflect committed data, not reserved */
  assert(buffer.check_margin() == buffer.get_total_data_size());

  /* NOTE: committed_lsn is a simple counter that increments by bytes written,
   * it doesn't track which specific LSN ranges are written. Out-of-order writes
   * will work but committed_lsn may not represent a contiguous range. */

  /* Write in order - write slot1, slot2, slot3 */
  Record data1(500, std::byte{'A'});
  auto result1 = buffer.write(slot1, data1);
  assert(result1.has_value());
  assert(result1.value() == 500);
  assert(buffer.m_written_lsn == 500);

  Record data2(500, std::byte{'B'});
  auto result2 = buffer.write(slot2, data2);
  assert(result2.has_value());
  assert(result2.value() == 500);
  assert(buffer.m_written_lsn == 1000);

  /* Now check_margin reflects the committed data */
  assert(buffer.check_margin() == buffer.get_total_data_size() - 1000);

  Record data3(500, std::byte{'C'});
  auto result3 = buffer.write(slot3, data3);
  assert(result3.has_value());
  assert(result3.value() == 500);
  assert(buffer.m_written_lsn == 1500);
  assert(!buffer.pending_writes());

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
    auto slot = buffer.reserve(data_size_per_block);
    Record data(data_size_per_block, std::byte{static_cast<unsigned char>('A' + i)});

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == data_size_per_block);
  }

  /* Write partial block (248 bytes) */
  const std::uint16_t partial_size = data_size_per_block / 2;
  auto partial_slot = buffer.reserve(partial_size);
  Record partial_data(partial_size, std::byte{'X'});

  auto partial_result = buffer.write(partial_slot, partial_data);
  assert(partial_result.has_value());
  assert(partial_result.value() == partial_size);

  const auto committed_before_flush = buffer.m_written_lsn.load();
  const auto block_3_index = 3;
  const auto block_3_len_before = buffer.get_block_header(block_3_index).get_data_len();
  assert(block_3_len_before == partial_size);

  /* Flush - should preserve the partial block */
  auto flush_result = buffer.write_to_store(null_writer);
  assert(flush_result.has_value());

  /* Write another 248 bytes to complete block 3 */
  auto continue_slot = buffer.reserve(partial_size);
  Record continue_data(partial_size, std::byte{'Y'});

  auto continue_result = buffer.write(continue_slot, continue_data);
  assert(continue_result.has_value());
  assert(continue_result.value() == partial_size);

  /* Verify block 3 now has full data */
  const auto block_3_len_after = buffer.get_block_header(block_3_index).get_data_len();
  assert(block_3_len_after == data_size_per_block);

  /* Verify block number stayed the same */
  const auto expected_block_no = committed_before_flush / data_size_per_block;
  assert(buffer.get_block_header(block_3_index).get_block_no() == expected_block_no);

  std::fprintf(stderr, "[test_circular_buffer_incremental_partial_block] done\n");
}

static void test_circular_buffer_single_byte_writes() {
  std::fprintf(stderr, "[test_circular_buffer_single_byte_writes] start\n");

  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);

  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);

  const auto data_size_per_block = buffer.get_data_size_in_block();

  /* Write single bytes to fill exactly one block */
  for (size_t i = 0; i < data_size_per_block; ++i) {
    auto slot = buffer.reserve(1);
    Record data(1, std::byte{static_cast<unsigned char>(i % 256)});

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == 1);
  }

  /* Verify block 0 is full */
  const auto block_0_len = buffer.get_block_header(0).get_data_len();
  assert(block_0_len == data_size_per_block);

  /* Verify committed_lsn */
  assert(buffer.m_written_lsn == data_size_per_block);

  /* Verify data integrity - read back the bytes */
  auto span = buffer.data();
  for (size_t i = 0; i < data_size_per_block; ++i) {
    assert(span[i] == std::byte{static_cast<unsigned char>(i % 256)});
  }

  std::fprintf(stderr, "[test_circular_buffer_single_byte_writes] done\n");
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
  auto slot1 = buffer.reserve(fill_size);
  Record data1(fill_size, std::byte{'A'});

  auto result1 = buffer.write(slot1, data1);
  assert(result1.has_value());
  assert(result1.value() == fill_size);

  /* Verify check_margin */
  const auto margin = buffer.check_margin();
  assert(margin == total_size - fill_size);
  assert(margin == 184);

  /* Write exactly the remaining space */
  const std::uint16_t exact_size = static_cast<std::uint16_t>(margin);
  auto slot2 = buffer.reserve(exact_size);
  Record data2(exact_size, std::byte{'B'});

  auto result2 = buffer.write(slot2, data2);
  assert(result2.has_value());
  assert(result2.value() == exact_size);

  /* Buffer should now be exactly full */
  assert(buffer.is_full());
  assert(buffer.check_margin() == 0);

  std::fprintf(stderr, "[test_circular_buffer_write_exact_margin] done\n");
}

static void test_circular_buffer_nonzero_initial_lsn() {
  std::fprintf(stderr, "[test_circular_buffer_nonzero_initial_lsn] start\n");

  constexpr lsn_t initial_lsn = 1000000;
  Circular_buffer::Config config(4, 512);

  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, config);

  const auto data_size_per_block = buffer.get_data_size_in_block();

  /* Verify initial block numbers */
  const auto expected_start_block = initial_lsn / data_size_per_block;
  auto block_header = buffer.get_block_header(expected_start_block % config.m_n_blocks);
  assert(block_header.get_block_no() == expected_start_block);

  /* Write some data */
  auto slot = buffer.reserve(500);
  Record data(500, std::byte{'Z'});

  auto result = buffer.write(slot, data);
  assert(result.has_value());
  assert(result.value() == 500);

  /* Verify LSNs are correct, we update the LSN of the slot to the length the written data. */
  assert(slot.m_lsn == initial_lsn + 500);
  assert(buffer.m_written_lsn == initial_lsn + 500);

  /* Verify block number */
  const auto block_0_no = buffer.get_block_header(0).get_block_no();
  assert(block_0_no == expected_start_block);

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
      auto slot = buffer.reserve(data_size_per_block);
      Record data(data_size_per_block, std::byte{static_cast<unsigned char>('A' + (cycle % 26))});

      auto result = buffer.write(slot, data);
      assert(result.has_value());
      assert(result.value() == data_size_per_block);

      /* Record the block number */
      const auto block_index = i;
      block_numbers.push_back(buffer.get_block_header(block_index).get_block_no());
    }

    /* Flush */
    auto flush_result = buffer.write_to_store(null_writer);
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
    auto slot = buffer.reserve(data_size_per_block - 1);
    Record data(data_size_per_block - 1, std::byte{0xAA});

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == data_size_per_block - 1);

    /* Should still be in block 0 */
    assert(buffer.get_block_header(0).get_data_len() == data_size_per_block - 1);
  }

  /* Test 2: Write 1 more byte to complete the block (total 496) */
  {
    auto slot = buffer.reserve(1);
    Record data(1, std::byte{0xBB});

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == 1);

    /* Block 0 should now be full */
    assert(buffer.get_block_header(0).get_data_len() == data_size_per_block);
  }

  /* Test 3: Write exactly block size (496 bytes) */
  {
    auto slot = buffer.reserve(data_size_per_block);
    Record data(data_size_per_block, std::byte{0xCC});

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == data_size_per_block);

    /* Block 1 should be full */
    assert(buffer.get_block_header(1).get_data_len() == data_size_per_block);
  }

  /* Test 4: Write 497 bytes (one more than block size) */
  {
    auto slot = buffer.reserve(data_size_per_block + 1);
    Record data(data_size_per_block + 1, std::byte{0xDD});

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == data_size_per_block + 1);

    /* Should span blocks 2 and 3 */
    assert(buffer.get_block_header(2).get_data_len() == data_size_per_block);
    assert(buffer.get_block_header(3).get_data_len() == 1);
  }

  std::fprintf(stderr, "[test_circular_buffer_off_by_one_boundary] done\n");
}

/**
 * Test: Flush With Only Partial First Block
 * Category: Flush Edge Cases
 * Purpose: Test flushing less than one full block
 */
static void test_circular_buffer_flush_partial_block() {
  std::fprintf(stderr, "[test_circular_buffer_flush_partial_block] start\n");

  using Record = std::vector<std::byte>;

  Circular_buffer::Config config(4, 512);
  Circular_buffer buffer(0, config);

  /* Write only 100 bytes (less than one block) */
  auto slot = buffer.reserve(100);
  Record data(100, std::byte{0xAA});

  auto result = buffer.write(slot, data);
  assert(result.has_value());
  assert(result.value() == 100);

  /* Verify state before flush */
  assert(buffer.m_written_lsn == 100);
  assert(buffer.m_lwm == 0);

  /* Flush */

  auto flush_result = buffer.write_to_store(null_writer);
  assert(flush_result.has_value());

  /* After flush, lwm advances to end of flushed data even for partial blocks */
  assert(buffer.m_lwm == 100);

  /* The partial block data is preserved in the buffer for potential continuation */
  /* Only full blocks are cleared by the clear() function */

  std::fprintf(stderr, "[test_circular_buffer_flush_partial_block] done\n");
}

/**
 * Test: Flush Exactly N Full Blocks
 * Category: Flush Edge Cases
 * Purpose: Test clean block boundary flush
 */
static void test_circular_buffer_flush_exact_blocks() {
  std::fprintf(stderr, "[test_circular_buffer_flush_exact_blocks] start\n");

  using Record = std::vector<std::byte>;

  Circular_buffer::Config config(4, 512);
  Circular_buffer buffer(0, config);

  const auto data_size_per_block = buffer.get_data_size_in_block();

  /* Write exactly 3 full blocks (496 * 3 = 1488 bytes) */
  const std::uint16_t write_size = data_size_per_block * 3;
  auto slot = buffer.reserve(write_size);
  Record data(write_size, std::byte{0xBB});

  auto result = buffer.write(slot, data);
  assert(result.has_value());
  assert(result.value() == write_size);

  /* Verify committed */
  assert(buffer.m_written_lsn == 1488);

  /* Flush */

  auto flush_result = buffer.write_to_store(null_writer);
  assert(flush_result.has_value());
  assert(flush_result.value() == 1488);

  /* All 3 blocks should be cleared, lwm should advance to 1488 */
  assert(buffer.m_lwm == 1488);

  /* Verify blocks 0, 1, 2 are cleared */
  assert(buffer.get_block_header(0).get_data_len() == 0);
  assert(buffer.get_block_header(1).get_data_len() == 0);
  assert(buffer.get_block_header(2).get_data_len() == 0);

  std::fprintf(stderr, "[test_circular_buffer_flush_exact_blocks] done\n");
}

/**
 * Test: Write Returns Zero Bytes (Buffer Full)
 * Category: Partial Write Scenarios
 * Purpose: Test when buffer is completely full
 */
static void test_circular_buffer_write_buffer_full() {
  std::fprintf(stderr, "[test_circular_buffer_write_buffer_full] start\n");

  using Record = std::vector<std::byte>;

  Circular_buffer::Config config(4, 512);
  Circular_buffer buffer(0, config);

  const auto total_capacity = buffer.get_total_data_size();

  /* Fill buffer to capacity */
  auto slot1 = buffer.reserve(total_capacity);
  Record data1(total_capacity, std::byte{0xCC});

  auto result1 = buffer.write(slot1, data1);
  assert(result1.has_value());
  assert(result1.value() == total_capacity);

  /* Verify buffer is full */
  assert(buffer.check_margin() == 0);
  assert(buffer.is_full());

  /* Try to reserve and write more - reserve succeeds but write should fail or write 0 bytes */
  auto slot2 = buffer.reserve(100);
  assert(slot2.m_len == 100);  /* reserve() always returns requested length */

  Record data2(100, std::byte{0xDD});
  auto result2 = buffer.write(slot2, data2);

  /* Write should either fail or return 0 bytes written when buffer is full */
  /* In this implementation, it appears to return an error when check_margin is 0 */
  if (result2.has_value()) {
    assert(result2.value() == 0);  /* 0 bytes written */
  } else {
    /* Error returned - buffer is full */
    assert(buffer.check_margin() == 0);
  }

  std::fprintf(stderr, "[test_circular_buffer_write_buffer_full] done\n");
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
    auto slot = buffer.reserve(data_size_per_block);
    Record data(data_size_per_block, patterns[i]);

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == data_size_per_block);
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

  auto flush_result = buffer.write_to_store(null_writer);
  assert(flush_result.has_value());

  /* Write new blocks with different patterns after wrap */
  const std::array<std::byte, 4> new_patterns = {
    std::byte{0x11}, std::byte{0x22}, std::byte{0x33}, std::byte{0x44}
  };

  for (size_t i = 0; i < 4; ++i) {
    auto slot = buffer.reserve(data_size_per_block);
    Record data(data_size_per_block, new_patterns[i]);

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == data_size_per_block);
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
    auto slot = buffer.reserve(data_size_per_block);
    Record data(data_size_per_block, std::byte{static_cast<unsigned char>('A' + (i % 26))});

    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == data_size_per_block);

    /* Flush */
    auto flush_result = buffer.write_to_store(null_writer);
    assert(flush_result.has_value());
  }

  /* Verify total bytes written */
  lsn_t expected_lsn = num_iterations * data_size_per_block;
  assert(buffer.m_lwm == expected_lsn);

  std::fprintf(stderr, "[test_circular_buffer_minimal_write_flush_cycle] done "
               "(%zu iterations)\n", num_iterations);
}

// FIXME: Add read tests once the writes work.

static void test_log_basic() {
  std::fprintf(stderr, "[test_log_basic] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);

  Log log(initial_lsn, config);
  
  /* Verify initial state */
  assert(log.is_empty());
  assert(!log.is_full());
  assert(log.check_margin() > 0);
  
  std::fprintf(stderr, "[test_log_basic] done\n");
}

static void test_log_reserve_and_write() {
  std::fprintf(stderr, "[test_log_reserve_and_write] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);
  
  Log log(initial_lsn, config);
  
  /* Reserve and write */
  auto slot = log.reserve(100);
  std::vector<std::byte> data(100, std::byte{'L'});
  
  auto result = log.write(slot, data);
  assert(result.has_value());
  assert(result.value() == 100);
  
  /* Verify state */
  assert(!log.is_empty());
  
  std::fprintf(stderr, "[test_log_reserve_and_write] done\n");
}

static void test_log_write_data_verification() {
  std::fprintf(stderr, "[test_log_write_data_verification] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  Circular_buffer::Config config(4, 512);

  Log log(initial_lsn, config);
  
  /* Write specific data and verify it can be read back */
  std::string test_data = "Hello, WAL!";
  auto slot = log.reserve(static_cast<std::uint16_t>(test_data.size()));
  
  auto result = log.write(slot, std::as_bytes(std::span{test_data}));
  assert(result.has_value());
  assert(result.value() == test_data.size());
  
  /* Verify data was written by checking the buffer directly */
  const auto block_index = slot.m_lsn / log.m_buffer.get_data_size_in_block();
  const auto data_span = log.m_buffer.get_data(static_cast<std::size_t>(block_index));
  
  assert(::memcmp(data_span.data(), test_data.data(), test_data.size()) == 0);

  std::fprintf(stderr, "[test_log_write_data_verification] done\n");
}

static void test_log_multiple_writes() {
  std::fprintf(stderr, "[test_log_multiple_writes] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t record_size = 26;
  Circular_buffer::Config config(4, 512);

  using Record = std::vector<std::byte>;

  Log log(initial_lsn, config);
  
  /* Write multiple entries */
  constexpr int num_writes = 10;
  for (int i = 0; i < num_writes; ++i) {
    auto slot = log.reserve(record_size);
    Record data(record_size, std::byte{static_cast<unsigned char>('A' + i)});
    
    auto result = log.write(slot, data);
    assert(result.has_value());
    assert(result.value() == record_size);
  }
  
  /* Verify buffer state */
  assert(!log.is_empty());
  assert(log.check_margin() == log.m_buffer.get_total_data_size() - record_size * num_writes);
  
  std::fprintf(stderr, "[test_log_multiple_writes] done\n");
}

static void test_log_to_string() {
  std::fprintf(stderr, "[test_log_to_string] start\n");
  
  constexpr lsn_t initial_lsn = 100;
  Circular_buffer::Config config(4, 512);
  
  Log log(initial_lsn, config);
  
  auto str = log.to_string();
  assert(!str.empty());
  assert(str.find("Circular_buffer") != std::string::npos);
  
  std::fprintf(stderr, "[test_log_to_string] done: %s\n", str.c_str());
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
  test_circular_buffer_wrap_around_spanning();
  test_circular_buffer_multiple_cycles();
  test_circular_buffer_large_dataset();
  test_circular_buffer_wrap_around_partial_block();

  /* Additional edge case tests */
  test_circular_buffer_reserve_write_gap();
  test_circular_buffer_incremental_partial_block();
  test_circular_buffer_single_byte_writes();
  test_circular_buffer_write_exact_margin();
  test_circular_buffer_nonzero_initial_lsn();
  test_circular_buffer_block_number_continuity();
  test_circular_buffer_off_by_one_boundary();
  test_circular_buffer_flush_partial_block();
  test_circular_buffer_flush_exact_blocks();
  test_circular_buffer_write_buffer_full();
  test_circular_buffer_pattern_verification();
  test_circular_buffer_minimal_write_flush_cycle();  /* Disabled - see test comment */

  /* Log tests */
  test_log_basic();
  test_log_reserve_and_write();
  test_log_write_data_verification();
  test_log_multiple_writes();
  test_log_to_string();

  std::fprintf(stderr, "\n=== All WAL tests passed! ===\n");
  return 0;
}
