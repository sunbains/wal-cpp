#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>
#include <string>
#include <span>
#include <algorithm>
#include <iterator>

#include "wal/wal.h"

using wal::Log;
using wal::Circular_buffer;
using wal::Status;
using wal::lsn_t;

static void test_circular_buffer_basic() {
  std::fprintf(stderr, "[test_circular_buffer_basic] start\n");
  
  constexpr lsn_t initial_lsn = 100;
  constexpr size_t n_blocks = 4;
  constexpr size_t block_size = 512;
  
  Circular_buffer buffer(initial_lsn, n_blocks, block_size);
  
  /* Verify initial state */
  assert(buffer.is_empty());
  assert(!buffer.is_full());
  assert(buffer.check_margin() == buffer.get_total_data_size());
  
  /* Test reserve */
  auto slot1 = buffer.reserve(100);
  assert(slot1.m_lsn == initial_lsn);
  assert(slot1.m_len == 100);
  assert(!slot1.m_committed);
  
  /* After reserve, HWM should be incremented but still empty until write */
  assert(buffer.check_margin() < buffer.get_total_data_size());
  assert(buffer.m_lwm == initial_lsn);
  assert(buffer.m_hwm == initial_lsn + 100);
  
  std::fprintf(stderr, "[test_circular_buffer_basic] done\n");
}

static void test_circular_buffer_reserve_and_write() {
  std::fprintf(stderr, "[test_circular_buffer_reserve_and_write] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t n_blocks = 4;
  constexpr size_t block_size = 512;
  
  Circular_buffer buffer(initial_lsn, n_blocks, block_size);
  
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
  constexpr size_t n_blocks = 2;
  constexpr size_t block_size = 512;
  constexpr size_t record_size = 64;  
  constexpr size_t n_records = (block_size/record_size) * n_blocks;
  
  Circular_buffer buffer(initial_lsn, n_blocks, block_size);
  using Record = std::vector<std::byte>;
  
  /* Write multiple slots */
  for (size_t i = 0; i < n_records; ++i) {
    auto slot = buffer.reserve(record_size);
    Record data(record_size, std::byte{static_cast<unsigned char>('0' + i)});
    
    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == record_size);
  }

  // Verify the data written matches the contents of the data array
  for (size_t i = 0; i < n_records; ++i) {
    auto span = buffer.get_data(i);
    assert(span.size() == record_size);

    Record data(record_size, std::byte{static_cast<unsigned char>('0' + i)});

    assert(::memcmp(span.data(), data.data(), 64) == 0);
  }

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
  constexpr size_t n_blocks = 2;
  constexpr size_t block_size = 512;

  using Record = std::vector<std::byte>;
  
  Circular_buffer buffer(initial_lsn, n_blocks, block_size);
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
  constexpr size_t n_blocks = 2;
  constexpr size_t block_size = 512;
  
  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, n_blocks, block_size);
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
  constexpr size_t n_blocks = 2;
  constexpr size_t block_size = 512;
  
  using Record = std::vector<std::byte>;
  Circular_buffer buffer(initial_lsn, n_blocks, block_size);

  auto null_writer = [](lsn_t lsn, const wal::Log::IO_vecs &iovecs) -> wal::Result<lsn_t> { 
    std::size_t bytes_written{};

    for (std::size_t i = 1; i < iovecs.size(); i += 3) {
      /* Skip the header and checksum */
      bytes_written += iovecs[i].iov_len;
    }
    return wal::Result<lsn_t>(lsn + bytes_written);
  };

  for (size_t i = 0; i < n_blocks; ++i) {
    auto slot = buffer.reserve(block_size);
    Record data(block_size, std::byte{'F'});
    
    auto result = buffer.write(slot, data);
    assert(result.has_value());
    assert(result.value() == block_size);
  }

  assert(buffer.is_full());
  assert(buffer.check_margin() == 0);
  assert(buffer.m_lwm == initial_lsn);
  assert(buffer.m_hwm == initial_lsn + n_blocks * block_size);

  auto result = buffer.write_to_store(null_writer);
  assert(result.has_value());
  assert(result.value() == initial_lsn + n_blocks * block_size);
  assert(buffer.is_empty());

  assert(buffer.m_lwm == result.value());
  assert(buffer.m_lwm == buffer.m_hwm);

  std::fprintf(stderr, "[test_circular_buffer_write_to_store] done\n");
}

// FIXME: Add read tests once the writes work.

static void test_log_basic() {
  std::fprintf(stderr, "[test_log_basic] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t n_blocks = 4;
  constexpr size_t block_size = 512;
  
  Log log(initial_lsn, n_blocks, block_size);
  
  /* Verify initial state */
  assert(log.is_empty());
  assert(!log.is_full());
  assert(log.check_margin() > 0);
  
  std::fprintf(stderr, "[test_log_basic] done\n");
}

static void test_log_reserve_and_write() {
  std::fprintf(stderr, "[test_log_reserve_and_write] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t n_blocks = 4;
  constexpr size_t block_size = 512;
  
  Log log(initial_lsn, n_blocks, block_size);
  
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
  constexpr size_t n_blocks = 4;
  constexpr size_t block_size = 512;
  
  using Record = std::vector<std::byte>;
  Log log(initial_lsn, n_blocks, block_size);
  
  /* Write specific data and verify it can be read back */
  std::string test_data = "Hello, WAL!";
  auto slot = log.reserve(static_cast<std::uint16_t>(test_data.size()));
  
  Record data_bytes;

  {
    auto bytes = std::as_bytes(std::span{test_data});
    data_bytes.assign(bytes.begin(), bytes.end());
  }

  auto result = log.write(slot, data_bytes);
  assert(result.has_value());
  assert(result.value() == test_data.size());
  
  /* Verify data was written by checking the buffer directly */
  const auto block_index = slot.m_lsn / log.m_buffer.get_data_size_in_block();
  const auto data_span = log.m_buffer.get_data(static_cast<std::size_t>(block_index));
  
  assert(data_span.size() == test_data.size());
  assert(::memcmp(data_span.data(), test_data.data(), test_data.size()) == 0);

  std::fprintf(stderr, "[test_log_write_data_verification] done\n");
}

static void test_log_multiple_writes() {
  std::fprintf(stderr, "[test_log_multiple_writes] start\n");
  
  constexpr lsn_t initial_lsn = 0;
  constexpr size_t n_blocks = 4;
  constexpr size_t block_size = 512;
  constexpr size_t record_size = 32;

  using Record = std::vector<std::byte>;

  Log log(initial_lsn, n_blocks, block_size);
  
  /* Write multiple entries */
  constexpr int num_writes = 10;
  for (int i = 0; i < num_writes; ++i) {
    auto slot = log.reserve(record_size);
    Record data(record_size, std::byte{static_cast<unsigned char>(i)});
    
    auto result = log.write(slot, data);
    assert(result.has_value());
    assert(result.value() == record_size);
  }
  
  /* Verify buffer state */
  assert(!log.is_empty());
  assert(log.check_margin() < log.m_buffer.get_total_data_size());
  
  std::fprintf(stderr, "[test_log_multiple_writes] done\n");
}

static void test_log_to_string() {
  std::fprintf(stderr, "[test_log_to_string] start\n");
  
  constexpr lsn_t initial_lsn = 100;
  constexpr size_t n_blocks = 4;
  constexpr size_t block_size = 512;
  
  Log log(initial_lsn, n_blocks, block_size);
  
  auto str = log.to_string();
  assert(!str.empty());
  assert(str.find("Circular_buffer") != std::string::npos);
  
  std::fprintf(stderr, "[test_log_to_string] done: %s\n", str.c_str());
}

int main() {
  test_circular_buffer_basic();
  test_circular_buffer_reserve_and_write();
  test_circular_buffer_multiple_writes();
  test_circular_buffer_block_boundary();
  test_circular_buffer_full();
  test_circular_buffer_write_to_store();
  
  test_log_basic();
  test_log_reserve_and_write();
  test_log_write_data_verification();
  test_log_multiple_writes();
  test_log_to_string();
  
  std::fprintf(stderr, "All WAL tests passed!\n");
  return 0;
}
