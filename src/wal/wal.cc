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

#include <algorithm>
#include <chrono>
#include <format>
#include <sys/uio.h>
#include <cstring>
#include <string>
#include <sstream>

#include <cassert>
#include <limits>

#include "util/checksum.h"
#include "util/metrics.h"
#include "wal/wal.h"
#include "wal/buffer_pool.h"

namespace wal {

 Buffer::Buffer(lsn_t hwm, const Config& config) noexcept
  : m_config(config),
    m_total_data_size(m_config.get_data_size_in_block() * m_config.m_n_blocks),
    m_checksum(m_config.m_checksum_algorithm) {

  /* Allocate buffer with required size - allocator handles alignment */
  const auto required_size = m_total_data_size + (sizeof(crc32_t) + sizeof(Block_header)) * m_config.m_n_blocks;

  m_buffer.resize(required_size);

  /* Buffer is already aligned by posix_memalign, so we can use it directly */
  auto aligned_ptr = reinterpret_cast<std::uintptr_t>(m_buffer.data());

  m_block_header_array = reinterpret_cast<Block_header*>(aligned_ptr);
  m_data_array = reinterpret_cast<std::byte*>(m_block_header_array + m_config.m_n_blocks);
  m_crc32_array = reinterpret_cast<crc32_t*>(m_data_array + m_total_data_size);

  /* Verify alignment */
  WAL_ASSERT(reinterpret_cast<std::uintptr_t>(m_block_header_array) % kCLS == 0);
  WAL_ASSERT(reinterpret_cast<std::uintptr_t>(m_data_array) % kCLS == 0);

  m_iovecs.resize((IOV_MAX / 3) * 3);

  initialize(hwm);
}

 Buffer::~Buffer() noexcept {}

void Buffer::initialize(lsn_t hwm) noexcept {
  m_hwm = hwm;
  m_lwm = hwm;

  /* Clear all block headers to ensure clean state when buffer is reused */
  std::memset(m_block_header_array, 0, sizeof(Block_header) * m_config.m_n_blocks);

  const auto data_size = m_config.get_data_size_in_block();
  [[maybe_unused]] const auto block_start_no{m_lwm / data_size};
  WAL_ASSERT(block_start_no + m_config.m_n_blocks < std::numeric_limits<block_no_t>::max());

  /* Set the initial block header at the HWM position */
  auto &block_header = m_block_header_array[block_start_no % m_config.m_n_blocks];

  block_header.set_flush_bit(true);
  block_header.set_data_len(uint16_t(hwm % data_size));
  block_header.set_block_no(block_no_t(block_start_no));
  block_header.set_first_rec_group(block_header.get_data_len());

  m_append_ptr = m_data_array + (m_hwm % data_size);
  WAL_ASSERT(m_append_ptr >= m_data_array);
  WAL_ASSERT(m_append_ptr < m_data_array + m_total_data_size);
}

/* write_to_store implementation is now in the header file as an inline function */

Buffer::Buffer(const Config& config) noexcept
  : Buffer(0, config) {}

std::string Buffer::to_string() const noexcept {
  return std::format(
    "Buffer: lwm={}, hwm={}, margin={}, total_size={}",
    m_lwm, m_hwm, margin(), get_total_data_size());
}

void Buffer::clear(lsn_t start_lsn, lsn_t end_lsn) noexcept {
  WAL_ASSERT(start_lsn <= end_lsn);
  WAL_ASSERT(end_lsn <= m_hwm);

  const auto end_ptr = m_block_header_array + m_config.m_n_blocks;
  const auto data_size = m_config.get_data_size_in_block();
  const auto first_block_no = start_lsn / data_size;
  const auto last_block_no = end_lsn / data_size;

  if (last_block_no == first_block_no) {
    return;
  }

  /* Clear blocks from first_block_no up to (but not including) last_block_no */
  /* This ensures we don't clear the block at end_lsn */
  const auto n_blocks_to_clear = last_block_no - first_block_no;
  [[maybe_unused]] auto n_bytes_to_clear = n_blocks_to_clear * sizeof(Block_header);

  WAL_ASSERT(n_bytes_to_clear <= sizeof(Block_header) * m_config.m_n_blocks);

  const auto start_block_index = first_block_no % m_config.m_n_blocks;
  auto ptr = m_block_header_array + start_block_index;

  {
    using uptrdiff_t = std::make_unsigned_t<std::ptrdiff_t>;

    WAL_ASSERT(sizeof(Block_header) * n_blocks_to_clear >= n_bytes_to_clear);
    const auto len = std::min(n_bytes_to_clear, uptrdiff_t(end_ptr - ptr) * sizeof(Block_header));
    std::memset(ptr, 0, len);
    n_bytes_to_clear -= len;
  }

  /* Check for wrap around. */
  if (n_bytes_to_clear > 0) {
    std::memset(m_block_header_array, 0, n_bytes_to_clear);
    n_bytes_to_clear = 0;
  }

  WAL_ASSERT(n_bytes_to_clear == 0);
}

namespace {

std::size_t prepare_batch(Buffer& buffer, block_no_t start, block_no_t end, std::size_t batch_size) noexcept {
  const auto n_slots = batch_size * 3;
  const auto data_size = buffer.m_config.get_data_size_in_block();

  auto block_no = start;
  const auto total_data_len = buffer.m_hwm - buffer.m_lwm;

  WAL_ASSERT(total_data_len <= buffer.get_total_data_size());

  auto remaining_data_len = total_data_len;
  auto block_index = block_no % buffer.m_config.m_n_blocks;

  for (std::size_t i{}; i < n_slots; i += 3,  ++block_no) {
    auto [header, span, crc32] = buffer.get_block(block_index);

    header->set_block_no(block_no);
    header->set_data_len(std::uint16_t(std::min(remaining_data_len, data_size)));

    const auto is_last_block = header->get_block_no() == end;

    if (is_last_block) [[unlikely]] {
      header->set_flush_bit(true);
      if (header->get_first_rec_group() == 0) [[unlikely]] {
        header->set_first_rec_group(header->get_data_len());
      }
    } else {
      header->set_first_rec_group(0);
    }

    WAL_ASSERT(header->m_data.m_len == uint16_t(data_size)
           || (is_last_block
               && header->get_flush_bit()
               && header->m_data.m_len < uint16_t(data_size)));

    buffer.m_checksum.reset();
    buffer.m_checksum.update(span);
    *crc32 = buffer.m_checksum.value();

    buffer.m_iovecs[i].iov_base = const_cast<Block_header::Data*>(&header->m_data);
    buffer.m_iovecs[i].iov_len = sizeof(Block_header::Data);
    buffer.m_iovecs[i + 1].iov_base = const_cast<std::byte*>(span.data());
    buffer.m_iovecs[i + 1].iov_len = span.size();
    buffer.m_iovecs[i + 2].iov_base = static_cast<void*>(crc32);
    buffer.m_iovecs[i + 2].iov_len = sizeof(crc32_t);

    block_index = (block_index + 1) % buffer.m_config.m_n_blocks;

    WAL_ASSERT(remaining_data_len >= header->get_data_len());
    remaining_data_len -= header->get_data_len();
  }

  return total_data_len;
}

} // anonymous namespace

Result<lsn_t> Buffer::write_to_store(Write_callback callback, lsn_t max_write_lsn) noexcept {
  WAL_ASSERT(!is_empty());
  WAL_ASSERT(is_write_pending());

  std::size_t total_batches = 0;
  std::size_t total_bytes_written = 0;
  std::size_t total_blocks_written = 0;

  const auto data_size{m_config.get_data_size_in_block()};

  /* We start writing from the LWM, which is the first block that has data
   * that has not been written to the store yet. */
  auto block_start_no = static_cast<block_no_t>(m_lwm / data_size);

  /* Limit write to max_write_lsn if specified (for sync operations) */
  const auto write_limit_lsn = (max_write_lsn > 0) ? std::min(m_hwm, max_write_lsn) : m_hwm;
  const auto n_bytes_to_flush = write_limit_lsn - m_lwm;

  auto n_blocks_to_flush = (n_bytes_to_flush + data_size - 1) / data_size;
  const auto last_block_no = static_cast<block_no_t>(block_start_no + n_blocks_to_flush - 1);

  /* Limit batch size to what iovecs array can hold */
  const auto iovecs_size = m_iovecs.size();
  const auto blocks_per_batch = iovecs_size / 3;

  std::size_t data_len{};
  auto start_lwm = m_lwm;
  std::size_t old_data_len = m_lwm % data_size;

  using Clock = std::chrono::steady_clock;

  Clock::time_point write_start;

  if (m_metrics != nullptr) [[likely]] {
    write_start = Clock::now();
  }

  /* Write in batches like async version */
  while (n_blocks_to_flush > 0) {
    const auto flush_batch_size = std::min(n_blocks_to_flush, blocks_per_batch);
    const auto n_slots = flush_batch_size * 3;
    ++total_batches;

    /* Time prepare_batch */
    Clock::time_point prepare_start;
    if (m_metrics != nullptr) [[likely]] {
      prepare_start = Clock::now();
    }

    data_len = prepare_batch(*this, block_start_no, last_block_no, flush_batch_size);

    if (m_metrics != nullptr) [[likely]] {
      const auto prepare_end = Clock::now();
      const auto prepare_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(prepare_end - prepare_start);

      m_metrics->add_timing(util::MetricType::WriteToStorePrepareBatch, prepare_duration);
    }

    n_blocks_to_flush -= flush_batch_size;
    block_start_no += static_cast<block_no_t>(flush_batch_size);
    total_blocks_written += flush_batch_size;

    Clock::time_point io_start;
    if (m_metrics != nullptr) [[likely]] {
      io_start = Clock::now();
    }

    auto io_result = callback(std::span<struct iovec>(m_iovecs.data(), n_slots));

    if (m_metrics != nullptr) [[likely]] {
      const auto io_end = Clock::now();
      const auto io_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(io_end - io_start);

      m_metrics->add_timing(util::MetricType::WriteToStoreIoCallback, io_duration);
      /* Record only payload bytes for I/O size metrics (exclude header + CRC). */
      m_metrics->add_io_size(data_len - old_data_len);
    }

    if (!io_result.has_value()) {
      return std::unexpected(io_result.error());
    }

    /* Metrics should reflect payload bytes, not physical bytes (headers + CRC). */
    data_len -= old_data_len;
    old_data_len = 0;
    const auto payload_bytes_written = data_len;
    total_bytes_written += payload_bytes_written;

    WAL_ASSERT(io_result.value() % data_size == 0);

    /* Time clear operation */
    Clock::time_point clear_start;

    if (m_metrics != nullptr) [[likely]] {
      clear_start = Clock::now();
    }

    /* Only clear if we wrote all the data in this batch (not limited by max_write_lsn).
     * If we're limited by max_write_lsn, we can't clear because there's still data in the buffer */
    if (start_lwm + data_len <= write_limit_lsn) {
      clear(start_lwm, start_lwm + data_len);
      start_lwm += data_len;
    } else {
      /* We hit the max_write_lsn limit - don't clear or update lwm, we'll write the rest later.
       * But we need to update lwm to what we actually wrote */
      const auto actual_written = write_limit_lsn - start_lwm;

      if (actual_written > 0) {
        /* Clear only the blocks we fully wrote */
        const auto clear_end_lsn = (actual_written / data_size) * data_size + start_lwm;
        if (clear_end_lsn > start_lwm) {
          clear(start_lwm, clear_end_lsn);
          start_lwm = clear_end_lsn;
        }
      }
      /* Break out of the loop since we've hit the limit */
      break;
    }

    if (m_metrics != nullptr) [[likely]] {
      const auto clear_end = Clock::now();
      const auto clear_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(clear_end - clear_start);
      m_metrics->add_timing(util::MetricType::WriteToStoreClear, clear_duration);
    }
  }

  m_lwm = start_lwm;
  m_append_ptr = m_data_array;

  /* Record overall timing and counters */
  if (m_metrics != nullptr) [[likely]] {
    auto write_end = Clock::now();
    auto write_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(write_end - write_start);
    m_metrics->add_timing(util::MetricType::WriteToStoreTotal, write_duration);
    m_metrics->inc(util::MetricType::WriteToStoreCalls);
    m_metrics->inc(util::MetricType::WriteToStoreBlocksWritten, total_blocks_written);
    m_metrics->inc(util::MetricType::WriteToStoreBatches, total_batches);
    m_metrics->inc(util::MetricType::WriteToStoreBytesWritten, total_bytes_written);
  }

  return Result<lsn_t>(m_lwm);
}

Result<lsn_t> Buffer::read_from_store(lsn_t, Read_callback) noexcept {
  // TODO: Implement this
  assert(false);

  // assert(m_lwm <= m_hwm);
  // IO_vecs iovecs;

  // m_lwm = lsn;
  // m_hwm = callback(m_lwm, iovecs);
  // assert(m_lwm <= m_hwm);
  // return m_hwm;
  return Result<lsn_t>(0);
 }

 Log::Log(lsn_t lsn, const Config &config)
  : m_pool(std::make_unique<Pool>(config.m_pool_config, config.m_buffer_config, lsn)) {}

 Log::~Log() noexcept {
  WAL_ASSERT(m_pool->m_active == nullptr);
 }


bool Log::is_full() const noexcept {
  WAL_ASSERT(m_pool->m_active != nullptr);
  return m_pool->m_active->m_buffer.is_full();
}

bool Log::is_empty() const noexcept {
  WAL_ASSERT(m_pool->m_active != nullptr);
  return m_pool->m_active->m_buffer.is_empty();
}

std::size_t Log::margin() const noexcept {
  WAL_ASSERT(m_pool->m_active != nullptr);
  return m_pool->m_active->m_buffer.margin();
}

void Log::start_io(Write_callback callback, util::Thread_pool* thread_pool) noexcept {
  m_thread_pool = thread_pool;
  m_write_callback = callback;

  /* Store sync callback for inline I/O when buffers exhausted */
  m_pool->m_sync_write_callback = callback;

  /* Create an adapter that converts Write_callback to a function
   * that takes Buffer& and returns Result<lsn_t>. Tasks are posted
   * directly by consumer, no background coroutine needed. */
  auto adapter = [this](Buffer& buffer) -> Result<lsn_t> {
    return buffer.write_to_store(m_write_callback);
  };

  /* Store the callback in Log - Log is the coordinator and controls orchestration */
  m_io_callback = adapter;
  m_pool->m_io_thread_running = true;
}

Result<bool> Log::write_to_store(Write_callback callback) noexcept {
  /* Create an adapter that converts Write_callback to a function
   * that takes Buffer& and returns Result<bool>. */
  auto adapter = [callback](Buffer& buffer) -> Result<bool> {
    return buffer.write_to_store(callback, 0);
  };

  return m_pool->write_to_store(adapter);
}

std::string Log::to_string() const noexcept {
  std::stringstream ss;

  WAL_ASSERT(m_pool->m_active == nullptr);

  ss << m_pool->to_string() << std::endl;
  return ss.str();
}

void Log::set_metrics(util::Metrics* metrics) noexcept {
  m_metrics = metrics;
  
  /* Set metrics on all buffers in the pool */
  if (m_pool != nullptr) [[likely]] {
    for (auto& entry : m_pool->m_buffers) {
      entry->m_buffer.set_metrics(metrics);
    }
  }
}


} // namespace wal
