#include <algorithm>
#include <chrono>
#include <format>
#include <sys/uio.h>
#include <cstring>

#include <cassert>
#include <limits>

#include "util/checksum.h"
#include "wal/wal.h"
#include "wal/buffer_pool.h"
#include "wal/async_io.h"

namespace wal {

 Circular_buffer::Circular_buffer(lsn_t hwm, const Config& config) noexcept
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

 Circular_buffer::~Circular_buffer() noexcept {}

void Circular_buffer::initialize(lsn_t hwm) noexcept {
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

Circular_buffer::Circular_buffer(const Config& config) noexcept
  : Circular_buffer(0, config) {}

std::string Circular_buffer::to_string() const noexcept {
  return std::format(
    "Circular_buffer: lwm={}, hwm={}, margin={}, total_size={}",
    m_lwm, m_hwm, margin(), get_total_data_size());
}

void Circular_buffer::clear(lsn_t start_lsn, lsn_t end_lsn) noexcept {
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
  auto n_bytes_to_clear = n_blocks_to_clear * sizeof(Block_header);

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

std::size_t prepare_batch(Circular_buffer& buffer, block_no_t start, block_no_t end, std::size_t batch_size) noexcept {
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

Result<lsn_t> Circular_buffer::write_to_store(Write_callback callback, lsn_t max_write_lsn) noexcept {
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

    auto io_result = callback(std::span<struct iovec>(m_iovecs.data(), n_slots), wal::Log::Sync_type::None);

    if (m_metrics != nullptr) [[likely]] {
      const auto io_end = Clock::now();
      const auto io_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(io_end - io_start);

      m_metrics->add_timing(util::MetricType::WriteToStoreIoCallback, io_duration);
      m_metrics->add_io_size(data_len);
    }

    if (!io_result.has_value()) {
      return std::unexpected(io_result.error());
    }

    data_len -= old_data_len;
    old_data_len = 0;
    total_bytes_written += data_len;

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

Result<lsn_t> Circular_buffer::read_from_store(lsn_t, Read_callback) noexcept {
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

 Log::Log(lsn_t lsn, size_t pool_size, const Circular_buffer::Config &config)
  : m_pool(std::make_unique<Pool>(pool_size, config, lsn)) {}

 Log::~Log() noexcept {
  WAL_ASSERT(m_pool->m_active == nullptr);
 }

Result<bool> Log::shutdown(Write_callback callback) noexcept {
  WAL_ASSERT(m_pool->m_active != nullptr);

  /* Disable writes after this. */
  m_pool->shutdown();

  WAL_ASSERT(m_pool->m_active == nullptr);

  /* Stop the I/O coroutine and wait for any remaining buffers to be processed */
  m_pool->stop_io_coroutine();

  /* Process any remaining buffers synchronously */
  auto adapter = [callback](Circular_buffer& buffer) -> Result<bool> {
   return  buffer.write_to_store(callback);
  };

  auto result = m_pool->write_to_store(adapter);
  
  /* Clear callbacks to break circular references before Log destruction.
   * The adapter lambda in m_io_callback captures 'this' and m_write_callback,
   * which can create cycles if the callbacks are not cleared */
  m_io_callback = {};
  m_write_callback = {};
  
  return result;
}


Result<Log::Slot> Log::append(std::span<const std::byte> span, [[maybe_unused]] util::Thread_pool* thread_pool) noexcept {
  WAL_ASSERT(span.size() > 0);
  WAL_ASSERT(span.size() <= std::numeric_limits<std::uint16_t>::max());
  WAL_ASSERT(m_pool->m_active != nullptr);

  auto entry_ptr = m_pool->acquire_buffer();
  auto buffer{&entry_ptr->m_buffer};
  auto result = buffer->append(span);

  Slot slot;

  if (!result.has_value()) [[unlikely]] {
    slot.m_len = 0;
    slot.m_lsn = buffer->m_hwm;
    WAL_ASSERT(result.error() == Status::Not_enough_space);
  } else if (result.value().m_len == span.size()) [[likely]] {
    return result;
  } else {
    slot = result.value();
  }

  /* Only the start LSN is required by the caller. */
  const auto lsn = slot.m_lsn;

  /* Span should fit in 64K a within a single buffer, it can overflow
   * a single buffer but the  remaining bytes must fit in the next buffer. */

  m_pool->prepare_buffer_for_io(entry_ptr, *thread_pool, m_write_callback);

   /* Get the next buffer */
  entry_ptr = m_pool->acquire_buffer();
  buffer = &entry_ptr->m_buffer;
  result = buffer->append(span.subspan(slot.m_len));

  WAL_ASSERT(result.has_value());
  WAL_ASSERT(result.value().m_len == span.size() - slot.m_len);

  return Slot { .m_lsn = lsn, .m_len = uint16_t(span.size()) };
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

  /* Create an adapter that converts Write_callback (takes span<iovec> and sync_type)
   * to a function that takes Circular_buffer& and returns Task<Result<lsn_t>>.
   * Tasks are posted directly by consumer, no background coroutine needed.
   * The adapter checks for pending sync requests and passes them to the callback */
  auto adapter = [this](Circular_buffer& buffer) -> Result<lsn_t> {

    /* Create a wrapper callback that passes the sync type */
    auto wrapped_callback = [this](std::span<struct iovec> span, Sync_type) -> Result<lsn_t> {
      return m_write_callback(span, Sync_type::None);
    };
    
    /* Write the full buffer - sync_target_lsn is just a marker for when to sync, not a write limit.
     * We always write the full buffer to maintain buffer state consistency */
    return buffer.write_to_store(wrapped_callback);
  };

  /* Store the callback in Log - Log is the coordinator and controls orchestration */
  m_io_callback = adapter;
  m_pool->m_io_thread_running = true;
}

Result<bool> Log::write_to_store(Write_callback callback) noexcept {
  
  /* Create an adapter that converts Write_callback (takes span<iovec> and sync_type)
   * to a function that takes Circular_buffer& and Thread_pool* and returns Task<Result<bool>> */
  auto adapter = [callback](Circular_buffer& buffer) -> Result<bool> {
    /* Create a wrapper callback that passes the sync type */
    auto wrapped_callback = [callback](std::span<struct iovec> span, Log::Sync_type) -> Result<lsn_t> {
      return callback(span, Sync_type::None);
    };
    return buffer.write_to_store(wrapped_callback);
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
