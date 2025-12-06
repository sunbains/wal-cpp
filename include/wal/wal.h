#pragma once

#include "wal/types.h"

namespace wal {

struct [[nodiscard]] Buffer {
  using IO_vecs = std::vector<struct iovec>;

  struct [[nodiscard]] Config {
    /** Constructor.
     *  @note The buffer must be capable of storing at least one records of uint16_t size.
     * 
     * @param[in] n_blocks The number of blocks in the circular buffer.
     * @param[in] block_size The size of the block in the circular buffer.
     */
    explicit Config(
      size_t n_blocks = kDefaultBlockCount,
      size_t block_size = kDefaultBlockSize,
      util::ChecksumAlgorithm algo = util::ChecksumAlgorithm::CRC32C) noexcept
      : m_n_blocks(n_blocks),
        m_block_size(block_size),
        m_checksum_algorithm(algo) {}

    ~Config() = default;

    const size_t m_n_blocks;
    const size_t m_block_size;
    const util::ChecksumAlgorithm m_checksum_algorithm;

    std::size_t get_data_size_in_block() const noexcept {
      return m_block_size - sizeof(Block_header::Data) - sizeof(crc32_t);
    }
  };

  /**
   * Callback function to read the buffer from disk.
   * 
   * @param lsn The LWM LSN from where the data should be read
   * @param iovecs The IO vectors to read the data into.
   * @return The new LWM LSN.
   */
  using Read_callback = std::function<Result<lsn_t>(lsn_t lsn, IO_vecs& iovecs)>;

  /**
   * Callback function to flush the buffer to disk.
   * 
   * @param iovecs The IO vectors to flush (span of iovec elements).
   * @param sync_type Whether to sync after write (None, Fdatasync, or Fsync).
   * @return The new LWM LSN.
   */
  using Write_callback = std::function<Result<lsn_t>(std::span<struct iovec> span, Sync_type sync_type)>;

  struct [[nodiscard]] Slot {
    /** Start LSN of this slot. */
    lsn_t m_lsn{};
    /** Number of bytes reserved. */
    std::uint16_t m_len{};
  };

  using Block = std::tuple<Block_header*, std::span<const std::byte>, crc32_t*>;

  explicit Buffer(const Config& config) noexcept;

  Buffer(lsn_t hwm, const Config& config) noexcept;

  /* Move constructor - recalculates pointers after moving m_buffer */
  Buffer(Buffer&& other) noexcept
    : m_lwm(other.m_lwm),
      m_hwm(other.m_hwm),
      m_config(other.m_config),
      m_total_data_size(other.m_total_data_size),
      m_crc32_array(nullptr),
      m_data_array(nullptr),
      m_block_header_array(nullptr),
      m_iovecs(std::move(other.m_iovecs)),
      m_buffer(std::move(other.m_buffer)),
      m_checksum(std::move(other.m_checksum)) {
    /* Recalculate pointers after moving m_buffer - buffer is already aligned */
    auto aligned_ptr = reinterpret_cast<std::uintptr_t>(m_buffer.data());
    
    m_block_header_array = reinterpret_cast<Block_header*>(aligned_ptr);
    m_data_array = reinterpret_cast<std::byte*>(m_block_header_array + m_config.m_n_blocks);
    m_crc32_array = reinterpret_cast<crc32_t*>(m_data_array + m_total_data_size);
    
    /* Invalidate other's pointers */
    other.m_block_header_array = nullptr;
    other.m_data_array = nullptr;
    other.m_crc32_array = nullptr;
  }

  /* Move assignment is deleted because m_config and m_total_data_size are const */
  Buffer& operator=(Buffer&&) = delete;

  ~Buffer() noexcept;

  /**
   * Initialize the circular buffer.
   * 
   * @param[in] hwm The high water mark.
   */
  void initialize(lsn_t hwm) noexcept;

  [[nodiscard]] Block_header &get_block_header(std::size_t block_index) noexcept {
    WAL_ASSERT(block_index < m_config.m_n_blocks);

    return m_block_header_array[block_index];
  }

  [[nodiscard]] const Block_header &get_block_header(std::size_t block_index) const noexcept {
    WAL_ASSERT(block_index < m_config.m_n_blocks);
    return m_block_header_array[block_index];
  }

  [[nodiscard]] const std::span<const std::byte> get_data(std::size_t block_index) const noexcept {
    WAL_ASSERT(block_index < m_config.m_n_blocks);
    return std::span<std::byte>(&m_data_array[(m_config.get_data_size_in_block() * block_index)], m_config.get_data_size_in_block());
  }

  /** Get the size of the data in the block. */
  [[nodiscard]] std::size_t get_data_size_in_block() const noexcept {
    return m_config.get_data_size_in_block();
  }

  [[nodiscard]] std::size_t get_total_data_size() const noexcept {
    return m_total_data_size;
  }

  [[nodiscard]] std::size_t get_total_block_header_size() const noexcept {
    return sizeof(Block_header) * m_config.m_n_blocks;
  }

  [[nodiscard]] const crc32_t &get_crc32(std::size_t block_index) const noexcept {
    WAL_ASSERT(block_index < m_config.m_n_blocks);
    return m_crc32_array[block_index];
  }

  [[nodiscard]] crc32_t &get_crc32(std::size_t block_index) noexcept {
    return const_cast<crc32_t&>(const_cast<const Buffer*>(this)->get_crc32(block_index));
  }

  [[nodiscard]] Block get_block(std::size_t block_index) noexcept {
    WAL_ASSERT(block_index < m_config.m_n_blocks);
    return std::make_tuple(&get_block_header(block_index), get_data(block_index), &get_crc32(block_index));
  }

  /**
   * Check how much space is left in the buffer.
   * @note we cannot overwrite the data in the block where the LWM starts.
   * It's the first block that is not yet flushed to the store with new data.
   * @note margin() < total_data_size can still return empty.
   * @return The number of bytes left in the buffer.
   */
  [[nodiscard]] std::size_t margin() const noexcept {
    WAL_ASSERT(m_hwm >= m_lwm);

     const auto data_size = m_config.get_data_size_in_block();
     const auto total_data_size = m_total_data_size;
     const auto available_space = total_data_size - (m_hwm - m_lwm);

     /* Now we need to compensate for the date that in the block before the LWM. */

     return available_space - (m_lwm % data_size);
  }

  /** Copy the data into the buffer, doesn't update the header.
   * Copies what it can and returns the number of bytes copied in Slot.m_len.
   *
   * @param[in] span The span of the data to copy into the buffer.
   * @note The caller needs to check for partial copy and handle it accordingly.
   * @return { LSN , number of bytes copied into the buffer}
   */
  [[nodiscard]] Result<Slot> append(std::span<const std::byte> span) noexcept {
    WAL_ASSERT(span.size() > 0);
    WAL_ASSERT(span.size() <= std::numeric_limits<std::uint16_t>::max());
    WAL_ASSERT(m_hwm - m_lwm <= m_total_data_size);

    const auto space_left = static_cast<std::size_t>(std::ptrdiff_t(m_append_ptr - m_data_array));
    const auto available = std::min(span.size(), m_total_data_size - space_left);

    if (available == 0) [[unlikely]] {
      return std::unexpected(Status::Not_enough_space);
    }

    WAL_ASSERT(available <= std::numeric_limits<std::uint16_t>::max());

    std::memcpy(m_append_ptr, span.data(), available);

    const auto lsn = m_hwm;

    m_hwm += available;
    m_append_ptr += available;

    WAL_ASSERT(m_append_ptr <= m_data_array + m_total_data_size);

    return Slot { .m_lsn = lsn, .m_len = uint16_t(available) };
  }

  [[nodiscard]] std::span<std::byte> data() const noexcept {
    return std::span<std::byte>(m_data_array, m_total_data_size);
  }

  [[nodiscard]] std::span<Block_header> headers() const noexcept {
    return std::span<Block_header>(m_block_header_array, m_config.m_n_blocks);
  }

  [[nodiscard]] std::span<crc32_t> crc32s() const noexcept {
    return std::span<crc32_t>(m_crc32_array, m_config.m_n_blocks);
  }

  [[nodiscard]] bool is_write_pending() const noexcept {
    WAL_ASSERT(m_hwm >= m_lwm);
    return m_hwm > m_lwm;
  }

  [[nodiscard]] bool is_full() const noexcept {
    WAL_ASSERT(m_hwm - m_lwm <= get_total_data_size());
    return margin() == 0;
  }

  /** Empty is a little bit tricky, we have to compensate for any
   * data before the LWM. We cannot overwrite that data until that
   * block is flushed to the store.
   @return true if there is room to write more data. */
  [[nodiscard]] bool is_empty() const noexcept {
    return margin() == (get_total_data_size() - (m_lwm % get_data_size_in_block()));
  }

  /**
   * Read data from the store.
   * 
   * @param[in] lsn The LSN from where the data should be read.
   * @param[in] callback The callback function to read the data.
   * @return The new LWM LSN.
   */
  [[nodiscard]] Result<lsn_t> read_from_store(lsn_t lsn, Read_callback callback) noexcept;

  /**
   * Write data to the store using Tasks for async I/O.
   * on the provided thread pool (or synchronously if pool is nullptr).
   *
   * @param[in] callback The callback function to write the data.
   * @param[in] thread_pool Thread pool for executing I/O operations. If nullptr, executes synchronously.
   * @param[in] max_blocks_per_batch The maximum number of blocks to write per batch.
   * @return A Task that yields the new LWM LSN.
   */
  // [[nodiscard]] Task<Result<lsn_t>> write_to_store(Write_callback callback, util::Thread_pool* thread_pool = nullptr, std::size_t max_blocks_per_batch = kMaxBlocks) noexcept;

  /**
   * Write buffer to store synchronously - used for inline I/O when buffers exhausted.
   * @param[in] callback The callback function to write the data.
   * @return Result with new LWM LSN on success.
   */
  [[nodiscard]] Result<lsn_t> write_to_store(Write_callback callback, lsn_t max_write_lsn = 0) noexcept;

  /**
   * Clear the headers for the given range of LSNs.
   * @note: We need clear the header so that we can atomically update the data length.
   * 
   * @param[in] start_lsn The start LSN to clear.
   * @param[in] end_lsn The end LSN to clear.
  */
  void clear(lsn_t start_lsn, lsn_t end_lsn) noexcept;

  [[nodiscard]] std::string to_string() const noexcept;

  /**
   * Set the metrics collector for this buffer.
   * @param metrics Pointer to metrics collector (can be nullptr to disable)
   */
  void set_metrics(util::Metrics* metrics) noexcept {
    m_metrics = metrics;
  }

  /* Virtual offset, the low water mark, the data in the buffer that has not
   * been written to the store starts at this LSN. */
  lsn_t m_lwm{};
    
  /* Virtual offset, the high water mwark, we have copied data to the buffer up to this LSN.
   * When we write to store this must equal the LWM. */
  lsn_t m_hwm{};

  Config m_config;

  /** Total size of the data in the buffer. */
  const size_t m_total_data_size{};

  /** From where the next write will start. */
  std::byte *m_append_ptr{};

  /** Array of CRC32 values for each block. It points to an offset in m_buffer */
  crc32_t *m_crc32_array{};

  /** Array of data for each block. It points to an offset in m_buffer */
  std::byte *m_data_array{};

  /** Array of block headers for each block. It points to an offset in m_buffer */
  Block_header *m_block_header_array{};

  /** IO vectors for storing the data. */
  IO_vecs m_iovecs{};

  /** Buffer for all the data. */
  std::vector<std::byte, Aligned_allocator<std::byte>> m_buffer{};

  /** For computing the checksum of the data. */
  util::Checksum m_checksum{util::ChecksumAlgorithm::CRC32C};

  /** Optional metrics collector for observability. */
  util::Metrics* m_metrics{nullptr};
};

struct [[nodiscard]] Log {
  using Sync_type = wal::Sync_type;  /* Alias to namespace-level Sync_type */

  using Config = Buffer::Config;
  using Slot = Buffer::Slot;
  using IO_vecs = Buffer::IO_vecs;
  using Write_callback = Buffer::Write_callback;

  /**
   * Constructor.
   *
   * @param[in]  lsn The LSN to start the log from.
   * @param[in]  pool_size The size of the pool to use for the Buffers.
   * @param[in]  config The configuration for the circular buffer.
   */
  Log(lsn_t lsn, size_t pool_size, const Buffer::Config &config);

  ~Log() noexcept;

  /** Reserve and write the data in the span to the buffer.
   * @param[in] span The span of the data to write.
   * @return The slot that was reserved.
   */
  [[nodiscard]] inline Result<Slot> append(std::span<const std::byte> span, util::Thread_pool* thread_pool = nullptr) noexcept;
  
  [[nodiscard]] bool is_full() const noexcept;
  [[nodiscard]] bool is_empty() const noexcept;
  [[nodiscard]] std::size_t margin() const noexcept;

  /** Start the background I/O coroutine that continuously processes buffers.
   * 
   * @param[in] callback The callback function to write the data.
   * @param[in] pool Thread pool for executing I/O operations (optional).
   */
  void start_io(Write_callback callback, util::Thread_pool* pool = nullptr) noexcept;

  /** Write all pending buffers in the pool to the store using Tasks.

   * @param[in] callback The callback function to write the data.
   * @param[in] thread_pool Thread pool for executing I/O operations.
   * @return A Task that yields true if the write was successful, false otherwise.
   */
  [[nodiscard]] Result<bool> write_to_store(Write_callback callback) noexcept;

  /** Flush all pending and active buffers to the store and wait for them to complete.

   * @param[in] callback The callback function to write the data.
   * @param[in] pool Thread pool for executing I/O operations. If nullptr, executes synchronously.

   * @return A Task that yields true if the write was successful, false otherwise.
   */
  [[nodiscard]] Result<bool> shutdown(Write_callback callback) noexcept;

  /**
   * Convert the log state to a string.
   * @return The string representation of the log state.
   */
  [[nodiscard]] std::string to_string() const noexcept;

  /**
   * Set metrics collector for all buffers in the pool.
   * All buffers will share the same metrics instance for consolidated statistics.
   * @param metrics Pointer to metrics collector (can be nullptr to disable)
   */
  void set_metrics(util::Metrics* metrics) noexcept;

  /**
   * Get the metrics collector (if set).
   * @return Pointer to metrics collector or nullptr if not set
   */
  [[nodiscard]] util::Metrics* get_metrics() const noexcept {
    return m_metrics;
  }

  /** The thread pool to use for the write. */
  util::Thread_pool* m_thread_pool{nullptr};

  /* We want to avoid a circular dependency, buffer_pool.h includes this file. */
  std::unique_ptr<Pool> m_pool;

  /** Metrics collector shared by all buffers in the pool. */
  util::Metrics* m_metrics{nullptr};
  
  /** Original write callback stored for background I/O. */
  Write_callback m_write_callback{};
  
  /** I/O adapter callback - orchestrates buffer writes with sync handling. */
  std::function<Result<lsn_t>(Buffer&)> m_io_callback{};

  /** We have synced up to this LSN. */
  std::atomic<lsn_t> m_flushed_lsn{0};
};

using Config = Log::Config;
using Log_writer = Log::Write_callback;
} // namespace wal

// Include Pool definition for inline Log::append
#include "wal/buffer_pool.h"

namespace wal {

inline Result<Log::Slot> Log::append(std::span<const std::byte> span, [[maybe_unused]] util::Thread_pool* thread_pool) noexcept {
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

} // namespace wal
