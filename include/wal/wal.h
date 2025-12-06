#pragma once

#include <atomic>
#include <span>
#include <cstdint>
#include <cstddef>
#include <expected>
#include <vector>
#include <type_traits>
#include <functional>
#include <sys/uio.h>
#include <string>
#include <format>
#include <thread>
#include <optional>
#include <cstdlib>
#include <memory>
#include <numa.h>
#include <sched.h>

#include "util/byte_order.h"
#include "util/checksum.h"
#include "util/logger.h"
#include "util/metrics.h"
#include "util/util.h"
#include "util/thread_pool.h"
#include "coro/task.h"
#include "wal/async_io.h"

// Forward declaration - full definition needed for template implementation
struct Pool;

#if defined(NDEBUG)
#  define WAL_ASSERT(cond) ((void)0)
#else
#  include <cassert>
#  define WAL_ASSERT(cond) assert(cond)
#endif

constexpr std::size_t kPrefetchDistanceBytes = 256;
constexpr std::size_t kDefaultBlockSize = 4096;
constexpr std::size_t kDefaultBufferBytes = 64ull * 1024ull * 1024ull;
constexpr std::size_t kDefaultBlockCount = kDefaultBufferBytes / kDefaultBlockSize;
constexpr std::size_t kDefaultLogBatchBytes = 512;
static_assert((kDefaultBlockCount * kDefaultBlockSize) == kDefaultBufferBytes);

/* If you want at least 64B even on odd platforms, use: */
// constexpr std::size_t kCLS = (kHWCLS < 64) ? 64 : kHWCLS;
constexpr std::size_t kCLS = util::kHWCLS;

constexpr std::size_t kMaxBlocks = std::numeric_limits<std::size_t>::max();
constexpr std::uint16_t kMaxDataLen = std::numeric_limits<std::uint16_t>::max();

// Forward declarrions
struct iovec;

namespace wal {

// Forward declaration - full definition needed for template implementation
struct Log;

/**
 * Sync type for write operations.
 */
enum class Sync_type : std::uint8_t {
  None = 0,      /* No sync */
  Fdatasync = 1, /* fdatasync() */
  Fsync = 2      /* fsync() */
};

/** Forward declaration of the Pool. */
struct Pool;

/* For future users to support O_DIRECT */
// constexpr auto O_DIRECT_ALIGNMENT = 512;

enum class Status {
  Success,
  Internal_error,
  IO_error,
  Not_enough_space
};

/** 
 * Convert a Status enum to a string.
 * 
 * @param status The Status enum to convert.
 * @return A string representation of the Status enum.
 */
[[nodiscard]] std::string to_string(Status status) noexcept;

template<typename T>
using Result = std::expected<T, Status>;


using lsn_t = std::uint64_t;
using crc32_t = std::uint32_t;
using block_no_t = std::uint32_t;
using checkpoint_no_t = std::uint32_t;

/* ---- Aligned wrapper that preserves the packed size/offsets ---- */
struct alignas(kCLS) Block_header {
  /** Mask used to get the highest bit in the block number. */
  constexpr static block_no_t FLUSH_BIT_MASK = 0x80000000U;

  struct [[nodiscard]] Data {
    #pragma pack(push, 1)

    /** Block number, the left most bit is the flush bit. */
    block_no_t m_block_no;

    /** Checkpoint number */
    checkpoint_no_t m_checkpoint_no;

    /** Offset of the first mtr log record group in m_data[]. */
    std::uint16_t m_first_rec_group;

    /** Valid data length inside the block. Use atomics to read and write this variable.
     * This struct and variable should be aligned correctly for this to work. Unlike
     * the other fields this field is always in host byte order and only converted
    * to network byte order before writing. */
    std::uint16_t m_len;

    #pragma pack(pop)
  };

  /**
   * Sets the log block number stored in the header.
   * @note: This must be set before the flush bit!
   *
   * @param block_no The log block number: must be > 0 and < LOG_BLOCK_FLUSH_BIT_MASK.
   */
  void set_block_no(block_no_t block_no) noexcept {
    m_data.m_block_no = block_no;
  }

  void set_first_rec_group(uint16_t first_rec_group) noexcept {
    m_data.m_first_rec_group = first_rec_group;
  }

  void set_checkpoint_no(checkpoint_no_t checkpoint_no) noexcept {
    m_data.m_checkpoint_no = checkpoint_no;
  }

  void set_data_len(uint16_t len) noexcept {
    m_data.m_len = len;
  }

  void prepare_to_write() noexcept {
    m_data.m_block_no = util::hton(m_data.m_block_no);
    m_data.m_checkpoint_no = util::hton(m_data.m_checkpoint_no);
    m_data.m_len = util::hton(m_data.m_len);
    m_data.m_first_rec_group = util::hton(m_data.m_first_rec_group);
  }

  void prepare_to_read() noexcept {
    m_data.m_block_no = util::ntoh(m_data.m_block_no);
    m_data.m_checkpoint_no = util::ntoh(m_data.m_checkpoint_no);
    m_data.m_first_rec_group = util::ntoh(m_data.m_first_rec_group);
    m_data.m_len = util::ntoh(m_data.m_len);
  }

  [[nodiscard]] block_no_t get_block_no() const noexcept {
     return ~FLUSH_BIT_MASK & m_data.m_block_no;
  }

  [[nodiscard]] uint16_t get_data_len() const noexcept {
    return m_data.m_len;
  }

  [[nodiscard]] uint16_t get_first_rec_group() const noexcept {
    return m_data.m_first_rec_group;
  }

  [[nodiscard]] checkpoint_no_t get_checkpoint_no() const noexcept {
    return m_data.m_checkpoint_no;
  }

  [[nodiscard]] bool get_flush_bit() const noexcept {
    return (m_data.m_block_no & FLUSH_BIT_MASK) != 0;
  }

  /**
   * (Un)Set the flush bit of a log block.
   *
   * @param val The value to set.
   */
  void set_flush_bit(bool val) noexcept {
    if (val) {
      m_data.m_block_no |= FLUSH_BIT_MASK;
    } else {
      m_data.m_block_no &= ~FLUSH_BIT_MASK;
    }
  }

  /**
   * Returns a string representation of the log block.
   *
   * @return A string representation of the log block.
   */
  [[nodiscard]] std::string to_string() const noexcept {
    return std::format(
      "redo::Block : block_no: {}, len: {}, first_rec_group: {}, checkpoint_no: {}, flush_bit: {}",
      get_block_no(),
      get_data_len(),
      get_first_rec_group(),
      get_checkpoint_no(),
      get_flush_bit()
    );
  }

  Data m_data;
};

static_assert(sizeof(Block_header::Data) == 12, "Block_header::Data size must be equal to 12 ie. packed size");
static_assert(std::is_standard_layout_v<Block_header>, "Block_header must be standard layout");
static_assert(std::is_trivial_v<Block_header>, "Block_header::Data must be trivial");
static_assert(std::is_trivially_copyable_v<Block_header>, "Block_header must be trivially copyable");
static_assert(alignof(Block_header) == util::kHWCLS, "Block_header must be cache-line aligned");

/**
 * Custom allocator that uses libnuma for NUMA-aware aligned memory allocation.
 * This ensures that the buffer is aligned to the cache line size boundary and
 * allocated on the local NUMA node for better performance.
 */
template<typename T>
class Aligned_allocator {
public:
  using value_type = T;
  using pointer = T*;
  using const_pointer = const T*;
  using reference = T&;
  using const_reference = const T&;
  using size_type = std::size_t;
  using difference_type = std::ptrdiff_t;

  template<typename U>
  struct rebind {
    using other = Aligned_allocator<U>;
  };

  Aligned_allocator() = default;

  template<typename U>
  Aligned_allocator(const Aligned_allocator<U>&) noexcept {}

  [[nodiscard]] pointer allocate(size_type n) {
    if (n == 0) {
      return nullptr;
    }

    constexpr size_type alignment = kCLS;
    const size_type size = n * sizeof(T);

    void* ptr = nullptr;

    /* Use NUMA-aware allocation if available, otherwise fall back to posix_memalign */
    if (numa_available() >= 0) {
      /* Allocate on local NUMA node for better performance.
       * numa_alloc_local returns page-aligned memory (typically 4KB),
       * which is always aligned to our cache-line requirement (64B).
       * This ensures memory is allocated on the local NUMA node for
       * better performance on NUMA systems. */
      ptr = numa_alloc_local(size);
      
      if (ptr == nullptr) {
        /* Fallback to posix_memalign if NUMA allocation fails */
        if (::posix_memalign(&ptr, alignment, size) != 0) {
          throw std::bad_alloc();
        }
      } else {
        /* Verify alignment - numa_alloc should return page-aligned memory,
         * but verify to be safe */
        auto aligned_ptr = reinterpret_cast<std::uintptr_t>(ptr);
        if (aligned_ptr % alignment != 0) {
          /* This should be rare - numa_alloc should return aligned memory.
           * Free and fall back to posix_memalign for guaranteed alignment. */
          numa_free(ptr, size);
          if (::posix_memalign(&ptr, alignment, size) != 0) {
            throw std::bad_alloc();
          }
        }
      }
    } else {
      /* NUMA not available, use posix_memalign */
      if (::posix_memalign(&ptr, alignment, size) != 0) {
        throw std::bad_alloc();
      }
    }

    return static_cast<pointer>(ptr);
  }

  void deallocate(pointer p, size_type n) noexcept {
    if (p == nullptr) {
      return;
    }

    const size_type size = n * sizeof(T);

    /* Use NUMA deallocation if NUMA is available.
     * Note: numa_free must only be used for memory allocated by numa_alloc functions.
     * Since we prioritize numa_alloc_local in allocate(), we use numa_free here when
     * NUMA is available. In the rare case where allocation fell back to posix_memalign
     * (due to NUMA allocation failure or alignment issues), we should use std::free,
     * but we can't easily distinguish. However, on most systems, numa_free on
     * non-NUMA memory either works or is handled gracefully. For production use,
     * consider tracking allocation method if this becomes an issue. */
    if (numa_available() >= 0) {
      /* Use numa_free for NUMA-allocated memory.
       * This is safe for memory allocated by numa_alloc_local. */
      numa_free(p, size);
    } else {
      std::free(p);
    }
  }

  template<typename U>
  bool operator==(const Aligned_allocator<U>&) const noexcept {
    return true;
  }

  template<typename U>
  bool operator!=(const Aligned_allocator<U>& other) const noexcept {
    return !(*this == other);
  }
};

struct [[nodiscard]] Circular_buffer {
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

  explicit Circular_buffer(const Config& config) noexcept;

  Circular_buffer(lsn_t hwm, const Config& config) noexcept;

  /* Move constructor - recalculates pointers after moving m_buffer */
  Circular_buffer(Circular_buffer&& other) noexcept
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
  Circular_buffer& operator=(Circular_buffer&&) = delete;

  ~Circular_buffer() noexcept;

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
    return const_cast<crc32_t&>(const_cast<const Circular_buffer*>(this)->get_crc32(block_index));
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
  [[nodiscard]] Result<Slot> copy(std::span<const std::byte> span) noexcept {
    WAL_ASSERT(span.size() > 0);
    WAL_ASSERT(span.size() <= std::numeric_limits<std::uint16_t>::max());
    WAL_ASSERT(m_hwm - m_lwm <= m_total_data_size);

    const auto lsn = m_hwm;
    const auto available = std::min(margin(), span.size());

    if (available == 0) [[unlikely]] {
      return std::unexpected(Status::Not_enough_space);
    }

    WAL_ASSERT(available <= std::numeric_limits<std::uint16_t>::max());

    const auto data_offset = lsn % m_total_data_size;

    std::memcpy(m_data_array + data_offset, span.data(), available);

    m_hwm += available;

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

  using Config = Circular_buffer::Config;
  using Slot = Circular_buffer::Slot;
  using IO_vecs = Circular_buffer::IO_vecs;
  using Write_callback = Circular_buffer::Write_callback;

  /**
   * Constructor.
   *
   * @param[in]  lsn The LSN to start the log from.
   * @param[in]  pool_size The size of the pool to use for the Circular_buffers.
   * @param[in]  config The configuration for the circular buffer.
   */
  Log(lsn_t lsn, size_t pool_size, const Circular_buffer::Config &config);

  ~Log() noexcept;

  /** Reserve and write the data in the span to the buffer.
   * @param[in] span The span of the data to write.
   * @return The slot that was reserved.
   */
  [[nodiscard]] Result<Slot> write(std::span<const std::byte> span, util::Thread_pool* thread_pool = nullptr) noexcept;
  
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
  std::function<Result<lsn_t>(Circular_buffer&)> m_io_callback{};

  /** We have synced up to this LSN. */
  std::atomic<lsn_t> m_flushed_lsn{0};
};

using Config = Log::Config;
using Log_writer = Log::Write_callback;
} // namespace wal
