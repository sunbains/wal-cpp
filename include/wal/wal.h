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

#if defined(NDEBUG)
#  define WAL_ASSERT(cond) ((void)0)
#else
#  include <cassert>
#  define WAL_ASSERT(cond) assert(cond)
#endif
#include <thread>

constexpr std::size_t kPrefetchDistanceBytes = 256;
constexpr std::size_t kDefaultBlockSize = 4096;
constexpr std::size_t kDefaultBufferBytes = 64ull * 1024ull * 1024ull;
constexpr std::size_t kDefaultBlockCount = kDefaultBufferBytes / kDefaultBlockSize;
constexpr std::size_t kDefaultLogBatchBytes = 512;
static_assert((kDefaultBlockCount * kDefaultBlockSize) == kDefaultBufferBytes);

#if defined(__GNUC__) || defined(__clang__)
template<int Locality>
inline void prefetch_for_read(const void* ptr) noexcept {
  static_assert(Locality >= 0 && Locality <= 3);
  __builtin_prefetch(ptr, 0, Locality);
}

template<int Locality>
inline void prefetch_for_write(const void* ptr) noexcept {
  static_assert(Locality >= 0 && Locality <= 3);
  __builtin_prefetch(ptr, 1, Locality);
}
#else
template<int>
inline void prefetch_for_read(const void*) noexcept {}
template<int>
inline void prefetch_for_write(const void*) noexcept {}
#endif

#include "util/checksum.h"
#include "util/byte_order.h"
#include "util/logger.h"

/* ---- Use hardware_destructive_interference_size if available ---- */
#if defined(__cpp_lib_hardware_interference_size) && (__cpp_lib_hardware_interference_size >= 201703L)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winterference-size"
constexpr std::size_t kHWCLS = std::hardware_destructive_interference_size;
#pragma GCC diagnostic pop
#else
constexpr std::size_t kHWCLS = 64; // sensible fallback for most x86/ARM servers
#endif

/* If you want at least 64B even on odd platforms, use: */
// constexpr std::size_t kCLS = (kHWCLS < 64) ? 64 : kHWCLS;
constexpr std::size_t kCLS = kHWCLS;

// Forward declarrions
struct iovec;

namespace wal {

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

  void set_first_rec_group(uint16_t first_rec_group) noexcept {
    m_data.m_first_rec_group = first_rec_group;
  }

  void set_checkpoint_no(checkpoint_no_t checkpoint_no) noexcept {
    m_data.m_checkpoint_no = checkpoint_no;
  }

  [[nodiscard]] block_no_t get_block_no() const noexcept {
     return ~FLUSH_BIT_MASK & m_data.m_block_no;
  }

  void initialize(block_no_t block_no) noexcept {
    m_data.m_block_no = block_no;
    m_data.m_checkpoint_no = 0;
    m_data.m_first_rec_group = 0;
    m_data.m_len = 0;
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
static_assert(alignof(Block_header) == kCLS, "Block_header must be cache-line aligned");

struct [[nodiscard]] Circular_buffer {
  using IO_vecs = std::vector<struct iovec>;

  struct [[nodiscard]] Config {
    /** Constructor
     * 
     * @param[in] n_blocks The number of blocks in the circular buffer.
     * @param[in] block_size The size of the block in the circular buffer.
     */
    explicit Config(size_t n_blocks = kDefaultBlockCount, size_t block_size = kDefaultBlockSize, util::ChecksumAlgorithm algo = util::ChecksumAlgorithm::CRC32C) noexcept
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
   * @param lsn The LWM LSN from where the snapshot starts.
   * @param iovecs The IO vectors to flush.
   * @param n_slots The number of slots in iovecs to flush.
   * @return The new LWM LSN.
   */
  using Write_callback = std::function<Result<lsn_t>(lsn_t lsn, const IO_vecs& iovecs, std::size_t n_slots)>;

  struct [[nodiscard]] Slot {
    /** Start LSN of this slot. */
    lsn_t m_lsn{};
    /** Number of bytes reserved. */
    std::uint16_t m_len{};
  };

  using Block = std::tuple<Block_header*, std::span<const std::byte>, crc32_t*>;

  explicit Circular_buffer(lsn_t hwm, Config config) noexcept;

  ~Circular_buffer() noexcept;

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

  /** Reserve len number of bytes in the buffer cache.
   * The reserved slot (LSN) could be outside the bounds of this buffer.
   *
   * @return slot that was reserved.
   */
  [[nodiscard]] Slot reserve(std::uint16_t len) noexcept {
    /* Use relaxed ordering - the release semantics are provided by the write() operation */
    const auto lsn = m_hwm.fetch_add(len, std::memory_order_relaxed);
    m_n_pending_writes.fetch_add(1, std::memory_order_relaxed);

    return Slot {
      .m_lsn = lsn,
      .m_len = len
    }; 
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

  /** 
   * Write data to the buffer.
   * @note: There is no guarantee that the entire span will be written.
   * Only as many bytes that fit in the buffer will be written. The caller
   * should check the return value and handle the case where the entire span
   * was not written. The obvious thing to do is to write the data to persistent
   * store and retry the write.
   *
   * @param[in,out] slot The slot to write the data to.
   * @param[in] span The data to write.
   * 
   * @return The number of bytes written.
   */
  [[nodiscard]] Result<std::size_t> write(Slot &slot, std::span<const std::byte> span) noexcept {
    WAL_ASSERT(span.size() > 0);
    WAL_ASSERT(slot.m_len == span.size());

    using len_t = decltype(Block_header::Data::m_len);

    WAL_ASSERT(m_lwm <= slot.m_lsn);

    // log_inf("m_total_data_size: {}, slot.m_lsn: {}, m_lwm: {}, check_margin: {}", m_total_data_size, slot.m_lsn, m_lwm.load(), check_margin());

    /* Cache frequently used values to reduce function calls */
    const auto data_size_in_block = m_config.get_data_size_in_block();
    
    /* Use relaxed load for LWM check - we only need approximate value for space check */
    const auto lwm = m_lwm.load(std::memory_order_relaxed);
    /* Fast path: check if we have space without expensive modulo */
    const auto distance = slot.m_lsn - lwm;
    if (distance >= m_total_data_size) {
      return std::unexpected(Status::Not_enough_space);
    }
    static_cast<void>(distance);

    /* Use bit masking for modulo if m_total_data_size is power of 2, otherwise use modulo */
    std::size_t rel_off;
    if ((m_total_data_size & (m_total_data_size - 1)) == 0) {
      /* Power of 2 - use bit mask instead of modulo */
      rel_off = slot.m_lsn & (m_total_data_size - 1);
    } else {
      rel_off = static_cast<std::size_t>(slot.m_lsn % m_total_data_size);
    }
    const std::size_t len = std::min(size_t(slot.m_len), m_total_data_size - rel_off);

    WAL_ASSERT(len > 0);

    /* Use division and modulo for block calculations - optimize if data_size_in_block is power of 2 */
    auto block_no = slot.m_lsn / data_size_in_block;
    const auto off_in_blk = static_cast<std::size_t>(slot.m_lsn % data_size_in_block);

    WAL_ASSERT(block_no < std::numeric_limits<block_no_t>::max());

    /* Optimize block_index calculation - m_n_blocks is always power of 2, use bit mask */
    auto block_index = static_cast<std::size_t>(block_no) & (m_config.m_n_blocks - 1);
    WAL_ASSERT(block_index < m_config.m_n_blocks);

    /* Fast path: single block write (most common case) */
    const auto remaining_in_block = data_size_in_block - off_in_blk;
    if (len <= remaining_in_block) [[likely]] {
      /* Prefetch current and next destinations to reduce write latency */
      prefetch_for_write<3>(&m_block_header_array[block_index]);
      prefetch_for_write<2>(m_data_array + rel_off);
      
      /* Write fits entirely in one block - optimized path */
      std::memcpy(m_data_array + rel_off, span.data(), len);
      
      /* Inline block header access to avoid function call overhead */
      auto* hdr = &m_block_header_array[block_index];

      /* Update slot early to allow instruction-level parallelism */
      slot.m_lsn += len;
      const auto remaining_len = static_cast<std::uint16_t>(slot.m_len - len);
      slot.m_len = remaining_len;

      /* Unconditionally set block_no - avoid conditional branch on fast path */
      const auto current_block_no = static_cast<block_no_t>(block_no);
      hdr->m_data.m_block_no = (hdr->m_data.m_block_no & Block_header::FLUSH_BIT_MASK) | current_block_no;

      /* Update m_len - use direct atomic store for maximum performance */
      const auto new_len = static_cast<len_t>(off_in_blk + len);
      std::atomic_ref<len_t>(hdr->m_data.m_len).store(new_len, std::memory_order_relaxed);
      
      /* Always update m_written_lsn - this is the critical path */
      m_written_lsn.fetch_add(len, std::memory_order_relaxed);
      
      if (remaining_len == 0) {
        m_n_pending_writes.fetch_sub(1, std::memory_order_relaxed);
        return Result<std::size_t>(len);
      }
      
      return std::unexpected(Status::Not_enough_space);
    }

    /* Slow path: multi-block write */
    std::size_t copied{};
    std::byte* ptr = m_data_array + rel_off;

    /* Process current block, then subsequent blocks as needed. */
    std::size_t offset_in_block = off_in_blk;
    const auto lwm_block_index = (lwm / data_size_in_block) % m_config.m_n_blocks;
    
    /* Cache block header pointer to avoid repeated lookups */
    auto* hdr = &get_block_header(block_index);
    const auto initial_block_no = static_cast<block_no_t>(block_no);
    auto current_block_no = initial_block_no;
    
    /* Set block_no for first block only if needed */
    const auto raw_block_no = hdr->m_data.m_block_no & ~Block_header::FLUSH_BIT_MASK;
    if (raw_block_no != current_block_no) {
      hdr->set_block_no(current_block_no);
    }

    for (;;) {

      // log_inf("block_index: {}, copied: {}, check_margin: {}, committed_lsn: {}", block_index, copied, check_margin(), m_committed_lsn.load());

      /* Copy bounded by remaining bytes and remaining space in this block. */
      const std::size_t avail_in_block = data_size_in_block - offset_in_block;
      const std::size_t to_copy = std::min(len - copied, avail_in_block);

      // log_inf("writing {} bytes to block {} (block_no: {}, block_index: {})", to_copy, block_no, block_no, block_index);

      WAL_ASSERT(to_copy > 0);

      prefetch_for_write<2>(ptr);
      if (copied + kPrefetchDistanceBytes < span.size()) {
        prefetch_for_read<1>(span.data() + copied + kPrefetchDistanceBytes);
      }

      std::memcpy(ptr, span.data() + copied, to_copy);

      /* Advance pointers/counters. */
      copied += to_copy;
      const std::size_t write_end = offset_in_block + to_copy;
      rel_off = (rel_off + to_copy) % m_total_data_size;
      ptr = m_data_array + rel_off;

      /* Update m_len only when we finish a block or finish the write - reduces atomic operations */
      const bool block_finished = (write_end >= data_size_in_block);
      const bool write_finished = (copied == len);
      
      if (block_finished || write_finished) {
        const auto new_len = static_cast<len_t>(write_end > data_size_in_block ? data_size_in_block : write_end);
        std::atomic_ref<len_t>(hdr->m_data.m_len).store(new_len, std::memory_order_relaxed);
      }

      /* Check if we've finished writing all data */
      if (write_finished) {
        break;
      }

      /* Check if current block is full - if so, move to next block */
      if (block_finished) {
        /* Block is full, move to next block */
        ++current_block_no;
        offset_in_block = 0;
        block_index = (block_index + 1) % m_config.m_n_blocks;
        const auto next_block_index = (block_index + 1) % m_config.m_n_blocks;
        prefetch_for_write<2>(&m_block_header_array[next_block_index]);
        prefetch_for_write<1>(m_data_array + (rel_off % m_total_data_size));
        
        /* Check if we've wrapped around to the start of the circular buffer (LWM block) */
        if (block_index == lwm_block_index) {
          /* We've wrapped around and reached the LWM block - stop writing to avoid overwriting unflushed data */
          // log_inf("Wrapped around to LWM block (block_index: {}), stopping write. copied: {}, len: {}, remaining: {}", block_index, copied, len, len - copied);
          break;
        }
        
        /* Update block header pointer and set block_no for new block */
        hdr = &get_block_header(block_index);
        const auto raw_next_block_no = hdr->m_data.m_block_no & ~Block_header::FLUSH_BIT_MASK;
        if (raw_next_block_no != current_block_no) {
          hdr->set_block_no(current_block_no);
        }
        /* Continue to next iteration to write remaining data */
      } else {
        /* Block is not full and we haven't written all data - this means we should
         * continue writing in the same block. Update offset_in_block for next iteration. */
        offset_in_block = write_end;
        /* Continue to next iteration */
      }
    }

    WAL_ASSERT(copied <= slot.m_len);
    WAL_ASSERT(copied > 0 && copied <= len);

    slot.m_lsn += copied;
    slot.m_len -= uint16_t(copied);

    if (slot.m_len == 0) {
      m_n_pending_writes.fetch_sub(1, std::memory_order_relaxed);
    }

    m_written_lsn.fetch_add(copied, std::memory_order_relaxed);

    return slot.m_len == 0 ? Result<std::size_t>(copied) : std::unexpected(Status::Not_enough_space);
  }

  [[nodiscard]] bool is_full() const noexcept {
    WAL_ASSERT(m_written_lsn >= m_lwm);
    WAL_ASSERT(m_written_lsn <= m_hwm);
    WAL_ASSERT(m_written_lsn - m_lwm <= get_total_data_size());
    return m_written_lsn - m_lwm == get_total_data_size();
  }

  [[nodiscard]] bool is_empty() const noexcept {
    WAL_ASSERT(m_written_lsn >= m_lwm);
    WAL_ASSERT(m_written_lsn <= m_hwm);
    return m_written_lsn == m_lwm;
  }

  /**
   * Check if there are any pending writes.
   * This is to check if slots have been reserved but not yet written.
   * @return True if there are pending writes, false otherwise.
   */
  [[nodiscard]] bool pending_writes() const noexcept {
    return m_written_lsn < m_hwm;
  }

  /**
   * Check how much space is left in the buffer.
   * @return The number of bytes left in the buffer.
   */
  [[nodiscard]] std::size_t check_margin() const noexcept {
    WAL_ASSERT(m_written_lsn >= m_lwm);
    return m_total_data_size - (m_written_lsn - m_lwm);
  }

  /**
   * Read data from the store.
   * 
   * @param lsn The LSN from where the data should be read.
   * @param callback The callback function to read the data.
   * @return The new LWM LSN.
   */
  [[nodiscard]] Result<lsn_t> read_from_store(lsn_t lsn, Read_callback callback) noexcept;

  /**
   * Write data to the store.
   * 
   * @param callback The callback function to write the data.
   * @return The new LWM LSN.
   */
  [[nodiscard]] Result<lsn_t> write_to_store(Write_callback callback) noexcept;

  /**
   * Clear the headers for the given range of LSNs.
   * @note: We need clear the header so that we can atomically update the data length.
   * 
   * @param[in] start_lsn The start LSN to clear.
   * @param[in] end_lsn The end LSN to clear.
  */
  void clear(lsn_t start_lsn, lsn_t end_lsn) noexcept {
    WAL_ASSERT(start_lsn <= end_lsn);
    WAL_ASSERT(end_lsn <= m_written_lsn);

    const auto end_ptr = m_block_header_array + m_config.m_n_blocks;

    //log_inf("clearing from LSN {} to LSN {}", start_lsn, end_lsn);

    const auto data_size = m_config.get_data_size_in_block();
    const auto first_block_no = start_lsn / data_size;
    const auto last_block_no = end_lsn / data_size;

    if (last_block_no == first_block_no) {
      return;
    }

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

  [[nodiscard]] std::string to_string() const noexcept;

  /** Low water mark, the circular buffer starts at this LSN. */
  alignas(kCLS) std::atomic<lsn_t> m_lwm{};

  /** High water mark. We have reserved this many bytes to the buffer.
   * The writes may not have completed yet.
  */
  alignas(kCLS) std::atomic<lsn_t> m_hwm{};

  /** @note written_lsn is a simple counter that increments by bytes written, 
   * it doesn't track which specific LSN ranges are written. Out-of-order writes 
   * will work but written_lsn does not represent a contiguous range.
   */ 
  alignas(kCLS) std::atomic<lsn_t> m_written_lsn{};

  /** Number of pending writes. */
  alignas(kCLS) std::atomic<std::size_t> m_n_pending_writes{};

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
  std::vector<std::byte> m_buffer{};

  /** Checksum for the data. */
  util::Checksum m_checksum{util::ChecksumAlgorithm::CRC32C};
};

struct [[nodiscard]] Log {
  using Slot = Circular_buffer::Slot;
  using IO_vecs = Circular_buffer::IO_vecs;
  using Write_callback = Circular_buffer::Write_callback;
  struct Batch;

  /**
   * Constructor.
   *
   * @param[in]  lsn The LSN to start the log from.
   * @param[in]  config The configuration for the circular buffer.
   */
  Log(lsn_t lsn, const Circular_buffer::Config &config) noexcept;

  ~Log() noexcept;

  [[nodiscard]] Result<std::size_t> write(Slot &slot, std::span<const std::byte> span) noexcept {
    WAL_ASSERT(span.size() > 0);
    WAL_ASSERT(slot.m_len == span.size());

    auto result = m_buffer.write(slot, span);
    if (!result.has_value()) {
      return std::unexpected(result.error());
    }
    return result.value();
  }

  [[nodiscard]] Status write(Write_callback callback) noexcept;

  [[nodiscard]] std::string to_string() const noexcept;

  Slot reserve(std::uint16_t len) noexcept {
    return m_buffer.reserve(len);
  }

  bool is_full() const noexcept {
    return m_buffer.is_full();
  }

  bool is_empty() const noexcept {
    return m_buffer.is_empty();
  }

  std::size_t check_margin() const noexcept {
    return m_buffer.check_margin();
  }

  [[nodiscard]] Batch make_batch(std::size_t threshold = kDefaultLogBatchBytes);

  [[nodiscard]] Status write_buffered(std::span<const std::byte> span) noexcept;

  Circular_buffer m_buffer;
};

struct [[nodiscard]] Log::Batch {
  explicit Batch(Log& log, std::size_t threshold) noexcept
    : m_log(&log),
      m_threshold(std::max<std::size_t>(1, threshold)) {}

  Batch(const Batch&) = delete;
  Batch& operator=(const Batch&) = delete;
  Batch(Batch&&) = default;
  Batch& operator=(Batch&&) = default;

  [[nodiscard]] Status append(std::span<const std::byte> data) noexcept {
    if (data.empty()) {
      return Status::Success;
    }
    m_buffer.insert(m_buffer.end(), data.begin(), data.end());
    if (m_buffer.size() >= m_threshold) {
      return flush();
    }
    return Status::Success;
  }

  [[nodiscard]] Status flush() noexcept {
    if (m_buffer.empty()) {
      return Status::Success;
    }
    auto status = m_log->write_buffered(std::span<const std::byte>(m_buffer.data(), m_buffer.size()));
    if (status == Status::Success) {
      m_buffer.clear();
    }
    return status;
  }

  [[nodiscard]] std::size_t size() const noexcept { return m_buffer.size(); }

private:
  Log* m_log{};
  std::vector<std::byte> m_buffer;
  std::size_t m_threshold{};
};

/* Simple process() example. Customize as needed. */
template<typename T>
void process(const T& item) {
    /* Simulate work */
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    log_inf("[consumer] processed: {}", std::to_string(item));
}

} // namespace wal
