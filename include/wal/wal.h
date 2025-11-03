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
#include <cassert>
#include <thread>

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
    block_no_t m_block_no{0};

    /** Checkpoint number */
    checkpoint_no_t m_checkpoint_no{0};

    /** Offset of the first mtr log record group in m_data[]. */
    std::uint16_t m_first_rec_group{0};

    /** Valid data length inside the block. Use atomics to read and write this variable.
     * This struct and variable should be aligned correctly for this to work. Unlike
     * the other fields this field is always in host byte order and only converted
    * to network byte order before writing. */
    std::uint16_t m_len{0};

    #pragma pack(pop)
  };

  /**
   * Sets the log block number stored in the header.
   * @note: This must be set before the flush bit!
   *
   * @param block_no The log block number: must be > 0 and < LOG_BLOCK_FLUSH_BIT_MASK.
   */
  void set_block_no(block_no_t block_no) noexcept {
    m_data.m_block_no = util::hton(block_no);
  }

  void prepare_to_write() noexcept {
    m_data.m_len = util::hton(m_data.m_len);
  }

  void prepare_to_read() noexcept {
    m_data.m_len = util::ntoh(m_data.m_len);
  }

  void set_first_rec_group(uint16_t first_rec_group) noexcept {
    m_data.m_first_rec_group = util::hton(first_rec_group);
  }

  void set_checkpoint_no(checkpoint_no_t checkpoint_no) noexcept {
    m_data.m_checkpoint_no = util::hton(checkpoint_no);
  }

  [[nodiscard]] block_no_t get_block_no() const noexcept {
     return ~FLUSH_BIT_MASK & util::ntoh(m_data.m_block_no);
  }

  /** We handle data len as an exception to the other fields.
   * It increases incrementally when writing data to the block.
   * We convert it to network byte order when writing to disk
   * and back to host byte order when reading from disk. */
  [[nodiscard]] uint16_t get_data_len() const noexcept {
    return util::ntoh(m_data.m_len);
  }

  [[nodiscard]] uint16_t get_first_rec_group() const noexcept {
    return util::ntoh(m_data.m_first_rec_group);
  }

  [[nodiscard]] checkpoint_no_t get_checkpoint_no() const noexcept {
    return util::ntoh(m_data.m_checkpoint_no);
  }

  [[nodiscard]] bool get_flush_bit() const noexcept {
    return (util::ntoh(m_data.m_block_no) & FLUSH_BIT_MASK) != 0;
  }

  /**
   * (Un)Set the flush bit of a log block.
   *
   * @param val The value to set.
   */
  void set_flush_bit(bool val) noexcept {
    if (val) {
      set_block_no(util::ntoh(m_data.m_block_no) | FLUSH_BIT_MASK);
    } else {
      set_block_no(util::ntoh(m_data.m_block_no) & ~FLUSH_BIT_MASK);
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

  Data m_data{};
};

static_assert(sizeof(Block_header::Data) == 12, "Block_header::Data size must be equal to 12 ie. packed size");
static_assert(std::is_standard_layout_v<Block_header>, "Block_header must be standard layout");
static_assert(std::is_trivially_copyable_v<Block_header>, "Block_header must be trivially copyable");
static_assert(alignof(Block_header) == kCLS, "Block_header must be cache-line aligned");

struct [[nodiscard]] Circular_buffer {
  using IO_vecs = std::vector<struct iovec>;

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
   * @return The new LWM LSN.
   */
  using Write_callback = std::function<Result<lsn_t>(lsn_t lsn, const IO_vecs& iovecs)>;

  struct Slot {
    /** Start LSN of this slot. */
    lsn_t m_lsn{};
    /** Number of bytes reserved. */
    std::uint16_t m_len{};
    /* true, if the data has been written to the buffer. */
    std::uint16_t m_committed{false};
  };

  using Block = std::tuple<const Block_header*, std::span<const std::byte>, crc32_t*>;

  explicit Circular_buffer(lsn_t hwm, size_t n_blocks, size_t block_size) noexcept;

  ~Circular_buffer() noexcept;

  [[nodiscard]] Block_header &get_block_header(std::size_t block_index) noexcept {
    assert(block_index < m_n_blocks);

    const auto bhs{reinterpret_cast<Block_header*>(m_buffer.data())};
    return bhs[block_index];
  }

  [[nodiscard]] const Block_header &get_block_header(std::size_t block_index) const noexcept {
    assert(block_index < m_n_blocks);
    return m_block_header_array[block_index];
  }

  [[nodiscard]] const std::span<const std::byte> get_data(std::size_t block_index) const noexcept {
    assert(block_index < m_n_blocks);
    return std::span<std::byte>(&m_data_array[(get_data_size_in_block() * block_index)], get_data_size_in_block());
  }

  /** Get the size of the data in the block. */
  [[nodiscard]] std::size_t get_data_size_in_block() const noexcept {
    return m_data_size_in_block;
  }

  [[nodiscard]] std::size_t get_total_data_size() const noexcept {
    return m_total_data_size;
  }

  [[nodiscard]] std::size_t get_total_block_header_size() const noexcept {
    return sizeof(Block_header) * m_n_blocks;
  }

  [[nodiscard]] const crc32_t &get_crc32(std::size_t block_index) const noexcept {
    assert(block_index < m_n_blocks);
    return m_crc32_array[block_index];
  }

  [[nodiscard]] crc32_t &get_crc32(std::size_t block_index) noexcept {
    return const_cast<crc32_t&>(const_cast<const Circular_buffer*>(this)->get_crc32(block_index));
  }

  [[nodiscard]] Block get_block(std::size_t block_index) noexcept {
    assert(block_index < m_n_blocks);
    return std::make_tuple(&get_block_header(block_index), get_data(block_index), &get_crc32(block_index));
  }

  /** Reserve len number of bytes in the buffer cache.
   * The reserved slot (LSN) could be outside the bounds of this buffer.
   *
   * @return slot that was reserved.
   */
  [[nodiscard]] Slot reserve(std::uint16_t len) noexcept {
    const auto lsn = m_hwm.fetch_add(len, std::memory_order_release);

    return Slot {
      .m_lsn = lsn,
      .m_len = len,
      .m_committed = false
    }; 
  }

  [[nodiscard]] const Block_header &get_first_block_header() const noexcept {
    return get_block_header(0);
  }

  [[nodiscard]] const Block_header &get_last_block_header() const noexcept {
    return get_block_header(m_n_blocks - 1);
  }

  /** 
   * Write data to the buffer.
   * @note: There is no guarantee that the entire span will be written.
   * Only as many bytes that fit in the buffer will be written. The caller
   * should check the return value and handle the case where the entire span
   * was not written. The obvious thing to do is to write the data to persistent
   * store and retry the write.
   *
   * @param[in] slot The slot to write the data to.
   * @param[in] span The data to write.
   * 
   * @return The number of bytes written.
   */
  [[nodiscard]] Result<std::size_t> write(const Slot &slot, std::span<const std::byte> span) noexcept {
    assert(slot.m_len > 0);
    assert(slot.m_len == span.size());

    const auto block_no{slot.m_lsn / m_data_size_in_block};
    const auto fitrst_block_no = get_first_block_header().get_block_no();

    assert(block_no >= fitrst_block_no);

    if (block_no > get_last_block_header().get_block_no()) {
      /* Buffer is full because the LSN is outside the buffer bounds
       * we need to flush the buffer to disk first and move the flush LSN
       * forward. */
      return std::unexpected(Status::Not_enough_space);
    }

    /* Convert to relative offset within the data array. This is the offset
    from we start writing data to the data array. */
    auto relative_offset{slot.m_lsn % get_total_data_size()};
    
    /* Ensure we don't read beyond span or write beyond buffer */
    auto copy_len = std::min({static_cast<std::size_t>(slot.m_len), get_total_data_size() - relative_offset});

    assert(copy_len > 0);

    /* Calculate offset within the first block */
    auto offset_in_block{relative_offset % get_data_size_in_block()};
    auto block_index = static_cast<std::size_t>(block_no - fitrst_block_no);
    
    auto ptr{m_data_array + relative_offset};

    std::memcpy(ptr, span.data(), copy_len);

    auto remaining_bytes{copy_len};

    /* Update the block headers data length field. */
    while (remaining_bytes > 0 && block_index < m_n_blocks) {
      /* Calculate how many bytes we can write in the current block */
      const auto space_in_block = get_data_size_in_block() - offset_in_block;
      const auto n_written = std::min(space_in_block, remaining_bytes);
      
      /* Update the block header's data length */
      auto block_header = &m_block_header_array[block_index];
      std::atomic_ref<decltype(Block_header::Data::m_len)> data_len(block_header->m_data.m_len);
      data_len.fetch_add(static_cast<decltype(Block_header::Data::m_len)>(n_written));

      remaining_bytes -= n_written;
      
      /* Move to next block (no wraparound - caller handles buffer full) */
      if (remaining_bytes > 0) [[unlikely]] {
        ++block_index;
        /* Start at beginning of next block */
        offset_in_block = 0; 
      }
    }

    /* Return number of bytes actually written, caller should check and handle overflow. */
    return Result<std::size_t>(copy_len - remaining_bytes);
  }

  [[nodiscard]] bool is_full() const noexcept {
    return m_hwm - m_lwm >= get_total_data_size();
  }

  [[nodiscard]] bool is_empty() const noexcept {
    return m_hwm - m_lwm == 0;
  }

  /** Check how much space is left in the buffer.
   * @return The number of bytes left in the buffer.
   */
  [[nodiscard]] std::size_t check_margin() const noexcept {
    return get_total_data_size() - (m_hwm - m_lwm);
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

  [[nodiscard]] std::string to_string() const noexcept;

  /** Low water mark, the circular buffer starts at this LSN. */
  std::atomic<lsn_t> m_lwm{};

  /** High water mark. We have written this many bytes to the buffer. */
  std::atomic<lsn_t> m_hwm{};

  /** Number of blocks in the buffer. */
  const std::size_t m_n_blocks{};

  /** Size of the physical block. */
  const size_t m_block_size{};

  /** Size of the data in each block. */
  const size_t m_data_size_in_block{};

  /** Total size of the data in the buffer. */
  const size_t m_total_data_size{};

  /** Array of CRC32 values for each block. It points to an offset in m_buffer */
  crc32_t *m_crc32_array{};

  /** Array of data for each block. It points to an offset in m_buffer */
  std::byte *m_data_array{};

  /** Array of block headers for each block. It points to an offset in m_buffer */
  Block_header *m_block_header_array{};

  /** Buffer for all the data. */
  std::vector<std::byte> m_buffer{};
};

struct [[nodiscard]] Log {
  using Slot = Circular_buffer::Slot;
  using IO_vecs = Circular_buffer::IO_vecs;
  using Write_callback = Circular_buffer::Write_callback;

  /**
   * Constructor.
   *
   * @param lsn The LSN to start the log from.
   * @param n_blocks The number of blocks in the log.
   * @param block_size The size of the block in the log.
   */
  Log(lsn_t lsn, std::size_t n_blocks, std::size_t block_size) noexcept;

  ~Log() noexcept;

  [[nodiscard]] Result<std::size_t> write(Slot &slot, std::span<const std::byte> span) noexcept {
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

  Circular_buffer m_buffer;
};

/* Simple process() example. Customize as needed. */
template<typename T>
void process(const T& item) {
    /* Simulate work */
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    log_inf("[consumer] processed: {}", std::to_string(item));
}

} // namespace wal

