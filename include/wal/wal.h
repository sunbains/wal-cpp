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
    explicit Config(size_t n_blocks, size_t block_size) noexcept
      : m_n_blocks(n_blocks),
        m_block_size(block_size) {}

    ~Config() = default;

    const size_t m_n_blocks;
    const size_t m_block_size;

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

  using Block = std::tuple<Block_header*, std::span<const std::byte>, crc32_t*>;

  explicit Circular_buffer(lsn_t hwm, Config config) noexcept;

  ~Circular_buffer() noexcept;

  [[nodiscard]] Block_header &get_block_header(std::size_t block_index) noexcept {
    assert(block_index < m_config.m_n_blocks);

    return m_block_header_array[block_index];
  }

  [[nodiscard]] const Block_header &get_block_header(std::size_t block_index) const noexcept {
    assert(block_index < m_config.m_n_blocks);
    return m_block_header_array[block_index];
  }

  [[nodiscard]] const std::span<const std::byte> get_data(std::size_t block_index) const noexcept {
    assert(block_index < m_config.m_n_blocks);
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
    assert(block_index < m_config.m_n_blocks);
    return m_crc32_array[block_index];
  }

  [[nodiscard]] crc32_t &get_crc32(std::size_t block_index) noexcept {
    return const_cast<crc32_t&>(const_cast<const Circular_buffer*>(this)->get_crc32(block_index));
  }

  [[nodiscard]] Block get_block(std::size_t block_index) noexcept {
    assert(block_index < m_config.m_n_blocks);
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

  [[nodiscard]] std::span<std::byte> data() const noexcept {
    return std::span<std::byte>(m_data_array, m_total_data_size);
  }

  [[nodiscard]] std::span<Block_header> headers() const noexcept {
    return std::span<Block_header>(m_block_header_array, m_config.m_n_blocks);
  }

  [[nodiscard]] std::span<crc32_t> crc32s() const noexcept {
    return std::span<crc32_t>(m_crc32_array, m_config.m_n_blocks);
  }

  [[nodiscard]] std::size_t get_first_block_index() const noexcept {
    return (m_lwm / m_config.get_data_size_in_block()) % m_config.m_n_blocks;
  }

  [[nodiscard]] std::size_t get_last_block_index() const noexcept {
    assert(m_committed_lsn >= m_lwm);
    return ((m_committed_lsn - m_lwm) / (m_config.get_data_size_in_block() - 1)) % m_config.m_n_blocks;
  }

  [[nodiscard]] const Block_header &front() const noexcept {
    return get_block_header(get_first_block_index());
  }

  [[nodiscard]] const Block_header &back() const noexcept {
    return get_block_header(get_last_block_index());
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
    assert(span.size() > 0);
    assert(slot.m_len == span.size());

    using len_t = decltype(Block_header::Data::m_len);

    assert(m_lwm <= slot.m_lsn);

    log_inf("m_total_data_size: {}, slot.m_lsn: {}, m_lwm: {}, check_margin: {}", m_total_data_size, slot.m_lsn, m_lwm.load(), check_margin());

    if (m_total_data_size <= slot.m_lsn - m_lwm) {
      return std::unexpected(Status::Not_enough_space);
    }

    std::size_t rel_off = static_cast<std::size_t>(slot.m_lsn % m_total_data_size);
    const std::size_t len = std::min(size_t(slot.m_len), m_total_data_size - rel_off);

    log_inf("lwm: {}, writing {} bytes at LSN {} (rel_off: {}, len: {})", m_lwm.load(), slot.m_len, slot.m_lsn, rel_off, len);

    assert(len > 0);

    auto block_no = slot.m_lsn / m_config.get_data_size_in_block();

    const auto off_in_blk = static_cast<std::size_t>(slot.m_lsn % m_config.get_data_size_in_block());

    assert(block_no < std::numeric_limits<block_no_t>::max());

    auto block_index = (get_first_block_index() + (block_no - front().get_block_no())) % m_config.m_n_blocks;
    assert(block_index < m_config.m_n_blocks);

    std::size_t copied{};
    std::byte* ptr = m_data_array + rel_off;

    /* Process current block, then subsequent blocks as needed. */
    std::size_t offset_in_block = off_in_blk;
    const auto lwm_block_index = (m_lwm / m_config.get_data_size_in_block()) % m_config.m_n_blocks;

    while (copied < len) {

      log_inf("block_index: {}, copied: {}, check_margin: {}, committed_lsn: {}", block_index, copied, check_margin(), m_committed_lsn.load());

      auto &hdr = get_block_header(block_index);

      hdr.set_block_no(static_cast<block_no_t>(block_no));

      /* Copy bounded by remaining bytes and remaining space in this block. */
      const std::size_t avail_in_block = m_config.get_data_size_in_block() - offset_in_block;
      const std::size_t to_copy = std::min(len - copied, avail_in_block);

      log_inf("writing {} bytes to block {} (block_no: {}, block_index: {})", to_copy, block_no, block_no, block_index);

      assert(to_copy > 0);

      std::memcpy(ptr, span.data() + copied, to_copy);

      std::atomic_ref<len_t>(hdr.m_data.m_len).fetch_add(static_cast<len_t>(to_copy));

      /* Advance pointers/counters. */
      copied += to_copy;
      rel_off = (rel_off + to_copy) % m_total_data_size;
      ptr = m_data_array + rel_off;

      /* Next block starts at 0 */
      offset_in_block = 0;

      assert(std::atomic_ref<len_t>(hdr.m_data.m_len).load() <= m_config.get_data_size_in_block());

      ++block_no;
      block_index = (block_index + 1) % m_config.m_n_blocks;

      log_inf("block_index: {}, lwm_block_index: {}", block_index, lwm_block_index);
      /* Check if we have wrapped around to the first block. */
      if (block_index == lwm_block_index) {
        break;
      }
    }

    assert(copied > 0 && copied <= len);

    m_committed_lsn.fetch_add(copied, std::memory_order_release);

    log_inf("wrote {} bytes to slot {} (len: {}), m_committed_lsn: {}", copied, slot.m_lsn, len, m_committed_lsn.load());

    /* Caller can detect partial write via return value. */
    return Result<std::size_t>(copied);
  }

  [[nodiscard]] bool is_full() const noexcept {
    assert(m_committed_lsn >= m_lwm);
    assert(m_committed_lsn <= m_hwm);
    assert(m_committed_lsn - m_lwm <= get_total_data_size());
    return m_committed_lsn - m_lwm == get_total_data_size();
  }

  [[nodiscard]] bool is_empty() const noexcept {
    assert(m_committed_lsn >= m_lwm);
    assert(m_committed_lsn <= m_hwm);
    return m_committed_lsn == m_lwm;
  }

  /**
   * Check if there are any pending writes.
   * This is to check if slots have been reserved but not yet written.
   * @return True if there are pending writes, false otherwise.
   */
  [[nodiscard]] bool pending_writes() const noexcept {
    return m_committed_lsn < m_hwm;
  }

  /**
   * Check how much space is left in the buffer.
   * @return The number of bytes left in the buffer.
   */
  [[nodiscard]] std::size_t check_margin() const noexcept {
    assert(m_committed_lsn >= m_lwm);
    return m_total_data_size - (m_committed_lsn - m_lwm);
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
    assert(start_lsn <= end_lsn);
    assert(start_lsn >= m_lwm);
    assert(end_lsn <= m_committed_lsn);

    log_inf("clearing from LSN {} to LSN {}", start_lsn, end_lsn);

    auto n_blocks_to_clear = ((end_lsn - start_lsn) / m_config.get_data_size_in_block());
    auto n_bytes_to_clear = n_blocks_to_clear * sizeof(Block_header);

    assert(n_bytes_to_clear <= sizeof(Block_header) * m_config.m_n_blocks);

    const auto start_block_index = (start_lsn / m_config.get_data_size_in_block()) % m_config.m_n_blocks;

    auto ptr = m_block_header_array + start_block_index;
    const auto end_ptr = m_block_header_array + m_config.m_n_blocks;

    {
      using uptrdiff_t = std::make_unsigned_t<std::ptrdiff_t>;

      assert(sizeof(Block_header) * n_blocks_to_clear >= n_bytes_to_clear);
      const auto len = std::min(n_bytes_to_clear, uptrdiff_t(end_ptr - ptr) * sizeof(Block_header));
      log_inf("clearing {} bytes from block index {}", len, start_block_index);
      std::memset(ptr, 0, len);
      n_bytes_to_clear -= len;

      for (std::size_t i = 0; i < m_config.m_n_blocks; ++i) {
        log_inf("Block header {}", m_block_header_array[i].to_string());
      }
    }

    /* Check for wrap around. */
    if (n_bytes_to_clear > 0) {
      std::memset(m_block_header_array, 0, n_bytes_to_clear);
      n_bytes_to_clear = 0;
    }

    assert(n_bytes_to_clear == 0);
  }

  [[nodiscard]] std::string to_string() const noexcept;

  /** Low water mark, the circular buffer starts at this LSN. */
  alignas(kCLS) std::atomic<lsn_t> m_lwm{};

  /** High water mark. We have reserved this many bytes to the buffer.
   * The writes may not have completed yet.
  */
  alignas(kCLS) std::atomic<lsn_t> m_hwm{};

  /** LSN of the last write. This is the LSN of the last write that has been committed. */
  alignas(kCLS) std::atomic<lsn_t> m_committed_lsn{};

  Config m_config;

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
   * @param[in]  lsn The LSN to start the log from.
   * @param[in]  config The configuration for the circular buffer.
   */
  Log(lsn_t lsn, const Circular_buffer::Config &config) noexcept;

  ~Log() noexcept;

  [[nodiscard]] Result<std::size_t> write(Slot &slot, std::span<const std::byte> span) noexcept {
    assert(span.size() > 0);
    assert(slot.m_len == span.size());

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

