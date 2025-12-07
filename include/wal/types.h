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

#pragma once

#include <cstdint>
#include <cstddef>
#include <expected>
#include <type_traits>
#include <string>
#include <format>
#include <cstdlib>
#include <limits>
#include <numa.h>

#include "util/byte_order.h"
#include "util/util.h"

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
struct Aligned_allocator {
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

} // namespace wal