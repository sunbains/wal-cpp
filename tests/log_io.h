#pragma once

#include <cstring>
#include <fcntl.h>
#include <span>
#include <stdexcept>
#include <string>
#include <sys/uio.h>
#include <unistd.h>
#include <atomic>
#include <array>

#include "wal/wal.h"
#include "util/logger.h"


extern util::Logger<util::MT_logger_writer> g_logger;

/**
 * Log file wrapper for testing WAL I/O performance
 * Handles file operations with optional write disabling and statistics tracking
 */
struct Log_file {
  explicit Log_file(const std::string& path, off_t max_file_size, std::size_t block_size, bool disable_writes = false)
    : m_path(path),
      m_max_file_size(max_file_size),
      m_block_size(block_size),
      m_disable_writes(disable_writes) {

    m_fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
    if (m_fd == -1) {
      throw std::runtime_error("Failed to open log file: " + path);
    }
  }

  ~Log_file() noexcept {
    if (m_fd != -1) {
      ::close(m_fd);
      m_fd = -1;
    }
  }

  // Non-copyable, non-movable
  Log_file(const Log_file&) = delete;
  Log_file& operator=(const Log_file&) = delete;
  Log_file(Log_file&&) = delete;
  Log_file& operator=(Log_file&&) = delete;

  // Low-level write with physical offset
  wal::Result<std::size_t> write(std::span<struct iovec> iov_span, off_t phy_off) const noexcept {
    std::size_t iov_index{};
    std::size_t total_written{};
    const std::size_t iov_size{iov_span.size()};
    const std::size_t total_len_to_write{(iov_size / 3) * m_block_size};

    for (;;) {
      const ssize_t rc{::pwritev(m_fd, &iov_span[iov_index], int(iov_size - iov_index), phy_off)};

      if (rc == -1) [[unlikely]] {
        switch (errno) {
          case EINTR:
          case EAGAIN:
            continue;
          default:
            log_fatal("Failed to write to file descriptor {}: {}", m_fd, std::strerror(errno));
            return std::unexpected(wal::Status::IO_error);
        }
      }

      const std::size_t n{static_cast<std::size_t>(rc)};
      if (n == 0) [[unlikely]] {
        return std::unexpected(wal::Status::IO_error);
      }

      total_written += n;

      if (total_written < total_len_to_write) [[unlikely]] {
        m_stats.m_n_write_retries.fetch_add(1, std::memory_order_relaxed);
        std::size_t remaining = total_len_to_write - total_written;
        for (std::size_t i = iov_index; i < iov_size && remaining > 0; ++i) {
          const std::size_t len = iov_span[i].iov_len;
          if (len <= remaining) {
            ++iov_index;
            remaining -= len;
          } else {
            iov_span[i].iov_len = len - remaining;
            iov_span[i].iov_base = static_cast<void*>(static_cast<char*>(iov_span[i].iov_base) + remaining);
            break;
          }
        }
        phy_off += static_cast<off_t>(n);
        continue;
      }
      break;
    }

    return wal::Result<std::size_t>{total_written};
  }

  // High-level write from WAL buffer (handles wraparound)
  wal::Result<std::size_t> write(std::span<struct iovec> span) const noexcept {
    if (m_disable_writes) {
      // Just return success without actually writing
      std::size_t total_len = 0;
      for (const auto& iov : span) {
        total_len += iov.iov_len;
      }
      return wal::Result<std::size_t>{total_len};
    }

    WAL_ASSERT(!span.empty());

    const auto n_blocks = span.size() / 3;
    const auto data_size = span[1].iov_len;

    m_stats.m_n_write_requests.fetch_add(1, std::memory_order_relaxed);
    m_stats.m_n_bytes_written.fetch_add(n_blocks * m_block_size, std::memory_order_relaxed);

    std::size_t start_index{};
    const auto block_header{reinterpret_cast<const wal::Block_header*>(span[0].iov_base)};
    const auto block_no{block_header->get_block_no()};
    off_t phy_off{static_cast<off_t>(block_no * m_block_size % static_cast<std::size_t>(m_max_file_size))};
    auto available_blocks{static_cast<std::size_t>((m_max_file_size - phy_off) / static_cast<off_t>(m_block_size))};
    const auto total_data_size = n_blocks * data_size;

    do {
      WAL_ASSERT(start_index % 3 == 0);

      span = span.subspan(start_index, std::min(span.size() - start_index, available_blocks * 3));

      auto result = write(span, phy_off);

      if (!result.has_value() || result.value() == total_data_size) [[likely]] {
        return result;
      }

      phy_off = 0;
      start_index += span.size();

    } while (start_index < span.size());

    return wal::Result<std::size_t>{total_data_size};
  }

  struct alignas(64) Stats {
    alignas(64) std::atomic<std::size_t> m_n_write_retries{0};
    alignas(64) std::atomic<std::size_t> m_n_write_requests{0};
    alignas(64) std::atomic<std::size_t> m_n_bytes_written{0};
  };

  mutable Stats m_stats;

  int m_fd{-1};
  std::string m_path{};
  off_t m_max_file_size{};
  std::size_t m_block_size{};
  bool m_disable_writes{false};
};

/**
 * Simple test message structure for WAL performance testing
 * 32 bytes of data with padding to align to 64-byte cache line
 */
struct alignas(64) Test_message {
  static constexpr std::size_t SIZE = 32;

  Test_message() {
    std::memset(m_data, 0xAB, SIZE);
  }

  std::span<const std::byte> get_span() const noexcept {
    return std::span<const std::byte>(m_data, SIZE);
  }

  std::byte m_data[SIZE];
  std::byte m_padding[64 - SIZE];
};

static_assert(sizeof(Test_message) == 64, "Test_message should be 64 bytes");
