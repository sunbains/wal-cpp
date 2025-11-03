#include <format>
#include <sys/uio.h>

#include "util/checksum.h"
#include "wal/wal.h"

namespace wal {

  Circular_buffer::Circular_buffer(lsn_t hwm, size_t n_blocks, size_t block_size) noexcept
    : m_lwm(hwm),
      m_hwm(hwm),
      m_n_blocks(n_blocks),
      m_block_size(block_size),
      m_data_size_in_block(block_size - sizeof(Block_header) - sizeof(crc32_t)),
      m_total_data_size(m_data_size_in_block * n_blocks) {

    m_buffer.resize(m_total_data_size + (sizeof(crc32_t) + sizeof(Block_header)) * m_n_blocks);

    m_block_header_array = reinterpret_cast<Block_header*>(m_buffer.data());
    m_data_array = reinterpret_cast<std::byte*>(m_block_header_array + m_n_blocks);
    m_crc32_array = reinterpret_cast<crc32_t*>(m_data_array + m_total_data_size);
 }

  Circular_buffer::~Circular_buffer() noexcept {}

  std::string Circular_buffer::to_string() const noexcept {
    return std::format(
      "Circular_buffer: lwm: {}, hwm: {}, n_blocks: {}, block_size: {}, total_data_size: {}, data_size_in_block: {}",
      m_lwm.load(std::memory_order_relaxed), m_hwm.load(std::memory_order_relaxed),
      m_n_blocks, m_block_size, m_total_data_size, m_data_size_in_block
    );
  }

  Result<lsn_t> Circular_buffer::write_to_store(Write_callback callback) noexcept {
    assert(!is_empty());
    assert(m_hwm > m_lwm);

    IO_vecs iovecs;

    auto block_start = m_lwm / m_data_size_in_block;
    auto n_blocks_to_flush = ((m_hwm - m_lwm) / m_n_blocks) + 1;

    /* We create IO vectors for each block. One for the header,
    one for the data for the block and the last for the checksum. */
    iovecs.reserve((std::min(n_blocks_to_flush * 3, size_t(IOV_MAX))));

    while (n_blocks_to_flush > 0) {
      auto flush_batch_size = std::min(n_blocks_to_flush, iovecs.size() / 3);

      for (std::size_t i{}; i < flush_batch_size; ++i) {
        auto block = get_block(block_start + 1);
        auto[header, span, crc32] = block;

        *crc32 = util::Checksum::compute(span, util::ChecksumAlgorithm::CRC32C);

        iovecs[i].iov_base = const_cast<Block_header*>(header);
        iovecs[i].iov_len = sizeof(Block_header);
        iovecs[i + 1].iov_base = const_cast<std::byte*>(span.data());
        iovecs[i + 1].iov_len = span.size();
        iovecs[i + 2].iov_base = static_cast<void*>(crc32);
        iovecs[i + 2].iov_len = sizeof(crc32_t);
      }

      n_blocks_to_flush -= flush_batch_size;
    }

    auto result = callback(m_lwm, iovecs);
    if (!result.has_value()) {
      switch (result.error()) {
        case Status::Success:
          std::unreachable();
        case Status::Internal_error:
          return std::unexpected(Status::Internal_error);
        case Status::IO_error:
          return std::unexpected(Status::IO_error);
        case Status::Not_enough_space:
          return std::unexpected(Status::Not_enough_space);
      }
      return std::unexpected(result.error());
    }
    m_lwm = result.value();
    assert(m_lwm <= m_hwm);
    return result;
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

  Log::Log(lsn_t lsn, std::size_t n_blocks, std::size_t block_size) noexcept
    : m_buffer(lsn, n_blocks, block_size) {
  }

  Log::~Log() noexcept {
  }

  std::string Log::to_string() const noexcept {
    return m_buffer.to_string();
  }


std::string to_string(Status status) noexcept {
  switch (status) {
    case Status::Success:
      return "Success";
    case Status::Internal_error:
      return "Internal error";
    case Status::IO_error:
      return "IO error";
    case Status::Not_enough_space:
      return "Not enough space";
  }
  return "Unknown status";
}

} // namespace wal