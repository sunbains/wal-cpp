#include <format>
#include <sys/uio.h>

#include "util/checksum.h"
#include "wal/wal.h"

namespace wal {

  Circular_buffer::Circular_buffer(lsn_t hwm, Config config) noexcept
    : m_lwm(hwm),
      m_hwm(hwm),
      m_committed_lsn(hwm),
      m_config(config),
      m_total_data_size(m_config.get_data_size_in_block() * m_config.m_n_blocks) {

    m_buffer.resize(m_total_data_size + (sizeof(crc32_t) + sizeof(Block_header)) * m_config.m_n_blocks);

    m_block_header_array = reinterpret_cast<Block_header*>(m_buffer.data());
    m_data_array = reinterpret_cast<std::byte*>(m_block_header_array + m_config.m_n_blocks);
    m_crc32_array = reinterpret_cast<crc32_t*>(m_data_array + m_total_data_size);

    const auto block_start_no{m_lwm / m_config.get_data_size_in_block()};
    assert(block_start_no  + m_config.m_n_blocks < std::numeric_limits<block_no_t>::max());

    for (std::size_t i{}; i < m_config.m_n_blocks; ++i) {
      auto& block_header = m_block_header_array[i];
      block_header.initialize(block_no_t(block_start_no + i));
    }
 }

  Circular_buffer::~Circular_buffer() noexcept {}

  std::string Circular_buffer::to_string() const noexcept {
    return std::format(
      "Circular_buffer: lwm: {}, hwm: {}, n_blocks: {}, block_size: {}, total_data_size: {}, data_size_in_block: {}",
      m_lwm.load(std::memory_order_relaxed), m_hwm.load(std::memory_order_relaxed),
      m_config.m_n_blocks, m_config.m_block_size, m_total_data_size, m_config.get_data_size_in_block()
    );
  }

  Result<lsn_t> Circular_buffer::write_to_store(Write_callback callback) noexcept {
    assert(!is_empty());
    assert(m_hwm > m_lwm);

    IO_vecs iovecs;

    auto block_start_no = m_lwm / m_config.get_data_size_in_block();
    const auto n_bytes_to_flush = m_committed_lsn - m_lwm;

    /* Ceiling division: (a + b - 1) / b avoids modulo and conditional */
    auto n_blocks_to_flush = (n_bytes_to_flush + m_config.get_data_size_in_block() - 1) / m_config.get_data_size_in_block();

    /* We create IO vectors for each block. One for the header,
    one for the data for the block and the last for the checksum. */
    iovecs.resize((std::min(n_blocks_to_flush * 3, size_t(IOV_MAX))));

    [[maybe_unused]] auto expected_block_start_no = m_lwm / m_config.get_data_size_in_block();

    lsn_t persisted_lsn{};

    while (n_blocks_to_flush > 0) {
      const auto flush_batch_size{std::min(n_blocks_to_flush, iovecs.size() / 3)};
      const auto n_slots = flush_batch_size * 3;

      for (std::size_t i{}; i < n_slots; i += 3) {
        const auto block_index{(block_start_no + i / 3) % m_config.m_n_blocks};
        auto [header, span, crc32] = get_block(block_index);

        assert(header->get_block_no() == expected_block_start_no++);

        header->prepare_to_write();

        *crc32 = util::Checksum::compute(span, util::ChecksumAlgorithm::CRC32C);

        iovecs[i].iov_base = const_cast<Block_header::Data*>(&header->m_data);
        iovecs[i].iov_len = sizeof(Block_header::Data);
        iovecs[i + 1].iov_base = const_cast<std::byte*>(span.data());
        iovecs[i + 1].iov_len = span.size();
        iovecs[i + 2].iov_base = static_cast<void*>(crc32);
        iovecs[i + 2].iov_len = sizeof(crc32_t);
      }

      n_blocks_to_flush -= flush_batch_size;
      block_start_no = block_start_no + flush_batch_size;

      if (n_blocks_to_flush == 0) {
        auto data = reinterpret_cast<Block_header::Data*>(iovecs[iovecs.size() - 3].iov_base);

        data->m_block_no = util::ntoh(data->m_block_no);
        data->m_block_no |= Block_header::FLUSH_BIT_MASK;
        data->m_block_no = util::hton(data->m_block_no);
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

      /* The persisted LSN is on block boundaries, due to padding to avoid
      read before write issues. We advance the LWM by the number of valid
      bytes written. */
      persisted_lsn = result.value();

      assert(persisted_lsn >= m_lwm);
    }

    /* The last block may not be full and may have to be rewritten. We need to preseve its header,
    and advance the LWM only by the valid data length. Round down to lower block boundary */
    auto& block_header = m_block_header_array[get_last_block_index()];

    if (util::ntoh(block_header.get_data_len()) < m_config.get_data_size_in_block()) {
      /* Convert back from network byte order to host byte order. */
      block_header.prepare_to_read();
    }

    const auto end_lsn = (m_committed_lsn.load(std::memory_order_acquire) / m_config.get_data_size_in_block()) * m_config.get_data_size_in_block();

    clear(m_lwm.load(std::memory_order_acquire), end_lsn);

    m_lwm.store(std::min(persisted_lsn, m_committed_lsn.load()), std::memory_order_release);

    log_inf("m_lwm: {}", m_lwm.load(std::memory_order_relaxed));
    log_inf("m_hwm: {}", m_hwm.load(std::memory_order_relaxed));
    log_inf("m_committed_lsn: {}", m_committed_lsn.load(std::memory_order_relaxed));

    return Result<lsn_t>(persisted_lsn);
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

  Log::Log(lsn_t lsn, const Circular_buffer::Config &config) noexcept
    : m_buffer(lsn, config) {
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