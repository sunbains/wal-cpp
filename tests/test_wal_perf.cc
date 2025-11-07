#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <getopt.h>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "wal/wal.h"

namespace {

struct Null_writer {
  [[nodiscard]] wal::Result<wal::lsn_t>
  operator()(wal::lsn_t lsn, const wal::Log::IO_vecs &iovecs, std::size_t n_slots) const noexcept {
    std::size_t data_bytes{};

    for (std::size_t i = 1; i < n_slots; i += 3) {
      data_bytes += iovecs[i].iov_len;
    }

    return wal::Result<wal::lsn_t>(lsn + data_bytes);
  }
};

// Fast xorshift32 PRNG function - use regular function instead of lambda to eliminate overhead
inline std::uint32_t fast_rand(std::uint32_t& state) noexcept {
  state ^= state << 13;
  state ^= state >> 17;
  state ^= state << 5;
  return state;
}

// Common configuration
constexpr std::size_t kDefaultBlocks = 512 * 32;
constexpr std::size_t kDefaultBlockSizeBytes = 4096;
constexpr std::size_t kMarginCheckInterval = 50000;
constexpr std::size_t kDefaultPayloadBytes = 32;
constexpr double kDefaultDurationSeconds = 1.0;
constexpr std::size_t kLoopCheckInterval = 4'096;
constexpr std::size_t kDefaultBatchBytes = 512;

std::size_t g_payload_bytes = kDefaultPayloadBytes;
std::chrono::nanoseconds g_target_duration = std::chrono::seconds(1);
std::size_t g_batch_bytes = kDefaultBatchBytes;
bool g_disable_checksums = false;

struct Flush_metrics {
  std::chrono::nanoseconds total{};
  std::size_t count{};
};

// Helper function to finalize log and verify state
void finalize_log(wal::Log& log,
                  wal::Log::Write_callback& null_writer,
                  Flush_metrics& metrics) noexcept {
  assert(log.m_buffer.m_n_pending_writes.load(std::memory_order_relaxed) == 0);
  assert(log.m_buffer.m_written_lsn.load(std::memory_order_relaxed) == log.m_buffer.m_hwm.load(std::memory_order_relaxed));
  
  if (!log.m_buffer.is_empty()) {
    const auto flush_start = std::chrono::steady_clock::now();
    auto flush_result = log.m_buffer.write_to_store(null_writer);
    const auto flush_stop = std::chrono::steady_clock::now();
    metrics.total += std::chrono::duration_cast<std::chrono::nanoseconds>(flush_stop - flush_start);
    metrics.count++;
    assert(flush_result.has_value());
  }
}

// Helper function to calculate and report throughput
void report_throughput(const char* test_name,
                       std::size_t iterations,
                       std::size_t total_bytes,
                       std::chrono::nanoseconds elapsed_ns,
                       bool show_mb = false) noexcept {
  const double elapsed_s = static_cast<double>(elapsed_ns.count()) / 1'000'000'000.0;
  const double mib = total_bytes / (1024.0 * 1024.0);
  const double throughput_mib = (elapsed_s > 0.0) ? (mib / elapsed_s) : 0.0;
  
  if (show_mb) {
    const double mb = total_bytes / (1000.0 * 1000.0);
    const double throughput_mb = (elapsed_s > 0.0) ? (mb / elapsed_s) : 0.0;
    std::fprintf(stderr,
                 "[%s] iterations=%zu bytes=%zu elapsed=%.3fs throughput=%.2f MiB/s (%.2f MB/s)\n",
                 test_name,
                 iterations,
                 total_bytes,
                 elapsed_s,
                 throughput_mib,
                 throughput_mb);
  } else {
    std::fprintf(stderr,
                 "[%s] iterations=%zu bytes=%zu elapsed=%.3fs throughput=%.2f MiB/s\n",
                 test_name,
                 iterations,
                 total_bytes,
                 elapsed_s,
                 throughput_mib);
  }
}

// Test memcpy bandwidth as baseline
void test_memcpy_baseline() noexcept {
  constexpr std::size_t kMemcpyTestSize = 1024 * 1024 * 1024; // 1 GB
  constexpr std::size_t kMemcpyChunkSize = 4096; // 4 KB chunks
  constexpr std::size_t kMemcpyIterations = kMemcpyTestSize / kMemcpyChunkSize;
  
  std::vector<std::byte> src(kMemcpyChunkSize);
  std::vector<std::byte> dst(kMemcpyChunkSize);
  
  // Initialize source with some data
  for (std::size_t i = 0; i < kMemcpyChunkSize; ++i) {
    src[i] = std::byte{static_cast<unsigned char>(i & 0xFF)};
  }
  
  const auto start = std::chrono::steady_clock::now();
  
  for (std::size_t i = 0; i < kMemcpyIterations; ++i) {
    std::memcpy(dst.data(), src.data(), kMemcpyChunkSize);
  }
  
  const auto stop = std::chrono::steady_clock::now();
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start);
  
  report_throughput("memcpy.baseline", kMemcpyIterations, kMemcpyTestSize, elapsed_ns, true);
}

// Streaming memcpy baseline that walks the same sized ring buffer as wal::Circular_buffer
void test_memcpy_ring(const wal::Circular_buffer::Config& config,
                      std::size_t chunk_size,
                      std::chrono::nanoseconds target_duration) noexcept {
  const std::size_t ring_size = config.get_data_size_in_block() * config.m_n_blocks;

  std::vector<std::byte> ring(ring_size);
  std::vector<std::byte> src(chunk_size);

  for (std::size_t i = 0; i < chunk_size; ++i) {
    src[i] = std::byte{static_cast<unsigned char>((i * 131) & 0xFF)};
  }

  std::size_t iterations{};
  const auto start = std::chrono::steady_clock::now();
  auto now = start;
  std::size_t offset{};
  do {
    const auto ring_off = offset % ring_size;
    std::memcpy(ring.data() + ring_off, src.data(), chunk_size);
    offset += chunk_size;
    ++iterations;
    if ((iterations & (kLoopCheckInterval - 1)) == 0) {
      now = std::chrono::steady_clock::now();
    }
  } while (now - start < target_duration || iterations == 0);

  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start);
  const auto total_bytes = chunk_size * iterations;

  report_throughput("memcpy.ring", iterations, total_bytes, elapsed_ns, true);
}

// Test with randomized payload data
void test_wal_random_data(const wal::Circular_buffer::Config& config,
                          std::uint16_t write_size,
                          std::chrono::nanoseconds target_duration) noexcept {
  wal::Log log(0, config);
  wal::Log::Write_callback null_writer = Null_writer{};
  Flush_metrics flush_metrics{};

  std::uint32_t rng_state = 42;
  std::vector<std::byte> buffer(write_size);

  const auto start = std::chrono::steady_clock::now();
  auto now = start;

  std::size_t iterations{};
  std::size_t total_bytes{};

  auto batch = log.make_batch(g_batch_bytes);

  auto flush_buffer = [&]() {
    const auto flush_start = std::chrono::steady_clock::now();
    auto flush_result = log.m_buffer.write_to_store(null_writer);
    const auto flush_stop = std::chrono::steady_clock::now();
    flush_metrics.total += std::chrono::duration_cast<std::chrono::nanoseconds>(flush_stop - flush_start);
    flush_metrics.count++;
    WAL_ASSERT(flush_result.has_value());
  };

  auto append_data = [&](std::span<const std::byte> data) {
    auto status = batch.append(data);
    if (status == wal::Status::Not_enough_space) {
      flush_buffer();
      status = batch.append(data);
    }
    WAL_ASSERT(status == wal::Status::Success);
  };

  do {
    // Generate pseudo-random payload
    for (std::size_t j = 0; j < buffer.size(); j += sizeof(std::uint32_t)) {
      rng_state = fast_rand(rng_state);
      const auto remaining = std::min(sizeof(std::uint32_t), buffer.size() - j);
      std::memcpy(buffer.data() + j, &rng_state, remaining);
    }

    if ((iterations % kMarginCheckInterval == 0) && iterations != 0 &&
        log.check_margin() < static_cast<std::size_t>(write_size) * 100) {
      const auto flush_start = std::chrono::steady_clock::now();
      auto flush_result = log.m_buffer.write_to_store(null_writer);
      const auto flush_stop = std::chrono::steady_clock::now();
      flush_metrics.total += std::chrono::duration_cast<std::chrono::nanoseconds>(flush_stop - flush_start);
      flush_metrics.count++;
      assert(flush_result.has_value());
    }

    append_data(std::span<const std::byte>(buffer.data(), buffer.size()));

    total_bytes += buffer.size();
    ++iterations;
    if ((iterations & (kLoopCheckInterval - 1)) == 0) {
      now = std::chrono::steady_clock::now();
    }
  } while (now - start < target_duration || iterations == 0);

  auto flush_status = batch.flush();
  if (flush_status == wal::Status::Not_enough_space) {
    flush_buffer();
    flush_status = batch.flush();
  }
  WAL_ASSERT(flush_status == wal::Status::Success);

  
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start);

  finalize_log(log, null_writer, flush_metrics);

  const double flush_ratio = (elapsed_ns.count() == 0)
    ? 0.0
    : (static_cast<double>(flush_metrics.total.count()) / elapsed_ns.count()) * 100.0;

  report_throughput("wal_perf.single", iterations, total_bytes, elapsed_ns);
  std::fprintf(stderr,
               "[wal_perf.single] flushes=%zu flush_time=%.3fs (%.1f%% of test)\n",
               flush_metrics.count,
               static_cast<double>(flush_metrics.total.count()) / 1'000'000'000.0,
               flush_ratio);
}

// Test with fixed data (no data generation overhead)
void test_wal_fixed_data(const wal::Circular_buffer::Config& config,
                         std::uint16_t write_size,
                         std::chrono::nanoseconds target_duration) noexcept {
  wal::Log log(0, config);
  wal::Log::Write_callback null_writer = Null_writer{};
  Flush_metrics flush_metrics{};

  constexpr std::size_t kFixedMarginCheckInterval = 200000;  // Check less frequently for fixed data test

  // Pre-allocate a single fixed data buffer
  std::vector<std::byte> fixed_data(write_size);
  // Fill with a simple pattern (not random to avoid any PRNG overhead)
  for (std::size_t i = 0; i < write_size; ++i) {
    fixed_data[i] = std::byte{static_cast<unsigned char>(i & 0xFF)};
  }
  const std::span<const std::byte> fixed_span(fixed_data.data(), write_size);

  auto batch = log.make_batch(g_batch_bytes);
  
  const auto start = std::chrono::steady_clock::now();
  auto now = start;

  std::size_t total_bytes{};
  std::size_t flush_count{};
  std::size_t iterations{};
  
  // Call buffer methods directly to avoid Log wrapper overhead
  auto& buffer = log.m_buffer;

  auto flush_buffer = [&]() {
    const auto flush_start = std::chrono::steady_clock::now();
    auto flush_result = buffer.write_to_store(null_writer);
    const auto flush_stop = std::chrono::steady_clock::now();
    flush_metrics.total += std::chrono::duration_cast<std::chrono::nanoseconds>(flush_stop - flush_start);
    flush_metrics.count++;
    ++flush_count;
    WAL_ASSERT(flush_result.has_value());
  };

  auto append_data = [&](std::span<const std::byte> data) {
    auto status = batch.append(data);
    if (status == wal::Status::Not_enough_space) {
      flush_buffer();
      status = batch.append(data);
    }
    WAL_ASSERT(status == wal::Status::Success);
  };
  
  do {
    // Check margin much less frequently to reduce overhead - only flush when really needed
    if ((iterations % kFixedMarginCheckInterval == 0)) {
      const auto margin = buffer.check_margin();
      // Only flush if we're running low on space (less than 100MB remaining)
      if (margin < 100 * 1024 * 1024) {
        flush_buffer();
      }
    }
    
    append_data(fixed_span);
    
    total_bytes += write_size;
    ++iterations;
    if ((iterations & (kLoopCheckInterval - 1)) == 0) {
      now = std::chrono::steady_clock::now();
    }
  } while (now - start < target_duration || iterations == 0);
  
  auto flush_status = batch.flush();
  if (flush_status == wal::Status::Not_enough_space) {
    flush_buffer();
    flush_status = batch.flush();
  }
  WAL_ASSERT(flush_status == wal::Status::Success);
  
  const auto elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start);

  finalize_log(log, null_writer, flush_metrics);
  
  const double flush_ratio = (elapsed_ns.count() == 0)
    ? 0.0
    : (static_cast<double>(flush_metrics.total.count()) / elapsed_ns.count()) * 100.0;

  std::fprintf(stderr, "[wal_perf.fixed_data] flush_count=%zu ", flush_count);
  report_throughput("wal_perf.fixed_data", iterations, total_bytes, elapsed_ns, true);
  std::fprintf(stderr,
               "[wal_perf.fixed_data] flushes=%zu flush_time=%.3fs (%.1f%% of test)\n",
               flush_metrics.count,
               static_cast<double>(flush_metrics.total.count()) / 1'000'000'000.0,
               flush_ratio);
}

void print_usage(const char* program_name) noexcept {
  std::fprintf(stderr,
               "Usage: %s [OPTIONS]\n"
               "\n"
               "Options:\n"
               "  -p, --payload BYTES     Payload size in bytes (default: %zu)\n"
               "  -d, --duration SECONDS  Test duration in seconds (default: %.1f)\n"
               "  -b, --batch BYTES       Batch size in bytes (default: %zu)\n"
               "  -n, --blocks BLOCKS     Number of blocks in buffer (default: %zu)\n"
               "  -s, --block-size BYTES  Block size in bytes (default: %zu)\n"
               "  -t, --tests TESTS       Comma-separated test list: memcpy,ring,random,fixed (default: all)\n"
               "  -c, --no-checksums      Disable CRC32 checksum computation (for performance testing)\n"
               "  -h, --help              Show this help message\n"
               "\n"
               "Examples:\n"
               "  %s -p 4096 -d 5.0\n"
               "  %s --payload 1024 --duration 2.0 --batch 512\n"
               "  %s -t memcpy,fixed -p 4096\n"
               "  %s -n 16384 -s 4096 -p 4096\n"
               "  %s -c -p 4096 -d 1.0\n",
               program_name,
               kDefaultPayloadBytes,
               kDefaultDurationSeconds,
               kDefaultBatchBytes,
               kDefaultBlocks,
               kDefaultBlockSizeBytes,
               program_name,
               program_name,
               program_name,
               program_name,
               program_name);
}

struct TestFlags {
  bool memcpy_baseline{true};
  bool memcpy_ring{true};
  bool wal_random{true};
  bool wal_fixed{true};
};

TestFlags parse_test_list(const char* test_list) noexcept {
  TestFlags flags{};
  flags.memcpy_baseline = false;
  flags.memcpy_ring = false;
  flags.wal_random = false;
  flags.wal_fixed = false;

  // Make a copy for strtok (which modifies the string)
  const std::size_t len = std::strlen(test_list);
  std::vector<char> list_copy(len + 1);
  std::memcpy(list_copy.data(), test_list, len + 1);

  char* token = std::strtok(list_copy.data(), ",");
  while (token) {
    if (std::strcmp(token, "memcpy") == 0) {
      flags.memcpy_baseline = true;
    } else if (std::strcmp(token, "ring") == 0) {
      flags.memcpy_ring = true;
    } else if (std::strcmp(token, "random") == 0) {
      flags.wal_random = true;
    } else if (std::strcmp(token, "fixed") == 0) {
      flags.wal_fixed = true;
    } else {
      std::fprintf(stderr, "[wal_perf] unknown test: '%s'\n", token);
    }
    token = std::strtok(nullptr, ",");
  }

  return flags;
}

} // namespace

int main(int argc, char** argv) {
  std::size_t payload_bytes = kDefaultPayloadBytes;
  std::size_t n_blocks = kDefaultBlocks;
  std::size_t block_size = kDefaultBlockSizeBytes;
  bool show_help = false;
  TestFlags test_flags{};

  static const struct option long_options[] = {
    {"payload", required_argument, nullptr, 'p'},
    {"duration", required_argument, nullptr, 'd'},
    {"batch", required_argument, nullptr, 'b'},
    {"blocks", required_argument, nullptr, 'n'},
    {"block-size", required_argument, nullptr, 's'},
    {"tests", required_argument, nullptr, 't'},
    {"no-checksums", no_argument, nullptr, 'c'},
    {"help", no_argument, nullptr, 'h'},
    {nullptr, 0, nullptr, 0}
  };

  int opt;
  int option_index = 0;
  while ((opt = getopt_long(argc, argv, "p:d:b:n:s:t:ch", long_options, &option_index)) != -1) {
    switch (opt) {
      case 'p': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          std::fprintf(stderr, "[wal_perf] invalid payload size '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        if (parsed > std::numeric_limits<std::uint16_t>::max()) {
          std::fprintf(stderr,
                       "[wal_perf] payload must be <= %u bytes\n",
                       std::numeric_limits<std::uint16_t>::max());
          return EXIT_FAILURE;
        }
        payload_bytes = static_cast<std::size_t>(parsed);
        break;
      }
      case 'd': {
        char* end = nullptr;
        errno = 0;
        const double seconds = std::strtod(optarg, &end);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0) || seconds <= 0.0;
        if (invalid) {
          std::fprintf(stderr, "[wal_perf] invalid duration '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        g_target_duration = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::duration<double>(seconds));
        break;
      }
      case 'b': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          std::fprintf(stderr, "[wal_perf] invalid batch size '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        g_batch_bytes = static_cast<std::size_t>(parsed);
        break;
      }
      case 'n': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          std::fprintf(stderr, "[wal_perf] invalid number of blocks '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        n_blocks = static_cast<std::size_t>(parsed);
        break;
      }
      case 's': {
        char* end = nullptr;
        errno = 0;
        const auto parsed = std::strtoull(optarg, &end, 10);
        const bool invalid = (end == optarg) || (*end != '\0') || (errno != 0);
        if (invalid || parsed == 0) {
          std::fprintf(stderr, "[wal_perf] invalid block size '%s'\n", optarg);
          return EXIT_FAILURE;
        }
        block_size = static_cast<std::size_t>(parsed);
        break;
      }
      case 't': {
        test_flags = parse_test_list(optarg);
        break;
      }
      case 'c': {
        g_disable_checksums = true;
        break;
      }
      case 'h': {
        show_help = true;
        break;
      }
      default: {
        print_usage(argv[0]);
        return EXIT_FAILURE;
      }
    }
  }

  if (optind < argc) {
    std::fprintf(stderr, "[wal_perf] unexpected argument: '%s'\n", argv[optind]);
    print_usage(argv[0]);
    return EXIT_FAILURE;
  }

  if (show_help) {
    print_usage(argv[0]);
    return EXIT_SUCCESS;
  }

  g_payload_bytes = payload_bytes;
  const auto payload_u16 = static_cast<std::uint16_t>(payload_bytes);

  // Create buffer configuration from command-line options
  wal::Circular_buffer::Config config(n_blocks, block_size, g_disable_checksums ? util::ChecksumAlgorithm::NONE : util::ChecksumAlgorithm::CRC32C);

  const double duration_s = static_cast<double>(g_target_duration.count()) / 1'000'000'000.0;
  std::fprintf(stderr,
               "[wal_perf] start (payload=%zu bytes, duration=%.3fs, batch=%zu bytes, blocks=%zu, block_size=%zu bytes, checksums=%s)\n",
               g_payload_bytes,
               duration_s,
               g_batch_bytes,
               n_blocks,
               block_size,
               g_disable_checksums ? "disabled" : "enabled");
  
  if (test_flags.memcpy_baseline) {
    test_memcpy_baseline();
  }
  if (test_flags.memcpy_ring) {
    test_memcpy_ring(config, g_payload_bytes, g_target_duration);
  }
  if (test_flags.wal_random) {
    test_wal_random_data(config, payload_u16, g_target_duration);
  }
  if (test_flags.wal_fixed) {
    test_wal_fixed_data(config, payload_u16, g_target_duration);
  }
  
  return EXIT_SUCCESS;
}
