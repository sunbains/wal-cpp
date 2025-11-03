#pragma once

#include <atomic>
#include <chrono>
#include <array>
#include <limits>
#include <map>
#include <string>
#include <string_view>
#include <vector>
#include <print>

namespace util {

/**
 * Metric types for write_to_store operations.
 * These are known at compile time for efficient array indexing.
 */
enum class MetricType : std::size_t {
  // Timings
  WriteToStoreTotal = 0,
  WriteToStorePrepareBatch,
  WriteToStoreIoCallback,
  WriteToStoreClear,
  ProducerLatency,  /* End-to-end latency from producer send to flush */
  FdatasyncTiming,  /* fdatasync() operation timing */
  FsyncTiming,      /* fsync() operation timing */
  
  // Counters
  WriteToStoreCalls,
  WriteToStoreBlocksWritten,
  WriteToStoreBatches,
  WriteToStoreBytesWritten,
  FdatasyncCount,   /* Number of fdatasync() calls */
  FsyncCount,       /* Number of fsync() calls */
  ConsumerSyncCount, /* Number of sync operations performed synchronously from consumer thread */
  
  // Total count - must be last
  Count
};

/**
 * Observability framework for collecting various statistics.
 * 
 * This framework uses static arrays indexed by MetricType enum for
 * high-performance, lock-free metric collection.
 */
struct Metrics {
  using Counter = std::atomic<std::size_t>;
  using Timer = std::chrono::nanoseconds;
  static constexpr std::size_t kMetricCount = static_cast<std::size_t>(MetricType::Count);

  Metrics() = default;
  Metrics(Metrics&&) = default;
  Metrics(const Metrics&) = delete;
  Metrics& operator=(Metrics&&) = default;
  Metrics& operator=(const Metrics&) = delete;
  ~Metrics() = default;

  /**
   * Increment a counter by the specified amount.
   * @param type Counter type (must be a counter metric)
   * @param value Amount to increment (default: 1)
   */
  void inc(MetricType type, std::size_t value = 1) noexcept {
    const auto idx = static_cast<std::size_t>(type);
    if (idx < kMetricCount) {
      m_counters[idx].fetch_add(value, std::memory_order_relaxed);
    }
  }

  /**
   * Get the current value of a counter.
   * @param type Counter type
   * @return Current counter value
   */
  [[nodiscard]] std::size_t get_counter(MetricType type) const noexcept {
    const auto idx = static_cast<std::size_t>(type);
    if (idx < kMetricCount) {
      return m_counters[idx].load(std::memory_order_acquire);
    }
    return std::numeric_limits<std::size_t>::max();
  }

  /**
   * Add a timing measurement.
   * @param type Timer type (must be a timing metric)
   * @param duration Duration to add
   */
  void add_timing(MetricType type, Timer duration) noexcept {
    const auto idx = static_cast<std::size_t>(type);
    if (idx < kMetricCount) {
      m_timings[idx].push_back(duration);
    }
  }

  /**
   * Add an IO request size measurement.
   * @param size_bytes Size of the IO request in bytes
   */
  void add_io_size(std::size_t size_bytes) noexcept {
    m_io_sizes.push_back(size_bytes);
  }

  /**
   * Get the timings vector for a specific metric type.
   * @param type Metric type
   * @return Const reference to the timings vector
   */
  [[nodiscard]] const std::vector<Timer>& get_timings(MetricType type) const noexcept {
    const auto idx = static_cast<std::size_t>(type);
    if (idx < kMetricCount) {
      return m_timings[idx];
    }
    static const std::vector<Timer> empty{};
    return empty;
  }

  /**
   * Batch add multiple timing measurements efficiently.
   * More efficient than calling add_timing multiple times as it reserves space once.
   * @param type Timer type
   * @param timings Vector of timing measurements to add
   */
  void add_timings_batch(MetricType type, const std::vector<Timer>& timings) noexcept {
    const auto idx = static_cast<std::size_t>(type);
    if (idx < kMetricCount && !timings.empty()) {
      auto& target = m_timings[idx];
      /* Reserve space if needed to avoid multiple reallocations */
      const auto current_size = target.size();
      const auto new_size = current_size + timings.size();
      if (new_size > target.capacity()) {
        /* Reserve with some headroom to reduce future reallocations */
        target.reserve(new_size + (new_size / 4));  /* 25% headroom */
      }
      /* Use insert at end - efficient for large vectors */
      /* Use move semantics if possible to avoid copying */
      target.insert(target.end(), timings.begin(), timings.end());
    }
  }
  
  /**
   * Add timings batch using move semantics for better performance.
   * Takes ownership of the vector to avoid copying.
   */
  void add_timings_batch_move(MetricType type, std::vector<Timer>&& timings) noexcept {
    const auto idx = static_cast<std::size_t>(type);
    if (idx < kMetricCount && !timings.empty()) {
      auto& target = m_timings[idx];
      const auto current_size = target.size();
      const auto new_size = current_size + timings.size();
      if (new_size > target.capacity()) {
        target.reserve(new_size + (new_size / 4));  /* 25% headroom */
      }
      /* Move the entire vector contents - more efficient than copying */
      target.insert(target.end(), std::make_move_iterator(timings.begin()), std::make_move_iterator(timings.end()));
    }
  }

  /**
   * Generate a histogram from timing data.
   * @param timings Vector of timing measurements
   * @param num_buckets Number of histogram buckets
   * @return Map of bucket ranges to counts
   */
  [[nodiscard]] static std::map<std::string, std::size_t> generate_timing_histogram(
      const std::vector<Timer>& timings, std::size_t num_buckets = 20) noexcept {
    std::map<std::string, std::size_t> histogram;
    if (timings.empty()) {
      return histogram;
    }

    // Find min and max
    std::size_t min_ns = static_cast<std::size_t>(timings[0].count());
    std::size_t max_ns = static_cast<std::size_t>(timings[0].count());
    for (const auto& t : timings) {
      const auto ns = static_cast<std::size_t>(t.count());
      min_ns = std::min(min_ns, ns);
      max_ns = std::max(max_ns, ns);
    }

    if (min_ns == max_ns) {
      histogram[format_ns_range(min_ns, max_ns)] = timings.size();
      return histogram;
    }

    // Create buckets
    const auto bucket_size = (max_ns - min_ns) / num_buckets + 1;
    std::vector<std::size_t> buckets(num_buckets, 0);

    for (const auto& t : timings) {
      const auto ns = static_cast<std::size_t>(t.count());
      const auto bucket_idx = std::min((ns - min_ns) / bucket_size, num_buckets - 1);
      buckets[bucket_idx]++;
    }

    // Convert to map with formatted ranges
    for (std::size_t i = 0; i < num_buckets; ++i) {
      if (buckets[i] > 0) {
        const auto bucket_min = min_ns + i * bucket_size;
        const auto bucket_max = (i == num_buckets - 1) ? max_ns : min_ns + (i + 1) * bucket_size - 1;
        histogram[format_ns_range(bucket_min, bucket_max)] = buckets[i];
      }
    }

    return histogram;
  }

  /**
   * Generate a histogram from IO size data.
   * @param sizes Vector of IO sizes in bytes
   * @param num_buckets Number of histogram buckets
   * @return Map of bucket ranges to counts
   */
  [[nodiscard]] static std::map<std::string, std::size_t> generate_size_histogram(
      const std::vector<std::size_t>& sizes, std::size_t num_buckets = 20) noexcept {
    std::map<std::string, std::size_t> histogram;
    if (sizes.empty()) {
      return histogram;
    }

    // Find min and max
    std::size_t min_size = sizes[0];
    std::size_t max_size = sizes[0];
    for (const auto& s : sizes) {
      min_size = std::min(min_size, s);
      max_size = std::max(max_size, s);
    }

    if (min_size == max_size) {
      histogram[format_size_range(min_size, max_size)] = sizes.size();
      return histogram;
    }

    // Create buckets
    const auto bucket_size = (max_size - min_size) / num_buckets + 1;
    std::vector<std::size_t> buckets(num_buckets, 0);

    for (const auto& s : sizes) {
      const auto bucket_idx = std::min((s - min_size) / bucket_size, num_buckets - 1);
      buckets[bucket_idx]++;
    }

    // Convert to map with formatted ranges
    for (std::size_t i = 0; i < num_buckets; ++i) {
      if (buckets[i] > 0) {
        const auto bucket_min = min_size + i * bucket_size;
        const auto bucket_max = (i == num_buckets - 1) ? max_size : min_size + (i + 1) * bucket_size - 1;
        histogram[format_size_range(bucket_min, bucket_max)] = buckets[i];
      }
    }

    return histogram;
  }

private:
  /**
   * Format a nanosecond range for histogram display.
   */
  [[nodiscard]] static std::string format_ns_range(std::size_t min_ns, std::size_t max_ns) noexcept {
    auto format_ns = [](std::size_t ns) -> std::string {
      if (ns >= 1'000'000'000) {
        return std::format("{:.2f}s", static_cast<double>(ns) / 1'000'000'000.0);
      } else if (ns >= 1'000'000) {
        return std::format("{:.2f}ms", static_cast<double>(ns) / 1'000'000.0);
      } else if (ns >= 1'000) {
        return std::format("{:.2f}us", static_cast<double>(ns) / 1'000.0);
      } else {
        return std::format("{}ns", ns);
      }
    };
    return std::format("[{}, {}]", format_ns(min_ns), format_ns(max_ns));
  }

  /**
   * Format a size range for histogram display.
   */
  [[nodiscard]] static std::string format_size_range(std::size_t min_bytes, std::size_t max_bytes) noexcept {
    auto format_bytes = [](std::size_t bytes) -> std::string {
      if (bytes >= 1024 * 1024 * 1024) {
        return std::format("{:.2f}GiB", static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0));
      } else if (bytes >= 1024 * 1024) {
        return std::format("{:.2f}MiB", static_cast<double>(bytes) / (1024.0 * 1024.0));
      } else if (bytes >= 1024) {
        return std::format("{:.2f}KiB", static_cast<double>(bytes) / 1024.0);
      } else {
        return std::format("{}B", bytes);
      }
    };
    return std::format("[{}, {}]", format_bytes(min_bytes), format_bytes(max_bytes));
  }

public:

  /**
   * Record a single timing measurement using a scoped timer.
   * Usage: metrics.record_timing(MetricType::WriteToStoreTotal, [&]() { // code });
   */
  template<typename Func>
  auto record_timing(MetricType type, Func&& func) noexcept(noexcept(std::forward<Func>(func)())) {
    auto start = std::chrono::steady_clock::now();
    auto result = std::forward<Func>(func)();
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<Timer>(end - start);
    add_timing(type, duration);
    return result;
  }

  /**
   * Get timing statistics for a timer.
   * @param type Timer type
   * @return Struct with min, max, sum, count, avg (all in nanoseconds)
   */
  struct TimingStats {
    std::size_t m_count{0};
    std::size_t m_min_ns{0};
    std::size_t m_max_ns{0};
    std::size_t m_sum_ns{0};
    std::size_t m_avg_ns{0};
  };

  [[nodiscard]] TimingStats get_timing_stats(MetricType type) const noexcept {
    const auto idx = static_cast<std::size_t>(type);
    TimingStats stats{};
    
    if (idx >= kMetricCount || m_timings[idx].empty()) {
      return stats;
    }

    const auto& timings = m_timings[idx];
    stats.m_count = timings.size();
    
    if (stats.m_count == 0) {
      return stats;
    }

    stats.m_min_ns = static_cast<std::size_t>(timings[0].count());
    stats.m_max_ns = static_cast<std::size_t>(timings[0].count());
    stats.m_sum_ns = 0;

    for (const auto& t : timings) {
      const auto ns = static_cast<std::size_t>(t.count());
      stats.m_min_ns = std::min(stats.m_min_ns, ns);
      stats.m_max_ns = std::max(stats.m_max_ns, ns);
      stats.m_sum_ns += ns;
    }

    stats.m_avg_ns = stats.m_sum_ns / stats.m_count;
    return stats;
  }

  /**
   * Consolidate metrics from another Metrics instance.
   * This merges counters and timings from the other instance.
   * @note Not thread-safe: should be called when no concurrent updates are happening.
   */
  void consolidate(const Metrics& other) noexcept {
    // Merge counters
    for (std::size_t i = 0; i < kMetricCount; ++i) {
      auto value = other.m_counters[i].load(std::memory_order_acquire);
      if (value > 0) {
        m_counters[i].fetch_add(value, std::memory_order_relaxed);
      }
    }

    // Merge timings
    for (std::size_t i = 0; i < kMetricCount; ++i) {
      m_timings[i].insert(m_timings[i].end(), other.m_timings[i].begin(), other.m_timings[i].end());
    }

    // Merge IO sizes
    m_io_sizes.insert(m_io_sizes.end(), other.m_io_sizes.begin(), other.m_io_sizes.end());
  }

  /**
   * Reset all metrics.
   * @note Not thread-safe: should be called when no concurrent updates are happening.
   */
  void reset() noexcept {
    for (std::size_t i = 0; i < kMetricCount; ++i) {
      m_counters[i].store(0, std::memory_order_relaxed);
      m_timings[i].clear();
    }
    m_io_sizes.clear();
  }

  /**
   * Get metric name as string (for printing).
   */
  [[nodiscard]] static constexpr std::string_view get_metric_name(MetricType type) noexcept {
    switch (type) {
      case MetricType::WriteToStoreTotal: return "write_to_store.total";
      case MetricType::WriteToStorePrepareBatch: return "write_to_store.prepare_batch";
      case MetricType::WriteToStoreIoCallback: return "write_to_store.io_callback";
      case MetricType::WriteToStoreClear: return "write_to_store.clear";
      case MetricType::ProducerLatency: return "producer_latency";
      case MetricType::FdatasyncTiming: return "fdatasync.timing";
      case MetricType::FsyncTiming: return "fsync.timing";
      case MetricType::WriteToStoreCalls: return "write_to_store.calls";
      case MetricType::WriteToStoreBlocksWritten: return "write_to_store.blocks_written";
      case MetricType::WriteToStoreBatches: return "write_to_store.batches";
      case MetricType::WriteToStoreBytesWritten: return "write_to_store.bytes_written";
      case MetricType::FdatasyncCount: return "fdatasync.count";
      case MetricType::FsyncCount: return "fsync.count";
      case MetricType::ConsumerSyncCount: return "consumer_sync.count";
      default: return "unknown";
    }
  }

  /**
   * Print all metrics in a formatted way.
   * @note Not thread-safe: should be called when no concurrent updates are happening.
   */
  void print(std::string_view prefix = "") const noexcept {
    if (!prefix.empty()) {
      std::println("{}", prefix);
    }

    bool has_counters = false;
    for (std::size_t i = static_cast<std::size_t>(MetricType::WriteToStoreCalls); i < kMetricCount; ++i) {
      auto value = m_counters[i].load(std::memory_order_acquire);
      if (value > 0) {
        if (!has_counters) {
          std::println("Counters:");
          has_counters = true;
        }
        std::println("  {}: {}", get_metric_name(static_cast<MetricType>(i)), value);
      }
    }

    bool has_timings = false;
    for (std::size_t i = 0; i < static_cast<std::size_t>(MetricType::WriteToStoreCalls); ++i) {
      auto stats = get_timing_stats(static_cast<MetricType>(i));
      if (stats.m_count > 0) {
        if (!has_timings) {
          std::println("Timings:");
          has_timings = true;
        }

        /* Format with appropriate units */
        auto format_ns = [](std::size_t ns) -> std::string {
          if (ns >= 1'000'000'000) {
            return std::format("{:.3f}s", static_cast<double>(ns) / 1'000'000'000.0);
          } else if (ns >= 1'000'000) {
            return std::format("{:.3f}ms", static_cast<double>(ns) / 1'000'000.0);
          } else if (ns >= 1'000) {
            return std::format("{:.3f}us", static_cast<double>(ns) / 1'000.0);
          } else {
            return std::format("{}ns", ns);
          }
        };

        std::println("  {}:", get_metric_name(static_cast<MetricType>(i)));
        std::println("    count: {}", stats.m_count);
        std::println("    min: {}", format_ns(stats.m_min_ns));
        std::println("    max: {}", format_ns(stats.m_max_ns));
        std::println("    avg: {}", format_ns(stats.m_avg_ns));
        std::println("    total: {}", format_ns(stats.m_sum_ns));

        // Print histogram for IO callback timings and sync operations
        const auto current_type = static_cast<MetricType>(i);
        if ((current_type == MetricType::WriteToStoreIoCallback || 
             current_type == MetricType::FdatasyncTiming || 
             current_type == MetricType::FsyncTiming) && stats.m_count > 0) {
          auto histogram = Metrics::generate_timing_histogram(m_timings[i]);
          if (!histogram.empty()) {
            std::println("    histogram:");
            for (const auto& [range, count] : histogram) {
              double percentage = (100.0 * static_cast<double>(count)) / static_cast<double>(stats.m_count);
              std::println("      {}: {} ({:.2f}%)", range, count, percentage);
            }
          }
        }
      }
    }

    // Print IO size histogram
    if (!m_io_sizes.empty()) {
      std::println("IO Request Size Histogram:");
      auto histogram = Metrics::generate_size_histogram(m_io_sizes);
      const auto total_io_requests = m_io_sizes.size();
      for (const auto& [range, count] : histogram) {
        double percentage = (100.0 * static_cast<double>(count)) / static_cast<double>(total_io_requests);
        std::println("  {}: {} ({:.2f}%)", range, count, percentage);
      }
    }
  }

  /* Preallocated member arrays indexed by MetricType enum (not static storage) */
  std::array<Counter, kMetricCount> m_counters{};
  std::array<std::vector<Timer>, kMetricCount> m_timings{};
  std::vector<std::size_t> m_io_sizes{};  /* IO request sizes in bytes */
};

/**
 * RAII timer for measuring scoped operations.
 */
struct ScopedTimer {
  ScopedTimer(Metrics& metrics, MetricType type) noexcept
    : m_metrics(metrics), m_type(type), m_start(std::chrono::steady_clock::now()) {}

  ~ScopedTimer() noexcept {
    auto end = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<Metrics::Timer>(end - m_start);
    m_metrics.add_timing(m_type, duration);
  }

  ScopedTimer(const ScopedTimer&) = delete;
  ScopedTimer& operator=(const ScopedTimer&) = delete;
  ScopedTimer(ScopedTimer&&) = delete;
  ScopedTimer& operator=(ScopedTimer&&) = delete;

  Metrics& m_metrics;
  MetricType m_type;
  std::chrono::steady_clock::time_point m_start;
};

} // namespace util
