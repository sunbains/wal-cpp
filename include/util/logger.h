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

#include <atomic>
#include <chrono>
#include <concepts>
#include <filesystem>
#include <format>
#include <functional>
#include <iostream>
#include <source_location>
#include <string>
#include <string_view>
#include <syncstream>
#include <utility>

namespace util {

enum class Log_level : std::uint8_t {
  Trace = 0,
  Debug = 1,
  Info = 2,
  Warn = 3,
  Err = 4,
  Fatal = 5
};

[[nodiscard]] constexpr std::string_view to_string(Log_level level) noexcept {
  switch (level) {
    case Log_level::Trace: return "TRC";
    case Log_level::Debug: return "DBG";
    case Log_level::Info: return "INF";
    case Log_level::Warn: return "WRN";
    case Log_level::Err: return "ERRR";
    case Log_level::Fatal: return "FATAL";
    default: return "UNKNOWN";
  }
}

/* Extract filename and parent directory from full path */
[[nodiscard]] inline std::string get_short_filename(std::string_view path) noexcept {
  if (path.empty()) {
    return std::string{path};
  }
  
  try {
    std::filesystem::path p{path};
    const auto parent = p.parent_path();
    const auto filename = p.filename();
    
    /* If there's a parent directory, return "parent/filename", otherwise just filename */
    if (!parent.empty()) {
      const auto parent_name = parent.filename();
      if (!parent_name.empty()) {
        return (parent_name / filename).string();
      }
    }
    
    return filename.string();
  } catch (...) {
    /* Fallback to original path if filesystem operations fail */
    return std::string{path};
  }
}

template<typename Writer>
requires std::invocable<Writer, std::string_view>
struct Logger {
  explicit Logger(Writer writer, Log_level min_level = Log_level::Trace) noexcept
    : m_writer{std::move(writer)}, m_min_level{min_level} {}

  Logger(const Logger &) = delete;
  Logger &operator=(const Logger &) = delete;
  Logger(Logger &&) = delete;
  Logger &operator=(Logger &&) = delete;
  ~Logger() = default;

  void set_min_level(Log_level level) noexcept {
    m_min_level.store(level, std::memory_order_relaxed);
  }

  [[nodiscard]] Log_level get_min_level() const noexcept {
    return m_min_level.load(std::memory_order_relaxed);
  }

  template<typename... Args>
  void trace(const std::source_location &loc, std::string_view fmt, Args &&...args) noexcept {
    log_impl(Log_level::Trace, loc, fmt, std::forward<Args>(args)...);
  }

  template<typename... Args>
  void debug(const std::source_location &loc, std::string_view fmt, Args &&...args) noexcept {
    log_impl(Log_level::Debug, loc, fmt, std::forward<Args>(args)...);
  }

  template<typename... Args>
  void info(const std::source_location &loc, std::string_view fmt, Args &&...args) noexcept {
    log_impl(Log_level::Info, loc, fmt, std::forward<Args>(args)...);
  }

  template<typename... Args>
  void warn(const std::source_location &loc, std::string_view fmt, Args &&...args) noexcept {
    log_impl(Log_level::Warn, loc, fmt, std::forward<Args>(args)...);
  }

  template<typename... Args>
  void err(const std::source_location &loc, std::string_view fmt, Args &&...args) noexcept {
    log_impl(Log_level::Err, loc, fmt, std::forward<Args>(args)...);
  }

  template<typename... Args>
  void fatal(const std::source_location &loc, std::string_view fmt, Args &&...args) noexcept {
    log_impl(Log_level::Fatal, loc, fmt, std::forward<Args>(args)...);
    std::unreachable();
  }

 private:
  template<typename... Args>
  void log_impl(Log_level level, const std::source_location &loc, std::string_view fmt, Args &&...args) noexcept {
    if (level < m_min_level.load(std::memory_order_relaxed)) {
      return;
    }

    /* Format the log message with timestamp, level, location, and user message */
    const auto now = std::chrono::system_clock::now();
    const auto sec = std::chrono::duration_cast<std::chrono::seconds>( now.time_since_epoch());
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>( now.time_since_epoch()) % 1000;

    std::string msg;

    try {
      const auto user_msg = std::vformat(fmt, std::make_format_args(args...));
      msg = std::format("[{:%Y-%m-%d %H:%M:%S}.{:03d}] [{}] [{}:{}] {}\n",
                        std::chrono::system_clock::time_point{sec},
                        ms.count(),
                        to_string(level),
                        get_short_filename(loc.file_name()),
                        loc.line(),
                        // loc.function_name(),
                        user_msg);
    } catch (...) {
      /* Fallback if formatting fails */
      msg = std::format("[{}] [{}] Formatting error\n", to_string(level), get_short_filename(loc.file_name()));
    }

    /* Write to sink (writer should be thread-safe if needed) */
    m_writer(std::string_view{msg});
  }

  Writer m_writer;
  std::atomic<Log_level> m_min_level;
};

/* Thread-safe logger writer using std::osyncstream */
struct MT_logger_writer {
  /**
   * Constructor.
   *
   * @param stream The stream to write the log messages to.
   */
  explicit MT_logger_writer(std::ostream &stream) noexcept : m_stream{&stream} {}

  void operator()(std::string_view msg) noexcept {
    std::osyncstream{*m_stream} << msg;
  }

 private:
  std::ostream *m_stream;
};

/* Convenience factory function for creating a logger with a writer function */
template <typename W>
  requires std::invocable<W&, std::string_view>
constexpr auto make_logger(W writer, Log_level min_level = Log_level::Trace) 
    noexcept(noexcept(Logger<W>{std::move(writer), min_level}))
    -> Logger<W>
{
  return Logger<W>{std::move(writer), min_level};
}

} // namespace util

/* Macros to capture source_location at call site */
#define log_trace(fmt, ...) \
  (g_logger).trace(std::source_location::current(), fmt __VA_OPT__(,) __VA_ARGS__)

#define log_debug(fmt, ...) \
  (g_logger).debug(std::source_location::current(), fmt __VA_OPT__(,) __VA_ARGS__)

#define log_info(fmt, ...) \
  (g_logger).info(std::source_location::current(), fmt __VA_OPT__(,) __VA_ARGS__)

#define log_warn(fmt, ...) \
  (g_logger).warn(std::source_location::current(), fmt __VA_OPT__(,) __VA_ARGS__)

#define log_err(fmt, ...) \
  (g_logger).err(std::source_location::current(), fmt __VA_OPT__(,) __VA_ARGS__)

#define log_fatal(fmt, ...) \
  (g_logger).fatal(std::source_location::current(), fmt __VA_OPT__(,) __VA_ARGS__)
