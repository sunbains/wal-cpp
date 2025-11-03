#include <cassert>
#include <atomic>
#include <thread>
#include <vector>
#include <string>
#include <string_view>
#include <mutex>
#include <algorithm>
#include <cstdio>
#include <functional>

#include "util/logger.h"

using util::Log_level;

/* Writer that captures log messages for verification */
struct Capturing_writer {
  void operator()(std::string_view msg) {
    std::lock_guard<std::mutex> lock{m_mutex};
    m_messages.push_back(std::string{msg});
  }

  [[nodiscard]] std::vector<std::string> get_messages() const {
    std::lock_guard<std::mutex> lock{m_mutex};
    return m_messages;
  }

  void clear() {
    std::lock_guard<std::mutex> lock{m_mutex};
    m_messages.clear();
  }

  [[nodiscard]] std::size_t count() const {
    std::lock_guard<std::mutex> lock{m_mutex};
    return m_messages.size();
  }

  [[nodiscard]] bool contains(std::string_view substr) const {
    std::lock_guard<std::mutex> lock{m_mutex};
    return std::any_of(m_messages.begin(), m_messages.end(),
                      [substr](const std::string &msg) {
                        return msg.find(substr) != std::string::npos;
                      });
  }

 private:
  mutable std::mutex m_mutex;
  std::vector<std::string> m_messages;
};

static void test_all_levels() {
  std::fprintf(stderr, "[test_all_levels] start\n");
  Capturing_writer writer;
  auto logger = util::make_logger(std::ref(writer), Log_level::Trace);

  trace(logger, "trace message {}", 1);
  debug(logger, "debug message {}", 2);
  info(logger, "info message {}", 3);
  warn(logger, "warn message {}", 4);
  log_error(logger, "error message {}", 5);

  const auto messages = writer.get_messages();
  assert(messages.size() == 5);

  /* Verify each level appears in messages */
  assert(writer.contains("TRC"));
  assert(writer.contains("DBG"));
  assert(writer.contains("INF"));
  assert(writer.contains("WRN"));
  assert(writer.contains("ERRR"));

  /* Verify message content */
  assert(writer.contains("trace message 1"));
  assert(writer.contains("debug message 2"));
  assert(writer.contains("info message 3"));
  assert(writer.contains("warn message 4"));
  assert(writer.contains("error message 5"));

  std::fprintf(stderr, "[test_all_levels] done\n");
}

static void test_level_filtering() {
  std::fprintf(stderr, "[test_level_filtering] start\n");
  Capturing_writer writer;
  auto logger = util::make_logger(std::ref(writer), Log_level::Warn);

  /* These should be filtered out */
  trace(logger, "trace message");
  debug(logger, "debug message");
  info(logger, "info message");

  /* These should be logged */
  warn(logger, "warn message");
  log_error(logger, "error message");

  const auto messages = writer.get_messages();
  assert(messages.size() == 2);

  assert(writer.contains("WRN"));
  assert(writer.contains("ERRR"));
  assert(!writer.contains("TRC"));
  assert(!writer.contains("DBG"));
  assert(!writer.contains("INF"));

  /* Change min level at runtime */
  logger.set_min_level(Log_level::Err);
  writer.clear();

  warn(logger, "warn message 2"); /* Should be filtered */
  log_error(logger, "error message 2"); /* Should be logged */

  assert(writer.count() == 1);
  assert(writer.contains("ERRR"));
  assert(!writer.contains("WRN"));

  std::fprintf(stderr, "[test_level_filtering] done\n");
}

static void test_message_format() {
  std::fprintf(stderr, "[test_message_format] start\n");
  Capturing_writer writer;
  auto logger = util::make_logger(std::ref(writer), Log_level::Info);

  info(logger, "test message {}", 42);

  const auto messages = writer.get_messages();
  assert(messages.size() == 1);

  const auto &msg = messages[0];
  /* Verify format: [timestamp] [level] [file:line:function] message\n */
  /* Check for timestamp pattern (YYYY-MM-DD HH:MM:SS.mmm) */
  assert(msg.find("[") == 0);
  assert(msg.find("] [") != std::string::npos);
  assert(msg.find("INF") != std::string::npos);
  assert(msg.find("test_logger.cc") != std::string::npos);
  assert(msg.find("test message 42") != std::string::npos);
  assert(msg.back() == '\n');

  std::fprintf(stderr, "[test_message_format] done\n");
}

static void test_multi_threaded_writes() {
  std::fprintf(stderr, "[test_multi_threaded_writes] start\n");
  Capturing_writer writer;
  auto logger = util::make_logger(std::ref(writer), Log_level::Debug);

  constexpr int num_threads = 8;
  constexpr int messages_per_thread = 100;
  std::atomic<int> threads_ready{0};
  std::atomic<bool> start_flag{false};

  auto worker = [&](int thread_id) {
    threads_ready.fetch_add(1, std::memory_order_relaxed);
    while (!start_flag.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    for (int i = 0; i < messages_per_thread; ++i) {
      debug(logger, "thread {} message {}", thread_id, i);
      info(logger, "thread {} info {}", thread_id, i);
      warn(logger, "thread {} warn {}", thread_id, i);
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(worker, i);
  }

  /* Wait for all threads to be ready */
  while (threads_ready.load(std::memory_order_relaxed) < num_threads) {
    std::this_thread::yield();
  }

  /* Start all threads simultaneously */
  start_flag.store(true, std::memory_order_release);

  /* Wait for all threads to complete */
  for (auto &t : threads) {
    t.join();
  }

  /* Verify all messages were captured */
  const auto expected_messages = num_threads * messages_per_thread * 3; /* debug, info, warn */
  const auto actual_messages = writer.count();
  assert(actual_messages == expected_messages);

  /* Verify all thread IDs appear in messages */
  for (int i = 0; i < num_threads; ++i) {
    assert(writer.contains(std::format("thread {} message", i)));
    assert(writer.contains(std::format("thread {} info", i)));
    assert(writer.contains(std::format("thread {} warn", i)));
  }

  /* Verify all message numbers appear */
  for (int i = 0; i < messages_per_thread; ++i) {
    assert(writer.contains(std::format("message {}", i)));
  }

  std::fprintf(stderr, "[test_multi_threaded_writes] done (captured %zu messages)\n", actual_messages);
}

static void test_concurrent_level_changes() {
  std::fprintf(stderr, "[test_concurrent_level_changes] start\n");
  Capturing_writer writer;
  auto logger = util::make_logger(std::ref(writer), Log_level::Info);

  constexpr int num_threads = 4;
  std::atomic<bool> done{false};
  std::atomic<int> messages_logged{0};

  auto logger_thread = [&]() {
    while (!done.load(std::memory_order_acquire)) {
      info(logger, "concurrent message {}", messages_logged.fetch_add(1, std::memory_order_relaxed));
      std::this_thread::yield();
    }
  };

  auto level_changer = [&]() {
    Log_level levels[] = {Log_level::Trace, Log_level::Debug, Log_level::Info, 
                          Log_level::Warn, Log_level::Err};
    int level_idx = 0;
    while (!done.load(std::memory_order_acquire)) {
      logger.set_min_level(levels[level_idx]);
      level_idx = (level_idx + 1) % (sizeof(levels) / sizeof(levels[0]));
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  };

  std::vector<std::thread> threads;
  threads.emplace_back(level_changer);
  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back(logger_thread);
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  done.store(true, std::memory_order_release);

  for (auto &t : threads) {
    t.join();
  }

  /* Verify messages were logged without crashes */
  assert(writer.count() > 0);
  assert(messages_logged.load() > 0);

  std::fprintf(stderr, "[test_concurrent_level_changes] done (%d messages)\n", 
               messages_logged.load());
}

int main() {
  test_all_levels();
  test_level_filtering();
  test_message_format();
  test_multi_threaded_writes();
  test_concurrent_level_changes();
  std::fprintf(stderr, "All logger tests passed!\n");
  return 0;
}

