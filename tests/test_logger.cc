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
#include <print>

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
  Capturing_writer writer;
  auto g_logger = util::make_logger(std::ref(writer), Log_level::Trace);
  log_info("[test_all_levels] start");

  log_trace("trace message {}", 1);
  log_debug("debug message {}", 2);
  log_info("info message {}", 3);
  log_warn("warn message {}", 4);
  log_err("error message {}", 5);

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

  log_info( "[test_all_levels] done");
}

static void test_level_filtering() {
  Capturing_writer writer;
  auto g_logger = util::make_logger(std::ref(writer), Log_level::Warn);
  log_info("[test_level_filtering] start");

  /* These should be filtered out */
  log_trace("trace message");
  log_debug("debug message");
  log_info("info message");

  /* These should be logged */
  log_warn("warn message");
  log_err("error message");

  const auto messages = writer.get_messages();
  assert(messages.size() == 2);

  assert(writer.contains("WRN"));
  assert(writer.contains("ERRR"));
  assert(!writer.contains("TRC"));
  assert(!writer.contains("DBG"));
  assert(!writer.contains("INF"));

  /* Change min level at runtime */
  g_logger.set_min_level(Log_level::Err);
  writer.clear();

  log_warn("warn message 2"); /* Should be filtered */
  log_err("error message 2"); /* Should be logged */

  assert(writer.count() == 1);
  assert(writer.contains("ERRR"));
  assert(!writer.contains("WRN"));

  log_info( "[test_level_filtering] done");
}

static void test_message_format() {
  Capturing_writer writer;
  auto g_logger = util::make_logger(std::ref(writer), Log_level::Info);
  log_info("[test_message_format] start");

  log_info("test message {}", 42);

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

  log_info( "[test_message_format] done");
}

static void test_multi_threaded_writes() {
  Capturing_writer writer;
  auto g_logger = util::make_logger(std::ref(writer), Log_level::Debug);
  log_info("[test_multi_threaded_writes] start");

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
      log_debug("thread {} message {}", thread_id, i);
      log_info("thread {} info {}", thread_id, i);
      log_warn("thread {} warn {}", thread_id, i);
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

  log_info( "[test_multi_threaded_writes] done (captured {} messages)", actual_messages);
}

static void test_concurrent_level_changes() {
  Capturing_writer writer;
  auto g_logger = util::make_logger(std::ref(writer), Log_level::Info);
  log_info("[test_concurrent_level_changes] start");

  constexpr int num_threads = 4;
  std::atomic<bool> done{false};
  std::atomic<int> messages_logged{0};

  auto logger_thread = [&]() {
    while (!done.load(std::memory_order_acquire)) {
      log_info("concurrent message {}", messages_logged.fetch_add(1, std::memory_order_relaxed));
      std::this_thread::yield();
    }
  };

  auto level_changer = [&]() {
    Log_level levels[] = {Log_level::Trace, Log_level::Debug, Log_level::Info, 
                          Log_level::Warn, Log_level::Err};
    std::size_t level_idx = 0;
    while (!done.load(std::memory_order_acquire)) {
      g_logger.set_min_level(levels[level_idx]);
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

  log_info( "[test_concurrent_level_changes] done ({} messages)", 
               messages_logged.load());
}

int main() {
  test_all_levels();
  test_level_filtering();
  test_message_format();
  test_multi_threaded_writes();
  test_concurrent_level_changes();
  std::println(stderr, "All logger tests passed!");
  return 0;
}

