
#pragma once

#include <chrono>
#include <thread>
#include <stop_token>
#include <utility>
#include <optional>
#include <thread>

#include "task.h"
#include "util/thread_pool.h"
#include "consumer_guard.h"

namespace wal {

template<typename T>
using Event_queue = util::Bounded_channel<T>;

template<typename T>
[[nodiscard]] Task consumer(util::Thread_pool& pool, Event_queue<T>& queue, Consumer_guard& guard) noexcept {
    /* There should only be one consumer at a time */
    if (!guard.try_lock()) {
        co_return;
    }

    /* Switch to the pool to do the work */
    co_await pool.schedule();

    /* Process everything currently enqueued */
    for (;;) {
        auto item = queue.try_pop();
        if (!item) {
            break;
        }
        process(*item);
    }
    guard.unlock();

    co_return;
}

template<typename T>
struct Trigger_manager {
    Trigger_manager(util::Thread_pool& p, Event_queue<T>& q, Consumer_guard& g, std::size_t threshold)
        : m_pool(p), m_queue(q), m_guard(g), m_threshold(threshold) {}

    void force_trigger() noexcept {
        spawn_consumer();
    }

    void check_and_trigger() noexcept {
        if (m_queue.size() >= m_threshold) {
            force_trigger();
        }
    }

    void start_timer(std::chrono::milliseconds interval) noexcept {
        m_timer = std::jthread([this, interval](std::stop_token st) {
            while (!st.stop_requested()) {
                std::this_thread::sleep_for(interval);
                force_trigger();
            }
        });
    }

    ~Trigger_manager() = default;

private:
    void spawn_consumer() noexcept {
        detach(consumer<T>(m_pool, m_queue, m_guard));
    }

    util::Thread_pool& m_pool;
    std::jthread m_timer;
    Event_queue<T>& m_queue;
    Consumer_guard& m_guard;
    std::size_t m_threshold;
};

template<typename T>
[[nodiscard]] Task producer(util::Thread_pool& pool, Event_queue<T>& queue, Consumer_guard&, Trigger_manager<T>& trigger, T value) noexcept {
    /* Switch to pool to publish */
    co_await pool.schedule();

    queue.push(std::move(value));
    trigger.check_and_trigger();

    co_return;
}

} // namespace wal