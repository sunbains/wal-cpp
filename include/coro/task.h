
#pragma once

#include <coroutine>
#include <utility>
#include <exception>

/** A minimal fire-and-forget coroutine Task<void>
 * Self-destroys at final_suspend.
 */
struct [[nodiscard]] Task {
    struct Promise_type {
        [[nodiscard]] Task get_return_object() noexcept {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() const noexcept { return {}; }

        struct Final_awaiter {
            bool await_ready() const noexcept { return false; }
            void await_suspend(std::coroutine_handle<Promise_type> h) const noexcept {
                h.destroy();
            }
            void await_resume() const noexcept {}
        };

        Final_awaiter final_suspend() const noexcept { return {}; }
        void unhandled_exception() { std::terminate(); }
        void return_void() noexcept {}
    };

    using promise_type = Task::Promise_type;

    Task() = default;
    explicit Task(std::coroutine_handle<Promise_type> m_h) : m_h(m_h) {}
    Task(Task&& rhs) noexcept : m_h(std::exchange(rhs.m_h, nullptr)) {}

    Task& operator=(Task&& rhs) noexcept {
        if (this != &rhs) {
            if (m_h) {
                m_h.destroy();
            }
            m_h = std::exchange(rhs.m_h, nullptr);
        }
        return *this;
    }

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    ~Task() {
        /* If user forgot to start it, destroy to avoid leak. */
        if (m_h && !m_h.done()) {
            m_h.destroy();
        }
    }

    void start() noexcept {
        if (m_h) {
            /* Transfer ownership; promise will self-destroy */
            auto tmp = std::exchange(m_h, nullptr);
            tmp.resume();
        }
    }

    std::coroutine_handle<promise_type> m_h{nullptr};
};

/* Helper to start a Task immediately. */
inline void detach(Task t) noexcept { t.start(); }
