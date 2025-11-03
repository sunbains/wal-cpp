
#pragma once

#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <atomic>
#include <coroutine>
#include <utility>

namespace util {

struct Thread_pool {
    explicit Thread_pool(std::size_t nthreads = std::thread::hardware_concurrency())
        : m_stop(false) {
        if (nthreads == 0) {
            nthreads = 1;
        }
        initialize_queue();
        for (std::size_t i = 0; i < nthreads; ++i) {
            m_workers.emplace_back([this] { this->worker_loop(); });
        }
    }

    ~Thread_pool() noexcept {
        {
            std::lock_guard lk(m_mutex);
            m_stop = true;
        }
        m_cv.notify_all();
        for (auto& t : m_workers) {
            if (t.joinable()) {
                t.join();
            }
        }
    }

    template<typename Fn>
    void post(Fn&& fn) noexcept {
        using Ctx = std::decay_t<Fn>;
        auto* ctx = new Ctx(std::forward<Fn>(fn));
        Task task;
        task.fn = [](void* p) noexcept {
            auto* ctx_ptr = static_cast<Ctx*>(p);
            (*ctx_ptr)();
            delete ctx_ptr;
        };
        task.ctx = ctx;

        {
            std::lock_guard lk(m_mutex);
            push_task_locked(task);
        }
        m_cv.notify_one();
    }

    /* Awaitable that schedules the awaiting coroutine to resume on the pool. */
    struct Schedule_awaitable {
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) const {
            m_pool.post([h]() { h.resume(); });
        }
        void await_resume() const noexcept {}

        Thread_pool& m_pool;
    };

    [[nodiscard]] Schedule_awaitable schedule() { return Schedule_awaitable(*this); }

private:
    struct Task {
        void (*fn)(void*) noexcept;
        void* ctx;
    };

    void initialize_queue() {
        const std::size_t capacity = 1024;
        m_tasks.resize(capacity);
        m_mask = capacity - 1;
        m_head = 0;
        m_tail = 0;
    }

    void push_task_locked(Task task) {
        if (((m_tail + 1) & m_mask) == m_head) {
            grow_locked();
        }
        m_tasks[m_tail] = task;
        m_tail = (m_tail + 1) & m_mask;
    }

    bool pop_task_locked(Task& out) noexcept {
        if (m_head == m_tail) {
            return false;
        }
        out = m_tasks[m_head];
        m_head = (m_head + 1) & m_mask;
        return true;
    }

    void grow_locked() {
        std::size_t new_capacity = m_tasks.empty() ? 1024 : m_tasks.size() * 2;
        std::vector<Task> new_tasks(new_capacity);
        std::size_t index = 0;
        while (m_head != m_tail) {
            new_tasks[index++] = m_tasks[m_head];
            m_head = (m_head + 1) & m_mask;
        }
        m_tasks.swap(new_tasks);
        m_head = 0;
        m_tail = index;
        m_mask = new_capacity - 1;
    }

    void worker_loop() noexcept {
        for (;;) {
            Task task;
            {
                std::unique_lock lk(m_mutex);
                m_cv.wait(lk, [this]{ return m_stop || m_head != m_tail; });

                if (m_stop && m_head == m_tail) {
                    return;
                }
                pop_task_locked(task);
            }
            task.fn(task.ctx);
        }
    }

    alignas(64) bool m_stop;
    alignas(64) mutable std::mutex m_mutex;
    alignas(64) std::condition_variable m_cv;

    std::size_t m_head{0};
    std::size_t m_tail{0};
    std::size_t m_mask{0};

    std::vector<Task> m_tasks;
    std::vector<std::thread> m_workers;
};

} // namespace util