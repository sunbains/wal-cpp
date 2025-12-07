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

#include <coroutine>
#include <utility>
#include <exception>
#include <new>
#include <cstdlib>
#include <optional>
#include <cassert>

#include "util/memory_pool.h"

/** A coroutine Task<T> that can return values and be co_awaited.
 * Uses custom allocator for coroutine frames to reduce allocation overhead.
 *
 * Task<void> is fire-and-forget and self-destroys at final_suspend.
 * Task<T> can be co_awaited to retrieve the result value.
 */
template<typename T = void>
struct [[nodiscard]] Task;

/** Task<T> - awaitable coroutine that returns a value */
template<typename T>
struct [[nodiscard]] Task {
    struct Promise_type {
        std::optional<T> m_value;
        std::exception_ptr m_exception;
        std::coroutine_handle<> m_continuation;

        [[nodiscard]] Task get_return_object() noexcept {
            return Task{std::coroutine_handle<Promise_type>::from_promise(*this)};
        }

        std::suspend_always initial_suspend() const noexcept { return {}; }

        struct Final_awaiter {
            bool await_ready() const noexcept { return false; }

            template<typename Promise>
            std::coroutine_handle<> await_suspend(std::coroutine_handle<Promise> h) const noexcept {
                auto& promise = h.promise();
                if (promise.m_continuation) {
                    return promise.m_continuation;
                }
                return std::noop_coroutine();
            }

            void await_resume() const noexcept {}
        };

        Final_awaiter final_suspend() const noexcept { return {}; }

        void unhandled_exception() noexcept {
            m_exception = std::current_exception();
        }

        void return_value(T value) noexcept {
            m_value = std::move(value);
        }

        /* Custom allocator for coroutine frames */
        static void* operator new(std::size_t size) {
            auto& pool = util::get_coroutine_pool();
            void* ptr = nullptr;
            if (size <= 512) {
                ptr = pool.allocate();
            } else {
                /* Fall back to malloc for oversized frames */
                ptr = std::malloc(size);
            }
            /* C++20 standard requires operator new to throw std::bad_alloc on failure */
            if (ptr == nullptr) {
                throw std::bad_alloc();
            }
            return ptr;
        }

        static void operator delete(void* ptr, std::size_t size) noexcept {
            if (ptr == nullptr) {
                return;
            }
            auto& pool = util::get_coroutine_pool();
            if (size <= 512) {
                pool.deallocate(ptr);
            } else {
                std::free(ptr);
            }
        }
    };

    using promise_type = Promise_type;

    Task() = default;
    explicit Task(std::coroutine_handle<promise_type> h) : m_h(h) {}
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
        if (m_h) {
            m_h.destroy();
        }
    }

    /** Await support - makes Task<T> co_awaitable */
    bool await_ready() const noexcept {
        return false;
    }

    std::coroutine_handle<> await_suspend(std::coroutine_handle<> continuation) noexcept {
        m_h.promise().m_continuation = continuation;
        return m_h;
    }

    T await_resume() {
        if (m_h.promise().m_exception) {
            std::rethrow_exception(m_h.promise().m_exception);
        }
        assert(m_h.promise().m_value.has_value());
        return std::move(*m_h.promise().m_value);
    }

    /** Get result synchronously (blocks until completion) */
    T get() {
        if (!m_h) {
            throw std::runtime_error("Task is empty");
        }
        if (!m_h.done()) {
            m_h.resume();
        }
        while (!m_h.done()) {
            m_h.resume();
        }
        return await_resume();
    }

    std::coroutine_handle<promise_type> m_h{nullptr};
};

/** Task<void> - fire-and-forget coroutine */
template<>
struct [[nodiscard]] Task<void> {
    struct Promise_type {
        std::exception_ptr m_exception;

        [[nodiscard]] Task get_return_object() noexcept {
            return Task{std::coroutine_handle<Promise_type>::from_promise(*this)};
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

        /* Custom allocator for coroutine frames */
        static void* operator new(std::size_t size) {
            auto& pool = util::get_coroutine_pool();
            void* ptr = nullptr;
            if (size <= 512) {
                ptr = pool.allocate();
            } else {
                /* Fall back to malloc for oversized frames */
                ptr = std::malloc(size);
            }
            /* C++20 standard requires operator new to throw std::bad_alloc on failure */
            if (ptr == nullptr) {
                throw std::bad_alloc();
            }
            return ptr;
        }

        static void operator delete(void* ptr, std::size_t size) noexcept {
            if (ptr == nullptr) {
                return;
            }
            auto& pool = util::get_coroutine_pool();
            if (size <= 512) {
                pool.deallocate(ptr);
            } else {
                std::free(ptr);
            }
        }
    };

    using promise_type = Promise_type;

    Task() = default;
    explicit Task(std::coroutine_handle<promise_type> m_h) : m_h(m_h) {}
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
            /* Resume on current thread - coroutine will switch to pool thread via co_await schedule() */
            tmp.resume();
        }
    }
    
    template<typename ThreadPool>
    void start_on_pool(ThreadPool& pool) noexcept {
        start_on_pool_impl(pool);
    }
    
private:
    template<typename ThreadPool>
    void start_on_pool_impl(ThreadPool& pool) noexcept {
        if (m_h) {
            /* Transfer ownership; promise will self-destroy */
            auto tmp = std::exchange(m_h, nullptr);
            /* Post initial resume to thread pool instead of resuming on current thread */
            pool.post([tmp]() mutable { tmp.resume(); });
        }
    }

public:

    std::coroutine_handle<promise_type> m_h{nullptr};
};

/* Helper to start a Task<void> immediately. */
inline void detach(Task<void> t) noexcept { t.start(); }

/* Helper to start a Task<void> on a thread pool. */
template<typename ThreadPool>
inline void detach_on_pool(Task<void> t, ThreadPool& pool) noexcept { t.start_on_pool(pool); }
