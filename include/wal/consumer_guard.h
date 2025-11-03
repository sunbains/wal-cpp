
#pragma once

#include <atomic>

namespace wal {
struct Consumer_guard {
    [[nodiscard]] bool try_lock() const noexcept {
        bool expected = false;
        return m_active.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
    }
    void unlock() const noexcept {
        m_active.store(false, std::memory_order_release);
    }
private:
    mutable std::atomic<bool> m_active{false};
};

} // namespace wal