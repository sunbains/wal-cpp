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
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>
#include <mutex>

namespace util {

/**
 * Dynamic slab + arena allocator for fixed-size allocations.
 * Thread-safe using atomics for fast paths (allocate/deallocate).
 * Grows on demand when exhausted, shrinks via explicit API.
 *
 * Uses thread-local caching to minimize atomic contention.
 * Each thread maintains a small cache of blocks that can be
 * allocated/freed without touching shared structures.
 *
 * @tparam BlockSize Size of each allocation block
 * @tparam ArenaSize Number of blocks per arena (default 1024)
 * @tparam CacheSize Number of blocks in thread-local cache (default 64)
 */
template <std::size_t BlockSize, std::size_t ArenaSize = 1024, std::size_t CacheSize = 64>
struct Memory_pool {
  Memory_pool() {
    /* Allocate initial arena */
    grow();
  }

  ~Memory_pool() {
    /* Free all arenas */
    auto current = m_arenas.load(std::memory_order_acquire);

    while (current != nullptr) {
      auto next = current->m_next;
      delete current;
      current = next;
    }
  }

  Memory_pool(Memory_pool&&) = delete;
  Memory_pool(const Memory_pool&) = delete;
  Memory_pool& operator=(Memory_pool&&) = delete;
  Memory_pool& operator=(const Memory_pool&) = delete;

  /**
   * Allocate a block from the pool.
   * Fast path: allocate from thread-local cache (no atomics).
   * Slow path: refill cache from shared pool if empty.
   * @return Pointer to allocated block, or nullptr on failure
   */
  [[nodiscard]] void* allocate() noexcept {
    auto& cache = get_thread_cache();

    /* Fast path: allocate from thread-local cache */
    if (cache.m_count > 0) [[likely]] {
      --cache.m_count;
      auto* block = cache.m_blocks[cache.m_count];
      block->m_arena->m_free_count.fetch_sub(1, std::memory_order_relaxed);
      return &block->m_data;
    }

    /* Slow path: refill cache from shared pool */
    return allocate_slow(cache);
  }

  /**
   * Deallocate a block back to the pool.
   * Fast path: return to thread-local cache (no atomics).
   * Slow path: flush to shared pool if cache is full.
   * Cross-thread deallocation: return directly to shared pool.
   * @param ptr Pointer to block to deallocate
   */
  void deallocate(void* ptr) noexcept {
    if (ptr == nullptr) {
      return;
    }

    /* Calculate Block pointer from data pointer using offsetof */
    constexpr auto data_offset = offsetof(Block, m_data);
    auto* block = reinterpret_cast<Block*>(static_cast<std::byte*>(ptr) - data_offset);

    /* Fast path: return to thread-local cache */
    auto& cache = get_thread_cache();

    if (cache.m_count < CacheSize) [[likely]] {
      cache.m_blocks[cache.m_count] = block;
      ++cache.m_count;
      block->m_arena->m_free_count.fetch_add(1, std::memory_order_relaxed);
      return;
    }

    /* Slow path: cache is full, flush half and then add this block */
    deallocate_slow(cache, block);
  }

  /**
   * Allocate a new arena and add its blocks to the free list.
   * Thread-safe - multiple threads can call concurrently.
   * @return true on success, false on allocation failure
   */
  bool grow() noexcept {
    /* Serialize arena allocation with mutex */
    std::lock_guard<std::mutex> lock(m_arena_mutex);

    try {
      /* Allocate new arena and its block array */
      auto arena = new Arena();

      arena->m_blocks = new Block[ArenaSize]();  /* Value-initialize blocks */
      arena->m_free_count.store(ArenaSize, std::memory_order_relaxed);

      /* Initialize blocks and add to free list */
      for (std::size_t i = 0; i < ArenaSize; ++i) {
        arena->m_blocks[i].m_arena = arena;
        arena->m_blocks[i].m_next_free.store(nullptr, std::memory_order_relaxed);
        push_free_list(&arena->m_blocks[i]);
      }

      constexpr auto Release_order = std::memory_order_release;
      constexpr auto Acquire_order = std::memory_order_acquire;

      /* Link arena into arena list */
      auto old_head = m_arenas.load(std::memory_order_relaxed);
      do {
        arena->m_next = old_head;
      } while (!m_arenas.compare_exchange_weak(old_head, arena, Release_order, Acquire_order));

      m_total_arenas.fetch_add(1, std::memory_order_relaxed);
      return true;

    } catch (...) {
      /* Allocation failed */
      return false;
    }
  }

  /**
   * Shrink pool by deallocating fully free arenas.
   * Call this explicitly when you want to reclaim memory.
   * NOT thread-safe - caller must ensure no concurrent allocations.
   * @return Number of arenas freed
   */
  std::size_t shrink() noexcept {
    std::lock_guard<std::mutex> lock(m_arena_mutex);
    std::size_t freed_count = 0;
    std::vector<Arena*> to_free;

    Arena* prev = nullptr;
    auto current = m_arenas.load(std::memory_order_relaxed);

    while (current != nullptr) {
      Arena* next = current->m_next;

      /* Check if arena is fully free */
      if (current->m_free_count.load(std::memory_order_relaxed) == ArenaSize) {
        to_free.push_back(current);
        if (prev) {
          prev->m_next = next;
        } else {
          m_arenas.store(next, std::memory_order_relaxed);
        }
      } else {
        prev = current;
      }

      current = next;
    }

    if (to_free.empty()) {
      return 0;
    }

    std::vector<std::pair<Block*, Block*>> ranges;
    ranges.reserve(to_free.size());

    for (auto* arena : to_free) {
      ranges.emplace_back(arena->m_blocks, arena->m_blocks + ArenaSize);
    }

    Block* new_free_head = nullptr;
    auto block = m_free_list_head.load(std::memory_order_relaxed);

    while (block != nullptr) {
      auto next = block->m_next_free.load(std::memory_order_relaxed);
      bool keep = true;

      for (const auto& range : ranges) {
        if (block >= range.first && block < range.second) {
          keep = false;
          break;
        }
      }

      if (keep) {
        block->m_next_free.store(new_free_head, std::memory_order_relaxed);
        new_free_head = block;
      }

      block = next;
    }

    m_free_list_head.store(new_free_head, std::memory_order_relaxed);

    freed_count = to_free.size();
    m_total_arenas.fetch_sub(freed_count, std::memory_order_relaxed);

    for (auto* arena : to_free) {
      delete arena;
    }

    return freed_count;
  }

  /**
   * Get statistics about pool usage.
   */
  struct Stats {
    std::size_t m_total_arenas;
    std::size_t m_total_blocks;
    std::size_t m_approx_free_blocks;
  };

  Stats get_stats() const noexcept {
    Stats stats;

    stats.m_total_arenas = m_total_arenas.load(std::memory_order_relaxed);
    stats.m_total_blocks = stats.m_total_arenas * ArenaSize;

    /* Approximate free count by summing arena free counts */
    stats.m_approx_free_blocks = 0;
    auto current = m_arenas.load(std::memory_order_acquire);

    while (current != nullptr) [[likely]] {
      stats.m_approx_free_blocks += current->m_free_count.load(std::memory_order_relaxed);
      current = current->m_next;
    }

    return stats;
  }

private:
  struct Arena;

  struct Block {
    alignas(alignof(std::max_align_t)) std::byte m_data[BlockSize];

    /** Back-pointer to owning arena */
    Arena* m_arena{nullptr};

    /** Next block in free list */
    std::atomic<Block*> m_next_free{nullptr};
  };

  /** Thread-local cache to minimize atomic operations */
  struct Thread_cache {
    Block* m_blocks[CacheSize]{};
    std::size_t m_count{0};
  };

  struct Arena {
    ~Arena() {
      if (m_blocks) {
        delete[] m_blocks;
      }
    }

    /** Array of blocks */
    Block* m_blocks{nullptr};

    /** Number of free blocks in this arena */
    std::atomic<std::size_t> m_free_count{0};

    /** Linked list of arenas */
    Arena* m_next{nullptr};

  };

  /** Get thread-local cache */
  static Thread_cache& get_thread_cache() noexcept {
    thread_local Thread_cache cache;
    return cache;
  }

  /** Slow path: refill cache from shared pool */
  [[nodiscard]] void* allocate_slow(Thread_cache& cache) noexcept {
    /* Try to grab multiple blocks from shared pool to amortize cost */
    constexpr std::size_t refill_count = CacheSize / 2;

    for (std::size_t i = 0; i < refill_count; ++i) {
      auto block = pop_free_list();
      if (block == nullptr) {
        /* Shared pool exhausted - try to grow */
        if (i == 0) {
          if (!grow()) {
            return nullptr;
          }
          block = pop_free_list();
          if (block == nullptr) {
            return nullptr;
          }
        } else {
          break;  /* Got some blocks, that's enough */
        }
      }
      cache.m_blocks[cache.m_count++] = block;
    }

    /* Now allocate from refilled cache */
    if (cache.m_count > 0) {
      --cache.m_count;
      auto* block = cache.m_blocks[cache.m_count];
      block->m_arena->m_free_count.fetch_sub(1, std::memory_order_relaxed);
      return &block->m_data;
    }

    return nullptr;
  }

  /** Slow path: flush half of cache to shared pool */
  void deallocate_slow(Thread_cache& cache, Block* block) noexcept {
    /* Flush half the cache to shared pool */
    constexpr std::size_t flush_count = CacheSize / 2;

    for (std::size_t i = 0; i < flush_count; ++i) {
      --cache.m_count;
      auto flush_block = cache.m_blocks[cache.m_count];
      flush_block->m_arena->m_free_count.fetch_add(1, std::memory_order_relaxed);
      push_free_list(flush_block);
    }

    /* Now add the new block to cache */
    cache.m_blocks[cache.m_count] = block;
    ++cache.m_count;
    block->m_arena->m_free_count.fetch_add(1, std::memory_order_relaxed);
  }

  /**
   * Lock-free stack operations for free list.
   */
  void push_free_list(Block* block) noexcept {
    auto old_head = m_free_list_head.load(std::memory_order_relaxed);
    do {
      block->m_next_free.store(old_head, std::memory_order_relaxed);
    } while (!m_free_list_head.compare_exchange_weak(old_head, block, std::memory_order_release, std::memory_order_relaxed));
  }

  Block* pop_free_list() noexcept {
    constexpr auto Release_order = std::memory_order_release;
    constexpr auto Acquire_order = std::memory_order_acquire;

    auto head = m_free_list_head.load(std::memory_order_acquire);

    while (head != nullptr) {
      auto next = head->m_next_free.load(std::memory_order_relaxed);
      if (m_free_list_head.compare_exchange_weak(head, next, Release_order, Acquire_order)) {
        return head;
      }

      /* CAS failed, retry with updated head */
    }

    return nullptr;
  }

  /**
   * Lock-free stack of free blocks.
   */
  std::atomic<Block*> m_free_list_head{nullptr};
  
  /** Head of arena linked list */
  std::atomic<Arena*> m_arenas{nullptr};

  /** Total number of arenas */
  std::atomic<std::size_t> m_total_arenas{0};

  /** Protects arena allocation/deallocation only */
  std::mutex m_arena_mutex;
};

/**
 * Global memory pool for coroutine frames.
 * Coroutine frames are typically 256-512 bytes, so we use 512 bytes as block size.
 * Dynamic pool that grows on demand - starts with 2048 blocks per arena.
 */
inline Memory_pool<512, 2048>& get_coroutine_pool() noexcept {
  static Memory_pool<512, 2048> pool;
  return pool;
}

/**
 * Global memory pool for Thread_pool lambda closures.
 * Lambda closures are typically 16-32 bytes (just a coroutine_handle), so we use 64 bytes.
 * Dynamic pool that grows on demand - starts with 8192 blocks per arena.
 */
inline Memory_pool<64, 8192>& get_thread_pool_closure_pool() noexcept {
  static Memory_pool<64, 8192> pool;
  return pool;
}

} // namespace util
