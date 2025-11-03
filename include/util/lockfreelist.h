#pragma once

#include <atomic>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <limits>
#include <utility>
#include <optional>

namespace util {

struct Iterator_invalidated : public std::runtime_error {
  explicit Iterator_invalidated(const char* msg) : std::runtime_error(msg) {}
};

struct Node {
  using Link_type = uint32_t;
  using Version_type = uint8_t;

  static constexpr uint32_t VERSION_BITS_PER_LINK = 4;         // 4-bit nibbles
  static constexpr uint32_t TOTAL_VERSION_BITS = VERSION_BITS_PER_LINK * 2;
  static constexpr uint32_t LINK_BITS = (64 - TOTAL_VERSION_BITS) / 2;

  // DEL lives in the MSB of the next_version nibble
  static constexpr uint32_t DEL_BIT = 1u << (VERSION_BITS_PER_LINK - 1);
  static constexpr uint32_t VERSION_VALUE_MASK = DEL_BIT - 1;  // lower bits for version value
  static constexpr uint32_t VERSION_MASK = (1u << VERSION_BITS_PER_LINK) - 1; // raw nibble

  static constexpr auto NULL_PTR  = (1u << LINK_BITS) - 1;
  static constexpr auto NULL_LINK = std::numeric_limits<uint64_t>::max();
  static constexpr uint32_t MAX_RETRIES = 100;

  Node() = default;
  Node(Node&& rhs) noexcept : m_links(rhs.m_links.load(std::memory_order_relaxed)) {}
  Node(Link_type next, Link_type prev, Version_type version) noexcept;

  Node& operator=(Node&& rhs) noexcept {
    m_links.store(rhs.m_links.load(std::memory_order_relaxed), std::memory_order_relaxed);
    return *this;
  }

  bool is_null() const noexcept {
    return m_links.load(std::memory_order_acquire) == NULL_LINK;
  }

  // "Physical" invalidate once fully unlinked
  void invalidate() noexcept {
    m_links.store(NULL_LINK, std::memory_order_release);
  }

  std::atomic<uint64_t> m_links{NULL_LINK};
};

// Packing/unpacking with DEL in next_version nibble
inline constexpr uint64_t pack_links(Node::Link_type next,
                                     Node::Link_type prev,
                                     Node::Version_type next_version_value,
                                     Node::Version_type prev_version_value,
                                     bool del = false) noexcept
{
  constexpr uint32_t LINK_MASK          = (1u << Node::LINK_BITS) - 1;
  constexpr uint32_t PREV_VER_BITS      = Node::VERSION_BITS_PER_LINK;
  constexpr uint32_t PREV_LINK_SHIFT    = PREV_VER_BITS;
  constexpr uint32_t NEXT_VERSION_SHIFT = PREV_VER_BITS + Node::LINK_BITS;
  constexpr uint32_t NEXT_LINK_SHIFT    = PREV_VER_BITS + Node::LINK_BITS + Node::VERSION_BITS_PER_LINK;

  const uint32_t next_ver_nibble =
      (static_cast<uint32_t>(next_version_value) & Node::VERSION_VALUE_MASK) |
      (del ? Node::DEL_BIT : 0);

  return (static_cast<uint64_t>(next & LINK_MASK) << NEXT_LINK_SHIFT) |
         (static_cast<uint64_t>(next_ver_nibble & Node::VERSION_MASK) << NEXT_VERSION_SHIFT) |
         (static_cast<uint64_t>(prev & LINK_MASK) << PREV_LINK_SHIFT) |
         static_cast<uint64_t>(prev_version_value & Node::VERSION_VALUE_MASK);
}

struct Link_pack {
  Node::Link_type  next;
  Node::Link_type  prev;
  Node::Version_type next_version;  // value (no DEL)
  Node::Version_type prev_version;  // value
  bool del;
};

inline Link_pack unpack_links(uint64_t links) noexcept {
  constexpr uint32_t LINK_MASK          = (1u << Node::LINK_BITS) - 1;
  constexpr uint32_t PREV_VER_BITS      = Node::VERSION_BITS_PER_LINK;
  constexpr uint32_t PREV_LINK_SHIFT    = PREV_VER_BITS;
  constexpr uint32_t NEXT_VERSION_SHIFT = PREV_VER_BITS + Node::LINK_BITS;
  constexpr uint32_t NEXT_LINK_SHIFT    = PREV_VER_BITS + Node::LINK_BITS + Node::VERSION_BITS_PER_LINK;

  const uint32_t next_ver_raw = static_cast<uint32_t>((links >> NEXT_VERSION_SHIFT) & Node::VERSION_MASK);
  const bool del = (next_ver_raw & Node::DEL_BIT) != 0;

  return {
    static_cast<Node::Link_type>((links >> NEXT_LINK_SHIFT) & LINK_MASK),
    static_cast<Node::Link_type>((links >> PREV_LINK_SHIFT) & LINK_MASK),
    static_cast<Node::Version_type>(next_ver_raw & Node::VERSION_VALUE_MASK),
    static_cast<Node::Version_type>(links & Node::VERSION_VALUE_MASK),
    del
  };
}

inline Node::Version_type bump_val(Node::Version_type v) noexcept {
  return static_cast<Node::Version_type>((v + 1) & Node::VERSION_VALUE_MASK);
}

// Convenience helpers
inline bool is_del(uint64_t links) noexcept { return unpack_links(links).del; }

inline uint64_t with_del(uint64_t links, bool d) noexcept {
  auto lp = unpack_links(links);
  return pack_links(lp.next, lp.prev, lp.next_version, lp.prev_version, d);
}

template<typename T, auto N, bool IsConst = false>
struct List_iterator {
  using value_type = T;
  using difference_type = std::ptrdiff_t;
  using pointer = std::conditional_t<IsConst, const T*, T*>;
  using reference = std::conditional_t<IsConst, const T&, T&>;
  using iterator_category = std::bidirectional_iterator_tag;

  using result_type = typename std::invoke_result_t<decltype(N), T>;
  using node_type = typename std::remove_reference<result_type>::type;
  using node_pointer = std::conditional_t<IsConst, const node_type*, node_type*>;

  List_iterator() = default;

  template<bool WasConst, typename = std::enable_if_t<IsConst && !WasConst>>
  List_iterator(const List_iterator<T, N, WasConst>& rhs) noexcept
    : m_base(rhs.m_base),
      m_prev(rhs.m_prev),
      m_current(rhs.m_current) {}

  List_iterator(pointer base, node_pointer current, node_pointer prev) noexcept
    : m_base(base),
      m_prev(prev),
      m_current(current) {}

  [[nodiscard]] inline pointer to_item(node_pointer node) const noexcept {
    if (!node) return nullptr;
    auto ptr = reinterpret_cast<const std::byte*>(node);
    const auto offset = ptr - reinterpret_cast<const std::byte*>(m_base);
    return &const_cast<pointer>(m_base)[offset / sizeof(T)];
  }

  [[nodiscard]] inline node_pointer to_node(typename node_type::Link_type link) const noexcept {
    if (link == node_type::NULL_PTR) return nullptr;
    auto base = const_cast<T*>(m_base);
    auto& node = (base[link].*N)();
    return const_cast<node_pointer>(&node);
  }

  [[nodiscard]] inline bool operator==(const List_iterator& rhs) const noexcept {
    return m_current == rhs.m_current;
  }

  [[nodiscard]] inline bool operator!=(const List_iterator& rhs) const noexcept {
    return !(*this == rhs);
  }

  [[nodiscard]] inline reference operator*() const noexcept {
    return *operator->();
  }

  [[nodiscard]] inline pointer operator->() const noexcept {
    return to_item(m_current);
  }

  List_iterator& operator++() {
    if (!m_current) return *this;

    uint32_t retries{};
    auto cur_links = m_current->m_links.load(std::memory_order_acquire);

    // Skip tombstones
    while (cur_links != node_type::NULL_LINK && unpack_links(cur_links).del) {
      auto nxt = to_node(unpack_links(cur_links).next);
      m_prev = m_current;
      m_current = nxt;
      if (!m_current) return *this;
      cur_links = m_current->m_links.load(std::memory_order_acquire);
    }

    auto cur = unpack_links(cur_links);
    node_pointer next{to_node(cur.next)};

    // Validate back-link; if mismatch (concurrent edits), walk forward a bit
    if (to_node(cur.prev) != m_prev) {
      while (m_current && to_node(cur.prev) != m_prev && retries++ < node_type::MAX_RETRIES) {
        m_current = to_node(cur.next);
        if (!m_current) return *this;
        cur = unpack_links(m_current->m_links.load(std::memory_order_acquire));
        m_prev = to_node(cur.prev);
        }
      if (retries >= node_type::MAX_RETRIES) throw Iterator_invalidated("Iterator invalidated");
    }

    m_prev = m_current;
    m_current = next;
    return *this;
  }

  List_iterator operator++(int) noexcept {
    List_iterator tmp = *this;
    ++(*this);
    return tmp;
  }

  List_iterator& operator--() {
    if (!m_prev) return *this;

    uint32_t retries{};
    auto prev_links = m_prev->m_links.load(std::memory_order_acquire);

    while (prev_links != node_type::NULL_LINK && unpack_links(prev_links).del) {
      auto pv = to_node(unpack_links(prev_links).prev);
      m_current = m_prev;
      m_prev = pv;
      if (!m_prev) return *this;
      prev_links = m_prev->m_links.load(std::memory_order_acquire);
    }

    auto pl = unpack_links(prev_links);
    auto prev = to_node(pl.prev);

    if (to_node(pl.next) != m_current) {
      while (m_prev && to_node(pl.next) != m_current && retries++ < node_type::MAX_RETRIES) {
        m_prev = to_node(pl.prev);
        if (!m_prev) return *this;
        pl = unpack_links(m_prev->m_links.load(std::memory_order_acquire));
        m_current = to_node(pl.next);
        }
      if (retries >= node_type::MAX_RETRIES) throw Iterator_invalidated("Iterator invalidated");
    }

    m_current = m_prev;
    m_prev = prev;
    return *this;
  }

  List_iterator operator--(int) noexcept {
    List_iterator tmp = *this;
    --(*this);
    return tmp;
  }

  pointer m_base{};
  node_pointer m_prev{};
  node_pointer m_current{};
};

template <typename T, auto N>
struct List {
  using iterator = List_iterator<T, N, false>;
  using const_iterator = List_iterator<T, N, true>;
  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  using value_type = T;
  using node_type = std::remove_reference_t<typename std::invoke_result_t<decltype(N), T>>;
  using node_pointer = node_type*;
  using item_type = T;
  using item_pointer = item_type*;
  using item_reference = item_type&;

  List(item_pointer base, item_pointer end) noexcept : m_bounds(base, end) {
    assert(base <= end);
    assert(base != nullptr);
  }

  [[nodiscard]] static item_pointer to_item(const item_pointer base, const node_type& node) noexcept {
    auto ptr = reinterpret_cast<const std::byte*>(&node);
    std::ptrdiff_t offset = ptr - reinterpret_cast<const std::byte*>(base);
    std::size_t index = static_cast<std::size_t>(offset) / sizeof(T);
    return &const_cast<item_pointer>(base)[index];
  }

  /* Utility methods */
  [[nodiscard]] static node_pointer to_node(const item_pointer base, typename node_type::Link_type link) noexcept {
    if (link == node_type::NULL_PTR) [[unlikely]] {
      return nullptr;
    } else [[likely]] {
      auto& node = (base[link].*N)();
      return const_cast<node_pointer>(&node);
    }
  }

  [[nodiscard]] node_pointer to_node(typename node_type::Link_type link) const noexcept {
    if (link == node_type::NULL_PTR) [[unlikely]] {
      return nullptr;
    } else [[likely]] {
      return to_node(const_cast<item_pointer>(m_bounds.first), link);
    }
  }

  [[nodiscard]] typename node_type::Link_type to_link(const node_type& node) const noexcept {
    auto ptr = reinterpret_cast<const std::byte*>(&node);
    auto base = reinterpret_cast<const std::byte*>(m_bounds.first);
    std::ptrdiff_t diff = ptr - base;
    std::size_t index = static_cast<std::size_t>(diff) / sizeof(T);
    return static_cast<typename node_type::Link_type>(index);
  }
  [[nodiscard]] item_pointer to_item(const node_type& node) const noexcept {
    return to_item(const_cast<item_pointer>(m_bounds.first), node);
  }

  [[nodiscard]] item_pointer to_item(typename node_type::Link_type link) const noexcept {
    assert(link != node_type::NULL_PTR);
    return &const_cast<item_pointer>(m_bounds.first)[link];
  }

  item_pointer remove(item_reference item) noexcept {
    uint32_t outer{};
    auto& node = (item.*N)();
    const auto node_link = to_link(node);

    while (outer++ < Node::MAX_RETRIES) {
      uint64_t L = node.m_links.load(std::memory_order_acquire);
      if (L == Node::NULL_LINK) return nullptr;                // already fully removed
      auto ld = unpack_links(L);
      if (ld.del) {
        // already logically deleted → try to finish physical unlink
      } else {
        // 1) logical delete
        uint64_t Ldel = with_del(L, true);
        if (!node.m_links.compare_exchange_weak(L, Ldel, std::memory_order_acq_rel, std::memory_order_acquire))
        continue;
        L = Ldel;
        ld = unpack_links(L);
      }

      auto prev_node = to_node(ld.prev);
      auto next_node = to_node(ld.next);
      bool ok = true;

      // 2) swing prev->next to skip node
      if (prev_node) {
        uint32_t r=0;
        for (;;) {
          if (r++ >= Node::MAX_RETRIES) { ok=false; break; }
          uint64_t P = prev_node->m_links.load(std::memory_order_acquire);
          if (P == Node::NULL_LINK || unpack_links(P).del) { ok=false; break; }
          auto pd = unpack_links(P);
          if (pd.next != node_link) { ok=false; break; }
          uint64_t P2 = pack_links(ld.next, pd.prev, bump_val(pd.next_version), pd.prev_version, /*del*/false);
          if (prev_node->m_links.compare_exchange_weak(P, P2, std::memory_order_acq_rel, std::memory_order_acquire))
            break;
          }
      }

      // 3) swing next->prev to skip node
      if (ok && next_node) {
        uint32_t r=0;
        for (;;) {
          if (r++ >= Node::MAX_RETRIES) { ok=false; break; }
          uint64_t Nw = next_node->m_links.load(std::memory_order_acquire);
          if (Nw == Node::NULL_LINK || unpack_links(Nw).del) { ok=false; break; }
          auto nd = unpack_links(Nw);
          if (nd.prev != node_link) { ok=false; break; }
          uint64_t N2 = pack_links(nd.next, ld.prev, nd.next_version, bump_val(nd.prev_version), /*del*/false);
          if (next_node->m_links.compare_exchange_weak(Nw, N2, std::memory_order_acq_rel, std::memory_order_acquire))
            break;
          }
      }

      if (!ok) {
        // remain logically deleted; another remover/iterator can help later
        continue; // retry outer
      }

      // 4) update head/tail AFTER neighbors are fixed
      if (ld.prev == Node::NULL_PTR) {
        typename node_type::Link_type expect = node_link;
        (void)m_head.compare_exchange_strong(expect, ld.next, std::memory_order_acq_rel, std::memory_order_acquire);
      }
      if (ld.next == Node::NULL_PTR) {
        typename node_type::Link_type expect = node_link;
        (void)m_tail.compare_exchange_strong(expect, ld.prev, std::memory_order_acq_rel, std::memory_order_acquire);
      }

      // 5) physical removal
        node.invalidate();
        m_size.fetch_sub(1, std::memory_order_relaxed);
        return to_item(node);
      }
    return nullptr;
  }

  bool push_front(item_reference item) noexcept {
    auto& node = (item.*N)();
    const auto new_link = to_link(node);

    uint32_t outer{};
    while (outer++ < Node::MAX_RETRIES) {
      auto old_head = m_head.load(std::memory_order_acquire);
      node.m_links.store(pack_links(old_head, Node::NULL_PTR, 0, 0, /*del*/false), std::memory_order_relaxed);

      // 1) move head to new node if still old_head
      if (!m_head.compare_exchange_strong(old_head, new_link, std::memory_order_acq_rel, std::memory_order_acquire))
        continue;

      // 2) fix old head's prev
      if (old_head != Node::NULL_PTR) {
        auto oh = to_node(old_head);
        uint32_t r=0;
        for (;;) {
          if (r++ >= Node::MAX_RETRIES) break;
          uint64_t H = oh->m_links.load(std::memory_order_acquire);
          if (H == Node::NULL_LINK || unpack_links(H).del) break;
          auto hd = unpack_links(H);
          uint64_t H2 = pack_links(hd.next, new_link, hd.next_version, bump_val(hd.prev_version), /*del*/false);
          if (oh->m_links.compare_exchange_weak(H, H2, std::memory_order_acq_rel, std::memory_order_acquire))
            break;
        }
      } else {
        // was empty → also try to set tail
        typename node_type::Link_type expect_tail = Node::NULL_PTR;
        (void)m_tail.compare_exchange_strong(expect_tail, new_link, std::memory_order_acq_rel, std::memory_order_acquire);
      }

        m_size.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    node.invalidate();
    return false;
  }

  bool push_back(item_reference item) noexcept {
    auto& node = (item.*N)();
    const auto new_link = to_link(node);

    uint32_t outer{};
    while (outer++ < Node::MAX_RETRIES) {
      auto old_tail = m_tail.load(std::memory_order_acquire);
      node.m_links.store(pack_links(Node::NULL_PTR, old_tail, 0, 0, /*del*/false), std::memory_order_relaxed);

      if (!m_tail.compare_exchange_strong(old_tail, new_link, std::memory_order_acq_rel, std::memory_order_acquire))
        continue;

      if (old_tail != Node::NULL_PTR) {
        auto ot = to_node(old_tail);
        uint32_t r=0;
        for (;;) {
          if (r++ >= Node::MAX_RETRIES) break;
          uint64_t T_links = ot->m_links.load(std::memory_order_acquire);
          if (T_links == Node::NULL_LINK || unpack_links(T_links).del) break;
          auto td = unpack_links(T_links);
          uint64_t T2 = pack_links(new_link, td.prev, bump_val(td.next_version), td.prev_version, /*del*/false);
          if (ot->m_links.compare_exchange_weak(T_links, T2, std::memory_order_acq_rel, std::memory_order_acquire))
            break;
        }
      } else {
        typename node_type::Link_type expect_head = Node::NULL_PTR;
        (void)m_head.compare_exchange_strong(expect_head, new_link, std::memory_order_acq_rel, std::memory_order_acquire);
      }

        m_size.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    node.invalidate();
    return false;
  }

  bool insert_after(item_reference item, item_reference new_item) noexcept {
    auto& node = (item.*N)();
    auto& nn   = (new_item.*N)();

    uint32_t outer{};
    while (outer++ < Node::MAX_RETRIES) {
      uint64_t L = node.m_links.load(std::memory_order_acquire);
      if (L == Node::NULL_LINK || unpack_links(L).del) { nn.invalidate(); return false; }
      auto ld = unpack_links(L);
      const auto nn_link = to_link(nn);
      const auto node_link = to_link(node);

      // prep new-node links (relaxed ok; published via neighbor CAS)
      nn.m_links.store(pack_links(ld.next, node_link, 0, 0, /*del*/false), std::memory_order_relaxed);

      // 1) anchor: make node->next = nn
      uint64_t L2 = pack_links(nn_link, ld.prev, bump_val(ld.next_version), ld.prev_version, /*del*/false);
      if (!node.m_links.compare_exchange_strong(L, L2, std::memory_order_acq_rel, std::memory_order_acquire))
        continue;

      // 2) close the back-link at old next (or update tail)
      if (ld.next != Node::NULL_PTR) {
        auto next = to_node(ld.next);
        uint32_t r=0;
        for (;;) {
          if (r++ >= Node::MAX_RETRIES) break;
          uint64_t Nw = next->m_links.load(std::memory_order_acquire);
          if (Nw == Node::NULL_LINK || unpack_links(Nw).del) break;
          auto nd = unpack_links(Nw);
          if (nd.prev != node_link) break; // someone else spliced; undo anchor if still ours
          uint64_t N2 = pack_links(nd.next, nn_link, nd.next_version, bump_val(nd.prev_version), /*del*/false);
          if (next->m_links.compare_exchange_weak(Nw, N2, std::memory_order_acq_rel, std::memory_order_acquire)) {
            m_size.fetch_add(1, std::memory_order_relaxed);
            return true;
            }
        }
        // Undo anchor if still pointing at nn
        uint64_t C = node.m_links.load(std::memory_order_acquire);
        auto cd = unpack_links(C);
        if (cd.next == nn_link) {
          (void)node.m_links.compare_exchange_strong(C, L, std::memory_order_acq_rel, std::memory_order_acquire);
        }
        // retry
        } else {
        // was tail; try to move tail to nn
        typename node_type::Link_type expect = node_link;
        (void)m_tail.compare_exchange_strong(expect, nn_link, std::memory_order_acq_rel, std::memory_order_acquire);
        m_size.fetch_add(1, std::memory_order_relaxed);
        return true;
      }
    }

    nn.invalidate();
    return false;
  }

  bool insert_before(item_reference item, item_reference new_item) noexcept {
    auto& node = (item.*N)();
    auto& nn   = (new_item.*N)();

    uint32_t outer{};
    while (outer++ < Node::MAX_RETRIES) {
      uint64_t L = node.m_links.load(std::memory_order_acquire);
      if (L == Node::NULL_LINK || unpack_links(L).del) { nn.invalidate(); return false; }
      auto ld = unpack_links(L);
      const auto nn_link   = to_link(nn);
      const auto node_link = to_link(node);

      nn.m_links.store(pack_links(node_link, ld.prev, 0, 0, /*del*/false), std::memory_order_relaxed);

      // 1) anchor: make node->prev = nn
      uint64_t L2 = pack_links(ld.next, nn_link, ld.next_version, bump_val(ld.prev_version), /*del*/false);
      if (!node.m_links.compare_exchange_strong(L, L2, std::memory_order_acq_rel, std::memory_order_acquire))
        continue;

      // 2) close the forward link at old prev (or update head)
      if (ld.prev != Node::NULL_PTR) {
        auto prev = to_node(ld.prev);
        uint32_t r=0;
        for (;;) {
          if (r++ >= Node::MAX_RETRIES) break;
          uint64_t Pw = prev->m_links.load(std::memory_order_acquire);
          if (Pw == Node::NULL_LINK || unpack_links(Pw).del) break;
          auto pd = unpack_links(Pw);
          if (pd.next != node_link) break;
          uint64_t P2 = pack_links(nn_link, pd.prev, bump_val(pd.next_version), pd.prev_version, /*del*/false);
          if (prev->m_links.compare_exchange_weak(Pw, P2, std::memory_order_acq_rel, std::memory_order_acquire)) {
            m_size.fetch_add(1, std::memory_order_relaxed);
            return true;
            }
        }
        // Undo anchor if still ours
        uint64_t C = node.m_links.load(std::memory_order_acquire);
        auto cd = unpack_links(C);
        if (cd.prev == nn_link) {
          (void)node.m_links.compare_exchange_strong(C, L, std::memory_order_acq_rel, std::memory_order_acquire);
        }
        } else {
        // was head; try to move head to nn
        typename node_type::Link_type expect = node_link;
        (void)m_head.compare_exchange_strong(expect, nn_link, std::memory_order_acq_rel, std::memory_order_acquire);
        m_size.fetch_add(1, std::memory_order_relaxed);
        return true;
      }
    }

    nn.invalidate();
    return false;
  }

  template <typename Predicate>
  [[nodiscard]] item_pointer find(Predicate predicate) noexcept {
    uint32_t retries{};
    typename node_type::Link_type current = m_head.load(std::memory_order_acquire);

    while (current != Node::NULL_PTR && retries++ < Node::MAX_RETRIES) [[likely]] {
      auto node = to_node(current);
      auto item = to_item(*node);

      if (predicate(item)) [[unlikely]] {
        /* Verify node is still in the list */
        if (!node->is_null()) {
          return item;
        }
      }

      auto links = node->m_links.load(std::memory_order_acquire);

      if (links == Node::NULL_LINK) [[unlikely]] {
        /* Node was removed, try to recover from head */
        current = m_head.load(std::memory_order_acquire);
        retries++;
        continue;
      }

      current = unpack_links(links).next;
    }

    return nullptr;
  }

  [[nodiscard]] item_pointer pop_front() noexcept {
    uint32_t retries{};
    while (retries++ < Node::MAX_RETRIES) [[likely]] {
      auto link = m_head.load(std::memory_order_acquire);
      if (link == Node::NULL_PTR) [[unlikely]] {
        return nullptr;
      }
      if (auto* item = remove(*to_item(link))) [[likely]] {
        return item;
      }
    }
    return nullptr;
  }

  [[nodiscard]] item_pointer pop_back() noexcept {
    uint32_t retries{};
    while (retries++ < Node::MAX_RETRIES) [[likely]] {
      auto link = m_tail.load(std::memory_order_acquire);
      if (link == Node::NULL_PTR) [[unlikely]] {
        return nullptr;
      }
      if (auto* item = remove(*to_item(link))) [[likely]] {
        return item;
      }
    }
    return nullptr;
  }

  [[nodiscard]] iterator begin() noexcept {
    const auto head = m_head.load(std::memory_order_acquire);

    if (head != Node::NULL_PTR) [[likely]] {
      auto node = to_node(head);
      return iterator(m_bounds.first, node, nullptr);
    }
    return end();
  }

  [[nodiscard]] const_iterator begin() const noexcept {
    const auto head = m_head.load(std::memory_order_acquire);

    if (head != Node::NULL_PTR) [[likely]] {
      auto node = to_node(head);
      return const_iterator(m_bounds.first, node, nullptr);
    }
    return end();
  }

  [[nodiscard]] iterator end() noexcept {
    const auto tail = m_tail.load(std::memory_order_acquire);
    auto node = tail != Node::NULL_PTR ? to_node(tail) : nullptr;
    return iterator(m_bounds.first, nullptr, node);
  }

  [[nodiscard]] const_iterator end() const noexcept {
    const auto tail = m_tail.load(std::memory_order_acquire);
    auto node = tail != Node::NULL_PTR ? to_node(tail) : nullptr;
    return const_iterator(m_bounds.first, nullptr, node);
  }

  [[nodiscard]] reverse_iterator rbegin() noexcept {
    return reverse_iterator(end());
  }

  [[nodiscard]] const_reverse_iterator rbegin() const noexcept {
    return const_reverse_iterator(end());
  }

  [[nodiscard]] reverse_iterator rend() noexcept {
    return reverse_iterator(begin());
  }

  [[nodiscard]] const_reverse_iterator rend() const noexcept {
    return const_reverse_iterator(begin());
  }

#if UT_DEBUG
  /* Validation helper */
  bool validate_node_links(const node_type& node) const noexcept {
    auto links = node.m_links.load(std::memory_order_acquire);

    if (links == Node::NULL_LINK) [[likely]] {
      return true;  // Removed nodes are valid
    }

    auto link_data = unpack_links(links);

    /* Check next pointer consistency */
    if (link_data.next != Node::NULL_PTR) [[likely]] {
      auto next_node = to_node(link_data.next);

      if (!next_node || next_node->is_null()) {
        return false;
      }

      auto next_links = next_node->m_links.load(std::memory_order_acquire);

      if (next_links == Node::NULL_LINK) [[unlikely]] {
        return false;
      }

      auto next_link_data = unpack_links(next_links);

      if (next_link_data.prev != to_link(node)) [[unlikely]] {
        return false;
      }
    }

    /* Check prev pointer consistency */
    if (link_data.prev != Node::NULL_PTR) [[likely]] {
      auto prev_node = to_node(link_data.prev);

      if (!prev_node || prev_node->is_null()) [[unlikely]] {
        return false;
      }

      auto prev_links = prev_node->m_links.load(std::memory_order_acquire);

      if (prev_links == Node::NULL_LINK) [[unlikely]] {
        return false;
      }

      auto prev_link_data = unpack_links(prev_links);

      if (prev_link_data.next != to_link(node)) [[unlikely]] {
        return false;
      }
    }

    return true;
  }
#endif // UT_DEBUG

[[nodiscard]] std::size_t size() const noexcept {
  return m_size.load(std::memory_order_acquire);
}

private:
  std::atomic<std::size_t> m_size{0};
  std::pair<item_pointer, item_pointer> m_bounds{};
  std::atomic<typename node_type::Link_type> m_head{node_type::NULL_PTR};
  std::atomic<typename node_type::Link_type> m_tail{node_type::NULL_PTR};
};

} // namespace util
