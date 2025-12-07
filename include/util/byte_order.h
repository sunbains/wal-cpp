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

#include <cstdint>
#include <bit>
#include <type_traits>

namespace util {

inline constexpr bool is_little_endian = (std::endian::native == std::endian::little);
inline constexpr bool is_big_endian = (std::endian::native == std::endian::big);

inline constexpr uint16_t byteswap16(uint16_t value) noexcept {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap16(value);
#elif defined(_MSC_VER)
    return _byteswap_ushort(value);
#else
    return (value << 8) | (value >> 8);
#endif
}

inline constexpr uint32_t byteswap32(uint32_t value) noexcept {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap32(value);
#elif defined(_MSC_VER)
    return _byteswap_ulong(value);
#else
    return ((value & 0xFF000000) >> 24) |
           ((value & 0x00FF0000) >> 8)  |
           ((value & 0x0000FF00) << 8)  |
           ((value & 0x000000FF) << 24);
#endif
}

inline constexpr uint64_t byteswap64(uint64_t value) noexcept {
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_bswap64(value);
#elif defined(_MSC_VER)
    return _byteswap_uint64(value);
#else
    return ((value & 0xFF00000000000000ULL) >> 56) |
           ((value & 0x00FF000000000000ULL) >> 40) |
           ((value & 0x0000FF0000000000ULL) >> 24) |
           ((value & 0x000000FF00000000ULL) >> 8)  |
           ((value & 0x00000000FF000000ULL) << 8)  |
           ((value & 0x0000000000FF0000ULL) << 24) |
           ((value & 0x000000000000FF00ULL) << 40) |
           ((value & 0x00000000000000FFULL) << 56);
#endif
}

inline constexpr uint16_t ntoh16(uint16_t value) noexcept {
    if constexpr (is_little_endian) {
        return byteswap16(value);
    } else {
        return value;
    }
}

inline constexpr uint32_t ntoh32(uint32_t value) noexcept {
    if constexpr (is_little_endian) {
        return byteswap32(value);
    } else {
        return value;
    }
}

inline constexpr uint64_t ntoh64(uint64_t value) noexcept {
    if constexpr (is_little_endian) {
        return byteswap64(value);
    } else {
        return value;
    }
}

/* Generic template versions for convenience. */
template<typename T>
inline constexpr T ntoh(T value) noexcept {
    static_assert(std::is_integral_v<T>, "Type must be an integral type");

    if constexpr (sizeof(T) == 1) {
        return value;
    } else if constexpr (sizeof(T) == 2) {
        return ntoh16(value);
    } else if constexpr (sizeof(T) == 4) {
        return ntoh32(value);
    } else if constexpr (sizeof(T) == 8) {
        return ntoh64(value);
    } else {
        static_assert(sizeof(T) <= 8, "Type size not supported");
    }
}

template<typename T>
inline constexpr T hton(T value) noexcept {
    return ntoh(value);
}

} // namespace util
