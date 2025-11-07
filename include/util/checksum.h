#pragma once

#include <cstdint>
#include <cstring>
#include <span>
#include <concepts>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#include <cpuid.h>
#endif /* defined(__x86_64__) || defined(_M_X64) */

namespace util {

using crc32_t = uint32_t;

inline size_t get_cpuid(uint32_t level, uint32_t& eax, uint32_t& ebx, uint32_t& ecx, uint32_t& edx) noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    return static_cast<size_t>(__get_cpuid(level, &eax, &ebx, &ecx, &edx));
#else
    return 0;
#endif
}
/** Check for SSE4.2 support (includes CRC32C instruction)
 * @note This function is used to determine if hardware acceleration is available for CRC32C.
 * 
 * @return True if SSE4.2 is supported, false otherwise.
 */
inline bool has_sse42() noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    uint32_t eax, ebx, ecx, edx;
    if (get_cpuid(1, eax, ebx, ecx, edx)) {
        return (ecx & bit_SSE4_2) != 0;
    }
#endif
    return false;
}

/** Hardware-accelerated CRC32C (Castagnoli)
 * @note This function is used to compute the CRC32C checksum using hardware acceleration.
 * 
 * @param[in] crc Initial CRC value
 * @param[in] data Pointer to the data
 * @param[in] length Length of the data in bytes
 * @return CRC32C checksum value
 */
inline crc32_t crc32c_hw(crc32_t crc, const void* data, size_t length) noexcept {
#if defined(__x86_64__) || defined(_M_X64)
    const uint8_t* buf = static_cast<const uint8_t*>(data);

    /* Process 32-byte chunks (4x8 bytes) with loop unrolling for better throughput. */
    while (length >= 32) {
        crc = static_cast<crc32_t>(_mm_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(buf)));
        crc = static_cast<crc32_t>(_mm_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(buf + 8)));
        crc = static_cast<crc32_t>(_mm_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(buf + 16)));
        crc = static_cast<crc32_t>(_mm_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(buf + 24)));
        buf += 32;
        length -= 32;
    }

    /* Process remaining 8-byte chunks. */
    while (length >= 8) {
        crc = static_cast<crc32_t>(_mm_crc32_u64(crc, *reinterpret_cast<const uint64_t*>(buf)));
        buf += 8;
        length -= 8;
    }

    /* Process 4-byte chunk. */
    if (length >= 4) {
        crc = _mm_crc32_u32(crc, *reinterpret_cast<const crc32_t*>(buf));
        buf += 4;
        length -= 4;
    }

    /* Process remaining bytes. */
    while (length > 0) {
        crc = _mm_crc32_u8(crc, *buf);
        ++buf;
        --length;
    }
#endif /* defined(__x86_64__) || defined(_M_X64) */
    return crc;
}

/** Software fallback CRC32C implementation
 * @note This function is used to compute the CRC32C checksum using software fallback.
 * 
 * @param[in] crc Initial CRC value
 * @param[in] data Pointer to the data
 * @param[in] length Length of the data in bytes
 * @return CRC32C checksum value
 */
inline crc32_t crc32c_sw(crc32_t crc, const void* data, size_t length) noexcept {
    static constexpr crc32_t table[256] = {
        0x00000000, 0xF26B8303, 0xE13B70F7, 0x1350F3F4, 0xC79A971F, 0x35F1141C, 0x26A1E7E8, 0xD4CA64EB,
        0x8AD958CF, 0x78B2DBCC, 0x6BE22838, 0x9989AB3B, 0x4D43CFD0, 0xBF284CD3, 0xAC78BF27, 0x5E133C24,
        0x105EC76F, 0xE235446C, 0xF165B798, 0x030E349B, 0xD7C45070, 0x25AFD373, 0x36FF2087, 0xC494A384,
        0x9A879FA0, 0x68EC1CA3, 0x7BBCEF57, 0x89D76C54, 0x5D1D08BF, 0xAF768BBC, 0xBC267848, 0x4E4DFB4B,
        0x20BD8EDE, 0xD2D60DDD, 0xC186FE29, 0x33ED7D2A, 0xE72719C1, 0x154C9AC2, 0x061C6936, 0xF477EA35,
        0xAA64D611, 0x580F5512, 0x4B5FA6E6, 0xB93425E5, 0x6DFE410E, 0x9F95C20D, 0x8CC531F9, 0x7EAEB2FA,
        0x30E349B1, 0xC288CAB2, 0xD1D83946, 0x23B3BA45, 0xF779DEAE, 0x05125DAD, 0x1642AE59, 0xE4292D5A,
        0xBA3A117E, 0x4851927D, 0x5B016189, 0xA96AE28A, 0x7DA08661, 0x8FCB0562, 0x9C9BF696, 0x6EF07595,
        0x417B1DBC, 0xB3109EBF, 0xA0406D4B, 0x522BEE48, 0x86E18AA3, 0x748A09A0, 0x67DAFA54, 0x95B17957,
        0xCBA24573, 0x39C9C670, 0x2A993584, 0xD8F2B687, 0x0C38D26C, 0xFE53516F, 0xED03A29B, 0x1F682198,
        0x5125DAD3, 0xA34E59D0, 0xB01EAA24, 0x42752927, 0x96BF4DCC, 0x64D4CECF, 0x77843D3B, 0x85EFBE38,
        0xDBFC821C, 0x2997011F, 0x3AC7F2EB, 0xC8AC71E8, 0x1C661503, 0xEE0D9600, 0xFD5D65F4, 0x0F36E6F7,
        0x61C69362, 0x93AD1061, 0x80FDE395, 0x72966096, 0xA65C047D, 0x5437877E, 0x4767748A, 0xB50CF789,
        0xEB1FCBAD, 0x197448AE, 0x0A24BB5A, 0xF84F3859, 0x2C855CB2, 0xDEEEDFB1, 0xCDBE2C45, 0x3FD5AF46,
        0x7198540D, 0x83F3D70E, 0x90A324FA, 0x62C8A7F9, 0xB602C312, 0x44694011, 0x5739B3E5, 0xA55230E6,
        0xFB410CC2, 0x092A8FC1, 0x1A7A7C35, 0xE811FF36, 0x3CDB9BDD, 0xCEB018DE, 0xDDE0EB2A, 0x2F8B6829,
        0x82F63B78, 0x709DB87B, 0x63CD4B8F, 0x91A6C88C, 0x456CAC67, 0xB7072F64, 0xA457DC90, 0x563C5F93,
        0x082F63B7, 0xFA44E0B4, 0xE9141340, 0x1B7F9043, 0xCFB5F4A8, 0x3DDE77AB, 0x2E8E845F, 0xDCE5075C,
        0x92A8FC17, 0x60C37F14, 0x73938CE0, 0x81F80FE3, 0x55326B08, 0xA759E80B, 0xB4091BFF, 0x466298FC,
        0x1871A4D8, 0xEA1A27DB, 0xF94AD42F, 0x0B21572C, 0xDFEB33C7, 0x2D80B0C4, 0x3ED04330, 0xCCBBC033,
        0xA24BB5A6, 0x502036A5, 0x4370C551, 0xB11B4652, 0x65D122B9, 0x97BAA1BA, 0x84EA524E, 0x7681D14D,
        0x2892ED69, 0xDAF96E6A, 0xC9A99D9E, 0x3BC21E9D, 0xEF087A76, 0x1D63F975, 0x0E330A81, 0xFC588982,
        0xB21572C9, 0x407EF1CA, 0x532E023E, 0xA145813D, 0x758FE5D6, 0x87E466D5, 0x94B49521, 0x66DF1622,
        0x38CC2A06, 0xCAA7A905, 0xD9F75AF1, 0x2B9CD9F2, 0xFF56BD19, 0x0D3D3E1A, 0x1E6DCDEE, 0xEC064EED,
        0xC38D26C4, 0x31E6A5C7, 0x22B65633, 0xD0DDD530, 0x0417B1DB, 0xF67C32D8, 0xE52CC12C, 0x1747422F,
        0x49547E0B, 0xBB3FFD08, 0xA86F0EFC, 0x5A048DFF, 0x8ECEE914, 0x7CA56A17, 0x6FF599E3, 0x9D9E1AE0,
        0xD3D3E1AB, 0x21B862A8, 0x32E8915C, 0xC083125F, 0x144976B4, 0xE622F5B7, 0xF5720643, 0x07198540,
        0x590AB964, 0xAB613A67, 0xB831C993, 0x4A5A4A90, 0x9E902E7B, 0x6CFBAD78, 0x7FAB5E8C, 0x8DC0DD8F,
        0xE330A81A, 0x115B2B19, 0x020BD8ED, 0xF0605BEE, 0x24AA3F05, 0xD6C1BC06, 0xC5914FF2, 0x37FACCF1,
        0x69E9F0D5, 0x9B8273D6, 0x88D28022, 0x7AB90321, 0xAE7367CA, 0x5C18E4C9, 0x4F48173D, 0xBD23943E,
        0xF36E6F75, 0x0105EC76, 0x12551F82, 0xE03E9C81, 0x34F4F86A, 0xC69F7B69, 0xD5CF889D, 0x27A40B9E,
        0x79B737BA, 0x8BDCB4B9, 0x988C474D, 0x6AE7C44E, 0xBE2DA0A5, 0x4C4623A6, 0x5F16D052, 0xAD7D5351
    };

    const uint8_t* buf = static_cast<const uint8_t*>(data);
    crc = ~crc;

    for (size_t i = 0; i < length; i++) {
        crc = table[(crc ^ buf[i]) & 0xFF] ^ (crc >> 8);
    }

    return ~crc;
}

/* Checksum algorithm type
 * @note This enum is used to specify the checksum algorithm to use.
 @return The checksum algorithm to use.
 *         - CRC32C: CRC-32C (Castagnoli) - hardware accelerated when available
 */
enum class ChecksumAlgorithm {
    CRC32C, /* CRC-32C (Castagnoli) - hardware accelerated when available */
};

/** Main checksum class
 * @note This class is used to compute the checksum of a data stream.
 */
struct Checksum {
    using update_fn_t = crc32_t(*)(crc32_t, const void*, size_t) noexcept;

    explicit constexpr Checksum(ChecksumAlgorithm algo = ChecksumAlgorithm::CRC32C) noexcept
        : m_use_hw(false), m_state(0), m_algorithm(algo), m_update_fn(crc32c_sw) {
#if defined(__x86_64__) || defined(_M_X64)
        if (algo == ChecksumAlgorithm::CRC32C) {
            static const bool sse42_supported = has_sse42();
            m_use_hw = sse42_supported;
            m_update_fn = sse42_supported ? crc32c_hw : crc32c_sw;
        }
#endif /* defined(__x86_64__) || defined(_M_X64) */
    }

    /** Reset the checksum state
     * @note This method is used to reset the checksum state.
     */
    constexpr void reset() noexcept {
        m_state = 0;
    }

    /** Update checksum with new data
     * @note This method is used to update the checksum with new data.
     * 
     * @param[in] data Pointer to the data
     * @param[in] length Length of the data in bytes
     */
    template<typename T>
    requires std::is_trivially_copyable_v<T>
    void update(std::span<const T> data) noexcept {
        update(data.data(), data.size_bytes());
    }

    void update(const void* data, size_t length) noexcept {
        // Avoid branch: use function pointer set in constructor
        m_state = m_update_fn(m_state, data, length);
    }

    /** Get current checksum value
     * @note This method is used to get the current checksum value.
     * 
     * @return The current checksum value.
     */
    [[nodiscard]] constexpr crc32_t value() const noexcept {
        return m_state;
    }

    /** Check if hardware acceleration is being used
     * @note This method is used to check if hardware acceleration is being used.
     * 
     * @return True if hardware acceleration is being used, false otherwise.
     */
    [[nodiscard]] constexpr bool using_hardware() const noexcept {
        return m_use_hw;
    }

    /** Compute checksum for data in one call (convenience method)
     * @note This method is used to compute the checksum for data in one call.
     *
     * @param[in] data Pointer to the data
     * @param[in] length Length of the data in bytes
     * @return The checksum value.
     */
    template<typename T>
    requires std::is_trivially_copyable_v<T>
    [[nodiscard]] static crc32_t compute(std::span<const T> data, ChecksumAlgorithm algo = ChecksumAlgorithm::CRC32C) noexcept {
        Checksum cs(algo);
        cs.update(data);
        return cs.value();
    }

    /** Compute checksum for data in one call (convenience method)
     * @note This method is used to compute the checksum for data in one call.
     *
     * @param[in] data Pointer to the data
     * @param[in] length Length of the data in bytes
     * @return The checksum value.
     */
    [[nodiscard]] static crc32_t compute(const void* data, size_t length, ChecksumAlgorithm algo = ChecksumAlgorithm::CRC32C) noexcept {
        Checksum cs(algo);
        cs.update(data, length);
        return cs.value();
    }

private:
    bool m_use_hw{};
    crc32_t m_state{0};
    ChecksumAlgorithm m_algorithm{ChecksumAlgorithm::CRC32C};
    update_fn_t m_update_fn{crc32c_sw};
};

/**
 * Convenience function for computing CRC32C
 * @note This function is used to compute the CRC32C checksum for a data stream.
 * 
 * @param[in] data Pointer to the data
 * @param[in] length Length of the data in bytes
 * @return CRC32C checksum value
 */
[[nodiscard]] inline crc32_t crc32c(const void* data, size_t length) noexcept {
    return Checksum::compute(data, length, ChecksumAlgorithm::CRC32C);
}

} // namespace util
