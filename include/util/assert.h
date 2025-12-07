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

/** C++26-forward compatible assertion and contract checking implementation
 * Designed for easy migration to C++ Contracts (P2900, expected in C++26)
 *
 * Migration path when C++26 contracts are available:
 *   expects(cond, msg)     -> [[pre: cond]] + violation handler
 *   ensures(cond, msg)     -> [[post: cond]] + violation handler
 *   contract_assert(cond)  -> [[assert: cond]]
 *   assume(cond)           -> [[assume: cond]] (available in C++23)
 *
 * Build modes align with contract semantics:
 *   Audit mode    (NDEBUG undefined)          -> all checks enabled
 *   Default mode  (NDEBUG defined)            -> audit-level checks disabled
 *   Off mode      (DISABLE_ALL_ASSERTS)       -> all checks disabled
 *
 * Semantic levels (matching C++26 contracts):
 *   default   - Always checked in production (expects/ensures)
 *   audit     - Checked only in debug builds (debug_assert)
 *   axiom     - Never checked, optimizer hint only (assume) */

#include <cstdlib>
#include <source_location>

#include "util/logger.h"

// The global logger instance - must be defined by the user (typically in a .cc file)
extern ::util::Logger<::util::MT_logger_writer> g_logger;

// Contract-style assertions (C++26-forward compatible attempt)

#ifndef DISABLE_ALL_ASSERTS

// Precondition check - equivalent to C++26 [[pre: condition]]
// Always enabled unless DISABLE_ALL_ASSERTS
// Use at function entry to validate caller obligations
#define expects(condition, message, ...)                                       \
  do {                                                                         \
    if (!(condition)) [[unlikely]] {                                           \
      g_logger.err(std::source_location::current(),                            \
                   "Precondition violated: {} | " message, #condition,         \
                   ##__VA_ARGS__);                                             \
      std::abort();                                                            \
    }                                                                          \
  } while (false)

// Postcondition check - equivalent to C++26 [[post: condition]]
// Always enabled unless DISABLE_ALL_ASSERTS
// Use at function exit to validate function guarantees
#define ensures(condition, message, ...)                                       \
  do {                                                                         \
    if (!(condition)) [[unlikely]] {                                           \
      g_logger.err(std::source_location::current(),                            \
                   "Postcondition violated: {} | " message, #condition,        \
                   ##__VA_ARGS__);                                             \
      std::abort();                                                            \
    }                                                                          \
  } while (false)

// Runtime assertion - equivalent to C++26 [[assert: condition]]
// Always enabled unless DISABLE_ALL_ASSERTS
// Use for runtime invariants that must hold
#define contract_assert(condition, message, ...)                               \
  do {                                                                         \
    if (!(condition)) [[unlikely]] {                                           \
      g_logger.err(std::source_location::current(),                            \
                   "Assertion failed: {} | " message, #condition,              \
                   ##__VA_ARGS__);                                             \
      std::abort();                                                            \
    }                                                                          \
  } while (false)

// Fatal assertion - unrecoverable violation
// Always enabled unless DISABLE_ALL_ASSERTS
// Use when continuing after failure would be unsafe
#define contract_fatal(condition, message, ...)                                \
  do {                                                                         \
    if (!(condition)) [[unlikely]] {                                           \
      g_logger.fatal(std::source_location::current(),                          \
                     "Fatal contract violation: {} | " message, #condition,    \
                     ##__VA_ARGS__);                                           \
    }                                                                          \
  } while (false)

// Audit-level assertion - disabled by NDEBUG (equivalent to audit semantic)
// Use for expensive checks that should only run in debug/testing
#ifndef NDEBUG
#define audit_assert(condition, message, ...)                                  \
  do {                                                                         \
    if (!(condition)) [[unlikely]] {                                           \
      g_logger.err(std::source_location::current(),                            \
                   "Audit assertion failed: {} | " message, #condition,        \
                   ##__VA_ARGS__);                                             \
      std::abort();                                                            \
    }                                                                          \
  } while (false)
#else
#define audit_assert(condition, message, ...) ((void)0)
#endif // NDEBUG

// Optimizer assumption - equivalent to C++23 [[assume(condition)]]
// Never checked at runtime, provides optimization hints
// CAUTION: If assumption is false, behavior is undefined
#if defined(__clang__)
#define assume(condition) __builtin_assume(condition)
#elif defined(__GNUC__)
#define assume(condition)                                                      \
  do {                                                                         \
    if (!(condition)) [[unlikely]]                                             \
      __builtin_unreachable();                                                 \
  } while (false)
#elif defined(_MSC_VER)
#define assume(condition) __assume(condition)
#else
#define assume(condition) ((void)0)
#endif

#else // DISABLE_ALL_ASSERTS is defined

// All assertions disabled - compile to no-ops
#define expects(condition, message, ...) ((void)0)
#define ensures(condition, message, ...) ((void)0)
#define contract_assert(condition, message, ...) ((void)0)
#define contract_fatal(condition, message, ...) ((void)0)
#define audit_assert(condition, message, ...) ((void)0)

// Keep assume as optimizer hint even when assertions are disabled
#if defined(__clang__)
#define assume(condition) __builtin_assume(condition)
#elif defined(__GNUC__)
#define assume(condition)                                                      \
  do {                                                                         \
    if (!(condition)) [[unlikely]]                                             \
      __builtin_unreachable();                                                 \
  } while (false)
#elif defined(_MSC_VER)
#define assume(condition) __assume(condition)
#else
#define assume(condition) ((void)0)
#endif

#endif // DISABLE_ALL_ASSERTS

// Compatibility aliases for transition from old-style assertions

#define assert_always contract_assert
#define assert_fatal contract_fatal
#define debug_assert audit_assert
