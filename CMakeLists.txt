cmake_minimum_required(VERSION 3.20)

project(wal VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# ------------------------------- Options -------------------------------------
option(WAL_BUILD_TESTS "Build tests" ON)
# Build the wal library (may be WIP). OFF so tests can build independently.
option(WAL_BUILD_LIB "Build wal library" OFF)
# Warnings as errors are always enabled for all builds

# Sensible default build type for single-config generators.
# Default to Release if not specified.
if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS Debug Release RelWithDebInfo MinSizeRel)
elseif(NOT CMAKE_CONFIGURATION_TYPES AND CMAKE_BUILD_TYPE STREQUAL "")
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

message("Build type: " ${CMAKE_BUILD_TYPE})

# ---------------------------- C++ standard -----------------------------------
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# MSVC: prefer modern PDB handling without sprinkling flags.
if(MSVC)
  # /Zi + ProgramDatabase for all configs; linker will emit full PDBs.
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT ProgramDatabase)
endif()

# Non-MSVC: we'll add debug info + linker tweaks at the target level.

# ----------------------------- Linker configuration --------------------------
# Try mold if available (for both library and tests).
if(NOT MSVC)
  find_program(MOLD_LD mold PATHS /usr/bin /usr/local/bin)
  if(MOLD_LD)
    message(STATUS "Using mold linker: ${MOLD_LD}")
  else()
    message(STATUS "mold linker not found, using default linker")
  endif()
endif()

# ----------------------------- Platform feature checks -----------------------
# Check for __atomic builtin support (GCC/Clang)
include(CheckCXXSourceCompiles)
if(NOT MSVC)
  check_cxx_source_compiles("
    #include <cstddef>
    int main() {
      std::size_t val = 0;
      std::size_t expected = 0;
      std::size_t desired = 1;
      __atomic_load_n(&val, __ATOMIC_RELAXED);
      __atomic_store_n(&val, 1, __ATOMIC_RELAXED);
      __atomic_compare_exchange_n(&val, &expected, desired, false, __ATOMIC_RELAXED, __ATOMIC_RELAXED);
      return 0;
    }
  " HAVE_ATOMIC_BUILTINS)
  if(HAVE_ATOMIC_BUILTINS)
    message(STATUS "__atomic builtins are available")
  else()
    message(STATUS "__atomic builtins are not available, using std::atomic")
  endif()
endif()

# Option to enable/disable __atomic builtins (default: OFF, use std::atomic)
option(WAL_USE_ATOMIC_BUILTINS "Use __atomic builtins instead of std::atomic (experimental)" OFF)
if(WAL_USE_ATOMIC_BUILTINS AND NOT HAVE_ATOMIC_BUILTINS)
  message(WARNING "WAL_USE_ATOMIC_BUILTINS is ON but __atomic builtins are not available, disabling")
  set(WAL_USE_ATOMIC_BUILTINS OFF)
endif()

# Report which atomic implementation is being used
if(WAL_USE_ATOMIC_BUILTINS)
  message(STATUS "Using __atomic builtins for atomic operations (experimental)")
else()
  message(STATUS "Using std::atomic for atomic operations (default)")
endif()

# ----------------------------- wal library -----------------------------------
if(WAL_BUILD_LIB OR WAL_BUILD_TESTS)
  add_library(wal STATIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src/wal/wal.cc
  )
  add_library(wal::wal ALIAS wal)

  target_include_directories(wal
    PUBLIC
      $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
      $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  )

  # Add compile definition for __atomic builtins if enabled
  if(WAL_USE_ATOMIC_BUILTINS)
    target_compile_definitions(wal PUBLIC WAL_USE_ATOMIC_BUILTINS)
  endif()

  # Position independent for static and shared use.
  set_target_properties(wal PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    EXPORT_NAME wal
  )

  # Warnings (portable baseline) + always treat warnings as errors.
  # Base flags (warnings, etc.) that should be inherited by tests
  if(MSVC)
    set(WAL_COMPILE_OPTIONS_BASE_MSVC
      /W4
      /WX
    )
    set(WAL_COMPILE_OPTIONS_MSVC
      ${WAL_COMPILE_OPTIONS_BASE_MSVC}
      # Debug: no optimization
      $<$<CONFIG:Debug>:/Zi /Od>
      # Maximum safe optimization for Release builds
      $<$<CONFIG:Release>:/O2 /Ob2 /Oi /Ot /GL>
      $<$<CONFIG:RelWithDebInfo>:/O2 /Ob1 /Oi /Ot>
    )
    target_compile_options(wal PRIVATE ${WAL_COMPILE_OPTIONS_MSVC})
    # Test-specific flags for MSVC (respect build type)
    set(WAL_TEST_COMPILE_OPTIONS_DEBUG_MSVC
      ${WAL_COMPILE_OPTIONS_BASE_MSVC} /Zi
      $<$<CONFIG:Debug>:/Od>
      $<$<NOT:$<CONFIG:Debug>>:/O2>
    )
    set(WAL_TEST_COMPILE_OPTIONS_PERF_MSVC
      ${WAL_COMPILE_OPTIONS_BASE_MSVC} /Zi
      $<$<CONFIG:Debug>:/Od>
      $<$<NOT:$<CONFIG:Debug>>:/O2>
    )
  else()
    set(WAL_COMPILE_OPTIONS_BASE_GCC_CLANG
      -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion
      -Werror
      # SSE4.2 for CRC intrinsics in checksum.h
      -msse4.2
    )
    set(WAL_COMPILE_OPTIONS_GCC_CLANG
      ${WAL_COMPILE_OPTIONS_BASE_GCC_CLANG}
      $<$<CONFIG:Debug>:-g3 -O0 -ggdb3>
      # Maximum safe optimization for Release builds
      $<$<CONFIG:Release>:-g -O3 -flto -march=native -mtune=native -fno-omit-frame-pointer>
      $<$<CONFIG:RelWithDebInfo>:-g -O3 -flto -fno-omit-frame-pointer>
    )
    target_compile_options(wal PRIVATE ${WAL_COMPILE_OPTIONS_GCC_CLANG})

    # Test-specific flags for GCC/Clang (respect build type: -O0 for Debug, -O3 for others)
    set(WAL_TEST_COMPILE_OPTIONS_DEBUG_GCC_CLANG
      ${WAL_COMPILE_OPTIONS_BASE_GCC_CLANG} -ggdb3 -fno-omit-frame-pointer
      $<$<CONFIG:Debug>:-O0>
      $<$<NOT:$<CONFIG:Debug>>:-O3>
    )
    set(WAL_TEST_COMPILE_OPTIONS_PERF_LTO_GCC_CLANG
      ${WAL_COMPILE_OPTIONS_BASE_GCC_CLANG} -ggdb3 -fno-omit-frame-pointer
      $<$<CONFIG:Debug>:-O0>
      $<$<NOT:$<CONFIG:Debug>>:-O3 -flto -march=native -mtune=native>
    )
    set(WAL_TEST_COMPILE_OPTIONS_CORO_GCC_CLANG
      ${WAL_COMPILE_OPTIONS_BASE_GCC_CLANG} -ggdb3 -fno-omit-frame-pointer
      $<$<CONFIG:Debug>:-O0>
      $<$<NOT:$<CONFIG:Debug>>:-O3>
    )
  endif()

  # Linker options (non-MSVC): build-id and mold if present.
  if(NOT MSVC)
    if(MOLD_LD)
      target_link_options(wal PRIVATE -fuse-ld=mold)
    endif()
    target_link_options(wal PRIVATE
      -Wl,--build-id
      # Enable LTO in linker for Release builds
      $<$<CONFIG:Release>:-flto>
      $<$<CONFIG:RelWithDebInfo>:-flto>
    )
  endif()

  # Link libnuma for NUMA-aware memory allocation
  find_library(NUMA_LIBRARY numa)
  if(NUMA_LIBRARY)
    target_link_libraries(wal PRIVATE ${NUMA_LIBRARY})
    message(STATUS "Found libnuma: ${NUMA_LIBRARY}")
  else()
    message(WARNING "libnuma not found - NUMA-aware allocation will be disabled")
  endif()

  # ------------------------------ Install ------------------------------------
  install(TARGETS wal
    EXPORT walTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  )

  # Export (build tree) for find_package from the build dir.
  export(
    EXPORT walTargets
    NAMESPACE wal::
    FILE ${CMAKE_CURRENT_BINARY_DIR}/walTargets.cmake
  )

  # Package config + version.
  configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/walConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/walConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/wal
  )

  write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/walConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
  )

  install(EXPORT walTargets
    NAMESPACE wal::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/wal
  )

  install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/walConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/walConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/wal
  )
endif()

# Headers can install independently (so tests/consumers can include them).
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# -------------------------------- Tests --------------------------------------
if(WAL_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
