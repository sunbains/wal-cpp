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

  # Position independent for static and shared use.
  set_target_properties(wal PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    EXPORT_NAME wal
  )

  # Warnings (portable baseline) + always treat warnings as errors.
  if(MSVC)
    target_compile_options(wal PRIVATE
      /W4
      /WX
      # Maximum safe optimization for Release builds
      $<$<CONFIG:Release>:/O2 /Ob2 /Oi /Ot /GL>
      $<$<CONFIG:RelWithDebInfo>:/O2 /Ob1 /Oi /Ot>
    )
  else()
    target_compile_options(wal PRIVATE
      -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion
      -Werror
      # SSE4.2 for CRC intrinsics in checksum.h
      -msse4.2
      $<$<CONFIG:Debug>:-g3 -ggdb3>
      $<$<CONFIG:RelWithDebInfo>:-g3 -ggdb3>
      # Maximum safe optimization for Release builds
      $<$<CONFIG:Release>:-g -O3 -flto -march=native -mtune=native -fno-omit-frame-pointer>
      $<$<CONFIG:RelWithDebInfo>:-g -O3 -flto -fno-omit-frame-pointer>
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
