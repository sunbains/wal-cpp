cmake_minimum_required(VERSION 3.20)

# Project: Write Ahead Log (wal)
project(wal VERSION 0.1.0 LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

# Options
option(WAL_BUILD_TESTS "Build tests" ON)
option(WAL_WARNINGS_AS_ERRORS "Treat warnings as errors" OFF)

# Default to a reasonable build type if none specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "RelWithDebInfo" "MinSizeRel")
endif()

# C++ standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Library target (skeleton)
add_library(wal STATIC)
add_library(wal::wal ALIAS wal)

target_sources(wal
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/wal.cc
)

target_include_directories(wal
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(wal PROPERTIES
  POSITION_INDEPENDENT_CODE ON
  EXPORT_NAME wal
)

if(MSVC)
  target_compile_options(wal PRIVATE /W4 $<$<BOOL:${WAL_WARNINGS_AS_ERRORS}>:/WX>)
else()
  target_compile_options(wal PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion $<$<BOOL:${WAL_WARNINGS_AS_ERRORS}>:-Werror>)
endif()

# Install targets and headers
install(TARGETS wal
  EXPORT walTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# Export targets for build and install trees
export(
  EXPORT walTargets
  NAMESPACE wal::
  FILE ${CMAKE_CURRENT_BINARY_DIR}/walTargets.cmake
)

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

# Tests
if(WAL_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

