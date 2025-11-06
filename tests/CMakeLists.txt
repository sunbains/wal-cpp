add_executable(wal_tests_bounded_channel
  test_bounded_channel.cc
)

add_executable(wal_tests_logger
  test_logger.cc
)

add_executable(wal_tests
  test_wal.cc
)

add_executable(wal_tests_perf
  test_wal_perf.cc
)

# Only need public headers, not the wal library.
target_include_directories(wal_tests_bounded_channel PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests_logger PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests_perf PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_compile_features(wal_tests_bounded_channel PRIVATE cxx_std_23)
target_compile_features(wal_tests_logger PRIVATE cxx_std_23)
target_compile_features(wal_tests PRIVATE cxx_std_23)
target_compile_features(wal_tests_perf PRIVATE cxx_std_23)

if(MSVC)
  # Debug flags for MSVC: /Zi (debug info), /Od (no optimization), /WX (warnings as errors)
  target_compile_options(wal_tests_bounded_channel PRIVATE /W4 /WX /Zi /Od)
  target_compile_options(wal_tests_logger PRIVATE /W4 /WX /Zi /Od)
  target_compile_options(wal_tests PRIVATE /W4 /WX /Zi /Od)
  target_compile_options(wal_tests_perf PRIVATE /W4 /WX /Zi /Od)
else()
  # Debug flags for GDB: -ggdb3 (max debug info), -O0 (no optimization), -fno-omit-frame-pointer (better stack traces), -Werror (warnings as errors)
  target_compile_options(wal_tests_bounded_channel PRIVATE -ggdb3 -O0 -fno-omit-frame-pointer -Wall -Wextra -Wpedantic -Werror)
  target_compile_options(wal_tests_logger PRIVATE -ggdb3 -O0 -fno-omit-frame-pointer -Wall -Wextra -Wpedantic -Werror)
  target_compile_options(wal_tests PRIVATE -ggdb3 -O0 -fno-omit-frame-pointer -Wall -Wextra -Wpedantic -Werror)
  target_compile_options(wal_tests_perf PRIVATE -ggdb3 -O0 -fno-omit-frame-pointer -Wall -Wextra -Wpedantic -Werror)
endif()

add_test(NAME wal.tests.bounded_channel COMMAND wal_tests_bounded_channel)
add_test(NAME wal.tests.logger COMMAND wal_tests_logger)
add_test(NAME wal.tests COMMAND wal_tests)

# Threads for concurrency tests
find_package(Threads REQUIRED)
target_link_libraries(wal_tests_bounded_channel PRIVATE Threads::Threads)
target_link_libraries(wal_tests_logger PRIVATE Threads::Threads)
target_link_libraries(wal_tests PRIVATE Threads::Threads wal::wal)
target_link_libraries(wal_tests_perf PRIVATE Threads::Threads wal::wal)

# Force test_wal.cc and test_logger.cc to always be compiled with asserts enabled (even in Release builds)
# Undefine NDEBUG to ensure assert() macros are active
if(MSVC)
  set_source_files_properties(test_wal.cc PROPERTIES
    COMPILE_FLAGS "/U NDEBUG"
  )
  set_source_files_properties(test_logger.cc PROPERTIES
    COMPILE_FLAGS "/U NDEBUG"
  )
else()
  set_source_files_properties(test_wal.cc PROPERTIES
    COMPILE_FLAGS "-UNDEBUG"
  )
  set_source_files_properties(test_logger.cc PROPERTIES
    COMPILE_FLAGS "-UNDEBUG"
  )
endif()

# Linker options (non-MSVC): build-id and mold if present.
# Uses MOLD_LD variable from parent CMakeLists.txt
if(NOT MSVC)
  if(MOLD_LD)
    target_link_options(wal_tests_bounded_channel PRIVATE -fuse-ld=mold)
    target_link_options(wal_tests_logger PRIVATE -fuse-ld=mold)
    target_link_options(wal_tests PRIVATE -fuse-ld=mold)
    target_link_options(wal_tests_perf PRIVATE -fuse-ld=mold)
  endif()
  target_link_options(wal_tests_bounded_channel PRIVATE -Wl,--build-id)
  target_link_options(wal_tests_logger PRIVATE -Wl,--build-id)
  target_link_options(wal_tests PRIVATE -Wl,--build-id)
  target_link_options(wal_tests_perf PRIVATE -Wl,--build-id)
endif()
