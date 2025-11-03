add_executable(wal_tests_bounded_channel
  test_bounded_channel.cc
)

add_executable(wal_tests_logger
  test_logger.cc
)

add_executable(wal_tests
  test_wal.cc
)

# Only need public headers, not the wal library.
target_include_directories(wal_tests_bounded_channel PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests_logger PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_compile_features(wal_tests_bounded_channel PRIVATE cxx_std_23)
target_compile_features(wal_tests_logger PRIVATE cxx_std_23)
target_compile_features(wal_tests PRIVATE cxx_std_23)

if(MSVC)
  # Debug flags for MSVC: /Zi (debug info), /Od (no optimization)
  target_compile_options(wal_tests_bounded_channel PRIVATE /W4 /Zi /Od)
  target_compile_options(wal_tests_logger PRIVATE /W4 /Zi /Od)
  target_compile_options(wal_tests PRIVATE /W4 /Zi /Od)
else()
  # Debug flags for GDB: -ggdb3 (max debug info), -O0 (no optimization), -fno-omit-frame-pointer (better stack traces)
  target_compile_options(wal_tests_bounded_channel PRIVATE -ggdb3 -O0 -fno-omit-frame-pointer -Wall -Wextra -Wpedantic)
  target_compile_options(wal_tests_logger PRIVATE -ggdb3 -O0 -fno-omit-frame-pointer -Wall -Wextra -Wpedantic)
  target_compile_options(wal_tests PRIVATE -ggdb3 -O0 -fno-omit-frame-pointer -Wall -Wextra -Wpedantic)
endif()

add_test(NAME wal.tests.bounded_channel COMMAND wal_tests_bounded_channel)
add_test(NAME wal.tests.logger COMMAND wal_tests_logger)
add_test(NAME wal.tests COMMAND wal_tests)

# Threads for concurrency tests
find_package(Threads REQUIRED)
target_link_libraries(wal_tests_bounded_channel PRIVATE Threads::Threads)
target_link_libraries(wal_tests_logger PRIVATE Threads::Threads)
target_link_libraries(wal_tests PRIVATE Threads::Threads wal::wal)
