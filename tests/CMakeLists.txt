add_executable(wal_tests_bounded_channel
  test_bounded_channel.cc
)

add_executable(wal_tests_logger
  test_logger.cc
)

add_executable(wal_tests
  test_wal.cc
)

add_executable(wal_tests_bounded_queue_perf
  test_bounded_queue_perf.cc
)

add_executable(wal_tests_tp
  test_tp.cc
)

add_executable(wal_tests_spsc_per_producer
  test_spsc_per_producer.cc
)

add_executable(wal_tests_actor_model
  test_actor_model.cc
)

add_executable(wal_tests_log_service_actor
  test_log_service_actor.cc
)

add_executable(wal_tests_log_io_simple
  test_log_io_simple.cc
)

add_executable(wal_tests_io_uring_writer
  test_io_uring_writer.cc
)


# Only need public headers, not the wal library.
target_include_directories(wal_tests_bounded_channel PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests_logger PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests_bounded_queue_perf PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests_tp PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests_spsc_per_producer PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests_actor_model PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests_log_service_actor PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests_log_io_simple PRIVATE ${CMAKE_SOURCE_DIR}/include)
target_include_directories(wal_tests_io_uring_writer PRIVATE ${CMAKE_SOURCE_DIR}/include)

target_compile_features(wal_tests_bounded_channel PRIVATE cxx_std_23)
target_compile_features(wal_tests_logger PRIVATE cxx_std_23)
target_compile_features(wal_tests PRIVATE cxx_std_23)
target_compile_features(wal_tests_bounded_queue_perf PRIVATE cxx_std_23)
target_compile_features(wal_tests_tp PRIVATE cxx_std_23)
target_compile_features(wal_tests_spsc_per_producer PRIVATE cxx_std_23)
target_compile_features(wal_tests_actor_model PRIVATE cxx_std_23)
target_compile_features(wal_tests_log_service_actor PRIVATE cxx_std_23)
target_compile_features(wal_tests_log_io_simple PRIVATE cxx_std_23)
target_compile_features(wal_tests_io_uring_writer PRIVATE cxx_std_23)

# All compiler flags are inherited from parent CMakeLists.txt
if(MSVC)
  # Debug tests (no optimization due to coroutine bugs or debugging needs)
  target_compile_options(wal_tests_bounded_channel PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_MSVC})
  target_compile_options(wal_tests_logger PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_MSVC})
  target_compile_options(wal_tests PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_MSVC})
  # NOTE: Compiled with /Od due to optimizer bugs with C++20 coroutines
  target_compile_options(wal_tests_tp PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_MSVC})
  target_compile_options(wal_tests_spsc_per_producer PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_MSVC})
  target_compile_options(wal_tests_actor_model PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_MSVC})
  target_compile_options(wal_tests_log_service_actor PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_MSVC})
  target_compile_options(wal_tests_log_io_simple PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_MSVC})
  target_compile_options(wal_tests_io_uring_writer PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_MSVC})

  # Performance tests (optimized)
  target_compile_options(wal_tests_bounded_queue_perf PRIVATE ${WAL_TEST_COMPILE_OPTIONS_PERF_MSVC})
else()
  # Debug tests (full debug info, no optimization)
  target_compile_options(wal_tests_bounded_channel PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_GCC_CLANG})
  target_compile_options(wal_tests_logger PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_GCC_CLANG})
  target_compile_options(wal_tests PRIVATE ${WAL_TEST_COMPILE_OPTIONS_DEBUG_GCC_CLANG})

  # Performance tests with LTO
  target_compile_options(wal_tests_bounded_queue_perf PRIVATE ${WAL_TEST_COMPILE_OPTIONS_PERF_LTO_GCC_CLANG})

  # Coroutine tests (no optimization due to optimizer bugs with C++20 coroutines)
  target_compile_options(wal_tests_tp PRIVATE ${WAL_TEST_COMPILE_OPTIONS_CORO_GCC_CLANG})
  target_compile_options(wal_tests_spsc_per_producer PRIVATE ${WAL_TEST_COMPILE_OPTIONS_CORO_GCC_CLANG})
  target_compile_options(wal_tests_actor_model PRIVATE ${WAL_TEST_COMPILE_OPTIONS_CORO_GCC_CLANG})
  target_compile_options(wal_tests_log_service_actor PRIVATE ${WAL_TEST_COMPILE_OPTIONS_CORO_GCC_CLANG})
  target_compile_options(wal_tests_log_io_simple PRIVATE ${WAL_TEST_COMPILE_OPTIONS_CORO_GCC_CLANG})
  target_compile_options(wal_tests_io_uring_writer PRIVATE ${WAL_TEST_COMPILE_OPTIONS_CORO_GCC_CLANG})
endif()

add_test(NAME wal.tests.bounded_channel COMMAND wal_tests_bounded_channel)
add_test(NAME wal.tests.logger COMMAND wal_tests_logger)
add_test(NAME wal.tests COMMAND wal_tests)
add_test(NAME wal.tests.tp COMMAND wal_tests_tp)
add_test(NAME wal.tests.spsc_per_producer COMMAND wal_tests_spsc_per_producer)
add_test(NAME wal.tests.actor_model COMMAND wal_tests_actor_model)

# Threads for concurrency tests
find_package(Threads REQUIRED)
target_link_libraries(wal_tests_bounded_channel PRIVATE Threads::Threads)
target_link_libraries(wal_tests_logger PRIVATE Threads::Threads)
target_link_libraries(wal_tests PRIVATE Threads::Threads wal::wal)
target_link_libraries(wal_tests_bounded_queue_perf PRIVATE Threads::Threads)
target_link_libraries(wal_tests_tp PRIVATE Threads::Threads)
target_link_libraries(wal_tests_spsc_per_producer PRIVATE Threads::Threads)
target_link_libraries(wal_tests_actor_model PRIVATE Threads::Threads)
target_link_libraries(wal_tests_log_service_actor PRIVATE Threads::Threads wal::wal)
target_link_libraries(wal_tests_log_io_simple PRIVATE Threads::Threads wal::wal)
target_link_libraries(wal_tests_io_uring_writer PRIVATE Threads::Threads wal::wal uring)

# Pass __atomic builtin availability to tests (bounded_channel.h is header-only)
if(WAL_USE_ATOMIC_BUILTINS)
  target_compile_definitions(wal_tests_bounded_channel PRIVATE WAL_USE_ATOMIC_BUILTINS)
  target_compile_definitions(wal_tests_bounded_queue_perf PRIVATE WAL_USE_ATOMIC_BUILTINS)
  target_compile_definitions(wal_tests_tp PRIVATE WAL_USE_ATOMIC_BUILTINS)
  target_compile_definitions(wal_tests_spsc_per_producer PRIVATE WAL_USE_ATOMIC_BUILTINS)
  target_compile_definitions(wal_tests_actor_model PRIVATE WAL_USE_ATOMIC_BUILTINS)
  target_compile_definitions(wal_tests_log_io_simple PRIVATE WAL_USE_ATOMIC_BUILTINS)
  target_compile_definitions(wal_tests_io_uring_writer PRIVATE WAL_USE_ATOMIC_BUILTINS)
endif()

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
    target_link_options(wal_tests_bounded_queue_perf PRIVATE -fuse-ld=mold)
    target_link_options(wal_tests_tp PRIVATE -fuse-ld=mold)
    target_link_options(wal_tests_spsc_per_producer PRIVATE -fuse-ld=mold)
    target_link_options(wal_tests_actor_model PRIVATE -fuse-ld=mold)
    target_link_options(wal_tests_log_io_simple PRIVATE -fuse-ld=mold)
    target_link_options(wal_tests_io_uring_writer PRIVATE -fuse-ld=mold)
  endif()
  target_link_options(wal_tests_bounded_channel PRIVATE -Wl,--build-id)
  target_link_options(wal_tests_logger PRIVATE -Wl,--build-id)
  target_link_options(wal_tests PRIVATE -Wl,--build-id)
  target_link_options(wal_tests_bounded_queue_perf PRIVATE -Wl,--build-id -flto)
  target_link_options(wal_tests_tp PRIVATE -Wl,--build-id)
  target_link_options(wal_tests_spsc_per_producer PRIVATE -Wl,--build-id)
    target_link_options(wal_tests_actor_model PRIVATE -Wl,--build-id)
    target_link_options(wal_tests_log_io_simple PRIVATE -Wl,--build-id)
    target_link_options(wal_tests_io_uring_writer PRIVATE -Wl,--build-id)
endif()

