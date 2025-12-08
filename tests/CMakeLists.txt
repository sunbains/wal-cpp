# Define all test executables
set(TEST_TARGETS
  wal_tests_bounded_channel
  wal_tests_logger
  wal_tests
  wal_tests_bounded_queue_perf
  wal_tests_tp
  wal_tests_spsc_per_producer
  wal_tests_actor_model
  wal_tests_log_service_actor
  wal_tests_log_io_simple
)

# Create all test executables
foreach(TARGET ${TEST_TARGETS})
  get_filename_component(SOURCE_FILE ${TARGET} NAME_WE)
  if("${SOURCE_FILE}" STREQUAL "wal_tests")
    set(SOURCE_FILE "wal")
  else()
    string(REPLACE "wal_tests_" "" SOURCE_FILE ${SOURCE_FILE})
  endif()
  add_executable(${TARGET} test_${SOURCE_FILE}.cc)
endforeach()

# Common include directories for all tests
foreach(TARGET ${TEST_TARGETS})
  target_include_directories(${TARGET} PRIVATE ${CMAKE_SOURCE_DIR}/include)
endforeach()

# Common compile options for all tests (respect build type: -O0 for Debug, -O3 for others)
if(MSVC)
  set(TEST_COMPILE_OPTIONS
    /W4 /WX /Zi
    $<$<CONFIG:Debug>:/Od>
    $<$<NOT:$<CONFIG:Debug>>:/O2>
    ${SANITIZER_COMPILE_FLAGS}
  )
else()
  set(TEST_COMPILE_OPTIONS
    -Wall -Wextra -Wpedantic -Wconversion -Wsign-conversion
    -Werror
    -msse4.2
    -ggdb3
    -fno-omit-frame-pointer
    $<$<CONFIG:Debug>:-O0>
    $<$<NOT:$<CONFIG:Debug>>:-O3>
    ${SANITIZER_COMPILE_FLAGS}
  )

  # Performance test with LTO for non-Debug builds
  set(TEST_PERF_COMPILE_OPTIONS
    ${TEST_COMPILE_OPTIONS}
    $<$<NOT:$<CONFIG:Debug>>:-flto -march=native -mtune=native>
  )
endif()

# Apply compile options to all tests
foreach(TARGET ${TEST_TARGETS})
  if("${TARGET}" STREQUAL "wal_tests_bounded_queue_perf" AND NOT MSVC)
    target_compile_options(${TARGET} PRIVATE ${TEST_PERF_COMPILE_OPTIONS})
  else()
    target_compile_options(${TARGET} PRIVATE ${TEST_COMPILE_OPTIONS})
  endif()
endforeach()

# Register tests
add_test(NAME wal.tests.bounded_channel COMMAND wal_tests_bounded_channel)
add_test(NAME wal.tests.logger COMMAND wal_tests_logger)
add_test(NAME wal.tests COMMAND wal_tests)
add_test(NAME wal.tests.tp COMMAND wal_tests_tp)
add_test(NAME wal.tests.spsc_per_producer COMMAND wal_tests_spsc_per_producer)
add_test(NAME wal.tests.actor_model COMMAND wal_tests_actor_model)

# Link libraries
find_package(Threads REQUIRED)

# Tests that need wal library
set(TESTS_WITH_WAL
  wal_tests
  wal_tests_log_service_actor
  wal_tests_log_io_simple
)

foreach(TARGET ${TEST_TARGETS})
  target_link_libraries(${TARGET} PRIVATE Threads::Threads)
  list(FIND TESTS_WITH_WAL ${TARGET} INDEX)
  if(NOT INDEX EQUAL -1)
    target_link_libraries(${TARGET} PRIVATE wal::wal)
  endif()
endforeach()

# Pass __atomic builtin availability to tests
if(WAL_USE_ATOMIC_BUILTINS)
  foreach(TARGET ${TEST_TARGETS})
    target_compile_definitions(${TARGET} PRIVATE WAL_USE_ATOMIC_BUILTINS)
  endforeach()
endif()

# Force test_wal.cc and test_logger.cc to always be compiled with asserts enabled
if(MSVC)
  set_source_files_properties(test_wal.cc test_logger.cc PROPERTIES
    COMPILE_FLAGS "/U NDEBUG"
  )
else()
  set_source_files_properties(test_wal.cc test_logger.cc PROPERTIES
    COMPILE_FLAGS "-UNDEBUG"
  )
endif()

# Linker options (non-MSVC): build-id and mold if present
if(NOT MSVC)
  set(TEST_LINK_OPTIONS -Wl,--build-id ${SANITIZER_LINK_FLAGS})

  # Performance test gets LTO for non-Debug builds
  set(TEST_PERF_LINK_OPTIONS
    ${TEST_LINK_OPTIONS}
    $<$<NOT:$<CONFIG:Debug>>:-flto>
  )

  if(MOLD_LD)
    list(APPEND TEST_LINK_OPTIONS -fuse-ld=mold)
    list(APPEND TEST_PERF_LINK_OPTIONS -fuse-ld=mold)
  endif()

  foreach(TARGET ${TEST_TARGETS})
    if("${TARGET}" STREQUAL "wal_tests_bounded_queue_perf")
      target_link_options(${TARGET} PRIVATE ${TEST_PERF_LINK_OPTIONS})
    else()
      target_link_options(${TARGET} PRIVATE ${TEST_LINK_OPTIONS})
    endif()
  endforeach()
endif()
