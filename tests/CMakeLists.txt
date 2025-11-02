add_executable(wal_tests
  test_wal.cc
)

target_link_libraries(wal_tests PRIVATE wal::wal)
target_compile_features(wal_tests PRIVATE cxx_std_23)

if(MSVC)
  target_compile_options(wal_tests PRIVATE /W4)
else()
  target_compile_options(wal_tests PRIVATE -Wall -Wextra -Wpedantic)
endif()

add_test(NAME wal.tests.basic COMMAND wal_tests)

