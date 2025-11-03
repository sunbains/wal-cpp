#!/bin/bash
# Stress test for bounded_channel_coro to detect race conditions
# Usage: ./run_coro_stress_test.sh [iterations]

# Configuration
DEFAULT_ITERATIONS=100
ITERATIONS=${1:-$DEFAULT_ITERATIONS}
TIMEOUT=5

# Find the test binary
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$(dirname "$SCRIPT_DIR")/build/tests"
TEST_BIN="$BUILD_DIR/wal_tests_bounded_channel_coro"

# Try current directory if not found
if [ ! -f "$TEST_BIN" ]; then
    TEST_BIN="./wal_tests_bounded_channel_coro"
fi

# Try tests subdirectory
if [ ! -f "$TEST_BIN" ]; then
    TEST_BIN="./tests/wal_tests_bounded_channel_coro"
fi

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if test binary exists
if [ ! -f "$TEST_BIN" ]; then
    echo -e "${RED}Error: Test binary not found${NC}"
    echo "Searched in:"
    echo "  - $BUILD_DIR/wal_tests_bounded_channel_coro"
    echo "  - ./wal_tests_bounded_channel_coro"
    echo "  - ./tests/wal_tests_bounded_channel_coro"
    echo ""
    echo "Please run from the build directory, or build the tests first"
    exit 1
fi

echo "========================================="
echo "Bounded Channel Coro Stress Test"
echo "========================================="
echo "Test binary: $TEST_BIN"
echo "Iterations:  $ITERATIONS"
echo "Timeout:     ${TIMEOUT}s per run"
echo ""

# Statistics
passed=0
failed=0
start_time=$(date +%s)

# Create temp directory for failure logs
temp_dir=$(mktemp -d)
trap "rm -rf $temp_dir" EXIT

echo "Running tests..."
for i in $(seq 1 $ITERATIONS); do
    # Run test with timeout
    if timeout $TIMEOUT "$TEST_BIN" > "$temp_dir/output_$i.txt" 2>&1; then
        ((passed++))

        # Progress indicator every 10 runs
        if [ $((i % 10)) -eq 0 ]; then
            echo -ne "Progress: $i/$ITERATIONS runs completed... \r"
        fi
    else
        ((failed++))
        exit_code=$?

        echo ""
        echo -e "${RED}=========================================${NC}"
        echo -e "${RED}FAILED on run $i${NC}"
        echo -e "${RED}=========================================${NC}"
        echo ""
        echo "Exit code: $exit_code"
        echo ""
        echo "Last 20 lines of output:"
        tail -20 "$temp_dir/output_$i.txt"
        echo ""
        echo "Full output saved to: $temp_dir/output_$i.txt"

        # Ask if user wants to continue
        echo ""
        read -p "Continue testing? (y/n) " -n 1 -r
        echo ""
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            break
        fi
    fi
done

# Calculate statistics
end_time=$(date +%s)
elapsed=$((end_time - start_time))
total=$((passed + failed))

echo ""
echo "========================================="
echo "Test Results"
echo "========================================="
echo -e "Total runs:    $total"
echo -e "Passed:        ${GREEN}$passed${NC}"
echo -e "Failed:        ${RED}$failed${NC}"

if [ $total -gt 0 ]; then
    success_rate=$(awk "BEGIN {printf \"%.2f\", ($passed / $total) * 100}")
    echo -e "Success rate:  $success_rate%"
fi

echo -e "Elapsed time:  ${elapsed}s"

if [ $passed -gt 0 ]; then
    avg_time=$(awk "BEGIN {printf \"%.2f\", $elapsed / $passed}")
    echo -e "Avg per test:  ${avg_time}s"
fi

echo "========================================="

# Exit with failure if any tests failed
if [ $failed -gt 0 ]; then
    echo ""
    echo -e "${YELLOW}Warning: $failed test(s) failed${NC}"
    echo "This may indicate a race condition or timing-dependent bug"
    exit 1
else
    echo ""
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi
