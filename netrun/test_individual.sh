#!/bin/bash

# Script to run each test individually with a timeout to identify hanging tests
# Works on macOS (uses background process + sleep instead of timeout command)

cd /Users/lukastk/dev/20260113_w3pmcj__netrun2/netrun

TIMEOUT_SECONDS=120
TEST_DIR="src/tests"

# Function to run command with timeout (macOS compatible)
run_with_timeout() {
    local timeout=$1
    shift

    # Run command in background
    "$@" &
    local pid=$!

    # Wait for either completion or timeout
    local count=0
    while kill -0 $pid 2>/dev/null; do
        if [ $count -ge $timeout ]; then
            kill -9 $pid 2>/dev/null
            wait $pid 2>/dev/null
            return 124  # Same as GNU timeout exit code for timeout
        fi
        sleep 1
        count=$((count + 1))
    done

    wait $pid
    return $?
}

# Get list of all tests
echo "Collecting tests..."
TESTS=""
for FILE in $(find src/tests -name "test_*.py" -type f); do
    FUNCS=$(grep -E "^def test_|^async def test_" "$FILE" | sed 's/def //' | sed 's/async //' | sed 's/(.*$//')
    for FUNC in $FUNCS; do
        TESTS="$TESTS$FILE::$FUNC"$'\n'
    done
done

TOTAL=$(echo "$TESTS" | grep -c "::" || echo 0)
echo "Found $TOTAL tests"
echo "Running each test with ${TIMEOUT_SECONDS}s timeout..."
echo "=========================================="

FAILED_TESTS=""
TIMEOUT_TESTS=""
PASSED_COUNT=0
FAILED_COUNT=0
TIMEOUT_COUNT=0

COUNT=0
while IFS= read -r TEST; do
    [ -z "$TEST" ] && continue

    COUNT=$((COUNT + 1))
    echo -n "[$COUNT/$TOTAL] $TEST ... "

    START=$(date +%s)
    run_with_timeout $TIMEOUT_SECONDS uv run pytest "$TEST" -x -q --tb=no 2>&1 >/dev/null
    EXIT_CODE=$?
    END=$(date +%s)
    DURATION=$((END - START))

    if [ $EXIT_CODE -eq 124 ]; then
        echo "TIMEOUT (${DURATION}s)"
        TIMEOUT_TESTS="$TIMEOUT_TESTS$TEST"$'\n'
        TIMEOUT_COUNT=$((TIMEOUT_COUNT + 1))
    elif [ $EXIT_CODE -eq 0 ]; then
        echo "PASSED (${DURATION}s)"
        PASSED_COUNT=$((PASSED_COUNT + 1))
    else
        echo "FAILED (${DURATION}s, exit=$EXIT_CODE)"
        FAILED_TESTS="$FAILED_TESTS$TEST"$'\n'
        FAILED_COUNT=$((FAILED_COUNT + 1))
    fi
done <<< "$TESTS"

echo "=========================================="
echo "SUMMARY"
echo "=========================================="
echo "Passed: $PASSED_COUNT"
echo "Failed: $FAILED_COUNT"
echo "Timeout: $TIMEOUT_COUNT"

if [ -n "$FAILED_TESTS" ]; then
    echo ""
    echo "FAILED TESTS:"
    echo "$FAILED_TESTS" | while read -r T; do
        [ -n "$T" ] && echo "  - $T"
    done
fi

if [ -n "$TIMEOUT_TESTS" ]; then
    echo ""
    echo "TIMEOUT TESTS (hung for ${TIMEOUT_SECONDS}s):"
    echo "$TIMEOUT_TESTS" | while read -r T; do
        [ -n "$T" ] && echo "  - $T"
    done
fi
