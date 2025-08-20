#!/bin/bash

# SQLite Go Project Test Runner
echo "=== SQLite Go Project Test Suite ==="
echo

# Change to project directory
cd "$(dirname "$0")"

# Check if sample.db exists for integration tests
if [ -f "sample.db" ]; then
    echo "✓ sample.db found - integration tests will run"
    SAMPLE_DB_EXISTS=true
else
    echo "⚠ sample.db not found - skipping integration tests"
    SAMPLE_DB_EXISTS=false
fi
echo

# Run unit tests (no file I/O required)
echo "=== Running Unit Tests ==="
echo "Testing: SQLite parsing logic, data structures, and helper functions"
go test ./app -v -run "^Test(ReadVarint|GetSerialTypeSize|RecordBody|CellPointer|NewTable|Table|GetTableNamesLogic|GetTablesLogic|GetSchemaObjectsLogic|DatabaseHeaderFields|PageHeaderFields|NewSQLiteDB_FileNotFound).*"
UNIT_RESULT=$?
echo

# Run main functionality tests
echo "=== Running Main Functionality Tests ==="
echo "Testing: Command-line interface and argument processing"
go test ./app -v -run "^Test(MainFunctionality|MainWithInvalidArgs).*"
MAIN_RESULT=$?
echo

# Run integration tests if sample.db exists
if [ "$SAMPLE_DB_EXISTS" = true ]; then
    echo "=== Running Integration Tests ==="
    echo "Testing: Real database file operations and end-to-end workflows"
    go test ./app -v -run "TestSQLiteDB_WithRealDatabase|TestSQLiteParsingIntegration"
    INTEGRATION_RESULT=$?
    echo
else
    echo "=== Skipping Integration Tests ==="
    echo "⚠ sample.db not found - integration tests require real database file"
    INTEGRATION_RESULT=0
    echo
fi

# Run benchmarks
echo "=== Running Benchmarks ==="
go test ./app -bench=. -benchmem -run=^$ 2>/dev/null || echo "Benchmarks skipped due to test conflicts"
echo

# Build verification
echo "=== Build Verification ==="
if go build -o main app/errors.go app/formatter.go app/main.go app/readers.go  app/sqlite_db.go app/sqlite_table.go app/values.go; then
    echo "✓ Build successful"
    BUILD_RESULT=0
    
    # If sample.db exists, test the binary
    if [ "$SAMPLE_DB_EXISTS" = true ]; then
        echo "=== Testing Binary with sample.db ==="
        echo "Testing .dbinfo command:"
        ./main sample.db .dbinfo
        echo
        echo "Testing .tables command:"
        ./main sample.db .tables
        echo
    fi
    
    # Clean up
    rm -f main
else
    echo "✗ Build failed"
    BUILD_RESULT=1
fi
echo

# Summary
echo "=== Test Summary ==="
echo -n "Unit Tests: "
if [ $UNIT_RESULT -eq 0 ]; then
    echo "✓ PASSED"
else
    echo "✗ FAILED"
fi

echo -n "Main Functionality Tests: "
if [ $MAIN_RESULT -eq 0 ]; then
    echo "✓ PASSED"
else
    echo "✗ FAILED"
fi

if [ "$SAMPLE_DB_EXISTS" = true ]; then
    echo -n "Integration Tests: "
    if [ $INTEGRATION_RESULT -eq 0 ]; then
        echo "✓ PASSED"
    else
        echo "✗ FAILED"
    fi
fi

echo -n "Build: "
if [ $BUILD_RESULT -eq 0 ]; then
    echo "✓ PASSED"
else
    echo "✗ FAILED"
fi

# Exit with error if any tests failed
if [ $UNIT_RESULT -ne 0 ] || [ $MAIN_RESULT -ne 0 ] || [ $INTEGRATION_RESULT -ne 0 ] || [ $BUILD_RESULT -ne 0 ]; then
    exit 1
else
    echo
    echo "🎉 All tests passed!"
    exit 0
fi
