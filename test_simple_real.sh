#!/bin/bash
# Simple test with REAL Plaso test data

cd /Users/ahmet/X/Projects/Binalyze/plaso

echo "==========================================="
echo "Simple Real Test - No Parser Filter"
echo "==========================================="
echo ""

# Kill anything on port 9098
lsof -ti :9098 | xargs kill -9 2>/dev/null || true
sleep 1

# Start receiver
echo "Starting HTTP receiver..."
python3 debug_http_test.py --port 9098 > /tmp/receiver_real.log 2>&1 &
RECEIVER_PID=$!
sleep 2
echo "✅ Receiver running (PID: $RECEIVER_PID)"
echo ""

# Use actual test file (Chrome cookies)
TEST_FILE="test_data/Cookies.binarycookies"

if [ ! -f "$TEST_FILE" ]; then
    echo "⚠️  Test file not found: $TEST_FILE"
    kill $RECEIVER_PID 2>/dev/null
    exit 1
fi

echo "Testing with: $TEST_FILE"
file "$TEST_FILE"
echo ""

# Run WITHOUT parser filter - let Plaso choose
echo "Running log2timeline..."
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
timeout 30 python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/test' \
  --single-process \
  "$TEST_FILE" \
  2>&1 | tee /tmp/test_real.log

echo ""
echo "==========================================="
echo "Checking for debug output..."
echo "==========================================="

echo ""
echo "=== Direct HTTP writer messages ==="
grep '🚀🚀🚀' /tmp/test_real.log || echo "❌ Direct HTTP writer not created"

echo ""
echo "=== Container messages ==="
grep '🔹' /tmp/test_real.log | head -5 || echo "❌ No containers"

echo ""
echo "=== Event processing ==="
grep '📝' /tmp/test_real.log | head -5 || echo "❌ No events"

echo ""
echo "=== HTTP batches ==="
grep '📤' /tmp/test_real.log || echo "❌ No batches sent"

echo ""
echo "==========================================="
echo "HTTP Receiver Output:"
echo "==========================================="
cat /tmp/receiver_real.log

echo ""
echo "==========================================="
echo "Last 20 lines of log2timeline output:"
echo "==========================================="
tail -20 /tmp/test_real.log

# Stop receiver
kill $RECEIVER_PID 2>/dev/null || true

echo ""
echo "Done!"

