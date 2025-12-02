#!/bin/bash
# Final debugging test

cd /Users/ahmet/X/Projects/Binalyze/plaso

echo "=============================================="
echo "STEP 1: Test DirectHTTPOutputStorageWriter"
echo "=============================================="

python3 test_minimal.py
if [ $? -ne 0 ]; then
    echo "❌ DirectHTTPOutputStorageWriter test failed!"
    exit 1
fi

echo ""
echo "=============================================="
echo "STEP 2: Kill any process on port 9098"
echo "=============================================="
lsof -ti :9098 | xargs kill -9 2>/dev/null || true
sleep 1
echo "✅ Port 9098 cleared"

echo ""
echo "=============================================="
echo "STEP 3: Start HTTP receiver"
echo "=============================================="
python3 debug_http_test.py --port 9098 > /tmp/receiver.log 2>&1 &
RECEIVER_PID=$!
sleep 2
echo "✅ Receiver running (PID: $RECEIVER_PID)"

echo ""
echo "=============================================="
echo "STEP 4: Run log2timeline with HTTP endpoint"
echo "=============================================="
echo ""
echo "Command:"
echo "  python3 plaso/scripts/log2timeline.py \\"
echo "    --http-endpoint 'http://localhost:9098/events' \\"
echo "    --single-process \\"
echo "    test_data/android_turbo.db"
echo ""

# Use 2>&1 to capture both stdout and stderr
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
timeout 60 python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/events' \
  --single-process \
  test_data/android_turbo.db \
  2>&1 | tee /tmp/log2timeline_output.log

EXIT_CODE=$?
echo ""
echo "=============================================="
echo "STEP 5: Results"
echo "=============================================="
echo "Exit code: $EXIT_CODE"
echo ""

echo "=== Key debug messages from log2timeline ==="
grep -E '🔍|🔧|✅|❌|🚀|🔹|📋|📝|📤' /tmp/log2timeline_output.log || echo "No debug messages found!"

echo ""
echo "=== HTTP Receiver Output ==="
cat /tmp/receiver.log

# Cleanup
kill $RECEIVER_PID 2>/dev/null || true

echo ""
echo "=============================================="
echo "DONE"
echo "=============================================="

