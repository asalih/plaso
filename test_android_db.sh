#!/bin/bash
# Test with the Android database file

cd /Users/ahmet/X/Projects/Binalyze/plaso

ANDROID_DB="/Users/ahmet/Documents/AllDiskImages/plstestimg_disk/pls_test_data/android_turbo.db"

echo "==========================================="
echo "Testing HTTP Endpoint with Android DB"
echo "==========================================="
echo ""

# Check if file exists
if [ ! -f "$ANDROID_DB" ]; then
    echo "❌ Android DB file not found: $ANDROID_DB"
    echo ""
    echo "Looking for test files..."
    find /Users/ahmet/Documents/AllDiskImages/plstestimg_disk -type f -name "*.db" 2>/dev/null | head -10
    exit 1
fi

echo "✅ Found Android DB: $ANDROID_DB"
ls -lh "$ANDROID_DB"
echo ""

# Check file type
echo "File type:"
file "$ANDROID_DB"
echo ""

# Kill any existing process on port 9098
echo "Preparing HTTP receiver..."
lsof -ti :9098 | xargs kill 2>/dev/null || true
sleep 1

# Start receiver
python3 debug_http_test.py --port 9098 > /tmp/receiver_android.log 2>&1 &
RECEIVER_PID=$!
sleep 2

echo "Receiver PID: $RECEIVER_PID"
echo ""
echo "==========================================="
echo "Running log2timeline on Android DB..."
echo "==========================================="
echo ""

# Run log2timeline
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/android' \
  "$ANDROID_DB" \
  2>&1 | tee /tmp/android_output.log

echo ""
echo "==========================================="
echo "Stopping receiver..."
echo "==========================================="
kill $RECEIVER_PID 2>/dev/null || true
sleep 1

echo ""
echo "==========================================="
echo "Results:"
echo "==========================================="
echo ""

echo "=== Debug Messages ==="
grep -E '🔍|🔧|✅|📝|📤' /tmp/android_output.log | head -30

echo ""
echo "=== HTTP Receiver Log ==="
cat /tmp/receiver_android.log

echo ""
echo "=== Statistics ==="
EVENT_COUNT=$(grep -c "📝 Processing event" /tmp/android_output.log 2>/dev/null || echo "0")
BATCH_COUNT=$(grep -c "📤 Flushing batch" /tmp/android_output.log 2>/dev/null || echo "0")
echo "Events processed: $EVENT_COUNT"
echo "Batches sent: $BATCH_COUNT"

echo ""
if [ "$EVENT_COUNT" -gt 0 ]; then
    echo "✅ SUCCESS! Events generated and sent via HTTP!"
else
    echo "❌ No events generated from Android DB"
    echo ""
    echo "Checking parser output..."
    grep -i "parser\|processing\|android" /tmp/android_output.log | head -10
fi

echo ""
echo "Done!"

