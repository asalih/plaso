#!/bin/bash
# Debug script to test --http-endpoint

set -x  # Print commands as they execute

cd /Users/ahmet/X/Projects/Binalyze/plaso

echo "==========================================="
echo "Starting HTTP Debug Test"
echo "==========================================="
echo ""

# Kill any existing process on port 9098
echo "Checking if port 9098 is in use..."
EXISTING_PID=$(lsof -ti :9098 2>/dev/null || true)
if [ ! -z "$EXISTING_PID" ]; then
    echo "⚠️  Port 9098 is in use by PID $EXISTING_PID, killing it..."
    kill $EXISTING_PID 2>/dev/null || true
    sleep 1
fi

# Start HTTP receiver in background
echo "Starting HTTP receiver on port 9098..."
python3 debug_http_test.py --port 9098 > /tmp/http_receiver.log 2>&1 &
RECEIVER_PID=$!
echo "HTTP receiver PID: $RECEIVER_PID"

# Give it time to start
sleep 2

# Check if receiver is running
if ps -p $RECEIVER_PID > /dev/null; then
    echo "✅ HTTP receiver is running"
else
    echo "❌ HTTP receiver failed to start"
    cat /tmp/http_receiver.log
    exit 1
fi

echo ""
echo "==========================================="
echo "Running log2timeline with --http-endpoint"
echo "==========================================="
echo ""

# Run log2timeline with all output to stderr so we can see it
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 -u plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/plaso-output' \
  --consolidated_timestamps \
  --vss_stores none \
  '/Users/ahmet/Documents/AllDiskImages/plstestimg_disk' \
  2>&1 | tee /tmp/log2timeline_output.log

EXIT_CODE=$?

echo ""
echo "==========================================="
echo "log2timeline exit code: $EXIT_CODE"
echo "==========================================="
echo ""

# Stop the receiver
echo "Stopping HTTP receiver..."
kill $RECEIVER_PID 2>/dev/null || true
wait $RECEIVER_PID 2>/dev/null || true

echo ""
echo "==========================================="
echo "HTTP Receiver Log:"
echo "==========================================="
cat /tmp/http_receiver.log

echo ""
echo "==========================================="
echo "log2timeline Output (last 50 lines):"
echo "==========================================="
tail -50 /tmp/log2timeline_output.log

echo ""
echo "==========================================="
echo "Looking for debug messages:"
echo "==========================================="
grep -E '🔍|🔧|✅|❌|🚀|📝|📤' /tmp/log2timeline_output.log || echo "No debug messages found"

echo ""
echo "Test complete!"

