#!/bin/bash
# Test with a simple file to verify HTTP endpoint works

cd /Users/ahmet/X/Projects/Binalyze/plaso

echo "Creating test files..."

# Create a simple test file
echo "This is a test log entry" > /tmp/test_simple.txt

# Create a test evtx file (just a text file for now)
echo "Event log test" > /tmp/test.log

echo ""
echo "==========================================="
echo "Starting HTTP receiver..."
echo "==========================================="

# Kill any existing process on port 9098
lsof -ti :9098 | xargs kill 2>/dev/null || true
sleep 1

# Start receiver
python3 debug_http_test.py --port 9098 > /tmp/receiver_simple.log 2>&1 &
RECEIVER_PID=$!
sleep 2

echo "Receiver PID: $RECEIVER_PID"
echo ""
echo "==========================================="
echo "Running log2timeline on simple file..."
echo "==========================================="
echo ""

# Run log2timeline with the simple file
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/test' \
  /tmp/test_simple.txt \
  2>&1 | tee /tmp/simple_test_output.log

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
grep -E '🔍|🔧|✅|📝|📤' /tmp/simple_test_output.log || echo "No debug messages"

echo ""
echo "=== HTTP Receiver Log ==="
cat /tmp/receiver_simple.log

echo ""
echo "=== Event Count ==="
grep -c "📝 Processing event" /tmp/simple_test_output.log 2>/dev/null || echo "0 events"

echo ""
echo "Done!"

