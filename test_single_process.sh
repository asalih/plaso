#!/bin/bash
# Test with single process mode

cd /Users/ahmet/X/Projects/Binalyze/plaso

echo "==========================================="
echo "Single Process Mode HTTP Test"
echo "==========================================="
echo ""

# Kill anything on port 9098
lsof -ti :9098 | xargs kill 2>/dev/null || true
sleep 1

# Start receiver
echo "Starting HTTP receiver..."
python3 debug_http_test.py --port 9098 > /tmp/receiver_single.log 2>&1 &
RECEIVER_PID=$!
sleep 2
echo "✅ Receiver running (PID: $RECEIVER_PID)"
echo ""

# Create syslog file
cat > /tmp/test.syslog << 'EOF'
Dec  1 10:30:15 hostname syslogd[123]: Test log entry 1
Dec  1 10:30:16 hostname kernel[0]: Test kernel message
Dec  1 10:30:17 hostname process[456]: Another test entry
EOF

echo "Created syslog file:"
cat /tmp/test.syslog
echo ""

echo "Running log2timeline with single-process mode..."
echo ""

# Run with --single-process flag
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/test' \
  --single-process \
  --parsers 'syslog' \
  /tmp/test.syslog \
  2>&1 | tee /tmp/single_process_test.log

echo ""
echo "==========================================="
echo "Looking for debug messages..."
echo "==========================================="

echo ""
echo "=== Session message ==="
grep '📋' /tmp/single_process_test.log || echo "No session message found"

echo ""
echo "=== Container messages ==="
grep '🔹' /tmp/single_process_test.log | head -10 || echo "No container messages found"

echo ""
echo "=== Event processing messages ==="
grep '📝' /tmp/single_process_test.log | head -10 || echo "No event messages found"

echo ""
echo "=== Batch messages ==="
grep '📤' /tmp/single_process_test.log || echo "No batch messages found"

echo ""
echo "==========================================="
echo "HTTP Receiver Log:"
echo "==========================================="
cat /tmp/receiver_single.log

# Stop receiver
echo ""
kill $RECEIVER_PID 2>/dev/null || true

echo ""
echo "Done!"

