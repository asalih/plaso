#!/bin/bash
# Test with actual parseable files

cd /Users/ahmet/X/Projects/Binalyze/plaso

echo "==========================================="
echo "Testing HTTP Endpoint with Real Files"
echo "==========================================="
echo ""

# Kill any existing process on port 9098
lsof -ti :9098 | xargs kill 2>/dev/null || true
sleep 1

# Start receiver
echo "Starting HTTP receiver on port 9098..."
python3 debug_http_test.py --port 9098 > /tmp/receiver_real.log 2>&1 &
RECEIVER_PID=$!
sleep 2

echo "Receiver PID: $RECEIVER_PID"
echo ""

# Test 1: Use test_data if available
if [ -d "test_data" ]; then
    echo "==========================================="
    echo "Test 1: Using plaso test_data directory"
    echo "==========================================="
    
    PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
    timeout 60 python3 plaso/scripts/log2timeline.py \
      --http-endpoint 'http://localhost:9098/test' \
      test_data/ \
      2>&1 | tee /tmp/test_output.log | grep -E '🔍|🔧|✅|📝|📤|Processing'
    
elif [ -f "/var/log/system.log" ]; then
    echo "==========================================="
    echo "Test 2: Using macOS system log"
    echo "==========================================="
    
    PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
    timeout 60 python3 plaso/scripts/log2timeline.py \
      --http-endpoint 'http://localhost:9098/test' \
      --parsers 'syslog' \
      /var/log/system.log \
      2>&1 | tee /tmp/test_output.log | grep -E '🔍|🔧|✅|📝|📤|Processing'
      
else
    echo "==========================================="
    echo "Test 3: Creating a proper syslog file"
    echo "==========================================="
    
    # Create a proper syslog format file
    cat > /tmp/test_syslog.log << 'EOF'
Dec  1 10:30:15 hostname syslogd[123]: Test log entry 1
Dec  1 10:30:16 hostname kernel[0]: Test kernel message
Dec  1 10:30:17 hostname process[456]: Another test entry
Dec  1 10:30:18 hostname daemon[789]: Yet another message
Dec  1 10:30:19 hostname service[1011]: Final test message
EOF

    echo "Created syslog file with 5 entries"
    echo ""
    
    PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
    python3 plaso/scripts/log2timeline.py \
      --http-endpoint 'http://localhost:9098/test' \
      --parsers 'syslog' \
      /tmp/test_syslog.log \
      2>&1 | tee /tmp/test_output.log | grep -E '🔍|🔧|✅|📝|📤|Processing'
fi

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
grep -E '🔍|🔧|✅|📝|📤' /tmp/test_output.log 2>/dev/null | head -20 || echo "No debug messages"

echo ""
echo "=== HTTP Receiver Log (First 30 lines) ==="
head -30 /tmp/receiver_real.log

echo ""
echo "=== Event Count ==="
EVENT_COUNT=$(grep -c "📝 Processing event" /tmp/test_output.log 2>/dev/null || echo "0")
echo "Events processed: $EVENT_COUNT"

echo ""
echo "=== Batch Count ==="
BATCH_COUNT=$(grep -c "📤 Flushing batch" /tmp/test_output.log 2>/dev/null || echo "0")
echo "Batches sent: $BATCH_COUNT"

echo ""
if [ "$EVENT_COUNT" -gt 0 ]; then
    echo "✅ SUCCESS! HTTP endpoint is working - events were generated and sent!"
else
    echo "❌ FAIL: No events were generated"
    echo ""
    echo "This could mean:"
    echo "1. The test file format isn't recognized by any parser"
    echo "2. Parsers are disabled or not loading"
    echo "3. There's an issue with the extraction engine"
fi

echo ""
echo "Done!"

