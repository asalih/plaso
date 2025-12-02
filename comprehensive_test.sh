#!/bin/bash
# Comprehensive test to figure out why no events are generated

cd /Users/ahmet/X/Projects/Binalyze/plaso

echo "==========================================="
echo "Comprehensive HTTP Endpoint Test"
echo "==========================================="
echo ""

# Kill anything on port 9098
lsof -ti :9098 | xargs kill 2>/dev/null || true
sleep 1

# Start receiver
echo "Starting HTTP receiver..."
python3 debug_http_test.py --port 9098 > /tmp/receiver_comprehensive.log 2>&1 &
RECEIVER_PID=$!
sleep 2
echo "✅ Receiver running (PID: $RECEIVER_PID)"
echo ""

# Test 1: Android Database
echo "==========================================="
echo "Test 1: Android Database"
echo "==========================================="

ANDROID_DB="/Users/ahmet/Documents/AllDiskImages/plstestimg_disk/pls_test_data/android_turbo.db"

if [ -f "$ANDROID_DB" ]; then
    echo "Testing: $ANDROID_DB"
    file "$ANDROID_DB"
    echo ""
    
    PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
    timeout 30 python3 plaso/scripts/log2timeline.py \
      --http-endpoint 'http://localhost:9098/test1' \
      "$ANDROID_DB" \
      2>&1 | tee /tmp/test1.log
    
    echo ""
    echo "Debug messages from Test 1:"
    grep -E '🔹|📝|📤' /tmp/test1.log | head -20
    echo ""
else
    echo "⚠️  Android DB not found at $ANDROID_DB"
fi

# Test 2: Plaso's test_data
echo "==========================================="
echo "Test 2: Plaso test_data directory"
echo "==========================================="

if [ -d "test_data" ]; then
    echo "Testing: test_data/"
    ls test_data/ | head -10
    echo ""
    
    PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
    timeout 30 python3 plaso/scripts/log2timeline.py \
      --http-endpoint 'http://localhost:9098/test2' \
      test_data/ \
      2>&1 | tee /tmp/test2.log
    
    echo ""
    echo "Debug messages from Test 2:"
    grep -E '🔹|📝|📤' /tmp/test2.log | head -20
    echo ""
else
    echo "⚠️  test_data directory not found"
fi

# Test 3: Created syslog file
echo "==========================================="
echo "Test 3: Created Syslog File"
echo "==========================================="

cat > /tmp/test.syslog << 'EOF'
Dec  1 10:30:15 hostname syslogd[123]: Test log entry 1
Dec  1 10:30:16 hostname kernel[0]: Test kernel message
Dec  1 10:30:17 hostname process[456]: Another test entry
EOF

echo "Created syslog file:"
cat /tmp/test.syslog
echo ""

PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
timeout 30 python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/test3' \
  --parsers 'syslog' \
  /tmp/test.syslog \
  2>&1 | tee /tmp/test3.log

echo ""
echo "Debug messages from Test 3:"
grep -E '🔹|📝|📤' /tmp/test3.log | head -20
echo ""

# Stop receiver
echo "==========================================="
echo "Stopping receiver..."
echo "==========================================="
kill $RECEIVER_PID 2>/dev/null || true
sleep 1

# Summary
echo ""
echo "==========================================="
echo "Summary of All Tests"
echo "==========================================="
echo ""

for i in 1 2 3; do
    if [ -f /tmp/test${i}.log ]; then
        CONTAINERS=$(grep -c "🔹 AddAttributeContainer" /tmp/test${i}.log 2>/dev/null || echo "0")
        EVENTS=$(grep -c "📝 Processing event" /tmp/test${i}.log 2>/dev/null || echo "0")
        BATCHES=$(grep -c "📤 Flushing batch" /tmp/test${i}.log 2>/dev/null || echo "0")
        
        echo "Test $i:"
        echo "  Containers received: $CONTAINERS"
        echo "  Events processed: $EVENTS"
        echo "  Batches sent: $BATCHES"
        echo ""
    fi
done

echo "==========================================="
echo "HTTP Receiver Log:"
echo "==========================================="
cat /tmp/receiver_comprehensive.log

echo ""
echo "Done!"

