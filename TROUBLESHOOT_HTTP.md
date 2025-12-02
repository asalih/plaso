# Troubleshooting HTTP Endpoint - Not Posting Data

## Issue
Running this command doesn't post data to the HTTP endpoint:
```bash
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 /Users/ahmet/X/Projects/Binalyze/plaso/plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/plaso-output' \
  --consolidated_timestamps \
  --vss_stores none \
  '/Users/ahmet/Documents/AllDiskImages/plstestimg_disk'
```

## What I've Added

I've added extensive **debug logging** to help us see exactly what's happening:

1. **When the HTTP writer opens** - Shows configuration
2. **When events are processed** - Shows first 3 events + every 1000th event
3. **When batches are sent** - Shows batch size and success/failure
4. **When the writer closes** - Shows final statistics

## Step-by-Step Debug Process

### 1. Start the Debug HTTP Receiver

```bash
# Terminal 1
cd /Users/ahmet/X/Projects/Binalyze/plaso
python3 debug_http_test.py --port 9098
```

This will show detailed logs of every HTTP request received.

### 2. Run log2timeline with Logging

```bash
# Terminal 2
cd /Users/ahmet/X/Projects/Binalyze/plaso

# Run with output to both screen and log file
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/plaso-output' \
  --consolidated_timestamps \
  --vss_stores none \
  '/Users/ahmet/Documents/AllDiskImages/plstestimg_disk' 2>&1 | tee /tmp/plaso_run.log
```

### 3. What to Look For

**In Terminal 2 (log2timeline output), you should see:**

```
🚀 Direct HTTP writer opened, sending to: http://localhost:9098/plaso-output
   Batch size: 100, Flush interval: 5.0s
   Event filter: False
   Consolidated timestamps: True
```

If you DON'T see this, the HTTP writer wasn't created - there's a configuration issue.

**Then you should see:**

```
📝 Processing event #1
📝 Processing event #2
📝 Processing event #3
📊 Processed 1000 events...
📊 Processed 2000 events...
```

If you DON'T see this, no events are being generated from the disk image.

**Then you should see:**

```
📤 Flushing batch of 100 events to http://localhost:9098/plaso-output
✅ Batch #1 sent successfully (100 events)
📤 Flushing batch of 100 events to http://localhost:9098/plaso-output
✅ Batch #2 sent successfully (100 events)
```

If you DON'T see this, events aren't being sent to HTTP.

**In Terminal 1 (HTTP receiver), you should see:**

```
[14:23:45.123] 📥 POST request received
  Path: /plaso-output
  Content-Length: 15234 bytes
  ✅ Valid JSON received
  📦 Batch #1: 100 events
```

If you DON'T see this, the HTTP requests aren't reaching the receiver.

## Common Problems & Solutions

### Problem 1: HTTP Writer Not Created

**Symptom:** No "🚀 Direct HTTP writer opened" message

**Causes:**
- Wrong command-line syntax
- Argument parsing issue

**Solution:**
```bash
# Verify the parameter is recognized
python3 plaso/scripts/log2timeline.py --help | grep http-endpoint
```

### Problem 2: No Events Generated

**Symptom:** No "📝 Processing event" messages

**Causes:**
- Disk image has no parseable data
- Parsers aren't finding files

**Solution:**
```bash
# Test with a simple known file
echo "test" > /tmp/test.txt
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/plaso-output' \
  /tmp/test.txt
```

### Problem 3: Events Generated But Not Sent

**Symptom:** See "Processing event" but no "Flushing batch" messages

**Causes:**
- Buffer not filling up (less than 100 events)
- Flush interval not triggering

**Solution:**
Wait 5 seconds after processing completes - the flush interval will trigger.
Or test with a file that generates 100+ events.

### Problem 4: HTTP Requests Not Reaching Receiver

**Symptom:** See "Flushing batch" but Terminal 1 shows nothing

**Causes:**
- HTTP receiver not running
- Wrong port
- Firewall blocking

**Solution:**
```bash
# Test the receiver works
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"events":[{"test":"data"}]}' \
  http://localhost:9098/plaso-output
```

## Quick Test

To quickly verify everything works:

```bash
# Terminal 1
python3 debug_http_test.py --port 9098

# Terminal 2 - Test with system log (macOS)
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/test' \
  --parsers 'syslog' \
  /var/log/system.log 2>&1 | grep -E '🚀|📝|📤|✅'
```

You should see:
```
🚀 Direct HTTP writer opened...
📝 Processing event #1
📝 Processing event #2
📝 Processing event #3
📤 Flushing batch of 100 events...
✅ Batch #1 sent successfully (100 events)
```

## Get Full Diagnostic Output

Run this to save all logs:

```bash
cd /Users/ahmet/X/Projects/Binalyze/plaso

# Start receiver in background
python3 debug_http_test.py --port 9098 > /tmp/receiver.log 2>&1 &
RECEIVER_PID=$!

# Run log2timeline
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/plaso-output' \
  --consolidated_timestamps \
  '/Users/ahmet/Documents/AllDiskImages/plstestimg_disk' \
  > /tmp/log2timeline.log 2>&1

# Stop receiver
kill $RECEIVER_PID

# Check logs
echo "=== LOG2TIMELINE OUTPUT ==="
cat /tmp/log2timeline.log | grep -E '🚀|📝|📤|✅|❌|ERROR'

echo -e "\n=== HTTP RECEIVER OUTPUT ==="
cat /tmp/receiver.log
```

## What to Report Back

After running the debug steps, please share:

1. **Do you see "🚀 Direct HTTP writer opened"?** (Yes/No)
2. **Do you see "📝 Processing event" messages?** (Yes/No + how many)
3. **Do you see "📤 Flushing batch" messages?** (Yes/No + how many)
4. **Does the HTTP receiver show any requests?** (Yes/No)
5. **Any error messages?** (Copy/paste them)

This will help us pinpoint exactly where the issue is!

## Expected Behavior

For a disk image with events, you should see:

**Terminal 2 (log2timeline):**
```
Checking availability and versions of dependencies.
[OK]

🚀 Direct HTTP writer opened, sending to: http://localhost:9098/plaso-output
   Batch size: 100, Flush interval: 5.0s
...
📝 Processing event #1
📝 Processing event #2
📝 Processing event #3
📊 Processed 1000 events...
📤 Flushing batch of 100 events to http://localhost:9098/plaso-output
✅ Batch #1 sent successfully (100 events)
...
Direct HTTP writer closed. Stats: 2547 events sent, 0 events filtered, 25 batches sent, 0 batches failed
```

**Terminal 1 (HTTP receiver):**
```
📥 POST request received
  📦 Batch #1: 100 events
  First event:
    data_type: fs:stat
    timestamp: 1234567890000000
✅ Sent 200 OK response

📥 POST request received
  📦 Batch #2: 100 events
✅ Sent 200 OK response
```

If you see all of this, it's working! 🎉

