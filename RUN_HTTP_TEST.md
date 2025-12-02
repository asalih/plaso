# Debug HTTP Endpoint Issue

## Problem
The `--http-endpoint` parameter isn't posting data to the HTTP receiver.

## Debugging Steps

### Step 1: Start the Debug HTTP Receiver

```bash
# In Terminal 1
cd /Users/ahmet/X/Projects/Binalyze/plaso
python3 debug_http_test.py --port 9098
```

This will start a debug HTTP receiver that shows detailed information about every request received.

### Step 2: Run log2timeline with Debug Logging

```bash
# In Terminal 2
cd /Users/ahmet/X/Projects/Binalyze/plaso

PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 -u plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/plaso-output' \
  --consolidated_timestamps \
  --vss_stores none \
  --log-file /tmp/plaso_debug.log \
  --debug \
  '/Users/ahmet/Documents/AllDiskImages/plstestimg_disk'
```

The `-u` flag ensures unbuffered output so you see logs immediately.

### Step 3: Check the Logs

After running, check the log file:

```bash
# See if the Direct HTTP writer was created
grep "Direct HTTP" /tmp/plaso_debug.log

# See if events were processed
grep "events" /tmp/plaso_debug.log | head -20

# See the full log
tail -100 /tmp/plaso_debug.log
```

### Step 4: Simple Test with Small File

If the disk image is large, test with a small file first:

```bash
# Create a simple test file
echo "test log entry" > /tmp/test.log

# Run on the small file
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/plaso-output' \
  --log-file /tmp/plaso_test.log \
  '/tmp/test.log'
```

### Step 5: Test the HTTP Receiver Separately

Verify the HTTP receiver works:

```bash
# With the receiver running in Terminal 1, in Terminal 2:
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"events":[{"test":"data"}],"batch_size":1}' \
  http://localhost:9098/plaso-output
```

You should see the request logged in Terminal 1.

## Common Issues

### 1. HTTP Receiver Not Accessible

Check if the port is open:
```bash
lsof -i :9098
```

If something else is using it, change the port:
```bash
python3 debug_http_test.py --port 9099
# Then update the log2timeline command to use 9099
```

### 2. Import Errors

Verify imports work:
```bash
cd /Users/ahmet/X/Projects/Binalyze/plaso
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 -c "from plaso.storage.direct_http_writer import DirectHTTPOutputStorageWriter; print('OK')"
```

### 3. No Events Generated

The disk image might not have parseable files. Try with a known file:
```bash
# macOS system log
PYTHONPATH=/Users/ahmet/X/Projects/Binalyze/plaso \
python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/plaso-output' \
  /var/log/system.log
```

### 4. Events Generated But Not Sent

Check if events are being processed:
```bash
# Add this temporary debug line to direct_http_writer.py
# In the _process_event method, add:
#   logging.warning(f'Processing event: {self._events_processed}')
```

## Expected Output

### Terminal 1 (HTTP Receiver)
```
🚀 Debug HTTP receiver running on http://localhost:9098
📡 Waiting for events from plaso...
====================================

[14:23:45.123] 📥 POST request received
  Path: /plaso-output
  Content-Length: 15234 bytes
  ✅ Valid JSON received
  📦 Batch #1: 100 events
  📊 Total so far: 100 events in 1 batches
  
  First event in batch:
    data_type: fs:stat
    timestamp: 1234567890000000
    parser: filestat
    message: /path/to/file
  ✅ Sent 200 OK response
```

### Terminal 2 (log2timeline)
```
Checking availability and versions of dependencies.
[OK]

Source path     : /Users/ahmet/Documents/AllDiskImages/plstestimg_disk
Source type     : storage media image
Processing time : 00:00:00

Processing started.
...
```

## Quick Verification Script

Create a file `test_http.sh`:

```bash
#!/bin/bash
set -e

echo "Starting HTTP receiver in background..."
python3 debug_http_test.py --port 9098 &
RECEIVER_PID=$!
sleep 2

echo "Testing with curl..."
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"events":[{"test":"event 1"},{"test":"event 2"}],"batch_size":2}' \
  http://localhost:9098/test

echo -e "\n\nIf you see 'Batch #1: 2 events' above, the receiver works!"
echo "Press Ctrl+C when done"

wait $RECEIVER_PID
```

Run it:
```bash
chmod +x test_http.sh
./test_http.sh
```

## Next Steps

After running these tests, you should see:
1. Whether events are being generated
2. Whether they're reaching the HTTP writer
3. Whether they're being sent over HTTP
4. Any error messages

Report back what you see in the logs and HTTP receiver output!

