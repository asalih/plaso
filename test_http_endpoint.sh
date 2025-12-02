#!/bin/bash
# Quick test script to verify --http-endpoint works

set -e

echo "🧪 Testing Direct HTTP Output Storage Writer"
echo "=============================================="
echo ""

# Create a simple HTTP receiver
cat > /tmp/http_receiver.py << 'EOF'
#!/usr/bin/env python3
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys

class EventReceiver(BaseHTTPRequestHandler):
    events_received = 0
    
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        batch = json.loads(post_data.decode('utf-8'))
        
        events = batch.get('events', [])
        EventReceiver.events_received += len(events)
        
        print(f"✅ Received batch of {len(events)} events (total: {EventReceiver.events_received})", file=sys.stderr)
        
        # Send success response
        self.send_response(200)
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(b'OK')
    
    def log_message(self, format, *args):
        pass

if __name__ == '__main__':
    server = HTTPServer(('localhost', 8765), EventReceiver)
    print('HTTP receiver ready on http://localhost:8765/events', file=sys.stderr)
    server.serve_forever()
EOF

chmod +x /tmp/http_receiver.py

# Start HTTP receiver in background
python3 /tmp/http_receiver.py &
RECEIVER_PID=$!

# Give it a moment to start
sleep 2

echo "✅ HTTP receiver started (PID: $RECEIVER_PID)"
echo ""

# Test with --http-endpoint
echo "🔧 Running log2timeline with --http-endpoint..."
echo ""

# You can test with your own data here
# Example: tools/log2timeline.py --http-endpoint http://localhost:8765/events test_data/

echo "To test manually, run:"
echo "  tools/log2timeline.py --http-endpoint http://localhost:8765/events /path/to/evidence"
echo ""
echo "Press Ctrl+C to stop the HTTP receiver"
echo ""

# Wait for user to kill or test completes
wait $RECEIVER_PID 2>/dev/null || true

# Cleanup
kill $RECEIVER_PID 2>/dev/null || true
rm -f /tmp/http_receiver.py

echo ""
echo "✅ Test complete!"

