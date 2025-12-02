#!/usr/bin/env python3
"""Debug HTTP receiver to test log2timeline --http-endpoint"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import datetime

class DebugEventReceiver(BaseHTTPRequestHandler):
    total_events = 0
    total_batches = 0
    
    def do_POST(self):
        try:
            # Log the request
            timestamp = datetime.datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"\n[{timestamp}] 📥 POST request received", file=sys.stderr)
            print(f"  Path: {self.path}", file=sys.stderr)
            print(f"  Headers: {dict(self.headers)}", file=sys.stderr)
            
            # Get the content
            content_length = int(self.headers.get('Content-Length', 0))
            print(f"  Content-Length: {content_length} bytes", file=sys.stderr)
            
            if content_length == 0:
                print("  ⚠️  Empty request body!", file=sys.stderr)
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'Empty body')
                return
            
            post_data = self.rfile.read(content_length)
            print(f"  Raw data size: {len(post_data)} bytes", file=sys.stderr)
            
            # Try to parse as JSON
            try:
                batch = json.loads(post_data.decode('utf-8'))
                print(f"  ✅ Valid JSON received", file=sys.stderr)
                
                # Check structure
                if 'events' in batch:
                    events = batch['events']
                    DebugEventReceiver.total_batches += 1
                    DebugEventReceiver.total_events += len(events)
                    
                    print(f"  📦 Batch #{DebugEventReceiver.total_batches}: {len(events)} events", file=sys.stderr)
                    print(f"  📊 Total so far: {DebugEventReceiver.total_events} events in {DebugEventReceiver.total_batches} batches", file=sys.stderr)
                    
                    # Show first event details
                    if events:
                        event = events[0]
                        print(f"\n  First event in batch:", file=sys.stderr)
                        print(f"    data_type: {event.get('data_type', 'N/A')}", file=sys.stderr)
                        print(f"    timestamp: {event.get('timestamp', 'N/A')}", file=sys.stderr)
                        print(f"    parser: {event.get('parser', 'N/A')}", file=sys.stderr)
                        if 'message' in event:
                            msg = event['message'][:100] if len(event.get('message', '')) > 100 else event.get('message', '')
                            print(f"    message: {msg}", file=sys.stderr)
                else:
                    print(f"  ⚠️  No 'events' key in JSON", file=sys.stderr)
                    print(f"  Keys found: {list(batch.keys())}", file=sys.stderr)
                    
            except json.JSONDecodeError as e:
                print(f"  ❌ JSON parse error: {e}", file=sys.stderr)
                print(f"  First 200 bytes: {post_data[:200]}", file=sys.stderr)
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b'Invalid JSON')
                return
            except Exception as e:
                print(f"  ❌ Error processing: {e}", file=sys.stderr)
                import traceback
                traceback.print_exc(file=sys.stderr)
            
            # Send success response
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
            print(f"  ✅ Sent 200 OK response\n", file=sys.stderr)
            
        except Exception as e:
            print(f"  ❌ Fatal error in handler: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)
            self.send_response(500)
            self.end_headers()
    
    def log_message(self, format, *args):
        # Suppress default logging (we do our own)
        pass

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='Debug HTTP receiver for plaso')
    parser.add_argument('--port', type=int, default=9098, help='Port to listen on')
    parser.add_argument('--host', default='localhost', help='Host to bind to')
    args = parser.parse_args()
    
    server = HTTPServer((args.host, args.port), DebugEventReceiver)
    print(f'🚀 Debug HTTP receiver running on http://{args.host}:{args.port}', file=sys.stderr)
    print(f'📡 Waiting for events from plaso...', file=sys.stderr)
    print(f'', file=sys.stderr)
    print(f'Run plaso with:', file=sys.stderr)
    print(f'  log2timeline.py --http-endpoint http://{args.host}:{args.port}/plaso-output /path/to/evidence', file=sys.stderr)
    print(f'', file=sys.stderr)
    print(f'='*60, file=sys.stderr)
    print(f'', file=sys.stderr)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f'\n\n📊 Final Statistics:', file=sys.stderr)
        print(f'  Total batches received: {DebugEventReceiver.total_batches}', file=sys.stderr)
        print(f'  Total events received: {DebugEventReceiver.total_events}', file=sys.stderr)
        print(f'\n✅ Server stopped', file=sys.stderr)

