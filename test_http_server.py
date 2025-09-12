#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Simple HTTP server for testing the HTTP streaming writer."""

import json
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
import time

class EventHandler(BaseHTTPRequestHandler):
    """HTTP request handler for receiving events."""
    
    def do_POST(self):
        """Handle POST requests with event data."""
        try:
            # Get the request body
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length)
            
            # Parse JSON
            data = json.loads(body.decode('utf-8'))
            
            # Log received data
            events = data.get('events', [])
            batch_size = data.get('batch_size', 0)
            timestamp = data.get('timestamp', 0)
            
            print(f"[{time.strftime('%H:%M:%S')}] Received batch with {batch_size} events")
            
            # Print first event as example
            if events:
                first_event = events[0]
                event_type = first_event.get('data_type', 'unknown')
                filename = first_event.get('filename', 'unknown')
                print(f"  Example event: {event_type} from {filename}")
            
            # Send success response
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            response = {
                'status': 'success',
                'received_events': batch_size,
                'timestamp': time.time()
            }
            
            self.wfile.write(json.dumps(response).encode('utf-8'))
            
        except Exception as e:
            print(f"Error processing request: {e}")
            self.send_response(500)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            error_response = {
                'status': 'error',
                'message': str(e)
            }
            
            self.wfile.write(json.dumps(error_response).encode('utf-8'))
    
    def log_message(self, format, *args):
        """Override to suppress default logging."""
        pass

def run_server(port=8888):
    """Run the test HTTP server."""
    server_address = ('', port)
    httpd = HTTPServer(server_address, EventHandler)
    
    print(f"Starting test HTTP server on port {port}...")
    print(f"Use this URL in log2timeline: http://localhost:{port}/events")
    print("Press Ctrl+C to stop the server")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down server...")
        httpd.shutdown()
        httpd.server_close()

if __name__ == '__main__':
    run_server()
