#!/usr/bin/env python3
"""Test if --http-endpoint argument is being parsed correctly"""

import sys
sys.path.insert(0, '/Users/ahmet/X/Projects/Binalyze/plaso')

from plaso.cli import log2timeline_tool

# Create the tool
tool = log2timeline_tool.Log2TimelineTool()

# Simulate the command line arguments
test_args = [
    '--http-endpoint', 'http://localhost:9098/plaso-output',
    '--consolidated_timestamps',
    '--vss_stores', 'none',
    '/Users/ahmet/Documents/AllDiskImages/plstestimg_disk'
]

print("Testing argument parsing...")
print(f"Args: {test_args}")
print()

# Parse arguments
result = tool.ParseArguments(test_args)
print(f"ParseArguments result: {result}")
print()

# Check what was set
print(f"tool.http_endpoint = {repr(tool.http_endpoint)}")
print(f"tool.json_stdout = {tool.json_stdout}")
print(f"tool.consolidated_timestamps = {tool.consolidated_timestamps}")
print()

# Check if _event_filter was set
if hasattr(tool, '_event_filter'):
    print(f"tool._event_filter = {tool._event_filter}")
else:
    print("tool._event_filter not set")

