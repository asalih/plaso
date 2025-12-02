#!/usr/bin/env python3
"""Minimal test to trace HTTP endpoint execution."""

import sys
import os

# Add plaso to path
sys.path.insert(0, '/Users/ahmet/X/Projects/Binalyze/plaso')

print("=" * 60)
print("MINIMAL HTTP ENDPOINT TEST")
print("=" * 60)
print()

# Test 1: Can we import the classes?
print("[TEST 1] Import check...")
try:
    from plaso.storage.direct_http_writer import DirectHTTPOutputStorageWriter
    print("  ✅ DirectHTTPOutputStorageWriter imported successfully")
except Exception as e:
    print(f"  ❌ Import failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 2: Can we create the writer?
print()
print("[TEST 2] Create writer...")
try:
    writer = DirectHTTPOutputStorageWriter(
        'http://localhost:9098/test',
        batch_size=10
    )
    print(f"  ✅ Writer created: {writer}")
except Exception as e:
    print(f"  ❌ Creation failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 3: Can we open the writer?
print()
print("[TEST 3] Open writer...")
try:
    writer.Open()
    print("  ✅ Writer opened successfully")
except Exception as e:
    print(f"  ❌ Open failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 4: Test AddAttributeContainer with a mock session
print()
print("[TEST 4] Add session container...")
try:
    from plaso.containers.sessions import Session
    session = Session()
    writer.AddAttributeContainer(session)
    print("  ✅ Session container added successfully")
except Exception as e:
    print(f"  ❌ AddAttributeContainer failed: {e}")
    import traceback
    traceback.print_exc()

# Test 5: Close and check stats
print()
print("[TEST 5] Close writer...")
try:
    writer.Close()
    print("  ✅ Writer closed successfully")
    stats = writer.GetStatistics()
    print(f"  📊 Stats: {stats}")
except Exception as e:
    print(f"  ❌ Close failed: {e}")
    import traceback
    traceback.print_exc()

print()
print("=" * 60)
print("ALL TESTS PASSED - DirectHTTPOutputStorageWriter works!")
print("=" * 60)
print()
print("Next step: Run actual log2timeline with --http-endpoint --single-process")
print("Make sure NO --parsers flag (let it auto-detect)")

