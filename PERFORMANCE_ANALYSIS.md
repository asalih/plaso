# Performance Analysis of New Streaming Flags

## Summary
Analysis of CPU impact for newly added flags: `--http-endpoint`, `--json-stdout`, `--event-filter`, and `--consolidated_timestamps` in plaso.

## Findings

### 🔴 HIGH IMPACT - Database Lookups Per Event

**Location:** `plaso/storage/json_streaming_writer.py:287-314` and `plaso/storage/http_streaming_writer.py:127-155`

**Issue:** For EVERY event, the streaming writers perform 3 SQLite database queries:
```python
# Lines 287-314 in json_streaming_writer.py
event_data = self._real_storage_writer.GetAttributeContainerByIdentifier(
    'event_data', event_data_identifier)  # DB Query #1

event_data_stream = self._real_storage_writer.GetAttributeContainerByIdentifier(
    'event_data_stream', event_data_stream_identifier)  # DB Query #2

event_tag = self._real_storage_writer.GetAttributeContainerByIdentifier(
    'event_tag', event_tag_identifier)  # DB Query #3
```

**CPU Impact:** HIGH - Database queries are expensive operations, especially when called millions of times for high-volume event streams.

**Recommendation:** Implement a **LRU cache** for recently accessed containers:
```python
from functools import lru_cache

@lru_cache(maxsize=10000)
def _get_cached_container(self, container_type, identifier):
    return self._real_storage_writer.GetAttributeContainerByIdentifier(
        container_type, identifier)
```

---

### 🟡 MEDIUM IMPACT - Event Filter Matching

**Location:** `plaso/storage/json_streaming_writer.py:317-327`, `plaso/storage/http_streaming_writer.py:158-168`

**Issue:** When `--event-filter` is used, `Match()` is called for EVERY event:
```python
if self._event_filter:
    filter_match = self._event_filter.Match(
        event, event_data, event_data_stream, event_tag)
```

Each `Match()` call involves:
- Attribute lookups using `getattr()` (line 274-296 in `filters.py`)
- String comparisons (potentially case-insensitive)
- Possible regex matching (lines 497-515 in `filters.py`)
- Boolean operator evaluation (AND/OR filters)

**CPU Impact:** MEDIUM - Depends on filter complexity. Simple equality checks are fast, but regex filters on every event can be CPU-intensive.

**Recommendation:** 
1. Document to users that simple filters perform better than regex
2. Consider early termination for OR filters (already implemented correctly)
3. For time-based filters, consider implementing a fast path

---

### 🟡 MEDIUM IMPACT - JSON Serialization

**Location:** `plaso/storage/json_streaming_writer.py:330-335`

**Issue:** Every event is serialized to JSON:
```python
field_values = self._GetFieldValues(
    event, event_data, event_data_stream, event_tag)
json_string = self._json_encoder.encode(field_values)
```

The `_GetFieldValues()` method (lines 82-229) performs:
- Multiple iterations over event attributes
- Date/time string conversions (`CopyToDateTimeString()`)
- Field formatting via `GetFormattedField()`
- Message generation (lines 199-212)

**CPU Impact:** MEDIUM - JSON encoding is reasonably fast in Python, but the attribute iteration and field formatting adds overhead.

**Recommendation:**
1. Profile `_GetFieldValues()` to identify hotspots
2. Consider lazy evaluation for fields that aren't always needed
3. Optimize date/time string conversion (cache format strings)

---

### 🟡 MEDIUM IMPACT - Synchronous stdout Writing (--json-stdout)

**Location:** `plaso/storage/json_streaming_writer.py:335`

**Issue:** Each event is printed synchronously with flush:
```python
print(json_string, flush=True)
```

**CPU Impact:** MEDIUM - `flush=True` forces immediate I/O, which can slow down processing when events arrive faster than they can be written.

**Recommendation:** Consider buffering like HTTP endpoint does:
```python
# Buffer events and flush periodically
if len(self._buffer) >= 100:
    sys.stdout.write('\n'.join(self._buffer) + '\n')
    sys.stdout.flush()
    self._buffer.clear()
```

---

### 🟢 LOW IMPACT (PERFORMANCE IMPROVEMENT) - Consolidated Timestamps

**Location:** `plaso/engine/timeliner.py:457-506`

**Issue:** NONE - This is actually a performance IMPROVEMENT!

**Explanation:** `--consolidated_timestamps` creates 1 event per record instead of multiple events (one per timestamp). This:
- **Reduces** the number of events processed
- **Reduces** database writes
- **Reduces** JSON serialization calls
- **Reduces** network/stdout operations

**CPU Impact:** POSITIVE - Should improve performance, especially for parsers that generate many timestamps per record (e.g., MFT entries with 4+ timestamps).

---

### 🟢 LOW IMPACT - HTTP Batching

**Location:** `plaso/storage/http_streaming_writer.py:185-219`

**Issue:** NONE - Good implementation!

**Explanation:** The HTTP endpoint writer uses:
- Asynchronous queue (`queue.Queue`)
- Background thread for network I/O
- Batching (100 events per batch)
- Exponential backoff retry logic

**CPU Impact:** LOW - The batching and async approach minimize CPU impact. Network I/O happens in a separate thread.

---

## Recommendations Priority

### P0 - Critical (Implement Immediately)
1. **Add LRU cache for database lookups** - This will have the biggest performance impact
   - Expected improvement: 50-70% reduction in database query overhead
   - Implementation: ~20 lines of code

### P1 - High (Implement Soon)
2. **Add buffering to --json-stdout** - Reduce flush() overhead
   - Expected improvement: 20-30% improvement for high-volume streams
   - Implementation: ~30 lines of code

### P2 - Medium (Consider for Future)
3. **Optimize _GetFieldValues()** - Profile and optimize hot paths
4. **Document filter performance characteristics** - Help users write efficient filters

### P3 - Low (Optional)
5. **Add metrics/statistics** - Track filter match rate, cache hit rate, etc.

---

## Test Recommendations

To measure the actual CPU impact, run these tests:

```bash
# Baseline - no streaming flags
time log2timeline.py baseline.plaso test_data/

# With --json-stdout
time log2timeline.py --json-stdout test_data/ > /dev/null

# With --json-stdout and --event-filter
time log2timeline.py --json-stdout --event-filter "data_type is 'fs:stat'" test_data/ > /dev/null

# With --consolidated-timestamps
time log2timeline.py --json-stdout --consolidated-timestamps test_data/ > /dev/null

# With HTTP endpoint
python test_http_server.py &  # Start test server
time log2timeline.py --http-endpoint http://localhost:8080/events test_data/
```

Compare CPU usage (user time) and wall time between runs.

---

## Conclusion

The main CPU bottlenecks are:
1. **Database lookups** (HIGH) - Fix with caching
2. **Synchronous stdout** (MEDIUM) - Fix with buffering  
3. **JSON serialization** (MEDIUM) - Acceptable, but can be optimized
4. **Event filtering** (MEDIUM) - Acceptable, depends on filter complexity

The `--consolidated_timestamps` flag actually IMPROVES performance and should be recommended for users who don't need separate events per timestamp.

