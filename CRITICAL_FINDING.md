# 🚨 CRITICAL FINDING: Multiprocess Mode Issue

## The Problem

Our `--http-endpoint` and `--json-stdout` with direct output **DOES NOT WORK** in multiprocess mode!

### Why?

In multiprocess mode, Plaso's architecture works like this:

1. **Main Process** creates the storage writer (`DirectOutputStorageWriter` or `DirectHTTPOutputStorageWriter`)
2. **Worker Processes** (separate Python processes) do the actual parsing
3. Workers create their **OWN task_storage_writers** (SQLite files) - see `extraction_process.py:325`
4. Workers write events to their temp SQLite files
5. Later, these SQLite files are **merged** back to the main storage writer

**The direct output writers never see events from workers!**

### Evidence

From `plaso/multi_process/extraction_process.py`:

```python
def _ProcessTask(self, task):
    # Line 325-326: Worker creates its OWN storage writer
    task_storage_writer = self._storage_factory.CreateTaskStorageWriter(
        self._processing_configuration.task_storage_format)
    
    # Line 334: Parser writes to THIS storage writer, not ours!
    self._parser_mediator.SetStorageWriter(task_storage_writer)
    
    # Line 338: Opens temp SQLite file
    task_storage_writer.Open(
        path=storage_file_path, session_identifier=task.session_identifier,
        task_identifier=task.identifier)
```

## The Solution

### Option 1: Force Single-Process Mode (Recommended for Now)

Add `--single-process` flag:

```bash
python3 plaso/scripts/log2timeline.py \
  --http-endpoint 'http://localhost:9098/events' \
  --single-process \
  /path/to/data
```

### Option 2: Intercept the Merge Process (Complex)

We would need to:
1. Monitor when worker task storage files are merged
2. Read events from the SQLite merge files
3. Stream them to HTTP/stdout during merge

This is significantly more complex.

### Option 3: Implement in Worker Processes (Most Complex)

Modify the worker process creation to:
1. Pass HTTP endpoint to workers
2. Have each worker stream directly
3. Coordinate batching across workers

## Testing

Run this to confirm single-process mode works:

```bash
./test_single_process.sh
```

You MUST use `--single-process` for now!

## Performance Impact

Single-process mode is slower than multiprocess, but for streaming use cases:

**Pros:**
- Events stream immediately as they're parsed
- No temporary storage needed
- Lower memory footprint (no task storage buffering)

**Cons:**
- No parallel processing across CPU cores
- Slower overall parsing speed

## Recommendation

For your use case (processing individual files via separate processes):
- Each process runs `log2timeline` on ONE file
- Use `--single-process` mode for that file
- Stream events to HTTP endpoint
- Run multiple `log2timeline` processes in parallel externally

This gives you:
- ✅ Parallel processing (at the file level)
- ✅ Event streaming
- ✅ No temp storage
- ✅ Each file processed independently

