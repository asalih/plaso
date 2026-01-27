# -*- coding: utf-8 -*-
"""HTTP streaming storage writer for sending events to HTTP endpoints."""

import http.client
import gzip
import json
import logging
import socket
import threading
import time
import queue
from urllib.parse import urlparse

from plaso.storage.json_streaming_writer import JSONStreamingStorageWriter


class HTTPStreamingStorageWriter(JSONStreamingStorageWriter):
  """HTTP streaming storage writer that sends events to an HTTP endpoint."""

  def __init__(self, endpoint_url, batch_size=100, flush_interval=5.0, 
               max_retries=3, headers=None, event_filter=None,
               consolidated_timestamps=False, relative_paths=False,
               storage_file_path=None, deduplicate_events=False,
               stream_storage='sqlite', store_events_in_storage=True,
               request_timeout=60, max_queue_size=10000,
               compress_payload=False, gzip_compress_level=1):
    """Initializes an HTTP streaming storage writer.

    Args:
      endpoint_url (str): HTTP endpoint URL to send events to.
      batch_size (Optional[int]): number of events to batch before sending.
      flush_interval (Optional[float]): maximum time in seconds to wait before
          sending a batch even if it's not full.
      max_retries (Optional[int]): maximum number of retry attempts for failed
          HTTP requests.
      headers (Optional[dict]): additional HTTP headers to send with requests.
      event_filter (Optional[EventObjectFilter]): event filter for filtering
          events by timestamp or other criteria.
      consolidated_timestamps (Optional[bool]): True if timestamps should be
          included as separate fields in the output (one event per record
          with all timestamps).
      relative_paths (Optional[bool]): True if file paths should be reported
          relative to the source path instead of as absolute paths.
      storage_file_path (Optional[str]): path to the storage file. If None,
          a temporary file will be created.
      deduplicate_events (Optional[bool]): True if events should be 
          deduplicated when streaming. Default is False.
      stream_storage (Optional[str]): storage backend for intermediate storage.
          Supported values are "sqlite" and "memory".
      store_events_in_storage (Optional[bool]): True to store final event
          containers in the intermediate storage.
      request_timeout (Optional[int]): request timeout in seconds.
      max_queue_size (Optional[int]): maximum number of streamed events to
          buffer in memory before applying backpressure.
      compress_payload (Optional[bool]): True to gzip-compress the JSON request
          body (Content-Encoding: gzip).
      gzip_compress_level (Optional[int]): gzip compression level (0-9).
    """
    super(HTTPStreamingStorageWriter, self).__init__(
        event_filter=event_filter,
        consolidated_timestamps=consolidated_timestamps,
        relative_paths=relative_paths,
        storage_file_path=storage_file_path,
        deduplicate_events=deduplicate_events,
        stream_storage=stream_storage,
        store_events_in_storage=store_events_in_storage)
    
    # Validate URL
    parsed_url = urlparse(endpoint_url)
    if not parsed_url.scheme or not parsed_url.netloc:
      raise ValueError(f'Invalid endpoint URL: {endpoint_url}')
    if parsed_url.scheme not in ('http', 'https'):
      raise ValueError(f'Unsupported endpoint scheme: {parsed_url.scheme!s}')
    
    self._endpoint_url = endpoint_url
    self._parsed_url = parsed_url
    self._request_timeout = int(request_timeout) if request_timeout else 30
    self._compress_payload = bool(compress_payload)
    self._gzip_compress_level = int(gzip_compress_level)
    self._batch_size = batch_size
    self._flush_interval = flush_interval
    self._max_retries = max_retries
    self._headers = headers or {}
    
    # Ensure Content-Type is set for JSON
    if 'Content-Type' not in self._headers:
      self._headers['Content-Type'] = 'application/json'
    
    # Event batching with bounded queue to avoid unbounded memory growth when
    # the endpoint is slow or unavailable.
    self._event_queue = queue.Queue(maxsize=max_queue_size or 0)
    self._batch_buffer = []
    self._last_flush_time = time.time()
    
    # Background thread for sending batches
    self._sender_thread = None
    self._stop_event = threading.Event()
    self._sender_running = False

    # Keep-alive HTTP connection (sender thread only).
    self._http_connection = None
    
    # Statistics
    self._events_sent = 0
    self._events_failed = 0
    self._batches_sent = 0
    self._batches_failed = 0

  def Open(self, path=None, **kwargs):
    """Opens the HTTP streaming storage writer."""
    super(HTTPStreamingStorageWriter, self).Open(path, **kwargs)
    
    # Start the background sender thread
    self._stop_event.clear()
    self._sender_thread = threading.Thread(target=self._sender_worker, daemon=True)
    self._sender_running = True
    self._sender_thread.start()
    
    logging.info(f'HTTP streaming writer opened, sending to: {self._endpoint_url}')

  def Close(self):
    """Closes the HTTP streaming storage writer and flushes remaining events."""
    if self._sender_running:
      # Signal the sender thread to stop
      self._stop_event.set()
      
      # Add a sentinel value to wake up the sender thread
      try:
        self._event_queue.put(None, timeout=1.0)
      except queue.Full:
        pass
      
      # Wait for the sender thread to finish
      if self._sender_thread and self._sender_thread.is_alive():
        self._sender_thread.join(timeout=10.0)
      
      self._sender_running = False
    
    # Send any remaining events in the buffer
    if self._batch_buffer:
      self._send_batch(self._batch_buffer)
      self._batch_buffer.clear()
    
    super(HTTPStreamingStorageWriter, self).Close()
    self._CloseHTTPConnection()
    
    logging.info(
        f'HTTP streaming writer closed. Stats: {self._events_sent} events sent, '
        f'{self._events_failed} events failed, {self._batches_sent} batches sent, '
        f'{self._batches_failed} batches failed')

  def AddAttributeContainer(self, container):
    """Adds an attribute container.

    Args:
      container (AttributeContainer): attribute container.
    """
    # Cache event_data and event_data_stream containers for later lookup
    # This avoids race conditions with SQLite write cache
    if container.CONTAINER_TYPE == 'event_data':
      identifier = container.GetIdentifier()
      if identifier:
        identifier_string = identifier.CopyToString()
        self._event_data_cache[identifier_string] = container
        self._event_data_cache.move_to_end(identifier_string, last=True)
        if (getattr(self, '_max_cached_event_data_containers', None) and
            len(self._event_data_cache) > self._max_cached_event_data_containers):
          self._event_data_cache.popitem(last=False)
    elif container.CONTAINER_TYPE == 'event_data_stream':
      identifier = container.GetIdentifier()
      if identifier:
        identifier_string = identifier.CopyToString()
        self._event_data_stream_cache[identifier_string] = container
        self._event_data_stream_cache.move_to_end(identifier_string, last=True)
        if (getattr(self, '_max_cached_event_data_containers', None) and
            len(self._event_data_stream_cache) > self._max_cached_event_data_containers):
          self._event_data_stream_cache.popitem(last=False)
    elif container.CONTAINER_TYPE == 'event':
      event = container
      event_data = None
      event_data_stream = None
      event_tag = None

      # Get event data from local cache first, then fall back to storage
      if hasattr(event, 'GetEventDataIdentifier'):
        event_data_identifier = event.GetEventDataIdentifier()
        if event_data_identifier:
          identifier_string = event_data_identifier.CopyToString()
          event_data = self._event_data_cache.get(identifier_string)
          if event_data:
            self._event_data_cache.move_to_end(identifier_string, last=True)
          if not event_data:
            try:
              event_data = self._real_storage_writer.GetAttributeContainerByIdentifier(
                  'event_data', event_data_identifier)
            except Exception:
              pass

      # Get event data stream from local cache first, then fall back to storage
      if event_data and hasattr(event_data, 'GetEventDataStreamIdentifier'):
        event_data_stream_identifier = event_data.GetEventDataStreamIdentifier()
        if event_data_stream_identifier:
          identifier_string = event_data_stream_identifier.CopyToString()
          event_data_stream = self._event_data_stream_cache.get(identifier_string)
          if event_data_stream:
            self._event_data_stream_cache.move_to_end(
                identifier_string, last=True)
          if not event_data_stream:
            try:
              event_data_stream = self._real_storage_writer.GetAttributeContainerByIdentifier(
                  'event_data_stream', event_data_stream_identifier)
            except Exception:
              pass

      # Get event tag (no caching needed, rarely used)
      if hasattr(event, 'GetEventTagIdentifier'):
        event_tag_identifier = event.GetEventTagIdentifier()
        if event_tag_identifier:
          try:
            event_tag = self._real_storage_writer.GetAttributeContainerByIdentifier(
                'event_tag', event_tag_identifier)
          except Exception:
            pass

      # Apply event filter if configured
      if self._event_filter:
        try:
          filter_match = self._event_filter.Match(
              event, event_data, event_data_stream, event_tag)
          # If filter doesn't match, skip this event
          if filter_match is False:
            self._real_storage_writer.AddAttributeContainer(container)
            return
        except Exception:
          # If filtering fails, include the event to be safe
          pass

      # Deduplication check: skip events we've already seen
      # The _event_values_hash is stored on event_data, not event
      # We need to combine it with timestamp AND timestamp_desc from event
      # because the hash excludes datetime values, so events with same hash
      # but different timestamps would incorrectly be considered duplicates
      if self._deduplicate_events and event_data:
        event_values_hash = getattr(event_data, '_event_values_hash', None)
        if event_values_hash:
          # Include timestamp value in the key because hash excludes datetime values
          timestamp = getattr(event, 'timestamp', 0)
          timestamp_desc = getattr(event, 'timestamp_desc', '')
          dedup_key = (event_values_hash, timestamp, timestamp_desc)
          
          if dedup_key in self._seen_event_keys:
            # Duplicate event, skip streaming but still write to storage
            self._duplicates_skipped += 1
            self._real_storage_writer.AddAttributeContainer(container)
            return
          
          # Remember this key for future deduplication
          self._seen_event_keys.add(dedup_key)

      # Get field values using parent's method
      field_values = self._GetFieldValues(
          event, event_data, event_data_stream, event_tag)

      # Queue the event for HTTP sending instead of printing to stdout
      try:
        # Apply backpressure when the endpoint is slow.
        self._event_queue.put(field_values)
        self._events_streamed += 1
      except Exception:
        logging.warning('Unable to enqueue event for streaming')
        self._events_failed += 1

    # Forward to real storage writer (but not to parent's AddAttributeContainer 
    # which would print to stdout)
    self._real_storage_writer.AddAttributeContainer(container)

  def _CloseHTTPConnection(self):
    """Closes the persistent HTTP connection if open."""
    connection = self._http_connection
    self._http_connection = None
    if connection:
      try:
        connection.close()
      except Exception:
        pass

  def _GetOrCreateHTTPConnection(self):
    """Gets or creates an HTTP keep-alive connection.

    Returns:
      http.client.HTTPConnection: connection object.
    """
    if self._http_connection:
      return self._http_connection

    hostname = self._parsed_url.hostname
    port = self._parsed_url.port
    if self._parsed_url.scheme == 'https':
      connection = http.client.HTTPSConnection(
          hostname, port=port, timeout=self._request_timeout)
    else:
      connection = http.client.HTTPConnection(
          hostname, port=port, timeout=self._request_timeout)

    self._http_connection = connection
    return connection

  def _sender_worker(self):
    """Background worker thread that sends batched events to HTTP endpoint."""
    # Drain the queue on shutdown to avoid dropping already-enqueued events.
    while True:
      try:
        # Try to get an event from the queue
        try:
          event_data = self._event_queue.get(timeout=self._flush_interval)
        except queue.Empty:
          # Timeout reached, check if we should flush
          if self._batch_buffer:
            current_time = time.time()
            if current_time - self._last_flush_time >= self._flush_interval:
              self._flush_batch()

          # If we're shutting down and there's nothing left to send, exit.
          if self._stop_event.is_set() and self._event_queue.empty():
            break
          continue

        try:
          # Check for sentinel value (None means stop)
          if event_data is None:
            break

          # Add event to batch buffer
          self._batch_buffer.append(event_data)

          # Check if we should send the batch
          if len(self._batch_buffer) >= self._batch_size:
            self._flush_batch()

        finally:
          # Mark task as done (including sentinel) so Close() can safely wait
          # on queue completion if needed.
          self._event_queue.task_done()

      except Exception as exception:
        logging.error(f'Error in sender worker: {exception}')

    # Flush any remaining events before exiting
    if self._batch_buffer:
      self._flush_batch()

  def _flush_batch(self):
    """Flushes the current batch to the HTTP endpoint."""
    if not self._batch_buffer:
      return

    batch_to_send = list(self._batch_buffer)
    self._batch_buffer.clear()
    self._last_flush_time = time.time()

    success = self._send_batch(batch_to_send)
    if success:
      self._events_sent += len(batch_to_send)
      self._batches_sent += 1
    else:
      self._events_failed += len(batch_to_send)
      self._batches_failed += 1

  def _send_batch(self, events):
    """Sends a batch of events to the HTTP endpoint.

    Args:
      events (list): list of event dictionaries to send.

    Returns:
      bool: True if the batch was sent successfully, False otherwise.
    """
    if not events:
      return True

    # Prepare the payload
    payload = {
      'events': events,
      'batch_size': len(events),
      'timestamp': time.time()
    }

    # Convert to JSON
    try:
      json_data = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
      json_bytes = json_data.encode('utf-8')
    except Exception as exception:
      logging.error(f'Failed to serialize batch to JSON: {exception}')
      return False

    body_bytes = json_bytes
    if self._compress_payload:
      try:
        body_bytes = gzip.compress(
            json_bytes, compresslevel=self._gzip_compress_level)
      except Exception as exception:
        logging.error(f'Failed to gzip-compress batch: {exception}')
        return False

    # Build request path (include query string if present).
    path = self._parsed_url.path or '/'
    if self._parsed_url.query:
      path = f'{path}?{self._parsed_url.query}'

    # Send with retries (keep-alive connection in sender thread).
    for attempt in range(self._max_retries + 1):
      try:
        connection = self._GetOrCreateHTTPConnection()

        headers = dict(self._headers)
        headers.setdefault('Connection', 'keep-alive')
        headers.setdefault('Content-Type', 'application/json')
        if self._compress_payload:
          headers['Content-Encoding'] = 'gzip'

        connection.request('POST', path, body=body_bytes, headers=headers)
        response = connection.getresponse()
        # Always drain the response body so the connection can be reused.
        try:
          response.read()
        except Exception:
          pass

        if 200 <= response.status < 300:
          logging.debug(f'Successfully sent batch of {len(events)} events')
          return True

        logging.warning(
            f'HTTP endpoint returned status {response.status} for batch')

      except (http.client.HTTPException, socket.timeout, OSError) as exception:
        # Connection-level errors: close and recreate on next retry.
        self._CloseHTTPConnection()
        logging.warning(
            f'HTTP request failed on attempt {attempt + 1}/{self._max_retries + 1}: '
            f'{exception!s}')
      except Exception as exception:
        self._CloseHTTPConnection()
        logging.warning(
            f'Unexpected error on attempt {attempt + 1}/{self._max_retries + 1}: '
            f'{exception!s}')

      # Wait before retrying (exponential backoff)
      if attempt < self._max_retries:
        wait_time = 2 ** attempt
        time.sleep(wait_time)

    logging.error(f'Failed to send batch of {len(events)} events after all retries')
    return False

  def get_statistics(self):
    """Gets sending statistics.

    Returns:
      dict: statistics about events and batches sent/failed.
    """
    return {
      'events_sent': self._events_sent,
      'events_failed': self._events_failed,
      'batches_sent': self._batches_sent,
      'batches_failed': self._batches_failed,
      'queue_size': self._event_queue.qsize(),
      'buffer_size': len(self._batch_buffer)
    }
