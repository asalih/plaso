# -*- coding: utf-8 -*-
"""Direct HTTP output storage writer that bypasses database storage entirely."""

import json
import logging
import threading
import time
import queue
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from plaso.storage.direct_output_writer import DirectOutputStorageWriter


class DirectHTTPOutputStorageWriter(DirectOutputStorageWriter):
  """Direct HTTP output storage writer that sends events without DB storage.
  
  This is the HTTP version of DirectOutputStorageWriter - it sends events
  directly to an HTTP endpoint without any database operations.
  """

  def __init__(self, endpoint_url, batch_size=100, flush_interval=5.0,
               max_retries=3, headers=None, event_filter=None,
               consolidated_timestamps=False):
    """Initializes a direct HTTP output storage writer.

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
    """
    # Initialize parent with 'dict' format since we'll handle JSON encoding once per batch
    super(DirectHTTPOutputStorageWriter, self).__init__(
        output_file=None,
        event_filter=event_filter,
        consolidated_timestamps=consolidated_timestamps,
        output_format='dict')  # Store as dicts, encode to JSON once per batch
    
    # Validate URL
    parsed_url = urlparse(endpoint_url)
    if not parsed_url.scheme or not parsed_url.netloc:
      raise ValueError(f'Invalid endpoint URL: {endpoint_url}')
    
    self._endpoint_url = endpoint_url
    
    # Don't need JSON encoder from parent (we'll encode batches)
    self._json_encoder = None
    self._batch_size = batch_size
    self._flush_interval = flush_interval
    self._max_retries = max_retries
    self._headers = headers or {}
    
    # Ensure Content-Type is set for JSON
    if 'Content-Type' not in self._headers:
      self._headers['Content-Type'] = 'application/json'
    
    # Event batching
    self._event_queue = queue.Queue()
    self._batch_buffer = []
    self._last_flush_time = time.time()
    
    # Background thread for sending batches
    self._sender_thread = None
    self._stop_event = threading.Event()
    self._sender_running = False
    
    # Statistics
    self._batches_sent = 0
    self._batches_failed = 0

  def Open(self, path=None, **kwargs):
    """Opens the HTTP storage writer."""
    super(DirectHTTPOutputStorageWriter, self).Open(path, **kwargs)
    
    # Start the background sender thread
    self._stop_event.clear()
    self._sender_thread = threading.Thread(target=self._sender_worker, daemon=True)
    self._sender_running = True
    self._sender_thread.start()
    
    print('🚀🚀🚀 DIRECT HTTP WRITER OPENED!')
    print(f'    Endpoint: {self._endpoint_url}')
    print(f'    Batch size: {self._batch_size}')
    print(f'    Single process mode should be enabled')
    
    logging.warning(f'🚀 Direct HTTP writer opened, sending to: {self._endpoint_url}')
    logging.warning(f'   Batch size: {self._batch_size}, Flush interval: {self._flush_interval}s')
    logging.warning(f'   Event filter: {self._event_filter is not None}')
    logging.warning(f'   Consolidated timestamps: {self._consolidated_timestamps}')

  def Close(self):
    """Closes the HTTP storage writer and flushes remaining events."""
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
    
    super(DirectHTTPOutputStorageWriter, self).Close()
    
    stats = self.GetStatistics()
    logging.info(
        f'Direct HTTP writer closed. Stats: {stats["events_output"]} events sent, '
        f'{stats["events_filtered"]} events filtered, {self._batches_sent} batches sent, '
        f'{self._batches_failed} batches failed')

  def _flush_output_buffer(self):
    """Flushes buffered output by queuing for HTTP sending."""
    if not self._output_buffer:
      return
    
    buffer_size = len(self._output_buffer)
    if buffer_size > 0:
      logging.debug(f'Queueing {buffer_size} events for HTTP sending')
    
    # Queue all buffered events for HTTP sending
    for event_dict in self._output_buffer:
      try:
        self._event_queue.put(event_dict, timeout=1.0)
      except queue.Full:
        logging.warning('Event queue full, dropping event')
        self._events_filtered += 1
    
    self._output_buffer.clear()

  def _sender_worker(self):
    """Background worker thread that sends batched events to HTTP endpoint."""
    while not self._stop_event.is_set():
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
          continue

        # Check for sentinel value (None means stop)
        if event_data is None:
          break

        # Add event to batch buffer
        self._batch_buffer.append(event_data)

        # Check if we should send the batch
        if len(self._batch_buffer) >= self._batch_size:
          self._flush_batch()

        # Mark task as done
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

    logging.warning(f'📤 Flushing batch of {len(batch_to_send)} events to {self._endpoint_url}')
    success = self._send_batch(batch_to_send)
    if success:
      self._batches_sent += 1
      logging.warning(f'✅ Batch #{self._batches_sent} sent successfully ({len(batch_to_send)} events)')
    else:
      self._batches_failed += 1
      logging.error(f'❌ Batch send failed ({len(batch_to_send)} events)')

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

    # Convert to JSON (single encoding - events are stored as dicts)
    try:
      json_data = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
      json_bytes = json_data.encode('utf-8')
    except Exception as exception:
      logging.error(f'Failed to serialize batch to JSON: {exception}')
      return False

    # Send with retries
    for attempt in range(self._max_retries + 1):
      try:
        # Create the request
        request = Request(self._endpoint_url, data=json_bytes)
        
        # Add headers
        for header_name, header_value in self._headers.items():
          request.add_header(header_name, header_value)

        # Send the request
        with urlopen(request, timeout=30) as response:
          if 200 <= response.status < 300:
            logging.debug(f'Successfully sent batch of {len(events)} events')
            return True
          else:
            logging.warning(
                f'HTTP endpoint returned status {response.status} for batch')

      except HTTPError as exception:
        logging.warning(
            f'HTTP error on attempt {attempt + 1}/{self._max_retries + 1}: '
            f'{exception.code} {exception.reason}')
      except URLError as exception:
        logging.warning(
            f'URL error on attempt {attempt + 1}/{self._max_retries + 1}: '
            f'{exception.reason}')
      except Exception as exception:
        logging.warning(
            f'Unexpected error on attempt {attempt + 1}/{self._max_retries + 1}: '
            f'{exception}')

      # Wait before retrying (exponential backoff)
      if attempt < self._max_retries:
        wait_time = 2 ** attempt
        time.sleep(wait_time)

    logging.error(f'Failed to send batch of {len(events)} events after all retries')
    return False

  def GetStatistics(self):
    """Gets processing statistics.
    
    Returns:
      dict: statistics about events processed and HTTP batches sent.
    """
    stats = super(DirectHTTPOutputStorageWriter, self).GetStatistics()
    stats.update({
        'batches_sent': self._batches_sent,
        'batches_failed': self._batches_failed,
        'queue_size': self._event_queue.qsize(),
        'buffer_size': len(self._batch_buffer)
    })
    return stats

