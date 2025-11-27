# -*- coding: utf-8 -*-
"""HTTP streaming storage writer for sending events to HTTP endpoints."""

import json
import logging
import threading
import time
import queue
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

from plaso.storage.json_streaming_writer import JSONStreamingStorageWriter


class HTTPStreamingStorageWriter(JSONStreamingStorageWriter):
  """HTTP streaming storage writer that sends events to an HTTP endpoint."""

  def __init__(self, endpoint_url, batch_size=100, flush_interval=5.0, 
               max_retries=3, headers=None, event_filter=None,
               consolidated_timestamps=False):
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
    """
    super(HTTPStreamingStorageWriter, self).__init__(
        event_filter=event_filter,
        consolidated_timestamps=consolidated_timestamps)
    
    # Validate URL
    parsed_url = urlparse(endpoint_url)
    if not parsed_url.scheme or not parsed_url.netloc:
      raise ValueError(f'Invalid endpoint URL: {endpoint_url}')
    
    self._endpoint_url = endpoint_url
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
    
    logging.info(
        f'HTTP streaming writer closed. Stats: {self._events_sent} events sent, '
        f'{self._events_failed} events failed, {self._batches_sent} batches sent, '
        f'{self._batches_failed} batches failed')

  def AddAttributeContainer(self, container):
    """Adds an attribute container.

    Args:
      container (AttributeContainer): attribute container.
    """
    if container.CONTAINER_TYPE == 'event':
      event = container
      event_data = None
      event_data_stream = None
      event_tag = None

      # Get event data
      if hasattr(event, 'GetEventDataIdentifier'):
        event_data_identifier = event.GetEventDataIdentifier()
        if event_data_identifier:
          try:
            event_data = self._real_storage_writer.GetAttributeContainerByIdentifier(
                'event_data', event_data_identifier)
          except Exception:
            pass

      # Get event data stream
      if event_data and hasattr(event_data, 'GetEventDataStreamIdentifier'):
        event_data_stream_identifier = event_data.GetEventDataStreamIdentifier()
        if event_data_stream_identifier:
          try:
            event_data_stream = self._real_storage_writer.GetAttributeContainerByIdentifier(
                'event_data_stream', event_data_stream_identifier)
          except Exception:
            pass

      # Get event tag
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

      # Get field values using parent's method
      field_values = self._GetFieldValues(
          event, event_data, event_data_stream, event_tag)

      # Queue the event for HTTP sending instead of printing to stdout
      try:
        self._event_queue.put(field_values, timeout=1.0)
      except queue.Full:
        logging.warning('Event queue full, dropping event')
        self._events_failed += 1

    # Forward to real storage writer (but not to parent's AddAttributeContainer 
    # which would print to stdout)
    self._real_storage_writer.AddAttributeContainer(container)

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
