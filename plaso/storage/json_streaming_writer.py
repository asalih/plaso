# -*- coding: utf-8 -*-
"""JSON streaming storage writer for outputting events directly to stdout."""

import json
import logging
import os
import tempfile
import uuid

from acstore.containers import interface
from dfdatetime import interface as dfdatetime_interface

from plaso.containers import events
from plaso.output import mediator
from plaso.output import shared_json
from plaso.serializer import json_serializer
from plaso.storage import factory as storage_factory
from plaso.storage import writer as storage_writer


class JSONStreamingStorageWriter(storage_writer.StorageWriter):
  """JSON streaming storage writer."""

  def __init__(self, output_file=None, event_filter=None,
               consolidated_timestamps=False, relative_paths=False,
               storage_file_path=None, deduplicate_events=False):
    """Initializes a JSON streaming storage writer.

    Args:
      output_file (Optional[TextIO]): output file-like object to write to.
          If None, stdout will be used.
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
          deduplicated when streaming. Default is False because streaming
          deduplication across runs is complex and may not match psort's
          behavior. Use psort with -a/--include-all if you need control
          over deduplication.
    """
    super(JSONStreamingStorageWriter, self).__init__()
    self._output_file = output_file
    self._serializer = json_serializer.JSONAttributeContainerSerializer()
    self._field_formatting_helper = shared_json.JSONFieldFormattingHelper()
    self._json_encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True)
    self._output_mediator = mediator.OutputMediator(
        storage_reader=self, relative_paths=relative_paths)
    self._event_filter = event_filter
    self._consolidated_timestamps = consolidated_timestamps
    
    # Local caches to avoid race conditions with SQLite write cache
    # These store containers by their identifier string for fast lookup
    self._event_data_cache = {}
    self._event_data_stream_cache = {}
    
    # Deduplication: OFF by default for streaming
    # Streaming deduplication is complex and may not match psort's behavior
    # which deduplicates consecutive sorted events
    self._seen_event_keys = set()
    self._deduplicate_events = deduplicate_events
    self._duplicates_skipped = 0
    self._events_streamed = 0
    
    # Use provided storage file path or create a temporary file
    if storage_file_path:
      self._storage_file_path = storage_file_path
      self._using_temp_file = False
    else:
      self._temp_file = tempfile.NamedTemporaryFile(suffix='.plaso', delete=False)
      self._temp_file.close()
      self._storage_file_path = self._temp_file.name
      self._using_temp_file = True
    
    # Create a real storage writer that writes to the storage file
    self._real_storage_writer = storage_factory.StorageFactory.CreateStorageWriter(
        'sqlite')
    
    # Set _store to the real storage writer's store to satisfy base class checks
    self._store = None  # Will be set in Open()

  def _RaiseIfNotWritable(self):
    """Raises if the storage writer is not writable."""
    if not self._real_storage_writer:
      raise IOError('Unable to write to closed storage writer.')
    
  def Open(self, path=None, **kwargs):
    """Opens the storage writer."""
    self._real_storage_writer.Open(path=self._storage_file_path)
    
    # Set _store to satisfy the base class _RaiseIfNotWritable check
    self._store = self._real_storage_writer._store
    
    # Load existing event keys for deduplication when reusing storage file
    if self._deduplicate_events and not self._using_temp_file:
      self._LoadExistingEventKeys()

  def _LoadExistingEventKeys(self):
    """Loads existing event keys from storage for deduplication.
    
    This reads all existing events and event_data from storage and builds a set of
    (event_values_hash, timestamp, timestamp_desc) tuples to detect duplicates.
    The hash is stored on event_data, and timestamp/timestamp_desc are on event.
    We include the timestamp because the hash excludes datetime values.
    """
    try:
      # First, build a map of event_data identifier to hash
      event_data_hash_map = {}
      num_event_data = self._real_storage_writer.GetNumberOfAttributeContainers('event_data')
      
      if num_event_data == 0:
        logging.info('Deduplication: No existing event_data found in storage')
        return
      
      logging.info(f'Deduplication: Loading from {num_event_data} existing event_data')
      
      for event_data in self._real_storage_writer.GetAttributeContainers('event_data'):
        event_values_hash = getattr(event_data, '_event_values_hash', None)
        if event_values_hash:
          identifier = event_data.GetIdentifier()
          if identifier:
            event_data_hash_map[identifier.CopyToString()] = event_values_hash
      
      logging.info(f'Deduplication: Found {len(event_data_hash_map)} event_data with hashes')
      
      # Now read events and combine with their event_data hash
      num_events = self._real_storage_writer.GetNumberOfAttributeContainers('event')
      if num_events == 0:
        logging.info('Deduplication: No existing events found in storage')
        return
      
      logging.info(f'Deduplication: Loading from {num_events} existing events')
      
      loaded_count = 0
      for event in self._real_storage_writer.GetAttributeContainers('event'):
        timestamp = getattr(event, 'timestamp', 0)
        timestamp_desc = getattr(event, 'timestamp_desc', '')
        
        # Get the event_data identifier from the event
        event_data_identifier = None
        if hasattr(event, 'GetEventDataIdentifier'):
          event_data_identifier = event.GetEventDataIdentifier()
        
        if event_data_identifier:
          identifier_string = event_data_identifier.CopyToString()
          event_values_hash = event_data_hash_map.get(identifier_string)
          if event_values_hash:
            # Include timestamp in key because hash excludes datetime values
            dedup_key = (event_values_hash, timestamp, timestamp_desc)
            self._seen_event_keys.add(dedup_key)
            loaded_count += 1
      
      logging.info(f'Deduplication: Loaded {loaded_count} event keys for deduplication')
    except Exception as e:
      # If loading fails, log the error and continue without deduplication history
      logging.warning(f'Deduplication: Failed to load existing hashes: {e}')

  def Close(self):
    """Closes the storage writer and cleans up temp file if used."""
    # Log deduplication stats before closing
    logging.info(
        f'Deduplication stats: {self._events_streamed} events streamed, '
        f'{self._duplicates_skipped} duplicates skipped, '
        f'{len(self._seen_event_keys)} unique hashes tracked')
    
    if self._real_storage_writer:
      self._real_storage_writer.Close()
      
    # Clear _store to satisfy base class
    self._store = None
    
    # Clear deduplication cache and counters
    self._seen_event_keys.clear()
    self._duplicates_skipped = 0
    self._events_streamed = 0
      
    # Clean up temp file only if we created one
    if self._using_temp_file:
      try:
        os.unlink(self._storage_file_path)
      except OSError:
        pass
    
  def _GetFieldValues(self, event, event_data, event_data_stream, event_tag):
    """Retrieves the output field values.

    Args:
      event (EventObject): event.
      event_data (EventData): event data.
      event_data_stream (EventDataStream): event data stream.
      event_tag (EventTag): event tag.

    Returns:
      dict[str, str]: output field values per name.
    """
    field_values = {
        '__container_type__': 'event',
        '__type__': 'AttributeContainer'}

    if event_data:
      for attribute_name, attribute_value in event_data.GetAttributes():
        # Ignore attribute container identifier values.
        if isinstance(attribute_value, interface.AttributeContainerIdentifier):
          continue

        # Handle date and time values based on consolidated mode
        if isinstance(attribute_value, dfdatetime_interface.DateTimeValues):
          if self._consolidated_timestamps:
            # In consolidated mode, include timestamps as ISO strings
            if hasattr(attribute_value, 'CopyToDateTimeString'):
              try:
                field_values[attribute_name] = attribute_value.CopyToDateTimeString()
              except (AttributeError, ValueError):
                field_values[attribute_name] = None
            else:
              field_values[attribute_name] = None
          continue

        # Handle lists of datetime values
        if (isinstance(attribute_value, list) and attribute_value and
            isinstance(attribute_value[0],
                       dfdatetime_interface.DateTimeValues)):
          if self._consolidated_timestamps:
            # In consolidated mode, include list of timestamps as ISO strings
            timestamp_strings = []
            for dt_value in attribute_value:
              if hasattr(dt_value, 'CopyToDateTimeString'):
                try:
                  timestamp_strings.append(dt_value.CopyToDateTimeString())
                except (AttributeError, ValueError):
                  timestamp_strings.append(None)
              else:
                timestamp_strings.append(None)
            field_values[attribute_name] = timestamp_strings
          continue

        # Ignore protected internal only attributes.
        if attribute_name[0] == '_' and attribute_name != '_parser_chain':
          continue

        field_value = self._field_formatting_helper.GetFormattedField(
            self._output_mediator, attribute_name, event, event_data,
            event_data_stream, event_tag)

        # Output _parser_chain as parser for backwards compatibility.
        if attribute_name == '_parser_chain':
          attribute_name = 'parser'

        field_values[attribute_name] = field_value

    if event_data_stream:
      for attribute_name, attribute_value in event_data_stream.GetAttributes():
        # Output path_spec as pathspec for backwards compatibility.
        if attribute_name == 'path_spec':
          attribute_name = 'pathspec'
          attribute_value = self._serializer.WriteSerializedDict(
              attribute_value)

        field_values[attribute_name] = attribute_value

    if event:
      for attribute_name, attribute_value in event.GetAttributes():
        # Ignore attribute container identifier values.
        if isinstance(attribute_value,
                      interface.AttributeContainerIdentifier):
          continue

        # In consolidated mode, skip datetime, timestamp, and timestamp_desc
        # since all individual timestamps are included as separate fields
        if self._consolidated_timestamps:
          if attribute_name in ('date_time', 'timestamp', 'timestamp_desc'):
            continue

        if attribute_name == 'date_time':
          # Map to "datetime" field name for OpenSearch compatibility
          attribute_name = 'datetime'
          # Format as ISO string for OpenSearch compatibility
          if hasattr(attribute_value, 'CopyToDateTimeString'):
            try:
              attribute_value = attribute_value.CopyToDateTimeString()
            except AttributeError:
              # Fallback to serialized dict if string conversion fails
              attribute_value = self._serializer.WriteSerializedDict(
                  attribute_value)
          else:
            attribute_value = self._serializer.WriteSerializedDict(
                attribute_value)

        field_values[attribute_name] = attribute_value

    # Add generated fields
    for field_name in ['display_name', 'filename', 'inode']:
      if field_name not in field_values:
        field_value = field_values.get(field_name, None)
        if field_value is None:
          field_value = self._field_formatting_helper.GetFormattedField(
              self._output_mediator, field_name, event, event_data, event_data_stream,
              event_tag)
          field_values[field_name] = field_value

    # Add message field with custom logic to avoid DEFAULT FORMATTER warning
    try:
      message = self._field_formatting_helper.GetFormattedField(
          self._output_mediator, 'message', event, event_data, event_data_stream,
          event_tag)
      
      # If message contains the default formatter warning, create a cleaner message
      if '<WARNING DEFAULT FORMATTER>' in message:
        message = self._CreateCleanMessage(event_data)
        
      field_values['message'] = message
    except Exception:
      # Fallback to a simple message if formatting fails
      field_values['message'] = self._CreateCleanMessage(event_data)

    if event_tag:
      event_tag_values = {
          '__container_type__': 'event_tag',
          '__type__': 'AttributeContainer'}

      for attribute_name, attribute_value in event_tag.GetAttributes():
        # Ignore attribute container identifier values.
        if isinstance(attribute_value,
                      interface.AttributeContainerIdentifier):
          continue

        event_tag_values[attribute_name] = attribute_value

      field_values['tag'] = event_tag_values

    return field_values

  def _CreateCleanMessage(self, event_data):
    """Creates a clean message without formatter warnings.

    Args:
      event_data (EventData): event data.

    Returns:
      str: clean message.
    """
    if not event_data:
      return 'No event data available'

    # Reserved attributes we want to skip
    reserved_attributes = {
        '_event_values_hash', '_parser_chain', 'data_type', 'date_time',
        'path_spec', 'timestamp', 'timestamp_desc'
    }

    # Try to build a meaningful message from event data attributes
    message_parts = []
    
    for attribute_name, attribute_value in event_data.GetAttributes():
      # Skip reserved attributes and internal attributes
      if (attribute_name in reserved_attributes or 
          attribute_name.startswith('_') or
          isinstance(attribute_value, interface.AttributeContainerIdentifier) or
          isinstance(attribute_value, dfdatetime_interface.DateTimeValues)):
        continue

      # Skip list of datetime values
      if (isinstance(attribute_value, list) and attribute_value and
          isinstance(attribute_value[0], dfdatetime_interface.DateTimeValues)):
        continue

      message_parts.append('{0}: {1}'.format(attribute_name, attribute_value))

    if message_parts:
      return ' '.join(message_parts)
    else:
      # If no meaningful attributes found, show data type
      data_type = getattr(event_data, 'data_type', 'unknown')
      return 'Event of type: {0}'.format(data_type)

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
    elif container.CONTAINER_TYPE == 'event_data_stream':
      identifier = container.GetIdentifier()
      if identifier:
        identifier_string = identifier.CopyToString()
        self._event_data_stream_cache[identifier_string] = container
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

      # Get field values
      field_values = self._GetFieldValues(
          event, event_data, event_data_stream, event_tag)

      try:
        json_string = self._json_encoder.encode(field_values)
        print(json_string, flush=True)
        self._events_streamed += 1
      except Exception:
        # Silently skip events that can't be converted
        pass

    # Forward to real storage writer
    self._real_storage_writer.AddAttributeContainer(container)
    
  def UpdateAttributeContainer(self, container):
    """Updates an attribute container."""
    self._real_storage_writer.UpdateAttributeContainer(container)
    
  def GetNumberOfAttributeContainers(self, container_type):
    """Gets the number of attribute containers."""
    return self._real_storage_writer.GetNumberOfAttributeContainers(container_type)
    
  def GetAttributeContainers(self, container_type, filter_expression=None):
    """Gets attribute containers."""
    return self._real_storage_writer.GetAttributeContainers(container_type, filter_expression)
    
  def GetAttributeContainerByIdentifier(self, container_type, identifier):
    """Gets an attribute container by identifier."""
    return self._real_storage_writer.GetAttributeContainerByIdentifier(container_type, identifier)
    
  def GetAttributeContainerByIndex(self, container_type, index):
    """Gets an attribute container by index."""
    return self._real_storage_writer.GetAttributeContainerByIndex(container_type, index)
    
  def GetFirstWrittenEventSource(self):
    """Gets the first written event source."""
    return self._real_storage_writer.GetFirstWrittenEventSource()
    
  def GetNextWrittenEventSource(self):
    """Gets the next written event source."""
    return self._real_storage_writer.GetNextWrittenEventSource()
    
  def GetFirstWrittenEventData(self):
    """Gets the first written event data."""
    return self._real_storage_writer.GetFirstWrittenEventData()
    
  def GetNextWrittenEventData(self):
    """Gets the next written event data."""
    return self._real_storage_writer.GetNextWrittenEventData() 