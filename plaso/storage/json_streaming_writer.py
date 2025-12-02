# -*- coding: utf-8 -*-
"""JSON streaming storage writer for outputting events directly to stdout."""

from functools import lru_cache
import json
import os
import sys
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
               consolidated_timestamps=False):
    """Initializes a JSON streaming storage writer.

    Args:
      output_file (Optional[TextIO]): output file-like object to write to.
          If None, stdout will be used.
      event_filter (Optional[EventObjectFilter]): event filter for filtering
          events by timestamp or other criteria.
      consolidated_timestamps (Optional[bool]): True if timestamps should be
          included as separate fields in the output (one event per record
          with all timestamps).
    """
    super(JSONStreamingStorageWriter, self).__init__()
    self._output_file = output_file
    self._serializer = json_serializer.JSONAttributeContainerSerializer()
    self._field_formatting_helper = shared_json.JSONFieldFormattingHelper()
    self._json_encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=False)
    self._output_mediator = mediator.OutputMediator(storage_reader=self)
    self._event_filter = event_filter
    self._consolidated_timestamps = consolidated_timestamps
    
    # Buffering for stdout output (reduce flush overhead)
    self._output_buffer = []
    self._buffer_size = 100  # Flush every 100 events
    
    # Create a temporary file for the real storage
    self._temp_file = tempfile.NamedTemporaryFile(suffix='.plaso', delete=False)
    self._temp_file.close()
    
    # Create a real storage writer that writes to the temp file
    self._real_storage_writer = storage_factory.StorageFactory.CreateStorageWriter(
        'sqlite')
    
    # Set _store to the real storage writer's store to satisfy base class checks
    self._store = None  # Will be set in Open()
    
    # Cache for container lookups (significant performance improvement)
    self._container_cache = {}

  def _RaiseIfNotWritable(self):
    """Raises if the storage writer is not writable."""
    if not self._real_storage_writer:
      raise IOError('Unable to write to closed storage writer.')
    
  def Open(self, path=None, **kwargs):
    """Opens the storage writer."""
    self._real_storage_writer.Open(path=self._temp_file.name)
    
    # Set _store to satisfy the base class _RaiseIfNotWritable check
    self._store = self._real_storage_writer._store

  def Close(self):
    """Closes the storage writer and cleans up temp file."""
    # Flush any remaining buffered output
    self._flush_output_buffer()
    
    if self._real_storage_writer:
      self._real_storage_writer.Close()
      
    # Clear _store to satisfy base class
    self._store = None
      
    # Clear container cache
    self._container_cache.clear()
      
    # Clean up temp file
    try:
      os.unlink(self._temp_file.name)
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

  def _get_cached_container(self, container_type, identifier):
    """Gets a container from cache or database.
    
    Args:
      container_type (str): container type.
      identifier (AttributeContainerIdentifier): container identifier.
    
    Returns:
      AttributeContainer: container or None if not found.
    """
    if identifier is None:
      return None
      
    # Create cache key from container type and identifier
    cache_key = (container_type, identifier.sequence_number)
    
    # Check cache first
    if cache_key in self._container_cache:
      return self._container_cache[cache_key]
    
    # Not in cache, fetch from database
    try:
      container = self._real_storage_writer.GetAttributeContainerByIdentifier(
          container_type, identifier)
      
      # Store in cache (limit cache size to prevent memory issues)
      if len(self._container_cache) < 10000:
        self._container_cache[cache_key] = container
      elif len(self._container_cache) >= 10000:
        # Cache full, clear oldest entries (simple LRU approximation)
        # Remove first 20% of items
        items_to_remove = list(self._container_cache.keys())[:2000]
        for key in items_to_remove:
          del self._container_cache[key]
        self._container_cache[cache_key] = container
        
      return container
    except Exception:
      return None

  def _flush_output_buffer(self):
    """Flushes buffered JSON output to stdout."""
    if not self._output_buffer:
      return
      
    output_file = self._output_file or sys.stdout
    for json_string in self._output_buffer:
      output_file.write(json_string)
      output_file.write('\n')
    output_file.flush()
    self._output_buffer.clear()

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

      # Get event data using cached lookup
      if hasattr(event, 'GetEventDataIdentifier'):
        event_data_identifier = event.GetEventDataIdentifier()
        event_data = self._get_cached_container('event_data', event_data_identifier)

      # Get event data stream using cached lookup
      if event_data and hasattr(event_data, 'GetEventDataStreamIdentifier'):
        event_data_stream_identifier = event_data.GetEventDataStreamIdentifier()
        event_data_stream = self._get_cached_container(
            'event_data_stream', event_data_stream_identifier)

      # Get event tag using cached lookup
      if hasattr(event, 'GetEventTagIdentifier'):
        event_tag_identifier = event.GetEventTagIdentifier()
        event_tag = self._get_cached_container('event_tag', event_tag_identifier)

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

      # Get field values
      field_values = self._GetFieldValues(
          event, event_data, event_data_stream, event_tag)

      try:
        json_string = self._json_encoder.encode(field_values)
        # Buffer output instead of immediate flush
        self._output_buffer.append(json_string)
        
        # Flush buffer when it reaches the threshold
        if len(self._output_buffer) >= self._buffer_size:
          self._flush_output_buffer()
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