# -*- coding: utf-8 -*-
"""Direct output storage writer that bypasses database storage entirely."""

import json
import sys
from acstore.containers import interface

from dfdatetime import interface as dfdatetime_interface

from plaso.serializer import json_serializer
from plaso.storage import writer as storage_writer


class DirectOutputStorageWriter(storage_writer.StorageWriter):
  """Direct output storage writer that avoids database operations.
  
  This writer keeps EventData, EventDataStream, and EventTag in memory
  and outputs complete event records directly without any database I/O.
  
  This is ideal for streaming scenarios where you don't need to persist
  a .plaso file and want maximum performance.
  """

  def __init__(self, output_file=None, event_filter=None,
               consolidated_timestamps=False, output_format='json'):
    """Initializes a direct output storage writer.

    Args:
      output_file (Optional[TextIO]): output file-like object to write to.
          If None, stdout will be used.
      event_filter (Optional[EventObjectFilter]): event filter for filtering
          events by timestamp or other criteria.
      consolidated_timestamps (Optional[bool]): True if timestamps should be
          included as separate fields in the output (one event per record
          with all timestamps).
      output_format (Optional[str]): output format ('json' or 'dict' for
          callback-based processing).
    """
    super(DirectOutputStorageWriter, self).__init__()
    self._output_file = output_file
    self._event_filter = event_filter
    self._consolidated_timestamps = consolidated_timestamps
    self._output_format = output_format
    
    # In-memory containers indexed by identifier
    self._event_data_containers = {}
    self._event_data_stream_containers = {}
    self._event_tag_containers = {}
    
    # Other containers that don't need special handling
    self._other_containers = {}
    
    # For JSON output
    self._serializer = json_serializer.JSONAttributeContainerSerializer()
    self._json_encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=False)
    
    # Output buffering
    self._output_buffer = []
    self._buffer_size = 100  # Flush every 100 events
    
    # Statistics
    self._events_processed = 0
    self._events_output = 0
    self._events_filtered = 0
    
    # Track container counts for GetNumberOfAttributeContainers
    self._container_counts = {}
    
    # Dummy store object to satisfy base class checks
    self._store = self

  def _RaiseIfNotWritable(self):
    """Raises if the storage writer is not writable."""
    if not self._store:
      raise IOError('Unable to write to closed storage writer.')

  def Open(self, path=None, **kwargs):
    """Opens the storage writer.
    
    Args:
      path (Optional[str]): unused, kept for interface compatibility.
    """
    # No database to open - just mark as ready
    self._store = self

  def Close(self):
    """Closes the storage writer."""
    # Flush any remaining buffered output
    self._flush_output_buffer()
    
    # Clear all in-memory containers
    self._event_data_containers.clear()
    self._event_data_stream_containers.clear()
    self._event_tag_containers.clear()
    self._other_containers.clear()
    
    # Mark as closed
    self._store = None

  def _flush_output_buffer(self):
    """Flushes buffered output."""
    if not self._output_buffer:
      return
      
    output_file = self._output_file or sys.stdout
    for output_data in self._output_buffer:
      if self._output_format == 'json':
        output_file.write(output_data)
        output_file.write('\n')
      # For 'dict' format, output_data is already handled by callback
    
    if self._output_format == 'json':
      output_file.flush()
    
    self._output_buffer.clear()

  def _get_container_by_identifier(self, container_type, identifier):
    """Gets a container by identifier from in-memory storage.
    
    Args:
      container_type (str): container type.
      identifier (AttributeContainerIdentifier): container identifier.
    
    Returns:
      AttributeContainer: container or None if not found.
    """
    if identifier is None:
      return None
    
    seq_num = identifier.sequence_number
    
    if container_type == 'event_data':
      return self._event_data_containers.get(seq_num)
    elif container_type == 'event_data_stream':
      return self._event_data_stream_containers.get(seq_num)
    elif container_type == 'event_tag':
      return self._event_tag_containers.get(seq_num)
    else:
      return self._other_containers.get((container_type, seq_num))

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
        # Ignore attribute container identifier values
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

        # Ignore protected internal only attributes
        if attribute_name[0] == '_' and attribute_name != '_parser_chain':
          continue

        # Output _parser_chain as parser for backwards compatibility
        if attribute_name == '_parser_chain':
          field_values['parser'] = attribute_value
        else:
          field_values[attribute_name] = attribute_value

    if event_data_stream:
      for attribute_name, attribute_value in event_data_stream.GetAttributes():
        # Ignore attribute container identifier values
        if isinstance(attribute_value, interface.AttributeContainerIdentifier):
          continue
          
        # Output path_spec as pathspec for backwards compatibility
        if attribute_name == 'path_spec':
          attribute_name = 'pathspec'
          attribute_value = self._serializer.WriteSerializedDict(attribute_value)

        field_values[attribute_name] = attribute_value

    if event:
      for attribute_name, attribute_value in event.GetAttributes():
        # Ignore attribute container identifier values
        if isinstance(attribute_value,
                      interface.AttributeContainerIdentifier):
          continue

        # In consolidated mode, skip datetime, timestamp, and timestamp_desc
        # since all individual timestamps are included as separate fields
        if self._consolidated_timestamps:
          if attribute_name in ('date_time', 'timestamp', 'timestamp_desc'):
            continue

        if attribute_name == 'date_time':
          # Map to "datetime" field name for compatibility
          attribute_name = 'datetime'
          # Format as ISO string
          if hasattr(attribute_value, 'CopyToDateTimeString'):
            try:
              attribute_value = attribute_value.CopyToDateTimeString()
            except AttributeError:
              # Fallback to serialized dict
              attribute_value = self._serializer.WriteSerializedDict(attribute_value)
          else:
            attribute_value = self._serializer.WriteSerializedDict(attribute_value)

        field_values[attribute_name] = attribute_value

    # Add simple message field
    if 'message' not in field_values:
      field_values['message'] = self._CreateCleanMessage(event_data)

    if event_tag:
      event_tag_values = {
          '__container_type__': 'event_tag',
          '__type__': 'AttributeContainer'}

      for attribute_name, attribute_value in event_tag.GetAttributes():
        # Ignore attribute container identifier values
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
    import sys
    container_type = container.CONTAINER_TYPE
    
    # Track container counts
    if container_type not in self._container_counts:
      self._container_counts[container_type] = 0
      print(f'🔹 First {container_type} container received!', file=sys.stderr)
    
    self._container_counts[container_type] += 1
    
    # Log first few of each type
    if self._container_counts[container_type] <= 3:
      print(f'🔹 AddAttributeContainer called: {container_type} #{self._container_counts[container_type]}', file=sys.stderr)
    
    # Store non-event containers in memory for later lookup
    if container_type == 'event_data':
      identifier = container.GetIdentifier()
      if identifier:
        self._event_data_containers[identifier.sequence_number] = container
      return
      
    elif container_type == 'event_data_stream':
      identifier = container.GetIdentifier()
      if identifier:
        self._event_data_stream_containers[identifier.sequence_number] = container
      return
      
    elif container_type == 'event_tag':
      identifier = container.GetIdentifier()
      if identifier:
        self._event_tag_containers[identifier.sequence_number] = container
      return
    
    # For events, output them immediately
    elif container_type == 'event':
      self._process_event(container)
      return
    
    # Handle session and other metadata containers
    elif container_type == 'session':
      print(f'📋 Session container received', file=sys.stderr)
      # Session containers don't need processing for direct output
      return
    
    elif container_type == 'event_source':
      # Store event_source containers - they need to be iterated for processing
      identifier = container.GetIdentifier()
      if identifier:
        self._other_containers[('event_source', identifier.sequence_number)] = container
      return
    
    elif container_type in ['source_configuration', 'system_configuration', 
                             'processing_configuration', 'task', 
                             'analysis_report', 'analysis_warning',
                             'extraction_warning', 'preprocessing_warning',
                             'recovery_warning']:
      # These containers don't need processing for direct output
      return
    
    # Store other container types that might be needed
    else:
      print(f'⚠️  Unknown container type received: {container_type}', file=sys.stderr)
      identifier = container.GetIdentifier()
      if identifier:
        self._other_containers[(container_type, identifier.sequence_number)] = container

  def _process_event(self, event):
    """Processes an event and outputs it immediately.
    
    Args:
      event (EventObject): event to process.
    """
    self._events_processed += 1
    
    # Log first few events for debugging
    if self._events_processed <= 3:
      print(f'📝 Processing event #{self._events_processed}', file=sys.stderr)
    elif self._events_processed % 1000 == 0:
      print(f'📊 Processed {self._events_processed} events...', file=sys.stderr)
    
    # Get related containers from in-memory storage
    event_data = None
    event_data_stream = None
    event_tag = None

    # Get event data
    if hasattr(event, 'GetEventDataIdentifier'):
      event_data_identifier = event.GetEventDataIdentifier()
      event_data = self._get_container_by_identifier('event_data', event_data_identifier)

    # Get event data stream
    if event_data and hasattr(event_data, 'GetEventDataStreamIdentifier'):
      event_data_stream_identifier = event_data.GetEventDataStreamIdentifier()
      event_data_stream = self._get_container_by_identifier(
          'event_data_stream', event_data_stream_identifier)

    # Get event tag
    if hasattr(event, 'GetEventTagIdentifier'):
      event_tag_identifier = event.GetEventTagIdentifier()
      event_tag = self._get_container_by_identifier('event_tag', event_tag_identifier)

    # Apply event filter if configured
    if self._event_filter:
      try:
        filter_match = self._event_filter.Match(
            event, event_data, event_data_stream, event_tag)
        # If filter doesn't match, skip this event
        if filter_match is False:
          self._events_filtered += 1
          return
      except Exception:
        # If filtering fails, include the event to be safe
        pass

    # Get field values
    field_values = self._GetFieldValues(
        event, event_data, event_data_stream, event_tag)

    # Output based on format
    try:
      if self._output_format == 'json':
        json_string = self._json_encoder.encode(field_values)
        self._output_buffer.append(json_string)
      elif self._output_format == 'dict':
        # For dict format, just store the dict
        self._output_buffer.append(field_values)
      
      self._events_output += 1
      
      # Flush buffer when it reaches the threshold
      if len(self._output_buffer) >= self._buffer_size:
        self._flush_output_buffer()
    except Exception:
      # Silently skip events that can't be converted
      pass

  def UpdateAttributeContainer(self, container):
    """Updates an attribute container.
    
    Args:
      container (AttributeContainer): attribute container.
    """
    # For in-memory storage, just update the reference
    container_type = container.CONTAINER_TYPE
    identifier = container.GetIdentifier()
    
    if not identifier:
      return
    
    seq_num = identifier.sequence_number
    
    if container_type == 'event_data':
      self._event_data_containers[seq_num] = container
    elif container_type == 'event_data_stream':
      self._event_data_stream_containers[seq_num] = container
    elif container_type == 'event_tag':
      self._event_tag_containers[seq_num] = container
    else:
      self._other_containers[(container_type, seq_num)] = container

  def GetNumberOfAttributeContainers(self, container_type):
    """Gets the number of attribute containers.
    
    Args:
      container_type (str): container type.
    
    Returns:
      int: number of containers of the specified type.
    """
    return self._container_counts.get(container_type, 0)

  def GetAttributeContainers(self, container_type, filter_expression=None):
    """Gets attribute containers.
    
    Args:
      container_type (str): container type.
      filter_expression (Optional[str]): filter expression.
    
    Yields:
      AttributeContainer: attribute container.
    """
    # Simple implementation - return all containers of the type
    # (filter_expression not fully implemented for simplicity)
    if container_type == 'event_data':
      for container in self._event_data_containers.values():
        yield container
    elif container_type == 'event_data_stream':
      for container in self._event_data_stream_containers.values():
        yield container
    elif container_type == 'event_tag':
      for container in self._event_tag_containers.values():
        yield container
    else:
      for (cont_type, seq_num), container in self._other_containers.items():
        if cont_type == container_type:
          yield container

  def GetAttributeContainerByIdentifier(self, container_type, identifier):
    """Gets an attribute container by identifier.
    
    Args:
      container_type (str): container type.
      identifier (AttributeContainerIdentifier): container identifier.
    
    Returns:
      AttributeContainer: attribute container or None.
    """
    return self._get_container_by_identifier(container_type, identifier)

  def GetAttributeContainerByIndex(self, container_type, index):
    """Gets an attribute container by index.
    
    Args:
      container_type (str): container type.
      index (int): container index.
    
    Returns:
      AttributeContainer: attribute container or None.
    """
    # Simple implementation - get by sequence number
    # In a real implementation, you might maintain separate indexing
    if container_type == 'event_data':
      return self._event_data_containers.get(index)
    elif container_type == 'event_data_stream':
      return self._event_data_stream_containers.get(index)
    elif container_type == 'event_tag':
      return self._event_tag_containers.get(index)
    return None

  def GetFirstWrittenEventSource(self):
    """Gets the first written event source.
    
    Returns:
      EventSource: the first event source container or None if empty.
    """
    # Get event sources from other_containers
    self._event_source_list = [
        container for (ctype, _), container in self._other_containers.items()
        if ctype == 'event_source'
    ]
    self._event_source_index = 0
    return self.GetNextWrittenEventSource()

  def GetNextWrittenEventSource(self):
    """Gets the next written event source.
    
    Returns:
      EventSource: the next event source container or None if exhausted.
    """
    if not hasattr(self, '_event_source_list') or self._event_source_list is None:
      return None
    if self._event_source_index >= len(self._event_source_list):
      self._event_source_list = None
      return None
    event_source = self._event_source_list[self._event_source_index]
    self._event_source_index += 1
    return event_source

  def GetFirstWrittenEventData(self):
    """Gets the first written event data.
    
    Returns:
      EventData: the first event data container or None if empty.
    """
    # Initialize iterator for event_data containers
    self._event_data_iterator = iter(self._event_data_containers.values())
    return self.GetNextWrittenEventData()

  def GetNextWrittenEventData(self):
    """Gets the next written event data.
    
    Returns:
      EventData: the next event data container or None if exhausted.
    """
    if not hasattr(self, '_event_data_iterator') or self._event_data_iterator is None:
      return None
    try:
      return next(self._event_data_iterator)
    except StopIteration:
      self._event_data_iterator = None
      return None

  def GetStatistics(self):
    """Gets processing statistics.
    
    Returns:
      dict: statistics about events processed and output.
    """
    return {
        'events_processed': self._events_processed,
        'events_output': self._events_output,
        'events_filtered': self._events_filtered,
        'memory_containers': {
            'event_data': len(self._event_data_containers),
            'event_data_stream': len(self._event_data_stream_containers),
            'event_tag': len(self._event_tag_containers),
            'other': len(self._other_containers)
        }
    }

