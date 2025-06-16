# -*- coding: utf-8 -*-
"""JSON streaming storage writer for outputting events directly to stdout."""

import json
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

  def __init__(self, output_file=None):
    """Initializes a JSON streaming storage writer.

    Args:
      output_file (Optional[TextIO]): output file-like object to write to.
          If None, stdout will be used.
    """
    super(JSONStreamingStorageWriter, self).__init__()
    self._output_file = output_file
    self._serializer = json_serializer.JSONAttributeContainerSerializer()
    self._field_formatting_helper = shared_json.JSONFieldFormattingHelper()
    self._json_encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True)
    self._output_mediator = mediator.OutputMediator(storage_reader=self)
    
    # Create a temporary file for the real storage
    self._temp_file = tempfile.NamedTemporaryFile(suffix='.plaso', delete=False)
    self._temp_file.close()
    
    # Create a real storage writer that writes to the temp file
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
    self._real_storage_writer.Open(path=self._temp_file.name)
    
    # Set _store to satisfy the base class _RaiseIfNotWritable check
    self._store = self._real_storage_writer._store

  def Close(self):
    """Closes the storage writer and cleans up temp file."""
    if self._real_storage_writer:
      self._real_storage_writer.Close()
      
    # Clear _store to satisfy base class
    self._store = None
      
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
        # Ignore attribute container identifier and date and time values.
        if isinstance(attribute_value, (
            interface.AttributeContainerIdentifier,
            dfdatetime_interface.DateTimeValues)):
          continue

        if (isinstance(attribute_value, list) and attribute_value and
            isinstance(attribute_value[0],
                       dfdatetime_interface.DateTimeValues)):
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

        if attribute_name == 'date_time':
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

    # Add message field
    field_values['message'] = self._field_formatting_helper.GetFormattedField(
        self._output_mediator, 'message', event, event_data, event_data_stream,
        event_tag)

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

      # Get field values
      field_values = self._GetFieldValues(
          event, event_data, event_data_stream, event_tag)

      try:
        json_string = self._json_encoder.encode(field_values)
        print(json_string, flush=True)
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