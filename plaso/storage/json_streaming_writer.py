# -*- coding: utf-8 -*-
"""JSON streaming storage writer for outputting events directly to stdout."""

import json
import os
import tempfile

from plaso.storage import factory as storage_factory
from plaso.storage import writer as storage_writer


class JSONStreamingStorageWriter(storage_writer.StorageWriter):
  """A wrapper storage writer that outputs events as JSON to stdout."""

  def __init__(self):
    """Initializes the JSON streaming storage writer."""
    super(JSONStreamingStorageWriter, self).__init__()
    
    self._json_encoder = json.JSONEncoder(ensure_ascii=False, sort_keys=True)
    
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
    
  def AddAttributeContainer(self, container):
    """Adds an attribute container and outputs events as JSON if they are events.
    
    Args:
      container: The attribute container to add.
    """
    # Add to the real storage writer first
    self._real_storage_writer.AddAttributeContainer(container)
    
    # Check if this is an event container we should output
    container_type = type(container).__name__
    if container_type == 'EventObject':
      # Convert the event to a JSON object and output it
      try:
        event_dict = self._ConvertEventToDict(container)
        if event_dict:
          json_line = self._json_encoder.encode(event_dict)
          print(json_line)
      except Exception:
        # Silently skip events that can't be converted
        pass
    
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
    
  def _ConvertEventToDict(self, event):
    """Converts an event to a dictionary for JSON output.
    
    Args:
      event: The event object to convert.
      
    Returns:
      dict: Event dictionary.
    """
    event_dict = {}
    
    # Add container type and type information
    event_dict['__container_type__'] = 'event'
    event_dict['__type__'] = 'AttributeContainer'
    
    # Get all attributes from the event
    if hasattr(event, 'GetAttributes'):
      for attribute_name, attribute_value in event.GetAttributes():
        # Skip internal attributes except for important ones
        if attribute_name.startswith('_') and attribute_name not in ['_parser_chain']:
          continue
          
        # Handle parser chain specially
        if attribute_name == '_parser_chain':
          event_dict['parser'] = attribute_value
        else:
          event_dict[attribute_name] = self._SerializeValue(attribute_value)
    else:
      # Fallback: get attributes from __dict__
      for attribute_name, attribute_value in event.__dict__.items():
        if not attribute_name.startswith('_'):
          event_dict[attribute_name] = self._SerializeValue(attribute_value)
    
    # Try to get event data from the storage writer if we have an event data identifier
    if hasattr(event, 'GetEventDataIdentifier'):
      event_data_identifier = event.GetEventDataIdentifier()
      if event_data_identifier:
        try:
          event_data = self._real_storage_writer.GetAttributeContainerByIdentifier(
              'event_data', event_data_identifier)
          if event_data:
            # Add event data attributes
            if hasattr(event_data, 'GetAttributes'):
              for attr_name, attr_value in event_data.GetAttributes():
                if not attr_name.startswith('_'):
                  event_dict[attr_name] = self._SerializeValue(attr_value)
            else:
              for attr_name, attr_value in event_data.__dict__.items():
                if not attr_name.startswith('_'):
                  event_dict[attr_name] = self._SerializeValue(attr_value)
        except Exception:
          # If we can't get event data, continue without it
          pass
    
    return event_dict
    
  def _SerializeValue(self, value):
    """Serializes a value for JSON output.
    
    Args:
      value: The value to serialize.
      
    Returns:
      The serialized value.
    """
    try:
      # Handle datetime objects
      if hasattr(value, 'timestamp'):
        return int(value.timestamp)
      elif hasattr(value, 'CopyToDateTimeString'):
        return value.CopyToDateTimeString()
      elif hasattr(value, '__dict__'):
        # For complex objects, try to serialize their dict representation
        return {k: v for k, v in value.__dict__.items() if not k.startswith('_')}
      else:
        return value
    except Exception:
      # If all else fails, convert to string
      return str(value) 