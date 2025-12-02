#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the direct output storage writer."""

import io
import json
import unittest

from acstore.containers import interface as containers_interface

from plaso.containers import events
from plaso.storage import direct_output_writer

from tests import test_lib as shared_test_lib


class DirectOutputStorageWriterTest(shared_test_lib.BaseTestCase):
  """Tests for the direct output storage writer."""

  def testInitialization(self):
    """Tests initialization."""
    output_file = io.StringIO()
    storage_writer = direct_output_writer.DirectOutputStorageWriter(
        output_file=output_file)

    self.assertIsNotNone(storage_writer)

  def testOpenClose(self):
    """Tests Open and Close methods."""
    output_file = io.StringIO()
    storage_writer = direct_output_writer.DirectOutputStorageWriter(
        output_file=output_file)

    storage_writer.Open()
    self.assertIsNotNone(storage_writer._store)

    storage_writer.Close()
    self.assertIsNone(storage_writer._store)

  def testAddEventData(self):
    """Tests adding EventData containers."""
    output_file = io.StringIO()
    storage_writer = direct_output_writer.DirectOutputStorageWriter(
        output_file=output_file)
    storage_writer.Open()

    # Create an EventData container
    event_data = events.EventData()
    event_data.data_type = 'test:event'
    event_data.filename = 'test.log'

    # Set an identifier
    identifier = containers_interface.AttributeContainerIdentifier(
        name='event_data', sequence_number=1)
    event_data.SetIdentifier(identifier)

    # Add it to the writer
    storage_writer.AddAttributeContainer(event_data)

    # Verify it's stored in memory
    self.assertEqual(len(storage_writer._event_data_containers), 1)
    self.assertIn(1, storage_writer._event_data_containers)

    # Verify we can retrieve it
    retrieved = storage_writer._get_container_by_identifier('event_data', identifier)
    self.assertIsNotNone(retrieved)
    self.assertEqual(retrieved.filename, 'test.log')

    storage_writer.Close()

  def testAddEventDataStream(self):
    """Tests adding EventDataStream containers."""
    output_file = io.StringIO()
    storage_writer = direct_output_writer.DirectOutputStorageWriter(
        output_file=output_file)
    storage_writer.Open()

    # Create an EventDataStream container
    event_data_stream = events.EventDataStream()

    # Set an identifier
    identifier = containers_interface.AttributeContainerIdentifier(
        name='event_data_stream', sequence_number=1)
    event_data_stream.SetIdentifier(identifier)

    # Add it to the writer
    storage_writer.AddAttributeContainer(event_data_stream)

    # Verify it's stored in memory
    self.assertEqual(len(storage_writer._event_data_stream_containers), 1)

    storage_writer.Close()

  def testCompleteEventOutput(self):
    """Tests outputting a complete event with related containers."""
    output_file = io.StringIO()
    storage_writer = direct_output_writer.DirectOutputStorageWriter(
        output_file=output_file)
    storage_writer.Open()

    # Create EventDataStream
    event_data_stream = events.EventDataStream()
    stream_id = containers_interface.AttributeContainerIdentifier(
        name='event_data_stream', sequence_number=1)
    event_data_stream.SetIdentifier(stream_id)
    storage_writer.AddAttributeContainer(event_data_stream)

    # Create EventData
    event_data = events.EventData()
    event_data.data_type = 'test:event'
    event_data.filename = 'test.log'
    event_data.message = 'Test message'
    data_id = containers_interface.AttributeContainerIdentifier(
        name='event_data', sequence_number=1)
    event_data.SetIdentifier(data_id)
    event_data.SetEventDataStreamIdentifier(stream_id)
    storage_writer.AddAttributeContainer(event_data)

    # Create Event
    event = events.EventObject()
    event.timestamp = 1234567890000000
    event.timestamp_desc = 'Creation Time'
    event.SetEventDataIdentifier(data_id)

    # Add the event - this should trigger output
    storage_writer.AddAttributeContainer(event)

    # Flush to ensure output
    storage_writer.Close()

    # Get the output
    output = output_file.getvalue()
    
    # Verify we got JSON output
    self.assertTrue(len(output) > 0)
    
    # Parse the JSON
    lines = output.strip().split('\n')
    self.assertEqual(len(lines), 1)
    
    event_dict = json.loads(lines[0])
    
    # Verify the event contains expected fields
    self.assertIn('data_type', event_dict)
    self.assertEqual(event_dict['data_type'], 'test:event')
    self.assertIn('filename', event_dict)
    self.assertEqual(event_dict['filename'], 'test.log')
    self.assertIn('timestamp', event_dict)
    self.assertEqual(event_dict['timestamp'], 1234567890000000)

  def testGetNumberOfAttributeContainers(self):
    """Tests GetNumberOfAttributeContainers method."""
    output_file = io.StringIO()
    storage_writer = direct_output_writer.DirectOutputStorageWriter(
        output_file=output_file)
    storage_writer.Open()

    # Add some containers
    event_data = events.EventData()
    event_data.data_type = 'test:event'
    data_id = containers_interface.AttributeContainerIdentifier(
        name='event_data', sequence_number=1)
    event_data.SetIdentifier(data_id)
    storage_writer.AddAttributeContainer(event_data)

    # Check count
    count = storage_writer.GetNumberOfAttributeContainers('event_data')
    self.assertEqual(count, 1)

    storage_writer.Close()

  def testGetStatistics(self):
    """Tests GetStatistics method."""
    output_file = io.StringIO()
    storage_writer = direct_output_writer.DirectOutputStorageWriter(
        output_file=output_file)
    storage_writer.Open()

    stats = storage_writer.GetStatistics()
    
    self.assertIn('events_processed', stats)
    self.assertIn('events_output', stats)
    self.assertIn('events_filtered', stats)
    self.assertIn('memory_containers', stats)

    self.assertEqual(stats['events_processed'], 0)
    self.assertEqual(stats['events_output'], 0)

    storage_writer.Close()


if __name__ == '__main__':
  unittest.main()

