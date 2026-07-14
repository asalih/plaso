#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the HTTP streaming storage writer."""

import queue
import unittest
from unittest import mock

from plaso.containers import events
from plaso.storage import http_streaming_writer

from tests.storage import test_lib


class HTTPStreamingStorageWriterTest(test_lib.StorageTestCase):
  """Tests for the HTTP streaming storage writer."""

  # pylint: disable=protected-access

  def testAddMatchingEventWithoutStorage(self):
    """Tests adding a matching event without storing final containers."""
    event_filter = mock.Mock()
    event_filter.Match.return_value = True

    storage_writer = http_streaming_writer.HTTPStreamingStorageWriter(
        'https://example.com/events', event_filter=event_filter,
        store_events_in_storage=False, stream_storage='memory')
    storage_writer._real_storage_writer = mock.Mock()
    storage_writer._GetFieldValues = mock.Mock(return_value={'event': 'test'})

    event_tag = events.EventTag()
    event = events.EventObject()

    storage_writer.AddAttributeContainer(event_tag)
    storage_writer._real_storage_writer.AddAttributeContainer.assert_not_called()

    storage_writer.AddAttributeContainer(event)

    event_filter.Match.assert_called_once()
    self.assertEqual(storage_writer._event_queue.get_nowait(), {'event': 'test'})
    storage_writer._real_storage_writer.AddAttributeContainer.assert_not_called()

  def testAddRejectedEventWithoutStorage(self):
    """Tests adding a rejected event without storing final containers."""
    event_filter = mock.Mock()
    event_filter.Match.return_value = False

    storage_writer = http_streaming_writer.HTTPStreamingStorageWriter(
        'https://example.com/events', event_filter=event_filter,
        store_events_in_storage=False, stream_storage='memory')
    storage_writer._real_storage_writer = mock.Mock()

    event_tag = events.EventTag()
    event = events.EventObject()

    storage_writer.AddAttributeContainer(event_tag)
    storage_writer._real_storage_writer.AddAttributeContainer.assert_not_called()

    storage_writer.AddAttributeContainer(event)

    event_filter.Match.assert_called_once()
    with self.assertRaises(queue.Empty):
      storage_writer._event_queue.get_nowait()
    storage_writer._real_storage_writer.AddAttributeContainer.assert_not_called()


if __name__ == '__main__':
  unittest.main()
