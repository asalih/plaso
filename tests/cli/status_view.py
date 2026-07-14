#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests for the status view."""

import json
import os
import stat
import sys
import unittest

import mock

from dfvfs.lib import definitions as dfvfs_definitions

import plaso

from plaso.cli import status_view
from plaso.engine import processing_status

from tests import test_lib as shared_test_lib
from tests.cli import test_lib


class StatusViewTest(test_lib.CLIToolTestCase):
  """Tests for the status view."""

  # pylint: disable=protected-access

  def _MockTime(self):
    """Mock function to simulate time.time()

    Returns:
      int: stored time via self._mocked_time
    """
    return self._mocked_time

  def setUp(self):
    """Makes preparations before running an individual test."""

    self.mock_time = mock.patch(
        'plaso.cli.status_view.time.time', self._MockTime)
    self._mocked_time = 0
    self.mock_time.start()

  def tearDown(self):
    """Cleans up after running an individual test."""
    self.mock_time.stop()

  def _CheckOutput(self, output, expected_output):
    """Compares the output against the expected output.

    The actual processing time is ignored, since it can vary.

    Args:
      output (str): tool output.
      expected_output (list[str]): expected tool output.
    """
    output = output.split('\n')

    self.assertEqual(output[:4], expected_output[:4])
    self.assertTrue(output[4].startswith('Processing time\t\t: '))
    self.assertEqual(output[5:], expected_output[5:])

  # TODO: add tests for _ClearScreen
  # TODO: add tests for _FormatAnalysisStatusTableRow
  # TODO: add tests for _FormatExtractionStatusTableRow
  # TODO: add tests for _FormatSizeInUnitsOf1024
  # TODO: add tests for _PrintAnalysisStatusHeader
  # TODO: add tests for _PrintAnalysisStatusUpdateLinear
  # TODO: add tests for _PrintAnalysisStatusUpdateWindow
  # TODO: add tests for _PrintEventsStatus

  def testPrintExtractionStatusUpdateLinear(self):
    """Tests the PrintExtractionStatusUpdateLinear function."""
    output_writer = test_lib.TestOutputWriter()

    test_view = status_view.StatusView(output_writer, 'test_tool')
    test_view.SetSourceInformation(
        '/test/source/path', dfvfs_definitions.SOURCE_TYPE_DIRECTORY)

    process_status = processing_status.ProcessingStatus()
    process_status.UpdateForemanStatus(
        'f_identifier', 'f_status', 123, 0,
        'f_test_file', 1, 29, 1, 2, 3, 456, 5, 6, 9, 10)
    test_view._PrintExtractionStatusUpdateLinear(process_status)

    expected_output = (
        'Processing time: 00:00:00\n'
        'f_identifier (PID: 123) status: f_status, event data produced: 2, '
        'events produced: 456, file: f_test_file\n'
        '\n')

    output = output_writer.ReadOutput()
    self.assertEqual(output, expected_output)

    process_status.UpdateWorkerStatus(
        'w_identifier', 'w_status', 123, 0,
        'w_test_file', 1, 2, 3, 4, 5, 6, 9, 10, 11, 12)
    test_view._PrintExtractionStatusUpdateLinear(process_status)

    expected_output = (
        'Processing time: 00:00:00\n'
        'f_identifier (PID: 123) status: f_status, event data produced: 2, '
        'events produced: 456, file: f_test_file\n'
        'w_identifier (PID: 123) status: w_status, event data produced: 6, '
        'file: w_test_file\n'
        '\n')

    output = output_writer.ReadOutput()
    self.assertEqual(output, expected_output)

  def testPrintExtractionStatusUpdateJSON(self):
    """Tests the _PrintExtractionStatusUpdateJSON function."""
    output_writer = test_lib.TestOutputWriter()
    test_view = status_view.StatusView(output_writer, 'test_tool')

    process_status = processing_status.ProcessingStatus()
    process_status.UpdateForemanStatus(
        'foreman', 'running', 123, 0, 'merge-task', 1, 2, 3, 4, 5, 6,
        0, 0, 0, 0)
    process_status.UpdateWorkerStatus(
        'worker-1', 'running', 456, 0, '/test/file', 7, 8, 9, 10, 11, 12,
        0, 0, 0, 0)

    tasks_status = processing_status.TasksStatus()
    tasks_status.number_of_abandoned_tasks = 1
    tasks_status.number_of_queued_tasks = 2
    tasks_status.number_of_skipped_sources = 3
    tasks_status.number_of_tasks_pending_merge = 4
    tasks_status.number_of_tasks_processing = 5
    tasks_status.total_number_of_tasks = 6
    process_status.UpdateTasksStatus(tasks_status)

    self._mocked_time = 123.5
    with shared_test_lib.TempDirectory() as temp_directory:
      status_file = os.path.join(temp_directory, 'status.json')
      test_view.SetMode(status_view.StatusView.MODE_JSON)
      test_view.SetStatusFile(status_file)

      callback = test_view.GetExtractionStatusUpdateCallback()
      callback(process_status)

      with open(status_file, 'r', encoding='utf-8') as file_object:
        status = json.load(file_object)

      self.assertEqual(os.listdir(temp_directory), ['status.json'])
      if os.name != 'nt':
        self.assertEqual(stat.S_IMODE(os.stat(status_file).st_mode), 0o600)

    self.assertEqual(status['timestamp'], 123.5)
    self.assertEqual(status['tasks']['skipped'], 3)
    self.assertEqual(status['foreman']['identifier'], 'foreman')
    self.assertEqual(status['workers'][0]['pid'], 456)
    self.assertEqual(status['workers'][0]['display_name'], '/test/file')

  def testPrintExtractionStatusUpdateJSONWriteFailure(self):
    """Tests that a transient status file write failure does not raise."""
    output_writer = test_lib.TestOutputWriter()
    test_view = status_view.StatusView(output_writer, 'test_tool')

    process_status = processing_status.ProcessingStatus()

    self._mocked_time = 123.5
    with shared_test_lib.TempDirectory() as temp_directory:
      status_file = os.path.join(temp_directory, 'status.json')
      test_view.SetMode(status_view.StatusView.MODE_JSON)
      test_view.SetStatusFile(status_file)

      callback = test_view.GetExtractionStatusUpdateCallback()

      sharing_error = OSError(13, 'file is in use')
      with mock.patch(
          'plaso.cli.status_view.os.replace', side_effect=sharing_error):
        callback(process_status)

      # The failed update must not leave a status or temporary file behind.
      self.assertEqual(os.listdir(temp_directory), [])

      # A subsequent update without failures must succeed.
      self._mocked_time = 124.5
      callback(process_status)

      with open(status_file, 'r', encoding='utf-8') as file_object:
        status = json.load(file_object)

    self.assertEqual(status['timestamp'], 124.5)

  def testPrintExtractionStatusUpdateWindow(self):
    """Tests the _PrintExtractionStatusUpdateWindow function."""
    output_writer = test_lib.TestOutputWriter()

    test_view = status_view.StatusView(output_writer, 'test_tool')
    test_view.SetSourceInformation(
        '/test/source/path', dfvfs_definitions.SOURCE_TYPE_DIRECTORY)

    process_status = processing_status.ProcessingStatus()
    process_status.UpdateForemanStatus(
        'f_identifier', 'f_status', 123, 0,
        'f_test_file', 1, 29, 1, 2, 3, 456, 5, 6, 9, 10)
    test_view._PrintExtractionStatusUpdateWindow(process_status)

    table_header = (
        'Identifier      '
        'PID     '
        'Status          '
        'Memory          '
        'Sources         '
        'Events          '
        'File')

    if not sys.platform.startswith('win'):
      table_header = '\x1b[1m{0:s}\x1b[0m'.format(table_header)

    expected_output = [
        'plaso - test_tool version {0:s}'.format(plaso.__version__),
        '',
        'Source path\t\t: /test/source/path',
        'Source type\t\t: directory',
        'Processing time\t\t: 00:00:00',
        '',
        table_header,
        ('f_identifier    '
         '123     '
         'f_status        '
         '0 B             '
         '29 (29)         '
         '456 (456)       '
         'f_test_file'),
        '',
        '']

    output = output_writer.ReadOutput()
    self._CheckOutput(output, expected_output)

    process_status.UpdateWorkerStatus(
        'w_identifier', 'w_status', 123, 0,
        'w_test_file', 1, 2, 3, 4, 5, 6, 9, 10, 11, 12)
    test_view._PrintExtractionStatusUpdateWindow(process_status)

    table_header = (
        'Identifier      '
        'PID     '
        'Status          '
        'Memory          '
        'Sources         '
        'Event Data      '
        'File')

    if not sys.platform.startswith('win'):
      table_header = '\x1b[1m{0:s}\x1b[0m'.format(table_header)

    expected_output = [
        'plaso - test_tool version {0:s}'.format(plaso.__version__),
        '',
        'Source path\t\t: /test/source/path',
        'Source type\t\t: directory',
        'Processing time\t\t: 00:00:00',
        '',
        table_header,
        ('f_identifier    '
         '123     '
         'f_status        '
         '0 B             '
         '29 (29)         '
         '2 (2)           '
         'f_test_file'),
        ('w_identifier    '
         '123     '
         'w_status        '
         '0 B             '
         '2 (2)           '
         '4 (4)           '
         'w_test_file'),
        '',
        '']

    output = output_writer.ReadOutput()
    self._CheckOutput(output, expected_output)

  def testFormatProcessingTime(self):
    """Tests the _FormatProcessingTime function."""
    output_writer = test_lib.TestOutputWriter()

    process_status = processing_status.ProcessingStatus()

    test_view = status_view.StatusView(output_writer, 'test_tool')
    test_view.SetSourceInformation(
        '/test/source/path', dfvfs_definitions.SOURCE_TYPE_DIRECTORY)

    process_status.start_time = 0
    processing_time = test_view._FormatProcessingTime(process_status)

    self.assertEqual(processing_time, '00:00:00')

    self._mocked_time = 12 * 60 * 60 + 31 * 60 +15
    processing_time = test_view._FormatProcessingTime(process_status)

    self.assertEqual(processing_time, '12:31:15')

    self._mocked_time = 24 * 60 * 60
    processing_time = test_view._FormatProcessingTime(process_status)

    self.assertEqual(processing_time, '1 day, 00:00:00')

    self._mocked_time = 5 * 24 * 60 * 60 + 5 * 60 * 60 + 61
    processing_time = test_view._FormatProcessingTime(process_status)

    self.assertEqual(processing_time, '5 days, 05:01:01')

  # TODO: add tests for _PrintTasksStatus
  # TODO: add tests for GetAnalysisStatusUpdateCallback
  # TODO: add tests for GetExtractionStatusUpdateCallback
  # TODO: add tests for PrintAnalysisReportsDetails

  def testPrintExtractionStatusHeader(self):
    """Tests the PrintExtractionStatusHeader function."""
    output_writer = test_lib.TestOutputWriter()

    test_view = status_view.StatusView(output_writer, 'test_tool')
    test_view.SetSourceInformation(
        '/test/source/path', dfvfs_definitions.SOURCE_TYPE_DIRECTORY)

    test_view.PrintExtractionStatusHeader(None)

  # TODO: add tests for PrintExtractionSummary
  # TODO: add tests for SetMode
  # TODO: add tests for SetSourceInformation
  # TODO: add tests for SetStorageFileInformation


if __name__ == '__main__':
  unittest.main()
