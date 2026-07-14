#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests the task-based multi-process engine."""

import errno
import os
import unittest

import mock

from plaso.containers import tasks
from plaso.engine import configurations
from plaso.lib import definitions
from plaso.multi_process import task_engine

from tests import test_lib as shared_test_lib


class TaskMultiProcessEngineTest(shared_test_lib.BaseTestCase):
  """Tests for the task-based multi-process engine."""

  # pylint: disable=protected-access

  def testStartTaskStorage(self):
    """Tests that StartTaskStorage creates one temporary task directory."""
    test_engine = task_engine.TaskMultiProcessEngine()
    test_engine._processing_configuration = (
        configurations.ProcessingConfiguration())

    with shared_test_lib.TempDirectory() as temp_directory:
      test_engine._storage_file_path = os.path.join(
          temp_directory, 'storage.plaso')

      with mock.patch(
          'plaso.multi_process.task_engine.tempfile.mkdtemp',
          wraps=task_engine.tempfile.mkdtemp) as mock_mkdtemp:
        test_engine._StartTaskStorage(definitions.STORAGE_FORMAT_SQLITE)

      self.assertEqual(mock_mkdtemp.call_count, 1)
      self.assertTrue(os.path.isdir(test_engine._task_storage_path))
      self.assertTrue(os.path.isdir(test_engine._merge_task_storage_path))
      self.assertTrue(os.path.isdir(test_engine._processed_task_storage_path))
      self.assertEqual(
          test_engine._processing_configuration.task_storage_path,
          test_engine._task_storage_path)

      test_engine._StopTaskStorage(
          definitions.STORAGE_FORMAT_SQLITE, 'session', abort=True)

  def testPrepareMergeTaskStorageRetry(self):
    """Tests retrying a Windows sharing violation during task rename."""
    test_engine = task_engine.TaskMultiProcessEngine()

    with shared_test_lib.TempDirectory() as temp_directory:
      test_engine._merge_task_storage_path = os.path.join(
          temp_directory, 'merge')
      test_engine._processed_task_storage_path = os.path.join(
          temp_directory, 'processed')
      os.mkdir(test_engine._merge_task_storage_path)
      os.mkdir(test_engine._processed_task_storage_path)

      task = tasks.Task()
      task.identifier = 'task'
      task.session_identifier = 'session'
      processed_path = test_engine._GetProcessedStorageFilePath(task)
      with open(processed_path, 'wb') as file_object:
        file_object.write(b'test')

      sharing_error = OSError(errno.EACCES, 'file is in use')
      sharing_error.winerror = 32
      original_rename = os.rename
      rename_attempts = []

      def _RenameWithSharingViolation(source, destination):
        rename_attempts.append((source, destination))
        if len(rename_attempts) == 1:
          raise sharing_error
        original_rename(source, destination)

      with mock.patch(
          'plaso.multi_process.task_engine.os.rename',
          side_effect=_RenameWithSharingViolation):
        with mock.patch('plaso.multi_process.task_engine.time.sleep'):
          test_engine._PrepareMergeTaskStorage(
              definitions.STORAGE_FORMAT_SQLITE, task)

      self.assertEqual(len(rename_attempts), 2)
      merge_path = test_engine._GetMergeTaskStorageFilePath(
          definitions.STORAGE_FORMAT_SQLITE, task)
      self.assertTrue(os.path.isfile(merge_path))


if __name__ == '__main__':
  unittest.main()
