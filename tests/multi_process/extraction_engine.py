#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Tests the multi-process processing engine."""

import collections
import os
import unittest

import mock

from dfvfs.lib import definitions as dfvfs_definitions
from dfvfs.path import factory as path_spec_factory

from plaso.containers import sessions
from plaso.containers import tasks
from plaso.lib import definitions
from plaso.engine import configurations
from plaso.multi_process import extraction_engine
from plaso.storage.sqlite import writer as sqlite_writer

from tests import test_lib as shared_test_lib


class EventSourceHeapTest(shared_test_lib.BaseTestCase):
  """Tests for the event source heap."""

  def testIsEmpty(self):
    """Tests the IsEmpty function."""
    heap = extraction_engine._EventSourceHeap()  # pylint: disable=protected-access
    self.assertTrue(heap.IsEmpty())

    event_source = mock.MagicMock()
    event_source.file_entry_type = dfvfs_definitions.FILE_ENTRY_TYPE_FILE
    heap.PushEventSource(event_source)
    self.assertFalse(heap.IsEmpty())

    heap.PopEventSource()
    self.assertTrue(heap.IsEmpty())


class ExtractionMultiProcessEngineTest(shared_test_lib.BaseTestCase):
  """Tests for the task-based multi-process extraction engine."""

  # pylint: disable=protected-access

  def _CreateEngineForMergeTest(self, pending_tasks):
    """Creates an engine with mocked internals for merge loop tests.

    Args:
      pending_tasks (list[Task]): tasks the task manager reports as pending
          merge, in order.

    Returns:
      ExtractionMultiProcessEngine: test engine.
    """
    test_engine = extraction_engine.ExtractionMultiProcessEngine()
    test_engine._task_storage_format = definitions.STORAGE_FORMAT_SQLITE

    task_manager = mock.MagicMock()
    task_manager.GetTaskPendingMerge.side_effect = pending_tasks + [None]
    test_engine._task_manager = task_manager

    return test_engine

  def _RunMergeTaskStorage(self, test_engine, drain):
    """Runs _MergeTaskStorage with storage and merge helpers mocked out.

    Args:
      test_engine (ExtractionMultiProcessEngine): test engine.
      drain (bool): drain argument to pass through.
    """
    merge_helper = mock.MagicMock()
    merge_helper.GetAttributeContainer.return_value = None
    merge_helper.fully_merged = True

    with mock.patch.object(
        test_engine, '_GetProcessedTaskIdentifiers', return_value=[]):
      with mock.patch.object(test_engine, '_GetMergeTaskStorage'):
        with mock.patch.object(test_engine, '_RemoveMergeTaskStorage'):
          with mock.patch(
              'plaso.multi_process.extraction_engine.merge_helpers'
              '.ExtractionTaskMergeHelper', return_value=merge_helper):
            test_engine._MergeTaskStorage(
                mock.MagicMock(), 'session', drain=drain)

  def testMergeTaskStorageMergesSingleTaskPerPass(self):
    """Tests that the default merge handles one task per scheduler pass."""
    pending_tasks = []
    for index in range(3):
      task = tasks.Task()
      task.identifier = f'task{index:d}'
      pending_tasks.append(task)

    test_engine = self._CreateEngineForMergeTest(pending_tasks)
    self._RunMergeTaskStorage(test_engine, drain=False)

    self.assertEqual(test_engine._task_manager.CompleteTask.call_count, 1)

  def testMergeTaskStorageDrainsBacklog(self):
    """Tests that drain mode merges the whole backlog in one pass."""
    pending_tasks = []
    for index in range(3):
      task = tasks.Task()
      task.identifier = f'task{index:d}'
      pending_tasks.append(task)

    test_engine = self._CreateEngineForMergeTest(pending_tasks)
    self._RunMergeTaskStorage(test_engine, drain=True)

    self.assertEqual(test_engine._task_manager.CompleteTask.call_count, 3)

  def _CreateTasksStatus(
      self, number_of_tasks_pending_merge=0, number_of_tasks_processing=0):
    """Creates a tasks status mock for drain condition tests.

    Args:
      number_of_tasks_pending_merge (Optional[int]): number of tasks
          pending merge.
      number_of_tasks_processing (Optional[int]): number of tasks being
          processed by workers.

    Returns:
      mock.MagicMock: tasks status.
    """
    tasks_status = mock.MagicMock()
    tasks_status.number_of_tasks_pending_merge = number_of_tasks_pending_merge
    tasks_status.number_of_tasks_processing = number_of_tasks_processing
    return tasks_status

  def testShouldDrainMergeBacklog(self):
    """Tests the _ShouldDrainMergeBacklog function."""
    test_engine = extraction_engine.ExtractionMultiProcessEngine()

    task_manager = mock.MagicMock()
    test_engine._task_manager = task_manager

    empty_heap = extraction_engine._EventSourceHeap()

    non_empty_heap = extraction_engine._EventSourceHeap()
    event_source = mock.MagicMock()
    event_source.file_entry_type = dfvfs_definitions.FILE_ENTRY_TYPE_FILE
    non_empty_heap.PushEventSource(event_source)

    # Nothing left to schedule: drain regardless of task manager state.
    result = test_engine._ShouldDrainMergeBacklog(None, None, empty_heap)
    self.assertTrue(result)
    task_manager.GetStatusInformation.assert_not_called()

    # Workers busy: never drain while event sources remain.
    task_manager.GetStatusInformation.return_value = self._CreateTasksStatus(
        number_of_tasks_pending_merge=10000, number_of_tasks_processing=4)
    result = test_engine._ShouldDrainMergeBacklog(None, None, non_empty_heap)
    self.assertFalse(result)

    # Workers idle with a merge backlog: drain even though new event
    # sources are still being discovered.
    task_manager.GetStatusInformation.return_value = self._CreateTasksStatus(
        number_of_tasks_pending_merge=10)
    result = test_engine._ShouldDrainMergeBacklog(None, None, non_empty_heap)
    self.assertTrue(result)

    # Workers idle without a merge backlog: nothing to drain.
    task_manager.GetStatusInformation.return_value = self._CreateTasksStatus(
        number_of_tasks_pending_merge=0)
    result = test_engine._ShouldDrainMergeBacklog(None, None, non_empty_heap)
    self.assertFalse(result)

  def testProcessSource(self):
    """Tests the PreprocessSource and ProcessSource functions."""
    test_artifacts_path = shared_test_lib.GetTestFilePath(['artifacts'])
    self._SkipIfPathNotExists(test_artifacts_path)

    test_engine = extraction_engine.ExtractionMultiProcessEngine(
        maximum_number_of_tasks=100)
    test_engine.BuildArtifactsRegistry(test_artifacts_path, None)

    test_file_path = self._GetTestFilePath(['ímynd.dd'])
    self._SkipIfPathNotExists(test_file_path)

    os_path_spec = path_spec_factory.Factory.NewPathSpec(
        dfvfs_definitions.TYPE_INDICATOR_OS, location=test_file_path)
    source_path_spec = path_spec_factory.Factory.NewPathSpec(
        dfvfs_definitions.TYPE_INDICATOR_TSK, location='/',
        parent=os_path_spec)

    session = sessions.Session()

    processing_configuration = configurations.ProcessingConfiguration()
    processing_configuration.data_location = shared_test_lib.DATA_PATH
    processing_configuration.parser_filter_expression = 'filestat'
    processing_configuration.task_storage_format = (
        definitions.STORAGE_FORMAT_SQLITE)

    with shared_test_lib.TempDirectory() as temp_directory:
      temp_file = os.path.join(temp_directory, 'storage.plaso')
      storage_writer = sqlite_writer.SQLiteStorageWriter()
      storage_writer.Open(path=temp_file)

      try:
        system_configurations = test_engine.PreprocessSource(
            [source_path_spec], storage_writer)

        # The method is named ProcessSourceMulti because pylint 2.6.0 and
        # later gets confused about keyword arguments when ProcessSource
        # is used.
        processing_status = test_engine.ProcessSourceMulti(
            storage_writer, session.identifier, processing_configuration,
            system_configurations, [source_path_spec],
            storage_file_path=temp_directory)

        number_of_events = storage_writer.GetNumberOfAttributeContainers(
            'event')
        number_of_extraction_warnings = (
            storage_writer.GetNumberOfAttributeContainers(
                'extraction_warning'))
        number_of_recovery_warnings = (
            storage_writer.GetNumberOfAttributeContainers(
                'recovery_warning'))

        parsers_counter = collections.Counter({
            parser_count.name: parser_count.number_of_events
            for parser_count in storage_writer.GetAttributeContainers(
                'parser_count')})

      finally:
        storage_writer.Close()

    self.assertFalse(processing_status.aborted)

    self.assertEqual(number_of_events, 15)
    self.assertEqual(number_of_extraction_warnings, 0)
    self.assertEqual(number_of_recovery_warnings, 0)

    expected_parsers_counter = collections.Counter({
        'filestat': 15,
        'total': 15})
    self.assertEqual(parsers_counter, expected_parsers_counter)


if __name__ == '__main__':
  unittest.main()
