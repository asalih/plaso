# -*- coding: utf-8 -*-
"""The log2timeline CLI tool."""

import argparse
import sys
import textwrap

import plaso

# The following import makes sure the output modules are registered.
from plaso import output  # pylint: disable=unused-import

from plaso.analyzers.hashers import manager as hashers_manager
from plaso.cli import extraction_tool
from plaso.cli import views
from plaso.cli.helpers import manager as helpers_manager
from plaso.filters import event_filter
from plaso.lib import definitions
from plaso.lib import errors
from plaso.lib import loggers
from plaso.parsers import manager as parsers_manager


class Log2TimelineTool(extraction_tool.ExtractionTool):
  """Log2timeline CLI tool.

  Attributes:
    consolidated_timestamps (bool): True if timestamps should be consolidated
        into a single event per record instead of one event per timestamp.
    dependencies_check (bool): True if the availability and versions of
        dependencies should be checked.
    list_archive_types (bool): True if the archive types should be listed.
    list_hashers (bool): True if the hashers should be listed.
    list_parsers_and_plugins (bool): True if the parsers and plugins should
        be listed.
    list_profilers (bool): True if the profilers should be listed.
    show_info (bool): True if information about hashers, parsers, plugins,
        etc. should be shown.
    json_stdout (bool): True if events should be output as JSON to stdout
        instead of being stored in a .plaso file.
    http_endpoint (str): HTTP endpoint URL to send events to, or None if not
        using HTTP streaming.
  """

  NAME = 'log2timeline'
  DESCRIPTION = textwrap.dedent('\n'.join([
      '',
      ('log2timeline is a command line tool to extract events from '
       'individual '),
      'files, recursing a directory (e.g. mount point) or storage media ',
      'image or device.',
      '',
      'More information can be gathered from here:',
      '    https://plaso.readthedocs.io/en/latest/sources/user/'
      'Using-log2timeline.html',
      '']))

  EPILOG = textwrap.dedent('\n'.join([
      '',
      'Example usage:',
      '',
      'Run the tool against a storage media image (full kitchen sink)',
      '    log2timeline.py /cases/mycase/storage.plaso ímynd.dd',
      '',
      'Instead of answering questions, indicate some of the options on the',
      'command line (including data from particular VSS stores).',
      '    log2timeline.py --vss_stores 1,2 /cases/plaso_vss.plaso image.E01',
      '',
      'Output events as JSON to stdout instead of creating a .plaso file:',
      '    log2timeline.py --json-stdout /path/to/source',
      '',
      'And that is how you build a timeline using log2timeline...',
      '']))

  def __init__(self, input_reader=None, output_writer=None):
    """Initializes a log2timeline CLI tool.

    Args:
      input_reader (Optional[InputReader]): input reader, where None indicates
          that the stdin input reader should be used.
      output_writer (Optional[OutputWriter]): output writer, where None
          indicates that the stdout output writer should be used.
    """
    super(Log2TimelineTool, self).__init__(
        input_reader=input_reader, output_writer=output_writer)
    self._storage_serializer_format = definitions.SERIALIZER_FORMAT_JSON

    self.consolidated_timestamps = False
    self.dependencies_check = True
    self.list_archive_types = False
    self.list_hashers = False
    self.list_parsers_and_plugins = False
    self.list_profilers = False
    self.show_info = False
    self.json_stdout = False
    self.http_endpoint = None

  def _GetPluginData(self):
    """Retrieves the version and various plugin information.

    Returns:
      dict[str, list[str]]: available parsers and plugins.
    """
    return_dict = {}

    return_dict['Versions'] = [
        ('plaso engine', plaso.__version__),
        ('python', sys.version)]

    hashers_information = hashers_manager.HashersManager.GetHashersInformation()
    parsers_information = parsers_manager.ParsersManager.GetParsersInformation()
    plugins_information = (
        parsers_manager.ParsersManager.GetParserPluginsInformation())
    presets_information = self._presets_manager.GetPresetsInformation()

    return_dict['Hashers'] = hashers_information
    return_dict['Parsers'] = parsers_information
    return_dict['Parser Plugins'] = plugins_information
    return_dict['Parser Presets'] = presets_information

    return return_dict

  def AddStorageOptions(self, argument_group):  # pylint: disable=arguments-renamed
    """Adds the storage options to the argument group.

    Args:
      argument_group (argparse._ArgumentGroup): argparse argument group.
    """
    argument_group.add_argument(
        '--storage_file', '--storage-file', dest='storage_file', metavar='PATH',
        type=str, default=None, help=(
            'The path of the storage file. If not specified, one will be made '
            'in the form <timestamp>-<source>.plaso'))

  def ParseArguments(self, arguments):
    """Parses the command line arguments.

    Args:
      arguments (list[str]): command line arguments.

    Returns:
      bool: True if the arguments were successfully parsed.
    """
    loggers.ConfigureLogging()

    argument_parser = argparse.ArgumentParser(
        description=self.DESCRIPTION, epilog=self.EPILOG, add_help=False,
        formatter_class=argparse.RawDescriptionHelpFormatter)

    self.AddBasicOptions(argument_parser)

    data_location_group = argument_parser.add_argument_group(
        'data location arguments')

    argument_helper_names = ['artifact_definitions', 'data_location']
    helpers_manager.ArgumentHelperManager.AddCommandLineArguments(
        data_location_group, names=argument_helper_names)

    extraction_group = argument_parser.add_argument_group(
        'extraction arguments')

    argument_helper_names = [
        'archives', 'artifact_filters', 'date_filters', 'extraction', 'filter_file', 'hashers',
        'parsers', 'yara_rules']
    helpers_manager.ArgumentHelperManager.AddCommandLineArguments(
        extraction_group, names=argument_helper_names)

    self.AddStorageMediaImageOptions(extraction_group)
    self.AddExtractionOptions(extraction_group)
    self.AddVSSProcessingOptions(extraction_group)
    self.AddCredentialOptions(extraction_group)

    info_group = argument_parser.add_argument_group('informational arguments')

    self.AddInformationalOptions(info_group)

    info_group.add_argument(
        '--info', dest='show_info', action='store_true', default=False,
        help='Print out information about supported plugins and parsers.')

    info_group.add_argument(
        '--use_markdown', '--use-markdown', dest='use_markdown',
        action='store_true', default=False, help=(
            'Output lists in Markdown format use in combination with '
            '"--hashers list", "--parsers list" or "--timezone list"'))

    info_group.add_argument(
        '--no_dependencies_check', '--no-dependencies-check',
        dest='dependencies_check', action='store_false', default=True,
        help='Disable the dependencies check.')

    info_group.add_argument(
        '--json-stdout', dest='json_stdout', action='store_true', 
        default=False, help=(
            'Output events as JSON to stdout instead of creating a .plaso file. '
            'When this option is used, the storage_file argument is ignored.'))

    info_group.add_argument(
        '--http-endpoint', dest='http_endpoint', type=str, metavar='URL',
        help=(
            'Send events as JSON to the specified HTTP endpoint instead of '
            'creating a .plaso file. Format: http://host:port/path. '
            'When this option is used, the storage_file argument is ignored.'))

    info_group.add_argument(
        '--event-filter', dest='event_filter', type=str, metavar='FILTER',
        help=(
            'Filter events by expression (for streaming modes only). '
            'Example: "date > \'2024-09-15 08:00:00\' and date < \'2024-09-15 18:00:00\'"'))

    info_group.add_argument(
        '--consolidated_timestamps', '--consolidated-timestamps',
        dest='consolidated_timestamps', action='store_true', default=False,
        help=(
            'Output a single event per record with all timestamps as separate '
            'fields instead of one event per timestamp. For example, an MFT '
            'entry will have creation_time, modification_time, access_time, '
            'and change_time as separate columns in a single event. '
            'Only works with --json-stdout or --http-endpoint.'))

    self.AddLogFileOptions(info_group)

    helpers_manager.ArgumentHelperManager.AddCommandLineArguments(
        info_group, names=['status_view'])

    processing_group = argument_parser.add_argument_group(
        'processing arguments')

    self.AddPerformanceOptions(processing_group)
    self.AddProcessingOptions(processing_group)

    processing_group.add_argument(
        '--sigsegv_handler', '--sigsegv-handler', dest='sigsegv_handler',
        action='store_true', default=False, help=(
            'Enables the SIGSEGV handler. WARNING this functionality is '
            'experimental and will a deadlock worker process if a real '
            'segfault is caught, but not signal SIGSEGV. This functionality '
            'is therefore primarily intended for debugging purposes'))

    profiling_group = argument_parser.add_argument_group('profiling arguments')

    helpers_manager.ArgumentHelperManager.AddCommandLineArguments(
        profiling_group, names=['profiling'])

    storage_group = argument_parser.add_argument_group('storage arguments')

    self.AddStorageOptions(storage_group)

    helpers_manager.ArgumentHelperManager.AddCommandLineArguments(
        storage_group, names=['storage_format'])

    argument_parser.add_argument(
        self._SOURCE_OPTION, action='store', metavar='SOURCE', nargs='?',
        default=None, type=str, help=(
            'Path to a source device, file or directory. If the source is '
            'a supported storage media device or image file, archive file '
            'or a directory, the files within are processed recursively.'))

    try:
      options = argument_parser.parse_args(arguments)
    except UnicodeEncodeError:
      # If we get here we are attempting to print help in a non-Unicode
      # terminal.
      self._output_writer.Write('\n')
      self._output_writer.Write(argument_parser.format_help())
      return False

    # Properly prepare the attributes according to local encoding.
    if self.preferred_encoding == 'ascii':
      self._PrintUserWarning((
          'the preferred encoding of your system is ASCII, which is not '
          'optimal for the typically non-ASCII characters that need to be '
          'parsed and processed. This will most likely result in an error.'))

    try:
      self.ParseOptions(options)
    except errors.BadConfigOption as exception:
      self._output_writer.Write(f'ERROR: {exception!s}\n')
      self._output_writer.Write('\n')
      self._output_writer.Write(argument_parser.format_usage())
      return False

    self._command_line_arguments = self.GetCommandLineArguments()

    self._WaitUserWarning()

    loggers.ConfigureLogging(
        debug_output=self._debug_mode, filename=self._log_file,
        quiet_mode=self._quiet_mode)

    return True

  def ParseOptions(self, options):
    """Parses the command line options.

    Args:
      options (argparse.Namespace): command line arguments.

    Raises:
      BadConfigOption: if the options are invalid.
    """
    # Parse the JSON stdout and HTTP endpoint options first
    self.json_stdout = getattr(options, 'json_stdout', False)
    self.http_endpoint = getattr(options, 'http_endpoint', None)
    self.consolidated_timestamps = getattr(
        options, 'consolidated_timestamps', False)
    
    # Validate consolidated_timestamps usage
    if self.consolidated_timestamps:
      if not (self.json_stdout or self.http_endpoint):
        raise errors.BadConfigOption(
            '--consolidated-timestamps can only be used with --json-stdout '
            'or --http-endpoint.')
    
    # Parse event filter for streaming modes
    event_filter_expression = getattr(options, 'event_filter', None)
    if event_filter_expression:
      if not (self.json_stdout or self.http_endpoint):
        raise errors.BadConfigOption(
            '--event-filter can only be used with --json-stdout or --http-endpoint.')
      
      # Create and compile the event filter
      event_filter_object = event_filter.EventObjectFilter()
      try:
        event_filter_object.CompileFilter(event_filter_expression)
        setattr(self, '_event_filter', event_filter_object)
      except errors.ParseError as exception:
        raise errors.BadConfigOption(
            f'Unable to compile event filter expression with error: {exception!s}')
    else:
      setattr(self, '_event_filter', None)
    
    # Validate that only one output mode is specified
    if self.json_stdout and self.http_endpoint:
      raise errors.BadConfigOption(
          'Cannot use both --json-stdout and --http-endpoint options.')
    
    # Validate HTTP endpoint URL format if specified
    if self.http_endpoint:
      from urllib.parse import urlparse
      parsed_url = urlparse(self.http_endpoint)
      if not parsed_url.scheme or not parsed_url.netloc:
        raise errors.BadConfigOption(
            f'Invalid HTTP endpoint URL: {self.http_endpoint}. '
            'URL must include scheme and host (e.g., http://localhost:8080/events)')
      if parsed_url.scheme not in ('http', 'https'):
        raise errors.BadConfigOption(
            f'Unsupported URL scheme: {parsed_url.scheme}. '
            'Only http and https are supported.')

    # The extraction options are dependent on the data location.
    helpers_manager.ArgumentHelperManager.ParseOptions(
        options, self, names=['data_location'])

    self._ReadParserPresetsFromFile()

    # Check the list options first otherwise required options will raise.
    argument_helper_names = ['archives', 'hashers', 'parsers', 'profiling']
    helpers_manager.ArgumentHelperManager.ParseOptions(
        options, self, names=argument_helper_names)

    self._ParseExtractionOptions(options)

    self.list_archive_types = self._archive_types_string == 'list'
    self.list_hashers = self._hasher_names_string == 'list'
    self.list_parsers_and_plugins = self._parser_filter_expression == 'list'
    self.list_profilers = self._profilers == 'list'

    self.show_info = getattr(options, 'show_info', False)
    self.show_troubleshooting = getattr(options, 'show_troubleshooting', False)

    if getattr(options, 'use_markdown', False):
      self._views_format_type = views.ViewsFactory.FORMAT_TYPE_MARKDOWN

    self.dependencies_check = getattr(options, 'dependencies_check', True)

    if (self.list_archive_types or self.list_hashers or
        self.list_language_tags or self.list_parsers_and_plugins or
        self.list_profilers or self.list_time_zones or self.show_info or
        self.show_troubleshooting):
      return

    self._ParseInformationalOptions(options)

    argument_helper_names = [
        'artifact_definitions', 'artifact_filters', 'extraction',
        'filter_file', 'status_view', 'storage_format', 'yara_rules']
    helpers_manager.ArgumentHelperManager.ParseOptions(
        options, self, names=argument_helper_names)

    self._ParseLogFileOptions(options)

    self._ParseStorageMediaOptions(options)

    self._ParsePerformanceOptions(options)
    self._ParseProcessingOptions(options)

    # Handle storage file for normal mode (not JSON stdout or HTTP endpoint)
    if not self.json_stdout and not self.http_endpoint:
      self._storage_file_path = self.ParseStringOption(options, 'storage_file')
      if not self._storage_file_path:
        self._storage_file_path = self._GenerateStorageFileName()

      if not self._storage_file_path:
        raise errors.BadConfigOption('Missing storage file option.')

      serializer_format = getattr(
          options, 'serializer_format', definitions.SERIALIZER_FORMAT_JSON)
      if serializer_format not in definitions.SERIALIZER_FORMATS:
        raise errors.BadConfigOption(
            f'Unsupported storage serializer format: {serializer_format:s}')
      self._storage_serializer_format = serializer_format
    else:
      # For JSON stdout or HTTP endpoint mode, we don't need a storage file
      self._storage_file_path = None

    helpers_manager.ArgumentHelperManager.ParseOptions(
        options, self, names=['status_view'])

    self._enable_sigsegv_handler = getattr(options, 'sigsegv_handler', False)

    self._EnforceProcessMemoryLimit(self._process_memory_limit)

  def ShowInfo(self):
    """Shows information about available hashers, parsers, plugins, etc."""
    title = ' log2timeline/plaso information '
    self._output_writer.Write(f'{title:=^80s}\n')

    plugin_list = self._GetPluginData()
    for header, data in plugin_list.items():
      table_view = views.ViewsFactory.GetTableView(
          self._views_format_type, column_names=['Name', 'Description'],
          title=header)
      for entry_header, entry_data in sorted(data):
        table_view.AddRow([entry_header, entry_data])
      table_view.Write(self._output_writer)
