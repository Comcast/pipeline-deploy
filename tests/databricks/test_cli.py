"""
Copyright 2022 Comcast Cable Communications Management, LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
"""

import os
import pytest

from click.testing import CliRunner
from databricks_cli.configure.provider import DatabricksConfig, set_config_provider
from databricks_cli.workspace.api import DIRECTORY, NOTEBOOK, WorkspaceFileInfo
from pipeline_deploy.databricks.cli import databricks_cli
from pytest_mock.plugin import MockerFixture
from tests.databricks.test_data import JOBS_DATA_LOCAL, JOBS_DATA_REMOTE
from tests.databricks.utils import FILE_PATH, MockConfigProvider
from unittest.mock import MagicMock, patch

@pytest.fixture()
def jobs_controller_mock():
    with patch('pipeline_deploy.databricks.cli.JobsController') as JobsControllerMock:
        _mock_jobs_controller = MagicMock()
        JobsControllerMock.return_value = _mock_jobs_controller
        yield _mock_jobs_controller

@pytest.fixture()
def notebooks_controller_mock():
    with patch('pipeline_deploy.databricks.cli.NotebooksController') as NotebooksControllerMock:
        _mock_notebooks_controller = MagicMock()
        NotebooksControllerMock.return_value = _mock_notebooks_controller
        yield _mock_notebooks_controller

class TestCli:
    def test_databricks_cli(self, jobs_controller_mock: MagicMock, notebooks_controller_mock: MagicMock, mocker: MockerFixture):
        mock_config = DatabricksConfig.from_token('test-host', 'test-token')
        set_config_provider(MockConfigProvider(mock_config))

        mock_enumerate_local_directories = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_directories')
        expected_local_directories = [os.path.join(FILE_PATH, 'directory')]
        mock_enumerate_local_directories.return_value = expected_local_directories

        mock_enumerate_local_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_jobs')
        mock_enumerate_local_jobs.return_value = JOBS_DATA_LOCAL
        expected_local_jobs_map = {job['name']: job for job in JOBS_DATA_LOCAL}

        mock_get_local_notebooks_map = mocker.patch('pipeline_deploy.databricks.utils.get_local_notebooks_map')
        expected_local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        mock_get_local_notebooks_map.return_value = expected_local_notebooks_map

        mock_enumerate_remote_paths = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_paths')
        remote_paths = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2'),
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]
        mock_enumerate_remote_paths.return_value = remote_paths

        mock_enumerate_remote_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_jobs')
        mock_enumerate_remote_jobs.return_value = JOBS_DATA_REMOTE
        expected_remote_jobs_map = {job['settings']['name']: job for job in JOBS_DATA_REMOTE}

        runner = CliRunner()
        runner.invoke(databricks_cli, ['--jobs-dir', FILE_PATH, '--notebooks-dir', FILE_PATH], catch_exceptions=False)

        expected_remote_notebooks = {
            '/remote/path/file-1',
            '/remote/path/sub-directory/file-2'
        }

        notebooks_controller_mock.update.assert_called_with(expected_local_notebooks_map,
                                                            {'/path/notebooks/streaming-job-1': [JOBS_DATA_REMOTE[1]]},
                                                            expected_remote_notebooks)
        notebooks_controller_mock.create.assert_called_with(expected_local_notebooks_map, expected_remote_notebooks)
        notebooks_controller_mock.delete.assert_called_with(expected_local_directories,
                                                            expected_local_notebooks_map,
                                                            {'/remote/path/sub-directory'},
                                                            expected_remote_notebooks)
        jobs_controller_mock.update.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.create.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map,
                                                       None)
        jobs_controller_mock.delete.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.restart.assert_called_with({})

    def test_databricks_cli_with_exclude_jobs(self, jobs_controller_mock: MagicMock, notebooks_controller_mock: MagicMock, mocker: MockerFixture):
        mock_config = DatabricksConfig.from_token('test-host', 'test-token')
        set_config_provider(MockConfigProvider(mock_config))

        mock_enumerate_local_directories = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_directories')
        expected_local_directories = [os.path.join(FILE_PATH, 'directory')]
        mock_enumerate_local_directories.return_value = expected_local_directories

        mock_enumerate_local_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_jobs')
        mock_enumerate_local_jobs.return_value = JOBS_DATA_LOCAL
        expected_local_jobs_map = {job['name']: job for job in JOBS_DATA_LOCAL}

        mock_get_local_notebooks_map = mocker.patch('pipeline_deploy.databricks.utils.get_local_notebooks_map')
        expected_local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        mock_get_local_notebooks_map.return_value = expected_local_notebooks_map

        mock_enumerate_remote_paths = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_paths')
        remote_paths = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2'),
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]
        mock_enumerate_remote_paths.return_value = remote_paths

        mock_enumerate_remote_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_jobs')
        mock_enumerate_remote_jobs.return_value = JOBS_DATA_REMOTE
        expected_remote_jobs_map = {job['settings']['name']: job for job in JOBS_DATA_REMOTE}

        expected_exclude_jobs = 'exclude-jobs'

        runner = CliRunner()
        runner.invoke(databricks_cli, ['--jobs-dir', FILE_PATH, '--notebooks-dir', FILE_PATH, '--exclude-jobs', expected_exclude_jobs], catch_exceptions=False)

        expected_remote_notebooks = {
            '/remote/path/file-1',
            '/remote/path/sub-directory/file-2'
        }

        mock_enumerate_remote_jobs.assert_called_with(mocker.ANY, [expected_exclude_jobs], [], None)
        mock_enumerate_local_jobs.assert_called_with([expected_exclude_jobs], [], FILE_PATH, None)

        notebooks_controller_mock.update.assert_called_with(expected_local_notebooks_map,
                                                            {'/path/notebooks/streaming-job-1': [JOBS_DATA_REMOTE[1]]},
                                                            expected_remote_notebooks)
        notebooks_controller_mock.create.assert_called_with(expected_local_notebooks_map, expected_remote_notebooks)
        notebooks_controller_mock.delete.assert_called_with(expected_local_directories,
                                                            expected_local_notebooks_map,
                                                            {'/remote/path/sub-directory'},
                                                            expected_remote_notebooks)
        jobs_controller_mock.update.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.create.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map,
                                                       None)
        jobs_controller_mock.delete.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.restart.assert_called_with({})

    def test_databricks_cli_with_exclude_notebooks(self, jobs_controller_mock: MagicMock, notebooks_controller_mock: MagicMock, mocker: MockerFixture):
        mock_config = DatabricksConfig.from_token('test-host', 'test-token')
        set_config_provider(MockConfigProvider(mock_config))

        mock_enumerate_local_directories = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_directories')
        expected_local_directories = [os.path.join(FILE_PATH, 'directory')]
        mock_enumerate_local_directories.return_value = expected_local_directories

        mock_enumerate_local_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_jobs')
        mock_enumerate_local_jobs.return_value = JOBS_DATA_LOCAL
        expected_local_jobs_map = {job['name']: job for job in JOBS_DATA_LOCAL}

        mock_get_local_notebooks_map = mocker.patch('pipeline_deploy.databricks.utils.get_local_notebooks_map')
        expected_local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        mock_get_local_notebooks_map.return_value = expected_local_notebooks_map

        mock_enumerate_remote_paths = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_paths')
        remote_paths = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2'),
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]
        mock_enumerate_remote_paths.return_value = remote_paths

        mock_enumerate_remote_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_jobs')
        mock_enumerate_remote_jobs.return_value = JOBS_DATA_REMOTE
        expected_remote_jobs_map = {job['settings']['name']: job for job in JOBS_DATA_REMOTE}

        expected_exclude_notebooks = 'exclude-notebooks'

        runner = CliRunner()
        runner.invoke(databricks_cli, ['--jobs-dir', FILE_PATH, '--notebooks-dir', FILE_PATH, '--exclude-notebooks', expected_exclude_notebooks], catch_exceptions=False)

        expected_remote_notebooks = {
            '/remote/path/file-1',
            '/remote/path/sub-directory/file-2'
        }

        mock_enumerate_local_directories.assert_called_with([expected_exclude_notebooks], [], FILE_PATH)
        mock_get_local_notebooks_map.assert_called_with([expected_exclude_notebooks], [], FILE_PATH, '/')
        mock_enumerate_remote_paths.assert_called_with(mocker.ANY, [expected_exclude_notebooks], [], '/')

        notebooks_controller_mock.update.assert_called_with(expected_local_notebooks_map,
                                                            {'/path/notebooks/streaming-job-1': [JOBS_DATA_REMOTE[1]]},
                                                            expected_remote_notebooks)
        notebooks_controller_mock.create.assert_called_with(expected_local_notebooks_map, expected_remote_notebooks)
        notebooks_controller_mock.delete.assert_called_with(expected_local_directories,
                                                            expected_local_notebooks_map,
                                                            {'/remote/path/sub-directory'},
                                                            expected_remote_notebooks)
        jobs_controller_mock.update.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.create.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map,
                                                       None)
        jobs_controller_mock.delete.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.restart.assert_called_with({})

    def test_databricks_cli_with_group_name(self, jobs_controller_mock: MagicMock, notebooks_controller_mock: MagicMock, mocker: MockerFixture):
        mock_config = DatabricksConfig.from_token('test-host', 'test-token')
        set_config_provider(MockConfigProvider(mock_config))

        mock_enumerate_local_directories = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_directories')
        expected_local_directories = [os.path.join(FILE_PATH, 'directory')]
        mock_enumerate_local_directories.return_value = expected_local_directories

        mock_enumerate_local_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_jobs')
        mock_enumerate_local_jobs.return_value = JOBS_DATA_LOCAL
        expected_local_jobs_map = {job['name']: job for job in JOBS_DATA_LOCAL}

        mock_get_local_notebooks_map = mocker.patch('pipeline_deploy.databricks.utils.get_local_notebooks_map')
        expected_local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        mock_get_local_notebooks_map.return_value = expected_local_notebooks_map

        mock_enumerate_remote_paths = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_paths')
        remote_paths = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2'),
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]
        mock_enumerate_remote_paths.return_value = remote_paths

        mock_enumerate_remote_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_jobs')
        mock_enumerate_remote_jobs.return_value = JOBS_DATA_REMOTE
        expected_remote_jobs_map = {job['settings']['name']: job for job in JOBS_DATA_REMOTE}

        expected_group_name = 'group_name'

        runner = CliRunner()
        runner.invoke(databricks_cli, ['--jobs-dir', FILE_PATH, '--notebooks-dir', FILE_PATH, '--group-name', expected_group_name], catch_exceptions=False)

        expected_remote_notebooks = {
            '/remote/path/file-1',
            '/remote/path/sub-directory/file-2'
        }

        notebooks_controller_mock.update.assert_called_with(expected_local_notebooks_map,
                                                            {'/path/notebooks/streaming-job-1': [JOBS_DATA_REMOTE[1]]},
                                                            expected_remote_notebooks)
        notebooks_controller_mock.create.assert_called_with(expected_local_notebooks_map, expected_remote_notebooks)
        notebooks_controller_mock.delete.assert_called_with(expected_local_directories,
                                                            expected_local_notebooks_map,
                                                            {'/remote/path/sub-directory'},
                                                            expected_remote_notebooks)
        jobs_controller_mock.update.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.create.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map,
                                                       None)
        jobs_controller_mock.delete.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.restart.assert_called_with({})

    def test_databricks_cli_with_include_jobs(self, jobs_controller_mock: MagicMock, notebooks_controller_mock: MagicMock, mocker: MockerFixture):
        mock_config = DatabricksConfig.from_token('test-host', 'test-token')
        set_config_provider(MockConfigProvider(mock_config))

        mock_enumerate_local_directories = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_directories')
        expected_local_directories = [os.path.join(FILE_PATH, 'directory')]
        mock_enumerate_local_directories.return_value = expected_local_directories

        mock_enumerate_local_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_jobs')
        mock_enumerate_local_jobs.return_value = JOBS_DATA_LOCAL
        expected_local_jobs_map = {job['name']: job for job in JOBS_DATA_LOCAL}

        mock_get_local_notebooks_map = mocker.patch('pipeline_deploy.databricks.utils.get_local_notebooks_map')
        expected_local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        mock_get_local_notebooks_map.return_value = expected_local_notebooks_map

        mock_enumerate_remote_paths = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_paths')
        remote_paths = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2'),
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]
        mock_enumerate_remote_paths.return_value = remote_paths

        mock_enumerate_remote_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_jobs')
        mock_enumerate_remote_jobs.return_value = JOBS_DATA_REMOTE
        expected_remote_jobs_map = {job['settings']['name']: job for job in JOBS_DATA_REMOTE}

        expected_include_jobs = 'include-jobs'

        runner = CliRunner()
        runner.invoke(databricks_cli, ['--jobs-dir', FILE_PATH, '--notebooks-dir', FILE_PATH, '--include-jobs', expected_include_jobs], catch_exceptions=False)

        expected_remote_notebooks = {
            '/remote/path/file-1',
            '/remote/path/sub-directory/file-2'
        }

        mock_enumerate_remote_jobs.assert_called_with(mocker.ANY, [], [expected_include_jobs], None)
        mock_enumerate_local_jobs.assert_called_with([], [expected_include_jobs], FILE_PATH, None)

        notebooks_controller_mock.update.assert_called_with(expected_local_notebooks_map,
                                                            {'/path/notebooks/streaming-job-1': [JOBS_DATA_REMOTE[1]]},
                                                            expected_remote_notebooks)
        notebooks_controller_mock.create.assert_called_with(expected_local_notebooks_map, expected_remote_notebooks)
        notebooks_controller_mock.delete.assert_called_with(expected_local_directories,
                                                            expected_local_notebooks_map,
                                                            {'/remote/path/sub-directory'},
                                                            expected_remote_notebooks)
        jobs_controller_mock.update.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.create.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map,
                                                       None)
        jobs_controller_mock.delete.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.restart.assert_called_with({})

    def test_databricks_cli_with_include_notebooks(self, jobs_controller_mock: MagicMock, notebooks_controller_mock: MagicMock, mocker: MockerFixture):
        mock_config = DatabricksConfig.from_token('test-host', 'test-token')
        set_config_provider(MockConfigProvider(mock_config))

        mock_enumerate_local_directories = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_directories')
        expected_local_directories = [os.path.join(FILE_PATH, 'directory')]
        mock_enumerate_local_directories.return_value = expected_local_directories

        mock_enumerate_local_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_jobs')
        mock_enumerate_local_jobs.return_value = JOBS_DATA_LOCAL
        expected_local_jobs_map = {job['name']: job for job in JOBS_DATA_LOCAL}

        mock_get_local_notebooks_map = mocker.patch('pipeline_deploy.databricks.utils.get_local_notebooks_map')
        expected_local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        mock_get_local_notebooks_map.return_value = expected_local_notebooks_map

        mock_enumerate_remote_paths = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_paths')
        remote_paths = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2'),
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]
        mock_enumerate_remote_paths.return_value = remote_paths

        mock_enumerate_remote_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_jobs')
        mock_enumerate_remote_jobs.return_value = JOBS_DATA_REMOTE
        expected_remote_jobs_map = {job['settings']['name']: job for job in JOBS_DATA_REMOTE}

        expected_include_notebooks = 'include-notebooks'

        runner = CliRunner()
        runner.invoke(databricks_cli, ['--jobs-dir', FILE_PATH, '--notebooks-dir', FILE_PATH, '--include-notebooks', expected_include_notebooks], catch_exceptions=False)

        expected_remote_notebooks = {
            '/remote/path/file-1',
            '/remote/path/sub-directory/file-2'
        }

        mock_enumerate_local_directories.assert_called_with([], [expected_include_notebooks], FILE_PATH)
        mock_get_local_notebooks_map.assert_called_with([], [expected_include_notebooks], FILE_PATH, '/')
        mock_enumerate_remote_paths.assert_called_with(mocker.ANY, [], [expected_include_notebooks], '/')

        notebooks_controller_mock.update.assert_called_with(expected_local_notebooks_map,
                                                            {'/path/notebooks/streaming-job-1': [JOBS_DATA_REMOTE[1]]},
                                                            expected_remote_notebooks)
        notebooks_controller_mock.create.assert_called_with(expected_local_notebooks_map, expected_remote_notebooks)
        notebooks_controller_mock.delete.assert_called_with(expected_local_directories,
                                                            expected_local_notebooks_map,
                                                            {'/remote/path/sub-directory'},
                                                            expected_remote_notebooks)
        jobs_controller_mock.update.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.create.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map,
                                                       None)
        jobs_controller_mock.delete.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.restart.assert_called_with({})

    def test_databricks_cli_with_owner(self, jobs_controller_mock: MagicMock, notebooks_controller_mock: MagicMock, mocker: MockerFixture):
        mock_config = DatabricksConfig.from_token('test-host', 'test-token')
        set_config_provider(MockConfigProvider(mock_config))

        mock_enumerate_local_directories = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_directories')
        expected_local_directories = [os.path.join(FILE_PATH, 'directory')]
        mock_enumerate_local_directories.return_value = expected_local_directories

        mock_enumerate_local_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_jobs')
        mock_enumerate_local_jobs.return_value = JOBS_DATA_LOCAL
        expected_local_jobs_map = {job['name']: job for job in JOBS_DATA_LOCAL}

        mock_get_local_notebooks_map = mocker.patch('pipeline_deploy.databricks.utils.get_local_notebooks_map')
        expected_local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        mock_get_local_notebooks_map.return_value = expected_local_notebooks_map

        mock_enumerate_remote_paths = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_paths')
        remote_paths = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2'),
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]
        mock_enumerate_remote_paths.return_value = remote_paths

        mock_enumerate_remote_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_jobs')
        mock_enumerate_remote_jobs.return_value = JOBS_DATA_REMOTE
        expected_remote_jobs_map = {job['settings']['name']: job for job in JOBS_DATA_REMOTE}

        expected_owner = 'owner'

        runner = CliRunner()
        runner.invoke(databricks_cli, ['--jobs-dir', FILE_PATH, '--notebooks-dir', FILE_PATH, '--owner', expected_owner], catch_exceptions=False)

        expected_remote_notebooks = {
            '/remote/path/file-1',
            '/remote/path/sub-directory/file-2'
        }

        mock_enumerate_remote_jobs.assert_called_with(mocker.ANY, [], [], expected_owner)

        notebooks_controller_mock.update.assert_called_with(expected_local_notebooks_map,
                                                            {'/path/notebooks/streaming-job-1': [JOBS_DATA_REMOTE[1]]},
                                                            expected_remote_notebooks)
        notebooks_controller_mock.create.assert_called_with(expected_local_notebooks_map, expected_remote_notebooks)
        notebooks_controller_mock.delete.assert_called_with(expected_local_directories,
                                                            expected_local_notebooks_map,
                                                            {'/remote/path/sub-directory'},
                                                            expected_remote_notebooks)
        jobs_controller_mock.update.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.create.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map,
                                                       expected_owner)
        jobs_controller_mock.delete.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.restart.assert_called_with({})

    def test_databricks_cli_with_prefix(self, jobs_controller_mock: MagicMock, notebooks_controller_mock: MagicMock, mocker: MockerFixture):
        mock_config = DatabricksConfig.from_token('test-host', 'test-token')
        set_config_provider(MockConfigProvider(mock_config))

        mock_enumerate_local_directories = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_directories')
        expected_local_directories = [os.path.join(FILE_PATH, 'directory')]
        mock_enumerate_local_directories.return_value = expected_local_directories

        mock_enumerate_local_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_jobs')
        mock_enumerate_local_jobs.return_value = JOBS_DATA_LOCAL
        expected_local_jobs_map = {job['name']: job for job in JOBS_DATA_LOCAL}

        mock_get_local_notebooks_map = mocker.patch('pipeline_deploy.databricks.utils.get_local_notebooks_map')
        expected_local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        mock_get_local_notebooks_map.return_value = expected_local_notebooks_map

        mock_enumerate_remote_paths = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_paths')
        remote_paths = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2'),
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]
        mock_enumerate_remote_paths.return_value = remote_paths

        mock_enumerate_remote_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_jobs')
        mock_enumerate_remote_jobs.return_value = JOBS_DATA_REMOTE
        expected_remote_jobs_map = {job['settings']['name']: job for job in JOBS_DATA_REMOTE}

        expected_prefix = 'prefix'

        runner = CliRunner()
        runner.invoke(databricks_cli, ['--jobs-dir', FILE_PATH, '--notebooks-dir', FILE_PATH, '--prefix', expected_prefix], catch_exceptions=False)

        expected_remote_notebooks = {
            '/remote/path/file-1',
            '/remote/path/sub-directory/file-2'
        }

        mock_enumerate_local_jobs.assert_called_with([], [], FILE_PATH, expected_prefix)

        notebooks_controller_mock.update.assert_called_with(expected_local_notebooks_map,
                                                            {'/path/notebooks/streaming-job-1': [JOBS_DATA_REMOTE[1]]},
                                                            expected_remote_notebooks)
        notebooks_controller_mock.create.assert_called_with(expected_local_notebooks_map, expected_remote_notebooks)
        notebooks_controller_mock.delete.assert_called_with(expected_local_directories,
                                                            expected_local_notebooks_map,
                                                            {'/remote/path/sub-directory'},
                                                            expected_remote_notebooks)
        jobs_controller_mock.update.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.create.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map,
                                                       None)
        jobs_controller_mock.delete.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.restart.assert_called_with({})

    def test_databricks_cli_with_remote_path(self, jobs_controller_mock: MagicMock, notebooks_controller_mock: MagicMock, mocker: MockerFixture):
        mock_config = DatabricksConfig.from_token('test-host', 'test-token')
        set_config_provider(MockConfigProvider(mock_config))

        mock_enumerate_local_directories = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_directories')
        expected_local_directories = [os.path.join(FILE_PATH, 'directory')]
        mock_enumerate_local_directories.return_value = expected_local_directories

        mock_enumerate_local_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_jobs')
        mock_enumerate_local_jobs.return_value = JOBS_DATA_LOCAL
        expected_local_jobs_map = {job['name']: job for job in JOBS_DATA_LOCAL}

        mock_get_local_notebooks_map = mocker.patch('pipeline_deploy.databricks.utils.get_local_notebooks_map')
        expected_local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        mock_get_local_notebooks_map.return_value = expected_local_notebooks_map

        mock_enumerate_remote_paths = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_paths')
        remote_paths = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2'),
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]
        mock_enumerate_remote_paths.return_value = remote_paths

        mock_enumerate_remote_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_jobs')
        mock_enumerate_remote_jobs.return_value = JOBS_DATA_REMOTE
        expected_remote_jobs_map = {job['settings']['name']: job for job in JOBS_DATA_REMOTE}

        expected_remote_path = '/path'

        runner = CliRunner()
        runner.invoke(databricks_cli, ['--jobs-dir', FILE_PATH, '--notebooks-dir', FILE_PATH, '--remote-path', expected_remote_path], catch_exceptions=False)

        expected_remote_notebooks = {
            '/remote/path/file-1',
            '/remote/path/sub-directory/file-2'
        }

        mock_get_local_notebooks_map.assert_called_with([], [], FILE_PATH, expected_remote_path)
        mock_enumerate_remote_paths.assert_called_with(mocker.ANY, [], [], expected_remote_path)

        notebooks_controller_mock.update.assert_called_with(expected_local_notebooks_map,
                                                            {'/path/notebooks/streaming-job-1': [JOBS_DATA_REMOTE[1]]},
                                                            expected_remote_notebooks)
        notebooks_controller_mock.create.assert_called_with(expected_local_notebooks_map, expected_remote_notebooks)
        notebooks_controller_mock.delete.assert_called_with(expected_local_directories,
                                                            expected_local_notebooks_map,
                                                            {'/remote/path/sub-directory'},
                                                            expected_remote_notebooks)
        jobs_controller_mock.update.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.create.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map,
                                                       None)
        jobs_controller_mock.delete.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.restart.assert_called_with({})

    def test_databricks_cli_with_skip_restart(self, jobs_controller_mock: MagicMock, notebooks_controller_mock: MagicMock, mocker: MockerFixture):
        mock_config = DatabricksConfig.from_token('test-host', 'test-token')
        set_config_provider(MockConfigProvider(mock_config))

        mock_enumerate_local_directories = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_directories')
        expected_local_directories = [os.path.join(FILE_PATH, 'directory')]
        mock_enumerate_local_directories.return_value = expected_local_directories

        mock_enumerate_local_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_jobs')
        mock_enumerate_local_jobs.return_value = JOBS_DATA_LOCAL
        expected_local_jobs_map = {job['name']: job for job in JOBS_DATA_LOCAL}

        mock_get_local_notebooks_map = mocker.patch('pipeline_deploy.databricks.utils.get_local_notebooks_map')
        expected_local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        mock_get_local_notebooks_map.return_value = expected_local_notebooks_map

        mock_enumerate_remote_paths = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_paths')
        remote_paths = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2'),
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]
        mock_enumerate_remote_paths.return_value = remote_paths

        mock_enumerate_remote_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_jobs')
        mock_enumerate_remote_jobs.return_value = JOBS_DATA_REMOTE
        expected_remote_jobs_map = {job['settings']['name']: job for job in JOBS_DATA_REMOTE}

        runner = CliRunner()
        runner.invoke(databricks_cli, ['--jobs-dir', FILE_PATH, '--notebooks-dir', FILE_PATH, '--skip-restart'], catch_exceptions=False)

        expected_remote_notebooks = {
            '/remote/path/file-1',
            '/remote/path/sub-directory/file-2'
        }

        notebooks_controller_mock.update.assert_called_with(expected_local_notebooks_map,
                                                            {'/path/notebooks/streaming-job-1': [JOBS_DATA_REMOTE[1]]},
                                                            expected_remote_notebooks)
        notebooks_controller_mock.create.assert_called_with(expected_local_notebooks_map, expected_remote_notebooks)
        notebooks_controller_mock.delete.assert_called_with(expected_local_directories,
                                                            expected_local_notebooks_map,
                                                            {'/remote/path/sub-directory'},
                                                            expected_remote_notebooks)
        jobs_controller_mock.update.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.create.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map,
                                                       None)
        jobs_controller_mock.delete.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.restart.assert_not_called()

    def test_databricks_cli_with_jobs_needing_restart(self, jobs_controller_mock: MagicMock, notebooks_controller_mock: MagicMock, mocker: MockerFixture):
        mock_config = DatabricksConfig.from_token('test-host', 'test-token')
        set_config_provider(MockConfigProvider(mock_config))

        mock_enumerate_local_directories = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_directories')
        expected_local_directories = [os.path.join(FILE_PATH, 'directory')]
        mock_enumerate_local_directories.return_value = expected_local_directories

        mock_enumerate_local_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_jobs')
        mock_enumerate_local_jobs.return_value = JOBS_DATA_LOCAL
        expected_local_jobs_map = {job['name']: job for job in JOBS_DATA_LOCAL}

        mock_get_local_notebooks_map = mocker.patch('pipeline_deploy.databricks.utils.get_local_notebooks_map')
        expected_local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        mock_get_local_notebooks_map.return_value = expected_local_notebooks_map

        mock_enumerate_remote_paths = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_paths')
        remote_paths = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2'),
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]
        mock_enumerate_remote_paths.return_value = remote_paths

        mock_enumerate_remote_jobs = mocker.patch('pipeline_deploy.databricks.utils.enumerate_remote_jobs')
        mock_enumerate_remote_jobs.return_value = JOBS_DATA_REMOTE
        expected_remote_jobs_map = {job['settings']['name']: job for job in JOBS_DATA_REMOTE}

        def mock_notebooks_update(local_notebooks_map, remote_streaming_jobs_map, remote_notebooks):
            yield 'Job 1', '1'
        notebooks_controller_mock.update.side_effect = mock_notebooks_update
        def mock_jobs_update(local_jobs_map, remote_jobs_map):
            yield 'Job 1', '1'
        jobs_controller_mock.update.side_effect = mock_jobs_update
        def mock_jobs_create(local_jobs_map, remote_jobs_map, owner):
            yield 'Job 1', '1'
        jobs_controller_mock.create.side_effect = mock_jobs_create

        runner = CliRunner()
        runner.invoke(databricks_cli, ['--jobs-dir', FILE_PATH, '--notebooks-dir', FILE_PATH], catch_exceptions=False)

        expected_remote_notebooks = {
            '/remote/path/file-1',
            '/remote/path/sub-directory/file-2'
        }

        notebooks_controller_mock.update.assert_called_with(expected_local_notebooks_map,
                                                            {'/path/notebooks/streaming-job-1': [JOBS_DATA_REMOTE[1]]},
                                                            expected_remote_notebooks)
        notebooks_controller_mock.create.assert_called_with(expected_local_notebooks_map, expected_remote_notebooks)
        notebooks_controller_mock.delete.assert_called_with(expected_local_directories,
                                                            expected_local_notebooks_map,
                                                            {'/remote/path/sub-directory'},
                                                            expected_remote_notebooks)
        jobs_controller_mock.update.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.create.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map,
                                                       None)
        jobs_controller_mock.delete.assert_called_with(expected_local_jobs_map,
                                                       expected_remote_jobs_map)
        jobs_controller_mock.restart.assert_called_with({ '1': 'Job 1' })
