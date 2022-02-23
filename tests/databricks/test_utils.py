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
import requests

import pytest

from databricks_cli.workspace.api import DIRECTORY, NOTEBOOK, WorkspaceFileInfo
from pipeline_deploy.databricks import utils
from pytest_mock import MockFixture
from requests.exceptions import HTTPError
from tests.databricks import test_data as data
from tests.databricks.utils import FILE_PATH
from tests.utils import get_mock_export_workspace

class TestEnumerateLocalDirectories:
    def test_with_no_exclude_and_no_include(self):
        actual = list(utils.enumerate_local_directories(None, None, FILE_PATH))
        expected = [os.path.join(FILE_PATH, 'directory')]

        assert actual == expected

    def test_with_exclude(self):
        actual = list(utils.enumerate_local_directories(['*directory*'], None, FILE_PATH))

        assert not actual

    def test_with_include(self):
        actual = list(utils.enumerate_local_directories(None, ['*foo*'], FILE_PATH))

        assert not actual

class TestEnumerateLocalJobs:
    def test_with_no_exclude_no_include_and_no_prefix(self):
        actual = [*utils.enumerate_local_jobs(None, None, FILE_PATH, None)]
        expected = [{'name': 'job 1'}, {'name': 'job 2'}]

        assert actual == expected

    def test_with_exclude(self):
        actual = [*utils.enumerate_local_jobs(['job 1'], None, FILE_PATH, None)]
        expected = [{'name': 'job 2'}]

        assert actual == expected

    def test_with_include(self):
        actual = [*utils.enumerate_local_jobs(None, ['job 1'], FILE_PATH, None)]
        expected = [{'name': 'job 1'}]

        assert actual == expected

    def test_with_prefix(self):
        actual = [*utils.enumerate_local_jobs(None, ['prefix job 1'], FILE_PATH, 'prefix ')]
        expected = [{'name': 'job 1'}]

        assert actual == expected

class TestEnumerateLocalNotebooks:
    def test_with_no_exclude_and_no_include(self):
        actual = [*utils.enumerate_local_notebooks(None, None, FILE_PATH)]
        expected = [
            os.path.join(FILE_PATH,'foo.py'),
            os.path.join(FILE_PATH, 'directory', 'bar.py')
        ]

        assert expected == actual

    def test_with_exclude(self):
        actual = [*utils.enumerate_local_notebooks(['*foo*'], None, FILE_PATH)]
        expected = [os.path.join(FILE_PATH, 'directory', 'bar.py')]

        assert expected == actual

    def test_with_include(self):
        actual = [*utils.enumerate_local_notebooks(None, ['*foo*'], FILE_PATH)]
        expected = [os.path.join(FILE_PATH,'foo.py')]

        assert expected == actual

class TestEnumerateRemoteJobs:
    def test_without_include_without_exclude_and_when_no_jobs_exist_for_the_owner(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_jobs = mocker.Mock(return_value=data.LIST_JOBS_NOT_OWNED)

        mock_get_job_owner = mocker.patch('pipeline_deploy.databricks.utils.get_job_owner')
        mock_get_job_owner.return_value = 'not_owner@company.com'

        actual = [*utils.enumerate_remote_jobs(mock_client, None, None, 'owner@company.com')]

        assert not actual

    def test_without_include_without_exclude_and_when_jobs_exist_for_the_owner(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_jobs = mocker.MagicMock(return_value=data.LIST_JOBS_OWNED)

        mock_get_job_owner = mocker.patch('pipeline_deploy.databricks.utils.get_job_owner')
        def _mock_get_job_owner(client, job_id):
            if job_id == '3':
                return 'not_owner@company.com'
            return 'owner@company.com'
        mock_get_job_owner.side_effect = _mock_get_job_owner

        def _mock_get_job(job_id):
            for job in data.LIST_JOBS_OWNED['jobs']:
                if job['job_id'] == job_id:
                    return job
        mock_client.get_job = mocker.MagicMock(side_effect=_mock_get_job)

        expected = [*data.LIST_JOBS_OWNED["jobs"][:2]]

        actual = [*utils.enumerate_remote_jobs(mock_client, None, None, 'owner@company.com')]

        assert actual == expected

    def test_with_exclude(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_jobs = mocker.MagicMock(return_value=data.LIST_JOBS_OWNED)

        def mock_get_job(job_id):
            for job in data.LIST_JOBS_OWNED['jobs']:
                if job['job_id'] == job_id:
                    return job
        mock_client.get_job = mocker.MagicMock(side_effect=mock_get_job)

        expected = [*data.LIST_JOBS_OWNED["jobs"][1:]]

        actual = [*utils.enumerate_remote_jobs(mock_client, ['Job 1'], None, None)]

        assert actual == expected

    def test_with_include(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_jobs = mocker.MagicMock(return_value=data.LIST_JOBS_OWNED)

        def mock_get_job(job_id):
            for job in data.LIST_JOBS_OWNED['jobs']:
                if job['job_id'] == job_id:
                    return job
        mock_client.get_job = mocker.MagicMock(side_effect=mock_get_job)

        expected = [data.LIST_JOBS_OWNED["jobs"][0]]

        actual = [*utils.enumerate_remote_jobs(mock_client, None, ['Job 1'], None)]

        assert actual == expected

class TestEnumerateRemotePaths:
    def test_without_include_and_without_exclude(self, mocker: MockFixture):
        mock_root_directory = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2')
        ]

        mock_sub_directory = [
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]

        def mock_list_objects(path):
            if path == '/remote/path':
                return mock_root_directory
            if path == '/remote/path/sub-directory':
                return mock_sub_directory
            return []

        mock_client = mocker.MagicMock()
        mock_client.list_objects = mocker.MagicMock(side_effect=mock_list_objects)

        actual = [*utils.enumerate_remote_paths(mock_client, None, None, '/remote/path')]
        expected = [*mock_root_directory, *mock_sub_directory]

        assert actual == expected

    def test_with_exclude(self, mocker: MockFixture):
        mock_root_directory = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2')
        ]

        mock_sub_directory = [
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]

        def mock_list_objects(path):
            if path == '/remote/path':
                return mock_root_directory
            if path == '/remote/path/sub-directory':
                return mock_sub_directory
            return []

        mock_client = mocker.MagicMock()
        mock_client.list_objects = mocker.MagicMock(side_effect=mock_list_objects)

        actual = [*utils.enumerate_remote_paths(mock_client, ['/remote/path/sub-directory*'], None, '/remote/path')]
        expected = [mock_root_directory[0]]

        assert actual == expected

    def test_with_include(self, mocker: MockFixture):
        mock_root_directory = [
            WorkspaceFileInfo('/remote/path/file-1', NOTEBOOK, '1'),
            WorkspaceFileInfo('/remote/path/sub-directory', DIRECTORY, '2')
        ]

        mock_sub_directory = [
            WorkspaceFileInfo('/remote/path/sub-directory/file-2', NOTEBOOK, '3'),
        ]

        def mock_list_objects(path):
            if path == '/remote/path':
                return mock_root_directory
            if path == '/remote/path/sub-directory':
                return mock_sub_directory
            return []

        mock_client = mocker.MagicMock()
        mock_client.list_objects = mocker.MagicMock(side_effect=mock_list_objects)

        actual = [*utils.enumerate_remote_paths(mock_client, None, ['/remote/path/sub-directory*'], '/remote/path')]
        expected = [mock_root_directory[1], *mock_sub_directory]

        assert actual == expected

class TestFilterJobs:
    def test_when_exclude_and_include_are_not_supplied(self):
        assert utils.filter_jobs(None, None, 'job name')

    def test_when_exclude_is_supplied_and_there_is_no_match(self):
        assert utils.filter_jobs(['exclude'], None, 'job name')

    def test_when_exclude_is_supplied_and_there_is_a_match(self):
        assert not utils.filter_jobs(['job*'], None, 'job name')

    def test_when_include_is_supplied_and_there_is_no_match(self):
        assert not utils.filter_jobs(None, ['include'], 'job name')

    def test_when_include_is_supplied_and_there_is_a_match(self):
        assert utils.filter_jobs(None, ['job*'], 'job name')

class TestFilterNotebooks:
    def test_when_exclude_and_include_are_not_supplied(self):
        assert utils.filter_notebooks(None, None, 'notebook-name')

    def test_when_exclude_is_supplied_and_there_is_no_match(self):
        assert utils.filter_notebooks(['exclude'], None, 'notebook-name')

    def test_when_exclude_is_supplied_and_there_is_a_match(self):
        assert not utils.filter_notebooks(['notebook*'], None, 'notebook-name')

    def test_when_include_is_supplied_and_there_is_no_match(self):
        assert not utils.filter_notebooks(None, ['include'], 'notebook-name')

    def test_when_include_is_supplied_and_there_is_a_match(self):
        assert utils.filter_notebooks(None, ['notebook*'], 'notebook-name')

class TestGetJobOwner:
    def test_when_the_owner_is_a_user(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        def mock_perform_query(method, path, query_data=None, headers=None):
            if method == 'GET':
                return data.PERMISSIONS_WITH_OWNER
            return None
        mock_client.perform_query = mocker.MagicMock(side_effect=mock_perform_query)

        actual = utils.get_job_owner(mock_client, '1234567')

        assert actual == 'owner@company.com'

    def test_when_the_owner_is_a_group(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        def mock_perform_query(method, path, query_data=None, headers=None):
            if method == 'GET':
                return data.PERMISSIONS_WITH_OWNER_GROUP
            return None
        mock_client.perform_query = mocker.MagicMock(side_effect=mock_perform_query)

        actual = utils.get_job_owner(mock_client, '1234567')

        assert actual == 'group'

    def test_when_the_owner_is_a_service_principal(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        def mock_perform_query(method, path, query_data=None, headers=None):
            if method == 'GET':
                return data.PERMISSIONS_WITH_OWNER_SERVICE_PRINCIPAL
            return None
        mock_client.perform_query = mocker.MagicMock(side_effect=mock_perform_query)

        actual = utils.get_job_owner(mock_client, '1234567')

        assert actual == 'service'

    def test_when_there_is_no_owner(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        def mock_perform_query(method, path, query_data=None, headers=None):
            if method == 'GET':
                return data.PERMISSIONS_WITH_ONLY_OWNER_AS_MANAGER
            return None
        mock_client.perform_query = mocker.MagicMock(side_effect=mock_perform_query)

        with pytest.raises(AttributeError) as exinfo:
            utils.get_job_owner(mock_client, '1234567')

        assert str(exinfo.value) == 'Owner for job 1234567 could not be found.'

class TestGetLocalNotebooksMap:
    def test_when_there_are_no_notebooks(self, mocker: MockFixture):
        mock_enumerate_local_notebooks = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_notebooks')
        mock_enumerate_local_notebooks.return_value = []

        exclude = ['exclude']
        include = ['include']

        assert not utils.get_local_notebooks_map(exclude, include, FILE_PATH, '/path/notebooks')
        mock_enumerate_local_notebooks.assert_called_with(exclude, include, FILE_PATH)

    def test_when_there_are_notebooks(self, mocker: MockFixture):
        mock_enumerate_local_notebooks = mocker.patch('pipeline_deploy.databricks.utils.enumerate_local_notebooks')
        mock_enumerate_local_notebooks.return_value = [
            os.path.join(FILE_PATH,'foo.py'),
            os.path.join(FILE_PATH, 'directory', 'bar.py')
        ]

        exclude = ['exclude']
        include = ['include']

        expected = {
            '/path/notebooks/foo': os.path.join(FILE_PATH,'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }

        actual = utils.get_local_notebooks_map(exclude, include, FILE_PATH, '/path/notebooks')

        assert expected == actual
        mock_enumerate_local_notebooks.assert_called_with(exclude, include, FILE_PATH)

class TestGetLanguageForNotebook:
    def test_with_python_files(self):
        assert utils.get_language_for_notebook('/path/directory/notebook.py') == 'PYTHON'

    def test_with_sql_files(self):
        assert utils.get_language_for_notebook('/path/directory/notebook.sql') == 'SQL'

    def test_with_scala_files(self):
        assert utils.get_language_for_notebook('/path/directory/notebook.scala') == 'SCALA'

    def test_with_r_files(self):
        assert utils.get_language_for_notebook('/path/directory/notebook.r') == 'R'

    def test_with_files_not_covered_by_the_function(self):
        with pytest.raises(AttributeError) as ex:
            utils.get_language_for_notebook('/path/directory/notebook.txt')

        assert str(ex.value) == 'Unknown extension .TXT.'

class TestGetNotebookPath:
    def test_retrieving_the_nested_notebook_path(self):
        expected = '/path/directory/notebook'
        job = {
            'settings': {
                'notebook_task': {
                    'notebook_path': expected
                }
            }
        }

        assert utils.get_notebook_path(job) == expected

class TestIsJobRunning:
    def test_when_there_are_no_running_jobs(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_runs = mocker.MagicMock(return_value=data.LIST_RUNS_JOB_NOT_RUNNING)

        assert not utils.is_job_running(mock_client, '123456', 'job-run')

    def test_when_a_job_is_stopping(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_runs = mocker.MagicMock(return_value=data.LIST_RUNS_JOB_STOPPING)

        assert utils.is_job_running(mock_client, '123456', 'job-run')

    def test_when_there_are_running_jobs(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_runs = mocker.MagicMock(return_value=data.LIST_RUNS_JOB_RUNNING)

        assert utils.is_job_running(mock_client, '123456', 'job-run')

    def test_when_there_are_pending_jobs(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_runs = mocker.MagicMock(return_value=data.LIST_RUNS_JOB_PENDING)

        assert utils.is_job_running(mock_client, '123456', 'job-run')

    def test_when_there_are_no_runs(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_runs = mocker.MagicMock(return_value={})

        assert not utils.is_job_running(mock_client, '123456', 'job-run')

    def test_when_the_job_doesnt_exist(self, mocker: MockFixture):
        def mock_list_runs(job_id, active_only, completed_only, offset, limit):
            response = mocker.MagicMock()
            response.json = mocker.MagicMock(return_value={'error_code':'RESOURCE_DOES_NOT_EXIST'})

            raise HTTPError(request=requests.Request(), response=response)
        mock_client = mocker.MagicMock()
        mock_client.list_runs = mocker.MagicMock(side_effect=mock_list_runs)

        assert not utils.is_job_running(mock_client, '123456', 'job-run')

class TestIsNotebookUpdated:
    def test_when_there_are_no_changes_to_the_notebok(self, mocker: MockFixture):
        mock_export_workspace = get_mock_export_workspace(data.EXPORT_WORKSPACE_UNCHANGED_NOTEBOOK)
        mock_client = mocker.MagicMock()
        mock_client.export_workspace = mocker.MagicMock(side_effect=mock_export_workspace)

        remote_path = '/path/notebooks/foo'
        local_path = os.path.join(FILE_PATH, 'foo.py')

        assert not utils.is_notebook_updated(mock_client, False, local_path, remote_path)

    def test_when_there_are_only_whitespace_changes_to_the_notebook(self, mocker: MockFixture):
        mock_export_workspace = get_mock_export_workspace(data.EXPORT_WORKSPACE_CHANGED_NOTEBOOK_ONLY_WHITESPACES)
        mock_client = mocker.MagicMock()
        mock_client.export_workspace = mocker.MagicMock(side_effect=mock_export_workspace)

        remote_path = '/path/notebooks/foo'
        local_path = os.path.join(FILE_PATH, 'foo.py')

        assert not utils.is_notebook_updated(mock_client, False, local_path, remote_path)

    def test_when_there_are_changes_to_the_notebook(self, mocker: MockFixture):
        mock_export_workspace = get_mock_export_workspace(data.EXPORT_WORKSPACE_CHANGED_NOTEBOOK)
        mock_client = mocker.MagicMock()
        mock_client.export_workspace = mocker.MagicMock(side_effect=mock_export_workspace)

        remote_path = '/path/notebooks/foo'
        local_path = os.path.join(FILE_PATH, 'foo.py')

        assert utils.is_notebook_updated(mock_client, False, local_path, remote_path)

    def test_when_there_are_changes_to_the_notebook_and_the_diff_flag_is_set(self, mocker: MockFixture):
        mock_export_workspace = get_mock_export_workspace(data.EXPORT_WORKSPACE_CHANGED_NOTEBOOK)
        mock_client = mocker.MagicMock()
        mock_client.export_workspace = mocker.MagicMock(side_effect=mock_export_workspace)

        remote_path = '/path/notebooks/foo'
        local_path = os.path.join(FILE_PATH, 'foo.py')

        assert utils.is_notebook_updated(mock_client, True, local_path, remote_path)

class TestIsStreamingjob:
    def test_when_the_job_has_retries_set_to_a_limit(self):
        assert not utils.is_streaming_job({'max_retries': 1})

    def test_when_the_job_has_retries_not_set(self):
        assert not utils.is_streaming_job({})

    def test_when_the_job_has_retries_set_to_unlimited_and_has_a_schedule(self):
        assert not utils.is_streaming_job({'max_retries': -1, 'schedule': {}})

    def test_when_the_job_has_retries_set_to_unlimited_and_has_no_schedule(self):
        assert utils.is_streaming_job({'max_retries': -1})

class TestIsStreamingNotebook:
    def test_when_there_are_no_jobs(self):
        target = utils.is_streaming_notebook([], FILE_PATH, '/path/notebooks')
        job = os.path.join(FILE_PATH.replace('\\', '/'), 'foo')

        assert not target(job)

    def test_when_the_notebook_is_not_in_the_jobs_list(self):
        target = utils.is_streaming_notebook(data.JOBS_DATA_REMOTE_STREAMING, FILE_PATH, '/path/notebooks')
        job = os.path.join(FILE_PATH.replace('\\', '/'), 'not-found-job')

        assert not target(job)

    def test_when_the_notebook_is_in_the_list(self):
        target = utils.is_streaming_notebook(data.JOBS_DATA_REMOTE_STREAMING, FILE_PATH, '/path/notebooks')
        job = os.path.join(FILE_PATH.replace('\\', '/'), 'streaming-job-1')

        assert target(job)

class TestRestartJob:
    def test_when_the_job_is_not_running(self, mocker: MockFixture):
        mock_runs_client = mocker.MagicMock()
        mock_runs_client.list_runs = mocker.MagicMock(return_value={"runs":[]})
        mock_runs_client.cancel_run = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()
        mock_jobs_client.run_now = mocker.MagicMock()

        utils.restart_job(mock_jobs_client, '123456', 'test-job', mock_runs_client)

        mock_runs_client.cancel_run.assert_not_called()
        mock_jobs_client.run_now.assert_called_once_with('123456', None, None, None, None)

    def test_when_the_job_is_running(self, mocker: MockFixture):
        mock_runs_client = mocker.MagicMock()
        run_query_count = 0
        def mock_list_runs(job_id, active_only, completed_only, offset, limit):
            nonlocal run_query_count
            run_query_count = run_query_count + 1

            if run_query_count > 3:
                return data.LIST_RUNS_JOB_NOT_RUNNING

            if run_query_count > 2:
                return data.LIST_RUNS_JOB_STOPPING

            return data.LIST_RUNS_JOB_RUNNING
        mock_runs_client.list_runs = mocker.MagicMock(side_effect=mock_list_runs)
        mock_runs_client.cancel_run = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()
        mock_jobs_client.run_now = mocker.MagicMock()

        utils.restart_job(mock_jobs_client, '123456', 'test-job', mock_runs_client)

        mock_runs_client.cancel_run.assert_called_once_with('1234567')
        mock_jobs_client.run_now.assert_called_once_with('123456', None, None, None, None)


    def test_when_the_job_is_pending(self, mocker: MockFixture):
        mock_runs_client = mocker.MagicMock()
        run_query_count = 0
        def mock_list_runs(job_id, active_only, completed_only, offset, limit):
            nonlocal run_query_count
            run_query_count = run_query_count + 1

            if run_query_count > 3:
                return data.LIST_RUNS_JOB_NOT_RUNNING

            if run_query_count > 2:
                return data.LIST_RUNS_JOB_STOPPING

            return data.LIST_RUNS_JOB_PENDING
        mock_runs_client.list_runs = mocker.MagicMock(side_effect=mock_list_runs)
        mock_runs_client.cancel_run = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()
        mock_jobs_client.run_now = mocker.MagicMock()

        utils.restart_job(mock_jobs_client, '123456', 'test-job', mock_runs_client)

        mock_runs_client.cancel_run.assert_called_once_with('1234567')
        mock_jobs_client.run_now.assert_called_once_with('123456', None, None, None, None)

class TestSetJobOwner:
    def test_if_the_job_already_has_the_proper_owner_set_for_the_job(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        def mock_perform_query(method, path, query_data=None, headers=None):
            if method == 'GET':
                return data.PERMISSIONS_WITH_OWNER
            return None
        mock_client.perform_query = mocker.MagicMock(side_effect=mock_perform_query)

        utils.set_job_owner(mock_client, '123456', 'owner@company.com')

        mock_client.perform_query.assert_called_with('GET', '/permissions/jobs/123456')

    def test_if_the_job_does_not_already_have_the_proper_owner_set(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        def mock_perform_query(method, path, query_data=None, headers=None):
            if method == 'GET':
                return data.PERMISSIONS_WITHOUT_OWNER
            return None
        mock_client.perform_query = mocker.MagicMock(side_effect=mock_perform_query)

        expected = {
            "access_control_list": [
                {
                    "user_name": 'owner@company.com',
                    "permission_level": "IS_OWNER"
                }
            ]
        }

        utils.set_job_owner(mock_client, '123456', 'owner@company.com')

        mock_client.perform_query.assert_any_call('GET', '/permissions/jobs/123456')
        mock_client.perform_query.assert_called_with('PUT', '/permissions/jobs/123456', expected)

class TestSetJobPermissions:
    def test_if_the_group_already_has_manager_permissions_for_the_job(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        def mock_perform_query(method, path, query_data=None, headers=None):
            if method == 'GET':
                return data.PERMISSIONS_WITH_OWNER_AS_MANAGER_AND_GROUP_WITH_MANAGER_PERMISSION
            return None
        mock_client.perform_query = mocker.MagicMock(side_effect=mock_perform_query)

        utils.set_job_permissions(mock_client, 'group', '123456')

        mock_client.perform_query.assert_called_with('GET', '/permissions/jobs/123456')

    def test_if_the_group_has_non_manager_permissions_for_the_job(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        def mock_perform_query(method, path, query_data=None, headers=None):
            if method == 'GET':
                return data.PERMISSIONS_WITH_OWNER_AS_MANAGER_AND_GROUP_WITH_VIEW_PERMISSION
            return None
        mock_client.perform_query = mocker.MagicMock(side_effect=mock_perform_query)

        expected = {
            "access_control_list": [
                {
                    "user_name": 'owner@company.com',
                    "permission_level": "CAN_MANAGE"
                },
                {
                    "group_name": "group",
                    "permission_level": "CAN_VIEW"
                },
                {
                    "group_name": "group",
                    "permission_level": "CAN_MANAGE"
                }
            ]
        }

        utils.set_job_permissions(mock_client, 'group', '123456')

        mock_client.perform_query.assert_any_call('GET', '/permissions/jobs/123456')
        mock_client.perform_query.assert_called_with('PUT', '/permissions/jobs/123456', expected)

    def test_if_the_group_has_no_permissions_for_the_job(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        def mock_perform_query(method, path, query_data=None, headers=None):
            if method == 'GET':
                return data.PERMISSIONS_WITH_ONLY_OWNER_AS_MANAGER
            return None
        mock_client.perform_query = mocker.MagicMock(side_effect=mock_perform_query)

        expected = {
            "access_control_list": [
                {
                    "user_name": 'owner@company.com',
                    "permission_level": "CAN_MANAGE"
                },
                {
                    "group_name": "group",
                    "permission_level": "CAN_MANAGE"
                }
            ]
        }

        utils.set_job_permissions(mock_client, 'group', '123456')

        mock_client.perform_query.assert_any_call('GET', '/permissions/jobs/123456')
        mock_client.perform_query.assert_called_with('PUT', '/permissions/jobs/123456', expected)

class TestStartJob:
    def test_invoking_the_client_call(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.run_now = mocker.MagicMock()

        utils.start_job(mock_client, '123456', 'job-name')

        mock_client.run_now.assert_called_with('123456', None, None, None, None)

class TestStopJob:
    def test_when_there_are_no_runs_for_the_job(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_runs = mocker.MagicMock(return_value={'runs': []})
        mock_client.cancel_run = mocker.MagicMock()

        utils.stop_job(mock_client, '123456', 'job-name')

        mock_client.cancel_run.assert_not_called()

    def test_when_there_are_no_active_runs_for_the_job(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_runs = mocker.MagicMock(return_value=data.LIST_RUNS_JOB_NOT_RUNNING)
        mock_client.cancel_run = mocker.MagicMock()

        utils.stop_job(mock_client, '123456', 'job-name')

        mock_client.cancel_run.assert_not_called()

    def test_when_the_job_does_not_exist(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        def mock_list_runs(job_id, active_only, completed_only, offset, limit):
            response = mocker.MagicMock()
            response.json = mocker.MagicMock(return_value={'error_code':'RESOURCE_DOES_NOT_EXIST'})

            raise HTTPError(request=requests.Request(), response=response)
        mock_client.list_runs = mocker.MagicMock(side_effect=mock_list_runs)
        mock_client.cancel_run = mocker.MagicMock()

        utils.stop_job(mock_client, '123456', 'job-name')

        mock_client.cancel_run.assert_not_called()

    def test_when_there_are_active_runs_for_the_job(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_runs = mocker.MagicMock(return_value=data.LIST_RUNS_JOB_RUNNING)
        mock_client.cancel_run = mocker.MagicMock()

        utils.stop_job(mock_client, '123456', 'job-name')

        mock_client.cancel_run.assert_called_with('1234567')

    def test_when_there_are_pending_runs_for_the_job(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_runs = mocker.MagicMock(return_value=data.LIST_RUNS_JOB_RUNNING)
        mock_client.cancel_run = mocker.MagicMock()

        utils.stop_job(mock_client, '123456', 'job-name')

        mock_client.cancel_run.assert_called_with('1234567')

    def test_when_there_are_active_runs_for_the_job_and_the_job_gets_deleted(self, mocker: MockFixture):
        mock_client = mocker.MagicMock()
        mock_client.list_runs = mocker.MagicMock(return_value=data.LIST_RUNS_JOB_RUNNING)
        def mock_cancel_run(run_id):
            response = mocker.MagicMock()
            response.json = mocker.MagicMock(return_value={'error_code':'RESOURCE_DOES_NOT_EXIST'})

            raise HTTPError(request=requests.Request(), response=response)
        mock_client.cancel_run = mocker.MagicMock(side_effect=mock_cancel_run)

        with pytest.raises(HTTPError) as exinfo:
            utils.stop_job(mock_client, '123456', 'job-name')

        assert exinfo