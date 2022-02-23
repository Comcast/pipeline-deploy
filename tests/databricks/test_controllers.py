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

from pytest_mock.plugin import MockerFixture
from pipeline_deploy.databricks.controllers import JobsController, NotebooksController
from tests.databricks.utils import FILE_PATH
from tests.databricks import test_data as data

class TestJobsController:
    def test_create_when_there_are_no_jobs_that_require_creation(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()

        target = JobsController(mock_api_client, False, False, 'group')
        target.jobs_client = mock_jobs_client

        local_jobs_map = dict((job['settings']['name'], job['settings'])
                              for job in data.LIST_JOBS_OWNED['jobs'])
        remote_jobs_map = dict((job['settings']['name'], job)
                               for job in data.LIST_JOBS_OWNED['jobs'])

        actual = list(target.create(local_jobs_map, remote_jobs_map, None))

        mock_jobs_client.create_job.assert_not_called()

        assert not actual

    def test_create_when_there_are_jobs_that_require_creation_and_its_a_dry_run(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()

        target = JobsController(mock_api_client, False, True, 'group')
        target.jobs_client = mock_jobs_client

        local_jobs_map = dict((job['settings']['name'], job['settings'])
                              for job in data.LIST_JOBS_OWNED['jobs'])
        remote_jobs_map = dict()

        actual = list(target.create(local_jobs_map, remote_jobs_map, None))

        mock_jobs_client.create_job.assert_not_called()

        assert not actual

    def test_create_when_there_are_jobs_that_require_creation(self, mocker: MockerFixture):
        mock_start_job = mocker.patch('pipeline_deploy.databricks.utils.start_job')
        mock_set_job_owner = mocker.patch('pipeline_deploy.databricks.utils.set_job_owner')
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()
        def mock_create_job(config):
            for job in data.LIST_JOBS_OWNED['jobs']:
                if job['settings']['name'] == config['name']:
                    return job

            return None
        mock_jobs_client.create_job = mocker.MagicMock(side_effect=mock_create_job)

        target = JobsController(mock_api_client, False, False)
        target.jobs_client = mock_jobs_client

        local_jobs_map = dict((job['settings']['name'], job['settings'])
                              for job in data.LIST_JOBS_OWNED['jobs'])
        remote_jobs_map = dict()
        owner = 'owner@company.com'

        actual = list(target.create(local_jobs_map, remote_jobs_map, owner))

        for job in data.LIST_JOBS_OWNED['jobs']:
            mock_jobs_client.create_job.assert_any_call(job['settings'])
            mock_set_job_owner.assert_any_call(mock_api_client, job['job_id'], owner)

        mock_start_job.assert_not_called()

        assert not actual

    def test_create_when_there_are_jobs_that_require_creation_and_no_owner_is_specified(self, mocker: MockerFixture):
        mock_start_job = mocker.patch('pipeline_deploy.databricks.utils.start_job')
        mock_set_job_owner = mocker.patch('pipeline_deploy.databricks.utils.set_job_owner')
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()
        def mock_create_job(config):
            for job in data.LIST_JOBS_OWNED['jobs']:
                if job['settings']['name'] == config['name']:
                    return job

            return None
        mock_jobs_client.create_job = mocker.MagicMock(side_effect=mock_create_job)

        target = JobsController(mock_api_client, False, False)
        target.jobs_client = mock_jobs_client

        local_jobs_map = dict((job['settings']['name'], job['settings'])
                              for job in data.LIST_JOBS_OWNED['jobs'])
        remote_jobs_map = dict()

        actual = list(target.create(local_jobs_map, remote_jobs_map, None))

        for job in data.LIST_JOBS_OWNED['jobs']:
            mock_jobs_client.create_job.assert_any_call(job['settings'])

        mock_set_job_owner.assert_not_called()
        mock_start_job.assert_not_called()

        assert not actual

    def test_create_when_there_are_streaming_jobs_that_require_creation(self, mocker: MockerFixture):
        mock_set_job_owner = mocker.patch('pipeline_deploy.databricks.utils.set_job_owner')
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()
        def mock_create_job(config):
            for job in data.LIST_STREAMING_JOBS_OWNED['jobs']:
                if job['settings']['name'] == config['name']:
                    return job

            return None
        mock_jobs_client.create_job = mocker.MagicMock(side_effect=mock_create_job)

        target = JobsController(mock_api_client, False, False)
        target.jobs_client = mock_jobs_client

        local_jobs_map = dict((job['settings']['name'], job['settings'])
                              for job in data.LIST_STREAMING_JOBS_OWNED['jobs'])
        remote_jobs_map = dict()
        owner = 'owner@company.com'

        actual = list(target.create(local_jobs_map, remote_jobs_map, owner))

        for job in data.LIST_STREAMING_JOBS_OWNED['jobs']:
            mock_jobs_client.create_job.assert_any_call(job['settings'])
            mock_set_job_owner.assert_any_call(mock_api_client, job['job_id'], owner)

            assert (job['settings']['name'], job['job_id']) in actual

    def test_create_when_there_are_jobs_that_require_creation_and_a_group_name_is_set(self, mocker: MockerFixture):
        mock_set_job_permissions = mocker.patch('pipeline_deploy.databricks.utils.set_job_permissions')
        mock_set_job_owner = mocker.patch('pipeline_deploy.databricks.utils.set_job_owner')
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()
        def mock_create_job(config):
            for job in data.LIST_STREAMING_JOBS_OWNED['jobs']:
                if job['settings']['name'] == config['name']:
                    return job

            return None
        mock_jobs_client.create_job = mocker.MagicMock(side_effect=mock_create_job)

        target = JobsController(mock_api_client, False, False, 'group')
        target.jobs_client = mock_jobs_client

        local_jobs_map = dict((job['settings']['name'], job['settings'])
                              for job in data.LIST_STREAMING_JOBS_OWNED['jobs'])
        remote_jobs_map = dict()
        owner = 'owner@company.com'

        actual = list(target.create(local_jobs_map, remote_jobs_map, owner))

        for job in data.LIST_STREAMING_JOBS_OWNED['jobs']:
            mock_jobs_client.create_job.assert_any_call(job['settings'])
            mock_set_job_owner.assert_any_call(mock_api_client, job['job_id'], owner)
            mock_set_job_permissions.assert_any_call(mock_api_client, 'group', job['job_id'])

        assert ('Job 1', '1') in actual
        assert ('Job 2', '2') in actual
        assert ('Job 3', '3') in actual

    def test_delete_when_there_are_no_jobs_that_require_deletion(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()

        target = JobsController(mock_api_client, False, False, 'group')
        target.jobs_client = mock_jobs_client

        local_jobs_map = dict((job['settings']['name'], job['settings'])
                              for job in data.LIST_JOBS_OWNED['jobs'])
        remote_jobs_map = dict((job['settings']['name'], job)
                               for job in data.LIST_JOBS_OWNED['jobs'])

        target.delete(local_jobs_map, remote_jobs_map)

        mock_jobs_client.delete_job.assert_not_called()

    def test_delete_when_there_are_jobs_that_require_deletion_and_its_a_dry_run(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()

        target = JobsController(mock_api_client, False, True, 'group')
        target.jobs_client = mock_jobs_client

        local_jobs_map = dict()
        remote_jobs_map = dict((job['settings']['name'], job)
                               for job in data.LIST_JOBS_OWNED['jobs'])

        target.delete(local_jobs_map, remote_jobs_map)

        mock_jobs_client.delete_job.assert_not_called()

    def test_delete_when_there_are_streaming_jobs_that_require_deletion(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()
        mock_jobs_client.delete_job = mocker.MagicMock()

        target = JobsController(mock_api_client, False, False)
        target.jobs_client = mock_jobs_client

        local_jobs_map = dict()
        remote_jobs_map = dict((job['settings']['name'], job)
                               for job in data.LIST_JOBS_OWNED['jobs'])

        target.delete(local_jobs_map, remote_jobs_map)

        for job in data.LIST_STREAMING_JOBS_OWNED['jobs']:
            mock_jobs_client.delete_job.assert_any_call(job['job_id'])

    def test_restart_when_there_are_no_jobs_to_restart(self, mocker: MockerFixture):
        mock_restart_job = mocker.patch('pipeline_deploy.databricks.utils.restart_job')
        mock_api_client = mocker.MagicMock()

        target = JobsController(mock_api_client, False, False)

        target.restart({})

        mock_restart_job.assert_not_called()

    def test_restart_when_there_are_jobs_to_restart_and_its_a_dry_run(self, mocker: MockerFixture):
        mock_restart_job = mocker.patch('pipeline_deploy.databricks.utils.restart_job')
        mock_api_client = mocker.MagicMock()

        target = JobsController(mock_api_client, False, True)

        target.restart({ '1': 'Job 1' })

        mock_restart_job.assert_not_called()

    def test_restart_when_there_are_jobs_to_restart(self, mocker: MockerFixture):
        mock_restart_job = mocker.patch('pipeline_deploy.databricks.utils.restart_job')
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()
        mock_runs_client = mocker.MagicMock()

        target = JobsController(mock_api_client, False, False)
        target.jobs_client = mock_jobs_client
        target.runs_client = mock_runs_client

        target.restart({ '1': 'Job 1' })

        mock_restart_job.assert_called_with(mock_jobs_client, '1', 'Job 1', mock_runs_client)

    def test_update_when_there_are_no_jobs_that_require_updating(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()

        target = JobsController(mock_api_client, False, False, 'group')
        target.jobs_client = mock_jobs_client

        local_jobs_map = dict((job['settings']['name'], job['settings'])
                              for job in data.LIST_JOBS_OWNED['jobs'])
        remote_jobs_map = dict((job['settings']['name'], job)
                               for job in data.LIST_JOBS_OWNED['jobs'])

        actual = list(target.update(local_jobs_map, remote_jobs_map))

        mock_jobs_client.reset_job.assert_not_called()

        assert not actual

    def test_update_when_there_are_jobs_that_require_updating_and_its_a_dry_run(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()

        target = JobsController(mock_api_client, False, True, 'group')
        target.jobs_client = mock_jobs_client

        local_jobs_map = {
            'Job 1': {
                "name": "Job 1",
                "max_retries": -1
            },
            "Job 2": {
                "name": "Job 2"
            },
            "Job 3": {
                "name": "Job 3"
            }
        }
        remote_jobs_map = dict((job['settings']['name'], job)
                               for job in data.LIST_JOBS_OWNED['jobs'])

        actual = list(target.update(local_jobs_map, remote_jobs_map))

        mock_jobs_client.reset_job.assert_not_called()

        assert ('Job 1', '1') in actual

    def test_update_when_there_are_jobs_that_require_updating(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()

        target = JobsController(mock_api_client, False, False, 'group')
        target.jobs_client = mock_jobs_client

        local_jobs_map = {
            'Job 1': {
                "name": "Job 1",
                "schedule": {}
            },
            "Job 2": {
                "name": "Job 2"
            },
            "Job 3": {
                "name": "Job 3"
            }
        }
        remote_jobs_map = dict((job['settings']['name'], job)
                               for job in data.LIST_JOBS_OWNED['jobs'])

        actual = list(target.update(local_jobs_map, remote_jobs_map))

        mock_jobs_client.reset_job.assert_called_with({
            'job_id': '1',
            'new_settings': local_jobs_map['Job 1']
        })

        assert not actual

    def test_update_when_there_are_streaming_jobs_that_require_updating(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()
        mock_runs_client = mocker.MagicMock()

        target = JobsController(mock_api_client, False, False, 'group')
        target.jobs_client = mock_jobs_client
        target.runs_client = mock_runs_client

        local_jobs_map = {
            'Job 1': {
                "name": "Job 1",
                "max_retries": -1,
                "new_cluster": {}
            },
            "Job 2": {
                "name": "Job 2",
                "max_retries": -1
            },
            "Job 3": {
                "name": "Job 3",
                "max_retries": -1
            }
        }
        remote_jobs_map = dict((job['settings']['name'], job)
                               for job in data.LIST_STREAMING_JOBS_OWNED['jobs'])

        actual = list(target.update(local_jobs_map, remote_jobs_map))

        assert ('Job 1', '1') in actual

class TestNotebooksController:
    def test_create_when_there_are_no_notebooks_that_require_creation(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_workspace_client = mocker.MagicMock()

        target = NotebooksController(mock_api_client, False, False, FILE_PATH, '/path/notebooks')
        target.workspace_client = mock_workspace_client

        local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH, 'foo.py')
        }
        remote_notebooks = {'/path/notebooks/foo'}

        target.create(local_notebooks_map, remote_notebooks)

        mock_workspace_client.import_workspace.assert_not_called()

    def test_create_when_there_are_notebooks_that_require_creation_and_its_a_dry_run(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_workspace_client = mocker.MagicMock()

        target = NotebooksController(mock_api_client, False, True, FILE_PATH, '/path/notebooks')
        target.workspace_client = mock_workspace_client

        local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH, 'foo.py')
        }
        remote_notebooks = set()

        target.create(local_notebooks_map, remote_notebooks)

        mock_workspace_client.import_workspace.assert_not_called()

    def test_create_when_there_are_notebooks_that_require_creation(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_workspace_client = mocker.MagicMock()

        target = NotebooksController(mock_api_client, False, False, FILE_PATH, '/path/notebooks')
        target.workspace_client = mock_workspace_client

        local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH, 'foo.py')
        }
        remote_notebooks = set()

        target.create(local_notebooks_map, remote_notebooks)

        mock_workspace_client.mkdirs.assert_called_with('/path/notebooks')
        mock_workspace_client.import_workspace.assert_called_with(
            local_notebooks_map['/path/notebooks/foo'],
            '/path/notebooks/foo',
            'PYTHON',
            'SOURCE',
            True
        )

    def test_delete_when_there_are_no_directories_or_notebooks_that_require_deletion(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_workspace_client = mocker.MagicMock()

        target = NotebooksController(mock_api_client, False, False, FILE_PATH, '/path/notebooks')
        target.workspace_client = mock_workspace_client

        local_directories = {os.path.join(FILE_PATH, 'directory')}
        local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH, 'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        remote_directories = {'/path/notebooks/directory'}
        remote_notebooks = {'/path/notebooks/foo', '/path/notebooks/directory/bar'}

        target.delete(local_directories, local_notebooks_map, remote_directories, remote_notebooks)

        mock_workspace_client.delete.assert_not_called()

    def test_delete_when_there_are_directories_to_delete_but_its_a_dry_run(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_workspace_client = mocker.MagicMock()

        target = NotebooksController(mock_api_client, False, True, FILE_PATH, '/path/notebooks')
        target.workspace_client = mock_workspace_client

        local_directories = {os.path.join(FILE_PATH, 'directory')}
        local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH, 'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        remote_directories = {'/path/notebooks/directory', '/path/notebooks/directory/to-delete'}
        remote_notebooks = {'/path/notebooks/foo', '/path/notebooks/directory/bar'}

        target.delete(local_directories, local_notebooks_map, remote_directories, remote_notebooks)

        mock_workspace_client.delete.assert_not_called()

    def test_delete_when_there_are_directories_to_delete(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_workspace_client = mocker.MagicMock()

        target = NotebooksController(mock_api_client, False, False, FILE_PATH, '/path/notebooks')
        target.workspace_client = mock_workspace_client

        local_directories = {os.path.join(FILE_PATH, 'directory')}
        local_notebooks_map = {
            '/path/notebooks/foo': os.path.join(FILE_PATH, 'foo.py'),
            '/path/notebooks/directory/bar': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        remote_directories = {'/path/notebooks/directory', '/path/notebooks/directory/to-delete'}
        remote_notebooks = {'/path/notebooks/foo', '/path/notebooks/directory/bar'}

        target.delete(local_directories, local_notebooks_map, remote_directories, remote_notebooks)

        mock_workspace_client.delete.assert_called_with('/path/notebooks/directory/to-delete', True)

    def test_update_when_there_are_no_notebooks_requiring_changes(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_workspace_client = mocker.MagicMock()
        mock_is_notebook_updated = mocker.patch('pipeline_deploy.databricks.utils.is_notebook_updated')
        mock_is_notebook_updated.return_value = False
        target = NotebooksController(mock_api_client, False, False, FILE_PATH, '/path/notebooks')
        target.workspace_client = mock_workspace_client

        local_notebooks_map = {
            '/path/notebooks/job-1': os.path.join(FILE_PATH, 'foo.py'),
            '/path/notebooks/streaming-job-1': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        remote_streaming_jobs_map = {'/path/notebooks/streaming-job-1': [data.JOBS_DATA_REMOTE[1]]}
        remote_notebooks = {'/path/notebooks/job-1', '/path/notebooks/streaming-job-1'}

        actual = list(target.update(local_notebooks_map, remote_streaming_jobs_map, remote_notebooks))

        mock_workspace_client.import_workspace.assert_not_called()

        assert not actual

    def test_update_when_there_are_notebooks_requiring_changes_and_its_a_dry_run(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_workspace_client = mocker.MagicMock()
        mock_is_notebook_updated = mocker.patch('pipeline_deploy.databricks.utils.is_notebook_updated')
        mock_is_notebook_updated.return_value = True
        target = NotebooksController(mock_api_client, False, True, FILE_PATH, '/path/notebooks')
        target.workspace_client = mock_workspace_client

        local_notebooks_map = {
            '/path/notebooks/job-1': os.path.join(FILE_PATH, 'foo.py'),
            '/path/notebooks/streaming-job-1': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        remote_streaming_jobs_map = {'/path/notebooks/streaming-job-1': [data.JOBS_DATA_REMOTE[1]]}
        remote_notebooks = {'/path/notebooks/job-1', '/path/notebooks/streaming-job-1'}

        actual = list(target.update(local_notebooks_map, remote_streaming_jobs_map, remote_notebooks))

        mock_workspace_client.import_workspace.assert_not_called()

        assert ('streaming-job-1', '2') in actual

    def test_update_when_there_are_notebooks_requiring_changes_and_the_notebooks_are_not_streaming(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_workspace_client = mocker.MagicMock()
        mock_is_notebook_updated = mocker.patch('pipeline_deploy.databricks.utils.is_notebook_updated')
        def mock_is_notebook_updated_side_effect(client, diff, local, remote):
            if remote == '/path/notebooks/job-1':
                return True
            return False
        mock_is_notebook_updated.side_effect = mock_is_notebook_updated_side_effect
        target = NotebooksController(mock_api_client, False, False, FILE_PATH, '/path/notebooks')
        target.workspace_client = mock_workspace_client

        local_notebooks_map = {
            '/path/notebooks/job-1': os.path.join(FILE_PATH, 'foo.py'),
            '/path/notebooks/streaming-job-1': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        remote_streaming_jobs_map = {'/path/notebooks/streaming-job-1': [data.JOBS_DATA_REMOTE[1]]}
        remote_notebooks = {'/path/notebooks/job-1', '/path/notebooks/streaming-job-1'}

        actual = list(target.update(local_notebooks_map, remote_streaming_jobs_map, remote_notebooks))

        mock_workspace_client.import_workspace.assert_called_with(os.path.join(FILE_PATH, 'foo.py'),
                                                                  '/path/notebooks/job-1',
                                                                  'PYTHON', 'SOURCE', True)

        assert not actual

    def test_update_when_there_are_notebooks_requiring_changes_and_the_notebooks_are_streaming(self, mocker: MockerFixture):
        mock_api_client = mocker.MagicMock()
        mock_jobs_client = mocker.MagicMock()
        mock_runs_client = mocker.MagicMock()
        mock_workspace_client = mocker.MagicMock()
        mock_is_notebook_updated = mocker.patch('pipeline_deploy.databricks.utils.is_notebook_updated')
        def mock_is_notebook_updated_side_effect(client, diff, local, remote):
            if remote == '/path/notebooks/job-1':
                return False
            return True
        mock_is_notebook_updated.side_effect = mock_is_notebook_updated_side_effect
        target = NotebooksController(mock_api_client, False, False, FILE_PATH, '/path/notebooks')
        target.workspace_client = mock_workspace_client
        target.jobs_client = mock_jobs_client
        target.runs_client = mock_runs_client

        local_notebooks_map = {
            '/path/notebooks/job-1': os.path.join(FILE_PATH, 'foo.py'),
            '/path/notebooks/streaming-job-1': os.path.join(FILE_PATH, 'directory', 'bar.py')
        }
        remote_streaming_jobs_map = {'/path/notebooks/streaming-job-1': [data.JOBS_DATA_REMOTE[1]]}
        remote_notebooks = {'/path/notebooks/job-1', '/path/notebooks/streaming-job-1'}

        actual = list(target.update(local_notebooks_map, remote_streaming_jobs_map, remote_notebooks))

        mock_workspace_client.import_workspace.assert_called_with(os.path.join(FILE_PATH, 'directory', 'bar.py'),
                                                                  '/path/notebooks/streaming-job-1',
                                                                  'PYTHON', 'SOURCE', True)
        assert ('streaming-job-1', '2') in actual
