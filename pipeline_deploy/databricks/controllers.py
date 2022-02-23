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

import logging
import os

from typing import Any, Dict, Generator, Set, Tuple

from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceApi
from pipeline_deploy.controllers import BaseController
from pipeline_deploy.databricks import utils


class DatabricksController(BaseController):
    def __init__(self, api_client: ApiClient, dry_run: bool) -> None:
        super().__init__(dry_run)

        self.api_client = api_client
        self.jobs_client = JobsApi(api_client)
        self.runs_client = RunsApi(api_client)
        self.workspace_client = WorkspaceApi(api_client)


class JobsController(DatabricksController):
    def __init__(self, api_client: ApiClient, diff: bool, dry_run: bool,
                 group_name: str = None) -> None:
        super().__init__(api_client, dry_run)

        self.diff = diff
        self.group_name = group_name

    def create(self, local_jobs_map: dict, remote_jobs_map: dict,
               owner: str) -> Generator[Tuple[str, str], None, None]:
        jobs_to_create = list(local_jobs_map.keys() - remote_jobs_map.keys())
        jobs_to_create.sort()

        for job_name in jobs_to_create:
            local = local_jobs_map[job_name]

            logging.info('Creating "%s" in the remote environment.', job_name)

            if not self.dry_run:
                resp = self.jobs_client.create_job(local)
                job_id = resp['job_id']

                if owner:
                    utils.set_job_owner(self.api_client, job_id, owner)

                if self.group_name:
                    utils.set_job_permissions(self.api_client, self.group_name, job_id)

            if utils.is_streaming_job(local):
                yield job_name, job_id

        if len(jobs_to_create) == 0:
            logging.info('No jobs require creation.')

    def delete(self, local_jobs_map: dict, remote_jobs_map: dict):
        jobs_to_delete = list(remote_jobs_map.keys() - local_jobs_map.keys())
        jobs_to_delete.sort()

        for job_name in jobs_to_delete:
            logging.info('Removing "%s" from the remote environment.', job_name)

            if not self.dry_run:
                self.jobs_client.delete_job(remote_jobs_map[job_name]['job_id'])

        if len(jobs_to_delete) == 0:
            logging.info('No jobs require deletion.')

    def restart(self, jobs: Dict[str, str]):
        for job_id, job_name in jobs.items():
            logging.info('Restarting streaming job "%s" in the remote environment.', job_name)

            if not self.dry_run:
                utils.restart_job(self.jobs_client, job_id, job_name, self.runs_client)

    def update(self, local_jobs_map: dict,
               remote_jobs_map: dict) -> Generator[Tuple[str, str], None, None]:
        def requires_reset(job_name: str):
            local_settings = local_jobs_map[job_name]
            remote_settings = remote_jobs_map[job_name]['settings']

            return local_settings != remote_settings

        jobs_to_reset = list(
            filter(requires_reset, remote_jobs_map.keys() & local_jobs_map.keys())
        )
        jobs_to_reset.sort()

        for job_name in jobs_to_reset:
            job_id = str(remote_jobs_map[job_name]['job_id'])
            local = local_jobs_map[job_name]

            if self.diff:
                logging.info('Changes detected for "%s" between the remote and local environment',
                             job_name)

                utils.print_job_diff(job_name, local, remote_jobs_map[job_name]['settings'])

            logging.info('Resetting "%s" in the remote environment.', job_name)

            if not self.dry_run:
                self.jobs_client.reset_job({'job_id': job_id, 'new_settings': local})

            if utils.is_streaming_job(local):
                yield job_name, job_id

        if len(jobs_to_reset) == 0:
            logging.info('No jobs require resetting.')


class NotebooksController(DatabricksController):
    def __init__(self, api_client: ApiClient, diff: bool, dry_run: bool, notebooks_dir: str,
                 remote_path: str) -> None:
        super().__init__(api_client, dry_run)

        self.diff = diff
        self.notebooks_dir = notebooks_dir
        self.remote_path = remote_path

    def create(self, local_notebooks_map: dict, remote_notebooks: Set[str]):
        notebooks_to_create_remote = list(sorted(local_notebooks_map.keys() - remote_notebooks))

        for remote in notebooks_to_create_remote:
            local = local_notebooks_map[remote]

            logging.info('Creating "%s" in the remote environment.', remote)

            if not self.dry_run:
                language = utils.get_language_for_notebook(local)
                self.workspace_client.mkdirs(os.path.dirname(remote))
                self.workspace_client.import_workspace(local, remote, language, 'SOURCE', True)

        if len(notebooks_to_create_remote) == 0:
            logging.info('No notebooks require creation.')

    def delete(self, local_directories: Set[str], local_notebooks_map: dict,
               remote_directories: Set[str], remote_notebooks: Set[str]):
        local_directories_mapped = {d.replace(self.notebooks_dir, self.remote_path)
                                    for d in local_directories}

        directories_to_delete_remote = set(remote_directories - local_directories_mapped)
        directories_to_delete_remote = list(sorted(directories_to_delete_remote, reverse=True))
        for directory in directories_to_delete_remote:
            logging.info('Deleting "%s" in the remote environment.', directory)

            if not self.dry_run:
                self.workspace_client.delete(directory, True)

        if len(directories_to_delete_remote) == 0:
            logging.info('No directories require deletion.')

        notebooks_to_delete_remote = list(remote_notebooks - local_notebooks_map.keys())
        for notebook in notebooks_to_delete_remote:
            directory = os.path.dirname(notebook)

            if directory in directories_to_delete_remote:
                continue

            logging.info('Deleting "%s" in the remote environment.', notebook)

            if not self.dry_run:
                self.workspace_client.delete(notebook, False)

        if len(notebooks_to_delete_remote) == 0:
            logging.info('No notebooks require deletion.')

    def update(self, local_notebooks_map: dict, remote_streaming_jobs_map: Dict[str, Any],
               remote_notebooks: Set[str]) -> Generator[Tuple[str, str], None, None]:
        existing_notebooks_remote = list(local_notebooks_map.keys() & remote_notebooks)
        existing_notebooks = [(key, local_notebooks_map[key])
                              for key in existing_notebooks_remote]

        notebooks_to_update = sorted(existing_notebooks)

        def _is_notebook_updated(tpl: Tuple[str, str]):
            (remote, local) = tpl
            return utils.is_notebook_updated(self.workspace_client, self.diff, local, remote)
        notebooks_to_update = [*filter(_is_notebook_updated, notebooks_to_update)]

        for remote, local in notebooks_to_update:
            logging.info('Updating "%s" in the remote environment.', remote)

            if not self.dry_run:
                self.workspace_client.import_workspace(local, remote,
                                                       utils.get_language_for_notebook(local),
                                                       'SOURCE', True)

            try:
                for job in remote_streaming_jobs_map[remote]:
                    yield job['settings']['name'], job['job_id']
            except KeyError:
                pass

        if len(notebooks_to_update) == 0:
            logging.info('No notebooks require updating.')
