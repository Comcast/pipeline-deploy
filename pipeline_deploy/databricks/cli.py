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

from itertools import groupby
from typing import Tuple

import click

from databricks_cli.sdk.api_client import ApiClient
from pipeline_deploy import click_types as types
from pipeline_deploy.configure.config import debug_option, dry_run_option
from pipeline_deploy.databricks.configure.config import profile_option, provide_api_client
from pipeline_deploy.databricks.controllers import JobsController, NotebooksController
from pipeline_deploy.databricks import utils
from pipeline_deploy.utils import CONTEXT_SETTINGS, eat_exceptions


@click.command(context_settings=CONTEXT_SETTINGS,
               short_help='Deploy notebooks and jobs to Databricks.',
               no_args_is_help=True)
@click.option('--diff', is_flag=True, help=types.DiffClickType.help)
@click.option('--exclude-jobs', multiple=True, help=types.ExcludeJobsClickType.help)
@click.option('--exclude-notebooks', multiple=True, help=types.ExcludeNotebooksClickType.help)
@click.option('--group-name', default=None, help=types.GroupNameClickType.help)
@click.option('--include-jobs', multiple=True, help=types.IncludeJobsClickType.help)
@click.option('--include-notebooks', multiple=True, help=types.IncludeNotebooksClickType.help)
@click.option('--jobs-dir',
              required=True,
              type=click.Path(exists=True, resolve_path=True, dir_okay=True),
              help=types.JobsDirClickType.help)
@click.option('--notebooks-dir',
              required=True,
              type=click.Path(exists=True, resolve_path=True, dir_okay=True),
              help=types.NotebooksDirClickType.help)
@click.option('--remote-path', default='/', help=types.RemotePathClickType.help)
@click.option('--owner', help=types.OwnerClickType.help)
@click.option('--prefix', help=types.PrefixClickType.help)
@click.option('--skip-restart', is_flag=True, help=types.SkipRestartClickType.help)
@debug_option
@dry_run_option
@profile_option
@provide_api_client
@eat_exceptions
def databricks_cli(api_client: ApiClient, diff: bool, dry_run: bool, exclude_jobs: Tuple[str],
                   exclude_notebooks: Tuple[str], group_name: str, include_jobs: Tuple[str],
                   include_notebooks: Tuple[str], jobs_dir: str, notebooks_dir: str, owner: str,
                   prefix: str, remote_path: str, skip_restart: bool):
    if isinstance(exclude_jobs, tuple):
        exclude_jobs_list = list(exclude_jobs)
    elif not isinstance(exclude_jobs, list):
        exclude_jobs_list = [exclude_jobs]
    if isinstance(exclude_notebooks, tuple):
        exclude_notebooks_list = list(exclude_notebooks)
    elif not isinstance(exclude_notebooks, list):
        exclude_notebooks_list = [exclude_notebooks]
    if isinstance(include_jobs, tuple):
        include_jobs_list = list(include_jobs)
    elif not isinstance(include_jobs, list):
        include_jobs_list = [include_jobs]
    if isinstance(include_notebooks, tuple):
        include_notebooks_list = list(include_notebooks)
    elif not isinstance(include_notebooks, list):
        include_notebooks_list = [include_notebooks]

    logging.info('Executing databricks deployment.')
    logging.debug('Parameters: %s',
                  dict(diff=diff, dry_run=dry_run, exclude_jobs=exclude_jobs_list,
                       exclude_notebooks=exclude_notebooks_list, group_name=group_name,
                       include_jobs=include_jobs_list, include_notebooks=include_notebooks_list,
                       jobs_dir=jobs_dir, notebooks_dir=notebooks_dir, owner=owner,
                       prefix=prefix, remote_path=remote_path, skip_restart=skip_restart))

    jobs_controller = JobsController(api_client, diff, dry_run, group_name)
    notebooks_controller = NotebooksController(api_client, diff, dry_run, notebooks_dir,
                                               remote_path)

    local_directories = utils.enumerate_local_directories(exclude_notebooks_list,
                                                          include_notebooks_list, notebooks_dir)
    local_notebooks_map = utils.get_local_notebooks_map(exclude_notebooks_list,
                                                        include_notebooks_list, notebooks_dir,
                                                        remote_path)

    remote_paths = [*utils.enumerate_remote_paths(notebooks_controller.workspace_client,
                                                  exclude_notebooks_list, include_notebooks_list,
                                                  remote_path)]
    remote_directories = {str(x.path) for x in remote_paths if x.is_dir}
    remote_notebooks = {str(x.path) for x in remote_paths if not x.is_dir}

    local_jobs = utils.enumerate_local_jobs(exclude_jobs_list, include_jobs_list, jobs_dir, prefix)
    local_jobs_map = {job['name']: job for job in local_jobs}

    remote_jobs = utils.enumerate_remote_jobs(notebooks_controller.jobs_client, exclude_jobs_list,
                                              include_jobs_list, owner)
    remote_jobs_map = {job['settings']['name']: job for job in remote_jobs}

    remote_streaming_jobs = filter(lambda x: utils.is_streaming_job(x['settings']), remote_jobs)
    remote_streaming_jobs_map = {k: [*g] for k, g in groupby(remote_streaming_jobs,
                                                             utils.get_notebook_path)}

    # Update existing notebooks.
    logging.info('Checking for notebooks that require updating.')
    notebooks_to_restart = notebooks_controller.update(local_notebooks_map,
                                                       remote_streaming_jobs_map,
                                                       remote_notebooks)
    notebooks_to_restart_map = {job_id: job_name for job_name, job_id in notebooks_to_restart}
    # Update existing jobs.
    logging.info('Checking for jobs that require updating.')
    jobs_to_restart = jobs_controller.update(local_jobs_map, remote_jobs_map)
    jobs_to_restart_map = {job_id: job_name for job_name, job_id in jobs_to_restart}
    # Deploy new notebooks.
    logging.info('Checking for notebooks that require creation.')
    notebooks_controller.create(local_notebooks_map, remote_notebooks)
    # Create new jobs.
    logging.info('Checking for jobs that require creation.')
    jobs_to_start = jobs_controller.create(local_jobs_map, remote_jobs_map, owner)
    jobs_to_start_map = {job_id: job_name for job_name, job_id in jobs_to_start}
    # Delete old jobs.
    logging.info('Checking for jobs that require deletion.')
    jobs_controller.delete(local_jobs_map, remote_jobs_map)
    # Delete existing notebooks.
    logging.info('Checking for notebooks and directories that require deletion.')
    notebooks_controller.delete(local_directories, local_notebooks_map, remote_directories,
                                remote_notebooks)
    # Restart all necessary jobs.
    if not skip_restart:
        jobs_controller.restart({
            **jobs_to_restart_map,
            **notebooks_to_restart_map,
            **jobs_to_start_map
        })
