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

import difflib
import json
import logging
import os
import re

from fnmatch import fnmatch
from itertools import chain
from os.path import splitext
from pathlib import Path
from tempfile import mkstemp
from time import sleep
from typing import List

import requests

from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceApi

RUNNING_REGEX = re.compile(r'running|pending|terminating', re.IGNORECASE)


def _explode_permissions(entry):
    """
    This function flattens permission objects in order to distribute permission attributes
    out to the individual permission entries.  For example, this code will take the following
    entry:

    ```
    {
        "user_name": "owner@company.com",
        "all_permissions": [
            {
                "permission_level": "CAN_MANAGE",
                "inherited": False,
            },
            {
                "permission_level": "CAN_VIEW",
                "inherited": True,
                "inherited_from_object": "/jobs/"
            }
        ]
    }
    ```

    and return the following entries:

    ```
    [
        {
            "user_name": "owner@company.com",
            "permission_level": "CAN_MANAGE",
            "inherited": False,
        },
        {
            "user_name": "owner@company.com",
            "permission_level": "CAN_VIEW",
            "inherited": True,
            "inherited_from_object": "/jobs/"
        }
    ]
    ```
    """
    all_permissions, rest = (lambda all_permissions, **rest: (all_permissions, rest))(**entry)
    return [{**permission, **rest} for permission in all_permissions]


def enumerate_local_directories(exclude: List[str], include: List[str], path: str):
    for i in Path(path).glob("**/*"):
        if not i.is_dir():
            continue

        result = str(i).strip()

        if os.path.basename(result).startswith('.'):
            continue

        if filter_notebooks(exclude, include, result):
            yield result


def enumerate_local_jobs(exclude: List[str], include: List[str], jobs_dir: str, prefix: str):
    for filename in Path(jobs_dir).glob("**/*.json"):
        path = os.path.join(jobs_dir, filename)

        with open(path, 'r') as src:
            try:
                data = json.load(src)

                job_name = data['name']
                if prefix:
                    job_name = prefix + job_name

                if filter_jobs(exclude, include, job_name):
                    yield data
            except json.decoder.JSONDecodeError as ex:
                raise RuntimeError(
                    f"Invalid JSON has been detected in {filename}") from ex


def enumerate_local_notebooks(exclude: List[str], include: List[str], path: str):
    for i in Path(path).glob("**/*"):
        if not i.is_file():
            continue

        result = str(i).strip()

        if os.path.basename(result).startswith('.'):
            continue

        try:
            file_format = get_language_for_notebook(i)
        except AttributeError as ex:
            print(ex)
            file_format = None

        if file_format and filter_notebooks(exclude, include, result):
            yield result


def enumerate_remote_jobs(client: JobsApi, exclude: List[str], include: List[str], owner: str):
    if owner:
        logging.info('Compiling a list of all remote jobs owned by %s.', owner)
    else:
        logging.info('Compiling a list of all remote jobs.')

    jobs = client.list_jobs()['jobs']

    for job in jobs:
        job_name = job['settings']['name']

        if not filter_jobs(exclude, include, job_name):
            continue

        if owner and get_job_owner(client.client.client, job['job_id']).lower() != owner:
            continue

        yield client.get_job(job['job_id'])


def enumerate_remote_paths(client: WorkspaceApi, exclude: List[str], include: List[str], path: str,
                           is_recursed=False):
    if not is_recursed:
        logging.info('Compiling a list of all remote paths within %s.', path)

    for obj in client.list_objects(path):
        if filter_notebooks(exclude, include, obj.path):
            yield obj

        if obj.is_dir:
            yield from enumerate_remote_paths(client, exclude, include, obj.path, True)


def filter_jobs(exclude: List[str], include: List[str], job_name: str):
    """This is distinct from `filter_notebooks` to allow for additional logic later
    on without too much additional re-work."""

    if exclude:
        for excl in exclude:
            if fnmatch(job_name, excl):
                return False

    if include:
        for incl in include:
            if fnmatch(job_name, incl):
                return True

        return False

    return True


def filter_notebooks(exclude: List[str], include: List[str], path: str):
    """This is distinct from `filter_jobs` to allow for additional logic later
    on without too much additional re-work."""

    if exclude:
        for excl in exclude:
            if fnmatch(path, excl):
                return False

    if include:
        for incl in include:
            if fnmatch(path, incl):
                return True

        return False

    return True


def get_job_owner(client: ApiClient, job_id: str):
    existing_permissions = client.perform_query('GET', f'/permissions/jobs/{job_id}')

    # Explode out the list of permissions for each user/group/service.
    acl = list(chain.from_iterable(map(_explode_permissions,
                                       existing_permissions['access_control_list'])))

    for entry in acl:
        if entry['permission_level'] == 'IS_OWNER':
            try:
                return entry['user_name']
            except KeyError:
                pass

            try:
                return entry['group_name']
            except KeyError:
                pass

            try:
                return entry['service_principal_name']
            except KeyError:
                pass

    raise AttributeError(f'Owner for job {job_id} could not be found.')


def get_local_notebooks_map(exclude: List[str], include: List[str], local_path: str,
                            remote_path: str):
    local_notebooks = enumerate_local_notebooks(exclude, include, local_path)

    return {splitext(notebook)[0].replace(local_path, remote_path): notebook
            for notebook in local_notebooks}


def get_language_for_notebook(path: str):
    extension = splitext(path)[1].lower()

    if extension.endswith('py'):
        return 'PYTHON'

    if extension.endswith('sql'):
        return 'SQL'

    if extension.endswith('scala'):
        return 'SCALA'

    if extension.endswith('r'):
        return 'R'

    raise AttributeError(f'Unknown extension {extension.upper()}.')


def get_notebook_path(job: dict):
    return job['settings']['notebook_task']['notebook_path']


def is_job_running(client: RunsApi, job_id: str, job_name: str):
    try:
        runs = client.list_runs(job_id, None, None, 0, 100)['runs']
        states = map(lambda x: x['state']['life_cycle_state'], runs)

        filtered = [*filter(RUNNING_REGEX.match, states)]

        return len(filtered) > 0
    except requests.exceptions.HTTPError as ex:
        if ex.response.json()['error_code'] == 'RESOURCE_DOES_NOT_EXIST':
            logging.debug('Job ID %s does not exist for %s.', job_id, job_name)
            return False

        raise
    except KeyError:
        return False


def is_notebook_updated(client: WorkspaceApi, diff: bool, local: str, remote: str):
    (_, temp_notebook) = mkstemp()

    client.export_workspace(remote, temp_notebook, 'SOURCE', True)

    with open(local, 'r') as local_notebook_stream:
        local_notebook_lines_raw = local_notebook_stream.read().splitlines(False)
        local_notebook_lines = remove_blank_lines(local_notebook_lines_raw)
        local_notebook_lines = remove_whitespace(local_notebook_lines)

        with open(temp_notebook, 'r') as remote_notebook_stream:
            remote_notebook_lines_raw = remote_notebook_stream.read().splitlines(False)
            remote_notebook_lines = remove_blank_lines(remote_notebook_lines_raw)
            remote_notebook_lines = remove_whitespace(remote_notebook_lines)

            delta = [*difflib.unified_diff(remote_notebook_lines,
                                           local_notebook_lines,
                                           fromfile=remote, tofile=local)]

            has_changes = len(delta) > 0

            if diff and has_changes:
                logging.info('Changes detected for "%s" between the remote and local environment',
                             remote)

                delta = difflib.unified_diff(remote_notebook_lines_raw,
                                             local_notebook_lines_raw,
                                             fromfile=remote, tofile=local)
                for line in delta:
                    logging.info(line)

    # Clean up the temporarily created file.
    os.remove(temp_notebook)

    return has_changes


def is_streaming_job(job):
    try:
        if job["max_retries"] != -1:
            return False
    except KeyError:
        return False

    try:
        return job["schedule"] is None
    except KeyError:
        return True


def is_streaming_notebook(jobs: List, notebooks_dir: str, remote_path: str):
    """Find all notebooks that are part of streaming jobs."""

    # Handle the potential inclusion of a variety of trailing characters
    notebooks_dir_clean = notebooks_dir.strip().rstrip('/\\')
    remote_path_clean = remote_path.strip().rstrip('/')

    def fn(local: str):
        for job in jobs:
            remote = os.path.splitext(local)[0] \
                            .replace(notebooks_dir_clean, remote_path_clean)

            try:
                if job['settings']['notebook_task']['notebook_path'] == remote:
                    return True
            except KeyError:
                pass

        return False

    return fn


def print_job_diff(job_name, local_job, remote_job):
    local_job_json = json.dumps(local_job, sort_keys=True, indent=4)
    remote_job_json = json.dumps(remote_job, sort_keys=True, indent=4)

    delta = difflib.unified_diff(remote_job_json.splitlines(keepends=False),
                                 local_job_json.splitlines(keepends=False),
                                 tofile=f'local/{job_name}',
                                 fromfile=f'remote/{job_name}')

    for diff in delta:
        logging.info(diff)


def remove_blank_lines(lines: List[str]):
    for line in lines:
        if line.rstrip():
            yield line


def remove_whitespace(lines: List[str]):
    return list([line.rstrip() for line in lines])


def restart_job(jobs_client: JobsApi, job_id: str, job_name: str, runs_client: RunsApi):
    if is_job_running(runs_client, job_id, job_name):
        stop_job(runs_client, job_id, job_name)

        logging.info('Waiting for all active runs of %s to stop.', job_name)
        while is_job_running(runs_client, job_id, job_name):
            sleep(5)

        logging.info('All active runs of %s have been stopped.', job_name)

    start_job(jobs_client, job_id, job_name)


def set_job_owner(client: ApiClient, job_id: str, owner: str):
    existing_permissions = client.perform_query('GET', f'/permissions/jobs/{job_id}')

    # Explode out the list of permissions for each user/group/service.
    acl = list(chain.from_iterable(map(_explode_permissions,
                                       existing_permissions['access_control_list'])))

    # Check for existing owner of the job.
    for entry in acl:
        try:
            if entry['user_name'] == owner and entry['permission_level'] == 'IS_OWNER':
                return
        except KeyError:
            pass
    # Remove inherited permissions from the list since those can't be updated/replaced.
    acl = filter(lambda entry: not entry['inherited'], acl)
    # Remove the existing owner from the permissions.
    acl = filter(lambda entry: entry['permission_level'] != 'IS_OWNER', acl)
    # Add the owner permission to the request.
    acl = [
        *((lambda inherited, **rest: rest)(**item) for item in acl),
        {"user_name": owner, "permission_level": "IS_OWNER"}
    ]
    # Submit the request.
    client.perform_query('PUT',
                         f'/permissions/jobs/{job_id}',
                         {"access_control_list": acl})


def set_job_permissions(client: ApiClient, group: str, job_id: str):
    """This is a temporary process until Databricks implements permissions API calls in code."""

    existing_permissions = client.perform_query('GET', f'/permissions/jobs/{job_id}')

    # Explode out the list of permissions for each user/group/service.
    acl = list(chain.from_iterable(map(_explode_permissions,
                                       existing_permissions['access_control_list'])))
    # Check for existing permissions in the job.
    for entry in acl:
        try:
            if entry['group_name'] == group and entry['permission_level'] == 'CAN_MANAGE':
                return
        except KeyError:
            pass
    # Remove inherited permissions from the list since those can't be updated/replaced.
    acl = filter(lambda entry: not entry['inherited'], acl)
    # Add the manager permission to the request.
    acl = [
        *((lambda inherited, **rest: rest)(**item) for item in acl),
        {"group_name": group, "permission_level": "CAN_MANAGE"}
    ]
    # Submit the request.
    client.perform_query('PUT',
                         f'/permissions/jobs/{job_id}',
                         {"access_control_list": acl})


def start_job(client: JobsApi, job_id: str, job_name: str):
    logging.info('Starting a new run for %s...', job_name)
    client.run_now(job_id, None, None, None, None)


def stop_job(client: RunsApi, job_id, job_name):
    try:
        runs = client.list_runs(job_id, None, None, 0, 100)['runs']
    except requests.exceptions.HTTPError as ex:
        if ex.response.json()['error_code'] == 'RESOURCE_DOES_NOT_EXIST':
            logging.debug('Job ID %s does not exist for %s.', job_id, job_name)
            return

        raise

    for run in runs:
        if re.match(r'running|pending', run['state']['life_cycle_state'], re.IGNORECASE):
            run_id = str(run['run_id'])

            try:
                logging.info('Halting run %s for %s.', run_id, job_name)
                client.cancel_run(run_id)
            except requests.exceptions.HTTPError as ex:
                if ex.response.json()['error_code'] == 'RESOURCE_DOES_NOT_EXIST':
                    logging.debug(
                        'Run ID %s does not exist for %s.', run_id, job_name)

                raise
