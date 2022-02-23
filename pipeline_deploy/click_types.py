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

import click

from click import ParamType, UsageError


class ContextObject:
    def __init__(self):
        self._profile = None
        self._debug = False

    def set_debug(self, debug=False):
        self._debug = debug

        if self._debug:
            logging.root.setLevel(logging.DEBUG)
        else:
            logging.root.setLevel(logging.INFO)
            return

        # pylint: disable=import-outside-toplevel # noqa
        try:
            import http.client as http_client
        except ImportError:
            # Python 2
            import httplib as http_client

        click.echo("HTTP debugging enabled")
        http_client.HTTPConnection.debuglevel = 1

    @property
    def debug_mode(self):
        return self._debug

    def set_profile(self, profile):
        if self._profile is not None:
            raise UsageError('--profile can only be provided once. '
                             'The profiles [{}, {}] were provided.'.format(self._profile, profile))
        self._profile = profile

    def get_profile(self):
        return self._profile


class DiffClickType(ParamType):
    name = 'DIFF'
    help = 'Display the difference between two files.'


class DryRunType(ParamType):
    name = 'DRY_RUN'
    help = "Dry Run Mode. Shows full changes but doesn't actually execute."


class ExcludeJobsClickType(ParamType):
    name = 'EXCLUDE_JOBS'
    help = 'A wildcard filter of jobs to exclude by job name.'


class ExcludeNotebooksClickType(ParamType):
    name = 'EXCLUDE_NOTEBOOKS'
    help = 'A wildcard filter of notebooks to exclude by notebook path.'


class GroupNameClickType(ParamType):
    name = 'GROUP_NAME'
    help = 'The name of group to be given management permissions for all created tasks.'


class IncludeJobsClickType(ParamType):
    name = 'INCLUDE_JOBS'
    help = 'A wildcard filter of jobs to include by job name.'


class IncludeNotebooksClickType(ParamType):
    name = 'INCLUDE_NOTEBOOKS'
    help = 'A wildcard filter of notebooks to include by notebook path.'


class JobsDirClickType(ParamType):
    name = 'JOBS_DIR'
    help = 'Directory containing JSON configuration files.'


class NotebooksDirClickType(ParamType):
    name = 'NOTEBOOKS_DIR'
    help = 'Directory containing notebook files.'


class OwnerClickType(ParamType):
    name = 'OWNER'
    help = 'The e-mail address of the owner for jobs to manage.'


class PrefixClickType(ParamType):
    name = 'PREFIX'
    help = 'A prefix to add to all job names.'


class RemotePathClickType(ParamType):
    name = 'REMOTE_PATH'
    help = 'The target path where all notebooks will be imported. Default: /'


class SkipRestartClickType(ParamType):
    name = 'SKIP_RESTART'
    help = 'Skip streaming job restarts. If a dependency has changed for a streaming job, the ' \
           'job will not be restarted.'