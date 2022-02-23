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

import click

from pipeline_deploy.click_types import ContextObject, DryRunType


def debug_option(fn):
    def callback(ctx, _, value): #  noqa
        try:
            context_object = ctx.ensure_object(ContextObject)
            context_object.set_debug(value)
        except ImportError:
            pass

    return click.option('--debug',
                        is_flag=True,
                        callback=callback,
                        expose_value=False,
                        help="Debug Mode. Shows detailed logging information.")(fn)


def dry_run_option(fn):
    return click.option('--dry-run',
                        default=False,
                        type=DryRunType(),
                        is_flag=True,
                        help=DryRunType.help)(fn)