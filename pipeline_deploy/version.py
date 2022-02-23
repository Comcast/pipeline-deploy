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

version = '0.3.4'  # pylint: disable=invalid-name # noqa


def print_version_callback(ctx, _, value):
    try:
        import click # pylint: disable=import-outside-toplevel # noqa

        if not value or ctx.resilient_parsing:
            return
        click.echo('Version {}'.format(version))
        ctx.exit()
    except ModuleNotFoundError:
        pass
