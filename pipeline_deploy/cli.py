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

from pipeline_deploy.databricks.cli import databricks_cli
from pipeline_deploy.utils import CONTEXT_SETTINGS
from pipeline_deploy.version import print_version_callback, version


logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")


@click.group(context_settings=CONTEXT_SETTINGS)
@click.option('--version', '-v', is_flag=True, callback=print_version_callback,
              expose_value=False, is_eager=True, help=version)
def cli():
    pass


cli.add_command(databricks_cli, name="databricks")

if __name__ == "__main__":
    cli()