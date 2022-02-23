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

from click.testing import CliRunner

import click

from pipeline_deploy.click_types import ContextObject
from pipeline_deploy.configure import config


class TestDebugOption:
    def test_when_flag_is_set(self):
        # Test that context object debug_mode property changes with --debug flag fed.
        @click.command()
        @click.option('--debug-fed', type=bool)
        @config.debug_option
        def test_debug(debug_fed): # noqa
            ctx = click.get_current_context()
            context_object = ctx.ensure_object(ContextObject)
            assert context_object.debug_mode is debug_fed
            assert logging.root.getEffectiveLevel() == logging.DEBUG

        result = CliRunner().invoke(test_debug, ['--debug', '--debug-fed', True])
        assert result.exit_code == 0

    def test_when_flag_is_not_set(self):
        # Test that context object debug_mode property changes with --debug flag fed.
        @click.command()
        @click.option('--debug-fed', type=bool)
        @config.debug_option
        def test_debug(debug_fed): # noqa
            ctx = click.get_current_context()
            context_object = ctx.ensure_object(ContextObject)
            assert context_object.debug_mode is debug_fed
            assert logging.root.getEffectiveLevel() == logging.INFO

        result = CliRunner().invoke(test_debug, ['--debug-fed', False])
        assert result.exit_code == 0


class TestDryRunOption:
    def test_when_flag_is_set(self):
        # Test that context object debug_mode property changes with --debug flag fed.
        @click.command()
        @config.dry_run_option
        def test_debug(dry_run): # noqa
            assert dry_run

        result = CliRunner().invoke(test_debug, ['--dry-run'])
        assert result.exit_code == 0

    def test_when_flag_is_not_set(self):
        # Test that context object debug_mode property changes with --debug flag fed.
        @click.command()
        @config.dry_run_option
        def test_debug(dry_run): # noqa
            assert not dry_run

        result = CliRunner().invoke(test_debug)
        assert result.exit_code == 0
