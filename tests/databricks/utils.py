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

from typing import Any

import decorator

from databricks_cli.configure.provider import DatabricksConfig, DatabricksConfigProvider, DEFAULT_SECTION, \
    update_and_persist_config

FILE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__),
                 '..',
                 'resources',
                 'files')
)

TEST_PROFILE = 'test-profile'


class MockConfigProvider(DatabricksConfigProvider):
    def __init__(self, config: Any) -> None:
        super().__init__()

        self._config = config

    def get_config(self):
        return self._config


def provide_conf(test):
    def wrapper(test, *args, **kwargs):
        config = DatabricksConfig.from_token('test-host', 'test-token')
        update_and_persist_config(DEFAULT_SECTION, config)
        config = DatabricksConfig.from_token('test-host-2', 'test-token-2')
        update_and_persist_config(TEST_PROFILE, config)
        return test(*args, **kwargs)

    return decorator.decorator(wrapper, test)