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

from pipeline_deploy import utils
from pytest_mock import MockerFixture
from requests import Response
from requests.exceptions import ConnectionError, HTTPError

class TestEatExceptions:
    def test_when_no_exceptions_happen(self):
        @utils.eat_exceptions(exit_on_error=False)
        def test_function(x):
            return x

        assert test_function(1) == 1


    def test_with_a_401_exception(self, mocker: MockerFixture):
        mock_error_and_quit = mocker.patch('pipeline_deploy.utils.error_and_quit')

        @utils.eat_exceptions(exit_on_error=False)
        def test_function():
            response = mocker.MagicMock()
            type(response).status_code = mocker.PropertyMock(return_value=401)

            raise HTTPError(response=response)

        test_function()

        assert mock_error_and_quit.call_count == 1
        assert 'Your authentication information' in mock_error_and_quit.call_args[0][1]


    def test_with_a_404_exception(self, mocker: MockerFixture):
        mock_error_and_quit = mocker.patch('pipeline_deploy.utils.error_and_quit')

        @utils.eat_exceptions(exit_on_error=False)
        def test_function():
            response = mocker.MagicMock()
            type(response).status_code = mocker.PropertyMock(return_value=404)

            raise HTTPError(response=response)

        test_function()

        assert mock_error_and_quit.call_count == 1
        assert 'The requested remote resource could not be located.' == mock_error_and_quit.call_args[0][1]

    def test_with_a_not_found_error(self, mocker: MockerFixture):
        mock_error_and_quit = mocker.patch('pipeline_deploy.utils.error_and_quit')

        @utils.eat_exceptions(exit_on_error=False)
        def test_function():
            response = mocker.MagicMock()
            response.json = mocker.MagicMock(return_value={'error_code': 'RESOURCE_DOES_NOT_EXIST'})

            raise HTTPError(response=response)

        test_function()

        assert mock_error_and_quit.call_count == 1
        assert 'The requested remote resource could not be located.' == mock_error_and_quit.call_args[0][1]

    def test_with_any_other_http_error(self, mocker: MockerFixture):
        mock_error_and_quit = mocker.patch('pipeline_deploy.utils.error_and_quit')

        @utils.eat_exceptions(exit_on_error=False)
        def test_function():
            response = mocker.MagicMock()
            type(response).content = mocker.PropertyMock(return_value='Error Content')

            raise HTTPError(response=response)

        test_function()

        assert mock_error_and_quit.call_count == 1
        assert 'Error Content' == mock_error_and_quit.call_args[0][1]

    def test_with_a_connection_error(self, mocker: MockerFixture):
        mock_error_and_quit = mocker.patch('pipeline_deploy.utils.error_and_quit')

        @utils.eat_exceptions(exit_on_error=False)
        def test_function():
            raise ConnectionError()

        test_function()

        assert mock_error_and_quit.call_count == 1
        assert 'A connection could not be established' in mock_error_and_quit.call_args[0][1]

    def test_with_an_unknown_error(self, mocker: MockerFixture):
        mock_error_and_quit = mocker.patch('pipeline_deploy.utils.error_and_quit')

        @utils.eat_exceptions(exit_on_error=False)
        def test_function():
            raise AttributeError()

        test_function()

        assert mock_error_and_quit.call_count == 1
        assert 'An unexpected error has occurred.' == mock_error_and_quit.call_args[0][1]
