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
import sys
import traceback

from json.decoder import JSONDecodeError

import click
import requests
import six

from pipeline_deploy.click_types import ContextObject

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])


def eat_exceptions(fn=None, exit_on_error=True):
    def _decorate(function):
        @six.wraps(function)
        def decorator(*args, **kwargs):
            try:
                return function(*args, **kwargs)
            except RuntimeError as ex:
                error_and_quit(ex, exit_on_error=exit_on_error)
            except requests.exceptions.ConnectionError as ex:
                error_and_quit(ex,
                               'A connection could not be established with the remote server.',
                               exit_on_error)
            except requests.exceptions.HTTPError as ex:
                try:
                    error_code = ex.response.json()['error_code']
                except JSONDecodeError:
                    error_code = None

                if ex.response.status_code == 404 or error_code == 'RESOURCE_DOES_NOT_EXIST':
                    error_and_quit(ex,
                                   'The requested remote resource could not be located.',
                                   exit_on_error)
                elif ex.response.status_code == 401:
                    error_and_quit(ex,
                                   'Your authentication information may be incorrect. Please '
                                   'check the CLI instructions for your platform.',
                                   exit_on_error)
                else:
                    error_and_quit(ex, ex.response.content, exit_on_error)
            except Exception as ex:  # pylint: disable=broad-except # noqa
                error_and_quit(ex, 'An unexpected error has occurred.', exit_on_error)

        decorator.__doc__ = function.__doc__
        return decorator

    if fn:
        return _decorate(fn)

    return _decorate


def error_and_quit(error: BaseException, message: str = None, exit_on_error: bool = True):
    ctx = click.get_current_context()
    context_object = ctx.ensure_object(ContextObject)

    if context_object.debug_mode:
        traceback.print_exc()
    elif message:
        logging.error(message)
    else:
        logging.error(str(error))

    if exit_on_error:
        sys.exit(1)
