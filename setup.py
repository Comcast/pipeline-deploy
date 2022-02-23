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

import io
import os
import sys

from setuptools import setup, find_packages

module_name = 'pipeline_deploy.version' # pylint: disable=invalid-name # noqa
module_path = os.path.join('pipeline_deploy', 'version.py')

if sys.version_info[0] == 3 and sys.version_info[1] >= 5:
    import importlib.util
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    version = module.version
elif sys.version_info[0] == 3 and sys.version_info[1] < 5:
    import importlib.machinery
    loader = importlib.machinery.SourceFileLoader(module_name, module_path)
    module = loader.load_module() # pylint: disable=deprecated-method,no-value-for-parameter # noqa
    version = module.version

setup(
    name='pipeline-deploy',
    version=version,
    packages=find_packages(exclude=['tests', 'tests.*']),
    install_requires=[
        'click>=6.7',
        'databricks-cli>=0.14.3',
    ],
    entry_points='''
        [console_scripts]
        pipeline-deploy=pipeline_deploy.cli:cli
    ''',
    zip_safe=False,
    author='Jordan Yaker',
    author_email='jordan_yaker@comcast.com',
    description='A deployment tool for ETL data pipelines.',
    long_description=io.open('README.md', encoding='utf-8').read(),
    license='Apache License 2.0',
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: Apache Software License',
    ],
    keywords='etl data pipeline databricks deploy cli devops',
    url='https://github.com/comcast/pipeline-deploy'
)