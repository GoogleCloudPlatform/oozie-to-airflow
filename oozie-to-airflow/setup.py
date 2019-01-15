# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

from setuptools import find_packages
from setuptools import setup

# Required for apache-airflow package to not use GPL code
os.environ['SLUGIFY_USES_TEXT_UNIDECODE'] = 'yes'

# Package meta-data
NAME = 'oozie-to-airflow'
DESCRIPTION = 'A tool to convert Apache Oozie workflows to Apache Airflow workflows'
URL = 'https://github.com/GoogleCloudPlatform/cloud-composer'
AUTHOR = 'Google Cloud Composer'
REQUIRES_PYTHON = '>=3.5.0'
VERSION = '0.1'

REQUIRED = [
    'apache-airflow>=1.10.0',
    'paramiko>=2.0.0',
    'sshtunnel>=0.1.3',
    'jinja2>=2.9',
    'pendulum==1.4.4',
    # For unit tests
    'parameterized',
]

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    author=AUTHOR,
    python_requires=REQUIRES_PYTHON,
    url=URL,
    packages=find_packages(exclude=('tests',)),
    install_requires=REQUIRED,
    include_package_data=True,
    license='Apache',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Development Status :: 4 - Beta',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
)
