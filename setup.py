# -*- coding: utf-8 -*-
# Copyright 2019 Google LLC
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
"""Setup script for Oozie to Airflow migration tool"""
from setuptools import setup

from o2a import NAME

with open("README.md") as fh:
    LONG_DESCRIPTION = fh.read()

with open("requirements.txt") as f:
    REQUIREMENTS = f.read().splitlines()

setup(
    name=NAME,
    version="0.0.13",
    author="Jarek Potiuk, Szymon Przedwojski, Kamil Breguła, Feng Lu, Cameron Moberg",
    author_email="jarek.potiuk@polidea.com, szymon.przedwojski@polidea.com, "
    "kamil.bregula@polidea.com, fenglu@google.com, cjmoberg@google.com",
    description="Oozie To Airflow migration tool",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url="https://github.com/GoogleCloudPlatform/oozie-to-airflow",
    include_package_data=True,
    setup_requires=["pytest-runner"],
    install_requires=REQUIREMENTS,
    tests_require=["pytest"],
    scripts=["bin/o2a", "bin/o2a-validate-workflows"],
    packages=["o2a"],
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
    ],
)
