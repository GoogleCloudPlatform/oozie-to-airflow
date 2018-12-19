#!/bin/bash
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

# Check for pip
command -v pip >/dev/null 2>&1 || { echo >&2 "pip appears to not be installed. Aborting."; exit 1; }

# Check for python3
command -v python3 >/dev/null 2>&1 || { echo >&2 "python 3 does not appear to be installed. Aborting."; exit 1}

# Apache airflow installation requires an environment variable set to be GPL safe, exporting that:
export SLUGIFY_USES_TEXT_UNIDECODE=yes

# Install required dependencies
pip install --user apache-airflow
pip install --user paramiko
pip install --user sshtunnel
pip install --user jinja2

