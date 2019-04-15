#!/usr/bin/env bash
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
set -euo pipefail
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Check for pip
command -v pip >/dev/null 2>&1 || { echo >&2 "pip appears to not be installed. Aborting."; exit 1; }

python - <<EOF
import platform
split_version = [int(x) for x in platform.python_version().split('.')]
if split_version[0] < 3 or split_version[0] == 3 and split_version[1] < 6:
    print("\n\nPython version {} not supported. Must be 3.6 or above.\n\n".format(platform.python_version()))
    sys.exit(1)
EOF


# Apache airflow installation requires an environment variable set to be GPL safe, exporting that:
# TODO: Remove me when 1.10.3 Airflow is released
export SLUGIFY_USES_TEXT_UNIDECODE=yes

# Install required dependencies
pip install -r ${MY_DIR}/../requirements.txt
