# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -xe
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
O2A_DIR="$( cd "${MY_DIR}/.." && pwd )"

# add user's pip binary path to PATH
export PATH="${HOME}/.local/bin:${PATH}"

if [[ ! -z "${KOKORO_BUILD_ID}" ]]; then # export vars only for Kokoro job
  # Setup service account credentials
  export GOOGLE_APPLICATION_CREDENTIALS=${KOKORO_GFILE_DIR}/kokoro/service-account-key.json

  # Setup project id
  export PROJECT_ID=$(cat "${KOKORO_GFILE_DIR}/kokoro/project-id.txt")
  export COMPOSER_TESTS_PROJECT_ID=PROJECT_ID
fi

# prepare python environment
pyenv install --skip-existing 3.6.15
pyenv install --skip-existing 3.7.10
pyenv install --skip-existing 3.8.10
pyenv install --skip-existing 3.9.5
pyenv install --skip-existing 3.10.9
pyenv global 3.6.15 3.7.10 3.8.10 3.9.5 3.10.9
python -m pip install -r ${O2A_DIR}/requirements.txt

echo -e "******************** Running unit tests... ********************\n"
"${O2A_DIR}/bin/o2a-run-all-unit-tests"
echo -e "******************** Unit tests complete.  ********************\n"
echo -e "******************** Running Conversion tests... ********************\n"
"${O2A_DIR}/bin/o2a-run-all-conversions" py
rm -rf output
"${O2A_DIR}/bin/o2a-run-all-conversions" dot
echo -e "******************** Conversion tests complete.  ********************\n"
