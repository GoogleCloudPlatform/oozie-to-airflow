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

MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=${MY_DIR}/..

python ${BASE_DIR}/o2a.py -i ${BASE_DIR}/examples/subwf/workflow.xml \
  -p ${BASE_DIR}/examples/subwf/job.properties \
  -o ${BASE_DIR}/output/subwf_test.py -d test_subwf_dag $@

gsutil cp ${BASE_DIR}/output/subwf_test.py ${BASE_DIR}/output/subdag_pig.py ${BASE_DIR}/examples/pig/id.pig \
    gs://europe-west1-o2a-integratio-f690ede2-bucket/dags/

gcloud composer environments run o2a-integration --location europe-west1 list_dags

gcloud composer environments run o2a-integration --location europe-west1 trigger_dag -- test_subwf_dag
