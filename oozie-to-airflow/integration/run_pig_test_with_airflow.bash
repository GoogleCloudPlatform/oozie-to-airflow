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

LOCAL_APPLICATION_DIR=${BASE_DIR}/examples/pig

HADOOP_USER=pig
EXAMPLE_DIR=examples/pig
CLUSTER_MASTER=cluster-o2a-m
CLUSTER_NAME=cluster-o2a

COMPOSER_BUCKET=gs://europe-west1-o2a-integratio-f690ede2-bucket
COMPOSER_NAME=o2a-integration
COMPOSER_LOCATION=europe-west1

PIG_DAG_NAME=test_pig_dag

if [[ ! -f ${LOCAL_APPLICATION_DIR}/configuration.properties ]]; then
    echo
    echo "Please copy ${LOCAL_APPLICATION_DIR}/configuration-template.properties to ${LOCAL_APPLICATION_DIR}/configuration.properties and update properties to match your case"
    echo
    exit 1
fi

python ${BASE_DIR}/o2a.py -i ${BASE_DIR}/examples/pig -o ${BASE_DIR}/output/pig_test -u ${HADOOP_USER} -d ${PIG_DAG_NAME} $@

gsutil cp ${BASE_DIR}/scripts/prepare.sh ${COMPOSER_BUCKET}/data/
gsutil cp ${BASE_DIR}/output/pig_test/* ${COMPOSER_BUCKET}/dags/

gcloud composer environments run ${COMPOSER_NAME} --location ${COMPOSER_LOCATION} list_dags
gcloud composer environments run ${COMPOSER_NAME} --location ${COMPOSER_LOCATION} trigger_dag -- test_pig_dag
