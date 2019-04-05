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

set -x
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOCAL_BASE_DIR=${MY_DIR}/..
LOCAL_EXAMPLE_DIR=examples
LOCAL_APP_DIR=pig

HADOOP_USER=pig
TEMP_APPLICATION_FOLDER=/tmp/tmp_pig_dir
EXAMPLE_DIR=examples
TEST_APP=test_pig_node

CLUSTER_MASTER=cluster-o2a-m
CLUSTER_NAME=cluster-o2a
REGION=europe-west3
ZONE=europe-west3-b


gcloud compute ssh ${CLUSTER_MASTER} --command "rm -rf ${TEMP_APPLICATION_FOLDER}" --zone=${ZONE}
gcloud compute ssh ${CLUSTER_MASTER} --command "mkdir -p ${TEMP_APPLICATION_FOLDER}" --zone=${ZONE}
gcloud compute scp --recurse ${LOCAL_BASE_DIR}/${LOCAL_EXAMPLE_DIR}/${LOCAL_APP_DIR} ${CLUSTER_MASTER}:${TEMP_APPLICATION_FOLDER}/${TEST_APP} --zone=${ZONE}

gcloud dataproc jobs submit pig --cluster=${CLUSTER_NAME} --region=${REGION} \
    --execute "fs -rm -r -f /user/${HADOOP_USER}/${EXAMPLE_DIR}"

gcloud dataproc jobs submit pig --cluster=${CLUSTER_NAME} --region=${REGION} \
    --execute "fs -mkdir -p /user/${HADOOP_USER}/${EXAMPLE_DIR}"

gcloud dataproc jobs submit pig --cluster=${CLUSTER_NAME} --region=${REGION} \
    --execute "fs -copyFromLocal ${TEMP_APPLICATION_FOLDER}/${TEST_APP} /user/${HADOOP_USER}/${EXAMPLE_DIR}/"
    # Note! The target folder will be /user/<USER>/examples/<TEST_APP>/
