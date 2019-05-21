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

while getopts ":c:r:d:m:" OPT; do
     case ${OPT} in
        c) CLUSTER=$OPTARG;;
        r) REGION=$OPTARG;;
        d) DEL_DIRS=$OPTARG;;
        m) MK_DIRS=$OPTARG;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument." >&2
            exit 1
            ;;
     esac
done

for DEL_DIR in ${DEL_DIRS}; do
    set +e
    gcloud dataproc jobs submit pig --cluster="${CLUSTER}" --region="${REGION}" --execute "fs -test -d \"${DEL_DIR}\'"
    # shellcheck disable=SC2181
    if [[ $? == "0" ]]; then
        gcloud dataproc jobs submit pig --cluster="${CLUSTER}" --region="${REGION}" --execute "fs -rm -r \"${DEL_DIR}\""
    fi
    set -e
done

for MK_DIR in ${MK_DIRS}; do
    gcloud dataproc jobs submit pig --cluster="${CLUSTER}" --region="${REGION}" --execute "fs -mkdir -p \"${MK_DIR}\""
done
