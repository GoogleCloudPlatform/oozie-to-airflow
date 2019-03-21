#!/usr/bin/env bash
set -x
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=${MY_DIR}/..
REMOTE_HOME=$1 # TODO There were issues with permission with SCP. Patched this way for now, needs rethinking.

gcloud dataproc jobs submit pig --cluster=cluster-o2a --region=europe-west3 \
--execute 'fs -mkdir -p /examples'

gcloud compute scp ${BASE_DIR}/examples/pig/test-data.txt cluster-o2a-m:${REMOTE_HOME} \
--zone=europe-west3-b

gcloud dataproc jobs submit pig --cluster=cluster-o2a --region=europe-west3 \
--execute 'fs -rm /examples/test-data.txt'

gcloud dataproc jobs submit pig --cluster=cluster-o2a --region=europe-west3 \
--execute 'fs -copyFromLocal '${REMOTE_HOME}'/test-data.txt /examples/test-data.txt'
