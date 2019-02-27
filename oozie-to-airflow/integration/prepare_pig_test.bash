#!/usr/bin/env bash
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=${MY_DIR}/..

gcloud compute scp  ${BASE_DIR}/examples/pig/test-data.txt cluster-o2a-m:/tmp/test-data.txt \
--zone=europe-west3-b

gcloud dataproc jobs submit pig --cluster=cluster-o2a --region=europe-west3 \
--execute 'fs -rm /examples/test-data.txt'

gcloud dataproc jobs submit pig --cluster=cluster-o2a --region=europe-west3 \
--execute 'fs -copyFromLocal /tmp/test-data.txt /examples/test-data.txt'
