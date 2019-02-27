#!/usr/bin/env bash
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=${MY_DIR}/..

gcloud dataproc jobs submit pig --cluster=cluster-o2a --region=europe-west3 \
--execute 'fs -rm -r /examples/output-data'

 # Submitting the actual Pig job
gcloud dataproc jobs submit pig --cluster=cluster-o2a --region=europe-west3 \
--file=${BASE_DIR}/examples/pig/id.pig \
--params="INPUT=/examples/test-data.txt,OUTPUT=/examples/output-data/demo/pig-node/"
