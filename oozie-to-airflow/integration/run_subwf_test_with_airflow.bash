#!/usr/bin/env bash
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=${MY_DIR}/..

python ${BASE_DIR}/oozie_converter.py -i ${BASE_DIR}/examples/subwf/workflow.xml \
  -p ${BASE_DIR}/examples/subwf/job.properties \
  -o ${BASE_DIR}/output/subwf_test.py -d test_subwf_dag $@

gsutil cp ${BASE_DIR}/output/subwf_test.py ${BASE_DIR}/output/subdag_pig.py ${BASE_DIR}/examples/pig/id.pig \
    gs://europe-west1-o2a-integratio-f690ede2-bucket/dags/

gcloud composer environments run o2a-integration --location europe-west1 list_dags

gcloud composer environments run o2a-integration --location europe-west1 trigger_dag -- test_subwf_dag
