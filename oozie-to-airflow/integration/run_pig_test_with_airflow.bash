#!/usr/bin/env bash
MY_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR=${MY_DIR}/..

if [[ ! -f ${BASE_DIR}/examples/pig/configuration.properties ]]; then
    echo
    echo "Please copy ${BASE_DIR}/examples/pig/configuration-template.properties to ${BASE_DIR}/examples/pig/configuration.properties} and update properties to match your case"
    echo
    exit 1
fi

python ${BASE_DIR}/oozie_converter.py -i ${BASE_DIR}/examples/pig/workflow.xml \
  -p ${BASE_DIR}/examples/pig/job.properties \
  -o ${BASE_DIR}/output/pig_test.py -c ${BASE_DIR}/examples/pig/configuration.properties -d test_pig_dag $@

gsutil cp ${BASE_DIR}/scripts/prepare.sh gs://europe-west1-o2a-integratio-f690ede2-bucket/data/

gsutil cp ${BASE_DIR}/output/pig_test.py ${BASE_DIR}/examples/pig/id.pig \
    gs://europe-west1-o2a-integratio-f690ede2-bucket/dags/

gcloud composer environments run o2a-integration --location europe-west1 list_dags

gcloud composer environments run o2a-integration --location europe-west1 trigger_dag -- test_pig_dag
