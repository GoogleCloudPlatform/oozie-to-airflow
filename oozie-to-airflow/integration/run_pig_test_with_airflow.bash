#!/usr/bin/env bash
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

python ${BASE_DIR}/oozie_converter.py -i ${BASE_DIR}/examples/pig -o ${BASE_DIR}/output/pig_test -u ${HADOOP_USER} -d ${PIG_DAG_NAME} $@

gsutil cp ${BASE_DIR}/scripts/prepare.sh ${COMPOSER_BUCKET}/data/
gsutil cp ${BASE_DIR}/output/pig_test/* ${COMPOSER_BUCKET}/dags/

gcloud composer environments run ${COMPOSER_NAME} --location ${COMPOSER_LOCATION} list_dags
gcloud composer environments run ${COMPOSER_NAME} --location ${COMPOSER_LOCATION} trigger_dag -- test_pig_dag
