#!/usr/bin/env bash
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
    gcloud dataproc jobs submit pig --cluster=${CLUSTER} --region=${REGION} --execute 'fs -test -d '${DEL_DIR}
    if [[ $? == "0" ]]; then
        gcloud dataproc jobs submit pig --cluster=${CLUSTER} --region=${REGION} --execute 'fs -rm -r '${DEL_DIR}
    fi
    set -e
done

for MK_DIR in ${MK_DIRS}; do
    gcloud dataproc jobs submit pig --cluster=${CLUSTER} --region=${REGION} --execute 'fs -mkdir -p '${MK_DIR}
done
