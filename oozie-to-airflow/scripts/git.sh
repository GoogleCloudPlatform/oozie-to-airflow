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

set -uo pipefail

#######################################
# Show usage help
# Globals:
#   CMDNAME
# Arguments:
#   None
# Returns:
#   None
#######################################
usage() {
      echo """
Usage: ${CMDNAME} [FLAGS]

Clone a Git repository into HDFS on dataproc cluster.

Flags:

-h, --help
        Shows this help message.

-g, --git-uri
        Address to the git repository

-b, --branch
        Tree in the repository to be downloaded. Defaults to \"master\"

-k, --key-path
        An SSH key that will be used to authorize access to the repository.

-d, --destination-path
        Location on HDFS, where the repository will be uploaded.

-c, --cluster <CLUSTER>
        Cluster used to run the operations on.

-r, --region <REGION>
        GCP Region where the cluster is located.
"""
}

#######################################
# Escapes special characters in a text, so it can be used in pig scripts.
# Globals:
#   None
# Arguments:
#   1 - text
# Returns:
#   None
#######################################
function pig_escape {
    # Escape characters and comments sequence
    printf "%q" "${1}" | sed 's/--/\\\-\\\-/g'
}

#######################################
# Escapes special characters in a text, so it can be used in bash.
# Globals:
#   None
# Arguments:
#   1 - text
# Returns:
#   None
#######################################
function bash_escape {
    # Escape characters
    printf "%q" "${1}"
}
#######################################
# Submit Pig Script to Dataproc Cluster
# Globals:
#   DATAPROC_CLUSTER_NAME
#   GCP_REGION
# Arguments:
#   1 - pig script
# Returns:
#   None
#######################################
function submit_pig {
    echo "Executing pig script \"${1}\""
    gcloud dataproc jobs submit pig \
        --cluster="${DATAPROC_CLUSTER_NAME}" \
        --region="${GCP_REGION}" \
        --execute "${1}"
    # shellcheck disable=SC2181
    if [[ $? -ne 0 ]]
    then
        echo "Could not execute the pig script"
        exit 1
    fi
}

#######################################
# Clone repository
# Globals:
#   GIT_URI
#   GIT_BRANCH_NAME
#   TMP_CHECKOUT_LOCAL_PATH
#   KEY_HDFS_PATH
# Arguments:
#   None
# Returns:
#   None
#######################################
function clone_repository {
    local git_command
    git_command="git clone $(bash_escape "${GIT_URI}") --branch $(bash_escape "${GIT_BRANCH_NAME}") --single-branch ${TMP_CHECKOUT_LOCAL_PATH}"
    if [[ -z "${KEY_HDFS_PATH}" ]]; then
        echo "Cloning repository to local directory"
        submit_pig "sh bash -c \'$(pig_escape "${git_command}")\'"
    else
        echo "Cloning repository to local directory using custom SSH KEY"
        TMP_KEY_LOCAL_PATH="/tmp/o2a-git-key-$(date | md5sum | head -c 7)"
        submit_pig "fs -copyToLocal $(pig_escape "${KEY_HDFS_PATH}") ${TMP_KEY_LOCAL_PATH}"
        local ssh_agent_command="chmod 600 ${TMP_KEY_LOCAL_PATH} && ssh-add ${TMP_KEY_LOCAL_PATH} && ${git_command}"
        submit_pig "sh ssh-agent bash -c \'$(pig_escape "${ssh_agent_command}")\'"
        submit_pig "sh rm -r ${TMP_KEY_LOCAL_PATH}"
    fi
    echo ""
}

#######################################
# Upload repository to HDFS
# Globals:
#   TMP_CHECKOUT_LOCAL_PATH
#   DESTINATION_HDFS_PATH
# Arguments:
#   None
# Returns:
#   None
#######################################
function upload_repository {
    echo "Uploading repository to HDFS"
    submit_pig "fs -copyFromLocal ${TMP_CHECKOUT_LOCAL_PATH} ${DESTINATION_HDFS_PATH}"
    echo ""
}

#######################################
# Clean-up temporary files
# Globals:
#   TMP_CHECKOUT_LOCAL_PATH
# Arguments:
#   None
# Returns:
#   None
######################################## Clean-up
function clean_up {
    echo "Remove local copy of repository"
    submit_pig "sh rm -r ${TMP_CHECKOUT_LOCAL_PATH}"
}

function main() {
    CMDNAME="$(basename -- "$0")"

    GIT_URI=""
    GIT_BRANCH_NAME="master"
    KEY_HDFS_PATH=""
    DESTINATION_HDFS_PATH=""
    DATAPROC_CLUSTER_NAME=""
    GCP_REGION=""

    local _SHORT_OPTIONS="h: g: b: k: d: c: r:"
    local _LONG_OPTIONS="help git-uri: branch: key-path: destination-path: cluster: region:"

    local PARAMS
    PARAMS=$(getopt \
        -o "${_SHORT_OPTIONS}" \
        -l "${_LONG_OPTIONS}" \
        --name "$CMDNAME" -- "$@")

    # shellcheck disable=SC2181
    if [[ $? -ne 0 ]]
    then
        usage
    fi

    eval set -- "${PARAMS}"
    unset PARAMS

    while true
    do
      case "${1}" in
        -h|--help)
          usage;
          exit 0 ;;
        -g|--git-uri)
          export GIT_URI="${2}";
          shift 2 ;;
        -b|--branch)
          export GIT_BRANCH_NAME="${2}";
          shift 2 ;;
        -k|--key-path)
          export KEY_HDFS_PATH="${2}";
          shift 2 ;;
        -d|--destination-path)
          export DESTINATION_HDFS_PATH="${2}";
          shift 2 ;;
        -c|--cluster)
          export DATAPROC_CLUSTER_NAME="${2}";
          shift 2 ;;
        -r|--region)
          export GCP_REGION="${2}";
          shift 2 ;;
        --)
          shift ;
          break ;;
        *)
          echo
          echo "ERROR: Unknown argument ${1}"
          echo
          exit 1
          ;;
      esac
    done

    echo ""

    if [[ -z "${GIT_URI}" ]]; then
        echo "You should provide a git URI."
        exit 1
    fi

    if [[ -z "${DESTINATION_HDFS_PATH}" ]]; then
        echo "You should provide destination URI."
        exit 1
    fi

    if [[ -z "${GCP_REGION}" ]]; then
        echo "You should provide GCP region."
        exit 1
    fi

    if [[ -z "${DATAPROC_CLUSTER_NAME}" ]]; then
        echo "You should provide cluster."
        exit 1
    fi

    TMP_CHECKOUT_LOCAL_PATH="/tmp/o2a-git-repo-$(date | md5sum | head -c 7)"

    clone_repository
    upload_repository
    clean_up
}

main "${@}"
