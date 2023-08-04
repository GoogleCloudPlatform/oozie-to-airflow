# -*- coding: utf-8 -*-
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
"""All FS EL functions"""

import os
import subprocess

from airflow import AirflowException


def _pig_job_executor(cmd: str):
    """
    Run command on Dataproc cluster.
    """
    cluster_name = os.environ.get("DATAPROC_CLUSTER")
    region = os.environ.get("DATAPROC_REGION")

    # The delimiter is used to split the output
    delimiter = "c822c1b63853ed273b89687ac505f9fa"  # md5 hash of Google

    dataproc_cmd = [
        "gcloud",
        "dataproc",
        "jobs",
        "submit",
        "pig",
        f"--execute=sh echo {delimiter};{cmd};sh echo {delimiter}",
        f"--cluster={cluster_name}",
        f"--region={region}",
    ]

    print("Executing for output: '{}'".format(" ".join(dataproc_cmd)))
    process = subprocess.Popen(args=cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = process.communicate()
    retcode = process.poll()

    if retcode:
        print("Error when executing '{}'".format(" ".join(cmd)))
        print("Stdout: {}".format(output.decode("utf-8")))
        print("Stderr: {}".format(err.decode("utf-8")))
        raise AirflowException(
            "Retcode {} on {} with stdout: {}, stderr: {}".format(
                retcode, " ".join(cmd), output.decode("utf-8"), err.decode("utf-8")
            )
        )

    _, out, _ = err.decode("utf-8").split(delimiter, 3)
    return out


def exists(path: str) -> bool:
    """
    It returns true or false depending if the specified path URI exists or not.
    """
    query = f"fs -getfattr -d {path}"
    try:
        _pig_job_executor(query)
        return True
    except AirflowException:
        return False


def is_dir(path: str) -> bool:
    """
    It returns true if the specified path URI exists and it is a directory,
    otherwise it returns false.

    This may not behave properly.
    """
    query = f"fs -getfattr -d {path}'"
    is_file = "." not in path
    try:
        _pig_job_executor(query)
        return is_file
    except AirflowException:
        return False


def dir_size(path: str):
    """
    It returns the size in bytes of all the files in the specified path. If the
    path is not a directory, or if it does not exist it returns -1. It does not
    work recursively, only computes the size of the files under the specified path.
    """
    query = f"fs -stat '%b' {path}"
    try:
        result = _pig_job_executor(query)
    except AirflowException:
        return -1

    value = result.strip()
    return float(value)


def file_size(path: str):
    """
    It returns the size in bytes of specified file. If the path is not a file,
    or if it does not exist it returns -1.
    """
    query = f"fs -stat '%b' {path}"
    try:
        result = _pig_job_executor(query)
    except AirflowException:
        return -1

    value = result.strip()
    return float(value)


def block_size(path: str):
    """
    It returns the block size in bytes of specified file. If the path is not a file,
    or if it does not exist it returns -1.
    """
    query = f"fs -stat '%o' {path}"
    try:
        result = _pig_job_executor(query)
    except AirflowException:
        return -1

    value = result.strip()
    return float(value)
