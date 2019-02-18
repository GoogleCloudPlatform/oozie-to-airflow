# Copyright 2018 Google LLC
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

# airflow DAG
import io
import logging
import subprocess


def read_hdfs_files(file_list):
    """
    Given a list of files, returns the corresponding list of StringIO objects
    that represent the data from the HDFS file.

    :param file_list: List of HDFS file names to be read.
    :return: A list of StringIO objects corresponding to file_list
    """
    str_io_list = []

    for f in file_list:
        str_io_list.append(read_hdfs_file(f))

    return str_io_list


def read_hdfs_file(file_name):
    """
    Returns a StringIO object with the contents of:
    `hadoop fs -cat FILE_NAME`

    :param file_name: Name of file on HDFS, non fully-qualified path is
        considered as a file on default HDFS
    """
    if file_name:
        try:
            output = subprocess.check_output(['hadoop', 'fs', '-cat', file_name])
            return io.StringIO(output.decode('utf-8'))
        except subprocess.SubprocessError:
            logging.WARNING('Could not read file: {}'.format(file_name))

