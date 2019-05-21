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
"""Main entry point for the Oozie to Airflow converter"""
import argparse
import logging
import os
import sys
import subprocess
from subprocess import CalledProcessError

from o2a.converter.mappers import ACTION_MAP, CONTROL_MAP
from o2a.converter.oozie_converter import OozieConverter
from o2a.converter.constants import HDFS_FOLDER
from o2a.utils.constants import CONFIGURATION_PROPERTIES, WORKFLOW_XML

INDENT = 4

PROJECT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))


# pylint: disable=missing-docstring
def main():
    args = parse_args(sys.argv[1:])
    input_directory_path = args.input_directory_path
    output_directory_path = args.output_directory_path

    start_days_ago = args.start_days_ago
    schedule_interval = args.schedule_interval
    dag_name = args.dag_name

    if not dag_name:
        dag_name = os.path.basename(input_directory_path)

    conf_path = os.path.join(input_directory_path, CONFIGURATION_PROPERTIES)
    if not os.path.isfile(conf_path):
        logging.warning(
            f"""

#################################### WARNING ###########################################

The '{CONFIGURATION_PROPERTIES}' file was not detected in {input_directory_path}.
It may be necessary to provide input parameters for the workflow.

In case of any conversion errors make sure this configuration file is really not needed.
Otherwise please provide it.

########################################################################################
        """
        )
    # If the validate-workflows script is present int the project path - use it to validate the workflow
    validate_workflows_script = os.path.join(PROJECT_PATH, "bin", "validate-workflows")
    if os.path.isfile(validate_workflows_script):
        try:
            subprocess.check_call(
                [validate_workflows_script, f"{input_directory_path}/{HDFS_FOLDER}/{WORKFLOW_XML}"]
            )
        except CalledProcessError:
            logging.error("Workflow failed schema validation. Please correct the workflow XML and try again.")
            exit(1)
    else:
        logging.info(f"Skipping workflow validation as the {validate_workflows_script} is missing")
    os.makedirs(output_directory_path, exist_ok=True)

    converter = OozieConverter(
        dag_name=dag_name,
        input_directory_path=input_directory_path,
        output_directory_path=output_directory_path,
        action_mapper=ACTION_MAP,
        control_mapper=CONTROL_MAP,
        user=args.user,
        start_days_ago=start_days_ago,
        schedule_interval=schedule_interval,
    )
    converter.recreate_output_directory()
    converter.convert()


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="Convert Apache Oozie workflows to Apache Airflow workflows."
    )
    parser.add_argument("-i", "--input-directory-path", help="Path to input directory", required=True)
    parser.add_argument("-o", "--output-directory-path", help="Desired output directory", required=True)
    parser.add_argument("-d", "--dag-name", help="Desired DAG name [defaults to input directory name]")
    parser.add_argument(
        "-u",
        "--user",
        help="The user to be used in place of all " "${user.name} [defaults to user who ran the conversion]",
    )
    parser.add_argument("-s", "--start-days-ago", help="Desired DAG start as number of days ago", default=0)
    parser.add_argument(
        "-v", "--schedule-interval", help="Desired DAG schedule interval as number of days", default=0
    )
    return parser.parse_args(args)
