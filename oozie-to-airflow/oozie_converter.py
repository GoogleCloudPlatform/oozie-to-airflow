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

import argparse
import jinja2
import json
import os
import sys
import uuid
import textwrap

import logging

from converter import oozie_parser
from definitions import TPL_PATH
from utils import el_utils

INDENT = 4


def main():
    args = parse_args(sys.argv[1:])

    in_file_name = args.input
    out_file_name = args.output

    params = {'user.name': args.user or os.environ['USER']}
    params = el_utils.parse_els(args.properties, params)

    # Each OozieParser class corresponds to one workflow, where one can get
    # the workflow's required dependencies (imports), operator relations,
    # and operator execution sequence.
    parser = oozie_parser.OozieParser(oozie_wflow=in_file_name, params=params)
    parser.parse_workflow()

    relations = parser.get_relations()
    depens = parser.get_dependencies()
    ops = parser.get_operators()
    parser.update_trigger_rules()

    create_dag_file(ops, depens, relations, params, out_file_name, start_days_ago=args.start,
                    schedule_interval=args.interval)


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="Convert Apache Oozie workflows to Apache Airflow workflows.")
    parser.add_argument('-i', '--input', help='Path to input file name',
                        required=True)
    parser.add_argument('-o', '--output', help='Desired output name')
    parser.add_argument('-d', '--dag', help='Desired DAG name')
    parser.add_argument('-p', '--properties',
                        help='Path to the properties file')
    parser.add_argument('-u', '--user',
                        help='The user to be used in place of all ${user.name},'
                             ' if empty, then user who ran the conversion is used')
    parser.add_argument('-s', '--start', help='Desired DAG start as number of days ago')
    parser.add_argument('-v', '--interval', help='Desired DAG schedule interval as number of days')
    return parser.parse_args(args)


def create_dag_file(operators, depends, relations, params, fn=None,
                    dag_name=None, schedule_interval=None, start_days_ago=None):
    """
    Writes to a file the Apache Oozie parsed workflow in Airflow's DAG format.

    :param operators: A dictionary of {'task_id': ParsedNode object}
    :param depends: A list of strings that will be interpreted as import
        statements
    :param relations: A list of strings corresponding to operator relations,
        such as task_1.set_downstream(task_2)
    :param params: A dictionary of params, in general this will be the parsed
        contents of job.properties
    :param fn: Desired output file name.
    :param dag_name: Desired output DAG name.
    :param schedule_interval: Desired DAG schedule interval, expressed as number of days
    :param start_days_ago: Desired DAG start date, expressed as number of days ago from the present day
    """
    if not fn:
        fn = '/tmp/' + str(uuid.uuid4())
    if not dag_name:
        dag_name = str(uuid.uuid4())
    if not start_days_ago:
        start_days_ago = 1
    if not schedule_interval:
        schedule_interval = 1

    with open(fn, 'w') as f:
        logging.info("Saving to file: {}".format(fn))

        write_dependencies(f, depends)
        f.write('PARAMS = ' + json.dumps(params, indent=INDENT) + '\n\n')
        write_dag_header(f, dag_name, schedule_interval, start_days_ago)

        write_operators(f, operators)
        f.write('\n\n')
        write_relations(f, relations)


def write_operators(fp, operators, indent=INDENT):
    """
    Writes the Airflow operators to the given opened file object.

    :param fp: The file pointer to write to.
    :param operators: Dictionary of {'task_id', ParsedNode}
    :param indent: integer of how many spaces to indent entire operator
    """
    for op in operators.values():
        fp.write(textwrap.indent(op.operator.convert_to_text(), indent * ' '))
        logging.info(
            "Wrote Airflow Task ID: {}".format(op.operator.get_task_id()))


def write_relations(fp, relations, indent=INDENT):
    """
    Each relation is in the form of: task_1.setdownstream(task_2)

    These are each written on a new line.
    """
    logging.info("Writing control flow dependencies to file.")
    for relation in relations:
        fp.write(textwrap.indent(relation, indent * ' '))
        fp.write('\n')


def write_dependencies(fp, depends):
    """
    Writes each dependency on a new line of the given file pointer.

    Of the form: from time import time, etc.
    """
    logging.info("Writing imports to file")
    fp.write('\n'.join(depends))
    fp.write('\n\n')


def write_dag_header(fp, dag_name, schedule_interval, start_days_ago, template='dag.tpl'):
    """
    Write the DAG header to the open file specified in the file pointer
    :param fp: Opened file to write to.
    :param dag_name: Desired name of DAG
    :param schedule_interval: Desired DAG schedule interval, expressed as number of days
    :param start_days_ago: Desired DAG start date, expressed as number of days ago from the present day
    :param template: Desired template to use when creating the DAG header.
    """
    template_loader = jinja2.FileSystemLoader(searchpath=TPL_PATH)
    template_env = jinja2.Environment(loader=template_loader)

    template = template_env.get_template(template)
    fp.write(template.render(dag_name=dag_name, schedule_interval=schedule_interval,
                             start_days_ago=start_days_ago))
    logging.info("Wrote DAG header.")


if __name__ == '__main__':
    main()
