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
    print(args)
    in_file_name = args.input
    out_file_name = args.output

    start = args.start
    interval = args.interval
    dag = args.dag

    converter = OozieConverter(
        in_file_name,
        out_file_name,
        args.properties,
        args.configuration,
        user=args.user,
        start_days_ago=start,
        schedule_interval=interval,
        dag_name=dag,
    )
    converter.convert()


def parse_args(args):
    parser = argparse.ArgumentParser(
        description="Convert Apache Oozie workflows to Apache Airflow workflows."
    )
    parser.add_argument("-i", "--input", help="Path to input file name", required=True)
    parser.add_argument("-o", "--output", help="Desired output name")
    parser.add_argument("-d", "--dag", help="Desired DAG name")
    parser.add_argument("-p", "--properties", help="Path to the properties file")
    parser.add_argument("-c", "--configuration", help="Path to the configuration file")
    parser.add_argument(
        "-u",
        "--user",
        help="The user to be used in place of all ${user.name},"
        " if empty, then user who ran the conversion is used",
    )
    parser.add_argument("-s", "--start", help="Desired DAG start as number of days ago", default=0)
    parser.add_argument("-v", "--interval", help="Desired DAG schedule interval as number of days", default=0)
    return parser.parse_args(args)


class OozieConverter:
    def __init__(
        self,
        in_file_name,
        out_file_name,
        properties,
        configuration,
        user=None,
        start_days_ago=None,
        schedule_interval=None,
        dag_name=None,
    ):
        """
        :param in_file_name: Oozie workflow input XML file.
        :param out_file_name: Desired output file name.
        :param properties: Parsed contents of job.properties.
        :param configuration: Parsed contents of configuration.properties.
        :param user: Username.  # TODO
        :param start: Desired DAG start date, expressed as number of days ago from the present day
        :param interval: Desired DAG schedule interval, expressed as number of days
        :param dag_name: Desired output DAG name.
        """
        # Each OozieParser class corresponds to one workflow, where one can get
        # the workflow's required dependencies (imports), operator relations,
        # and operator execution sequence.
        self.properties = properties
        self.configuration = configuration
        params = {"user.name": user or os.environ["USER"]}
        params = self.add_properties_to_params(params)
        params = el_utils.parse_els(configuration, params)
        self.params = params
        self.out_file_name = out_file_name
        self.start_days_ago = start_days_ago
        self.schedule_interval = schedule_interval
        self.dag_name = dag_name
        self.parser = oozie_parser.OozieParser(oozie_wflow=in_file_name, params=params, dag_name=dag_name)

    def convert(self):
        parser = self.parser
        parser.parse_workflow()
        relations = parser.get_relations()
        depends = parser.get_dependencies()
        ops = parser.get_operators()
        parser.update_trigger_rules()
        self.create_dag_file(ops, depends, relations)

    def add_properties_to_params(self, params):
        """
        Template method, can be overridden.
        """
        return el_utils.parse_els(self.properties, params)

    def create_dag_file(self, operators, depends, relations):
        """
        Writes to a file the Apache Oozie parsed workflow in Airflow's DAG format.

        :param operators: A dictionary of {'task_id': ParsedNode object}
        :param depends: A list of strings that will be interpreted as import
            statements
        :param relations: A list of strings corresponding to operator relations,
            such as task_1.set_downstream(task_2)
        """
        fn = self.out_file_name
        if not fn:
            fn = "/tmp/" + str(uuid.uuid4())
        if not self.dag_name:
            self.dag_name = str(uuid.uuid4())

        with open(fn, "w") as f:
            logging.info("Saving to file: {}".format(fn))
            self.write_dag(depends, f, operators, relations)

    def write_dag(self, depends, f, operators, relations):
        """
        Template method, can be overridden.
        """
        self.write_dependencies(f, depends)
        f.write("PARAMS = " + json.dumps(self.params, indent=INDENT) + "\n\n")
        self.write_dag_header(f, self.dag_name, self.schedule_interval, self.start_days_ago)
        self.write_operators(f, operators)
        f.write("\n\n")
        self.write_relations(f, relations)

    @staticmethod
    def write_operators(fp, operators, indent=INDENT):
        """
        Writes the Airflow operators to the given opened file object.

        :param fp: The file pointer to write to.
        :param operators: Dictionary of {'task_id', ParsedNode}
        :param indent: integer of how many spaces to indent entire operator
        """
        for op in operators.values():
            fp.write(textwrap.indent(op.operator.convert_to_text(), indent * " "))
            logging.info("Wrote Airflow Task ID: {}".format(op.operator.get_task_id()))

    @staticmethod
    def write_relations(fp, relations, indent=INDENT):
        """
        Each relation is in the form of: task_1.setdownstream(task_2)

        These are each written on a new line.
        """
        logging.info("Writing control flow dependencies to file.")
        for relation in relations:
            fp.write(textwrap.indent(relation, indent * " "))
            fp.write("\n")

    @staticmethod
    def write_dependencies(fp, depends, line_prefix=""):
        """
        Writes each dependency on a new line of the given file pointer.

        Of the form: from time import time, etc.
        """
        logging.info("Writing imports to file")
        fp.write(f"\n{line_prefix}".join(depends))
        fp.write("\n\n")

    @staticmethod
    def write_dag_header(fp, dag_name, schedule_interval, start_days_ago, template="dag.tpl"):
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
        fp.write(
            template.render(
                dag_name=dag_name, schedule_interval=schedule_interval, start_days_ago=start_days_ago
            )
        )
        logging.info("Wrote DAG header.")


class OozieSubworkflowConverter(OozieConverter):
    def __init__(
        self,
        in_file_name,
        out_file_name,
        properties,
        configuration,
        user=None,
        start_days_ago=None,
        schedule_interval=None,
        dag_name=None,
        config_properties=None,
    ):
        self.config_properties = config_properties
        OozieConverter.__init__(
            self,
            in_file_name,
            out_file_name,
            properties,
            configuration,
            user,
            start_days_ago,
            schedule_interval,
            dag_name,
        )

    def add_properties_to_params(self, params):
        if self.config_properties:
            # Instead of using properties from job.properties we use the config_properties dict passed as
            # argument (currently used if the workflow being converted is a sub-workflow).
            params = {**params, **self.config_properties}  # Merging two dicts
        else:
            params = el_utils.parse_els(self.properties, params)
        return params

    def write_dag(self, depends, f, operators, relations):
        self.write_dependencies(f, depends)
        f.write("PARAMS = " + json.dumps(self.params, indent=INDENT) + "\n\n")
        f.write("\ndef sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):\n")
        self.write_dag_header(
            f, self.dag_name, self.schedule_interval, self.start_days_ago, template="dag_subwf.tpl"
        )
        self.write_operators(f, operators, indent=INDENT + 4)
        f.write("\n\n")
        self.write_relations(f, relations, indent=INDENT + 4)
        f.write(textwrap.indent("\nreturn dag\n", INDENT * " "))


if __name__ == "__main__":
    main()
