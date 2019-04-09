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
"""Converts Oozie application workflow into Airflow's DAG
"""
import shutil
from typing import Dict, TextIO, Type, Set

import os
import json

import textwrap
import logging

from converter import parser
from converter.parsed_node import ParsedNode
from converter.relation import Relation
from mappers.action_mapper import ActionMapper
from mappers.base_mapper import BaseMapper
from utils import el_utils
from utils.template_utils import render_template

INDENT = 4


class OozieConverter:
    """Converts Oozie Workflow app to Airflow's DAG
    """

    def __init__(
        self,
        dag_name: str,
        input_directory_path: str,
        output_directory_path: str,
        action_mapper: Dict[str, Type[ActionMapper]],
        control_mapper: Dict[str, Type[BaseMapper]],
        user: str = None,
        start_days_ago: int = None,
        schedule_interval: str = None,
    ):
        """
        :param input_directory_path: Oozie workflow directory.
        :param output_directory_path: Desired output directory.
        :param user: Username.  # TODO remove me and use real ${user} EL
        :param start_days_ago: Desired DAG start date, expressed as number of days ago from the present day
        :param schedule_interval: Desired DAG schedule interval, expressed as number of days
        :param dag_name: Desired output DAG name.
        """
        # Each OozieParser class corresponds to one workflow, where one can get
        # the workflow's required dependencies (imports), operator relations,
        # and operator execution sequence.
        self.input_directory_path = input_directory_path
        self.output_directory_path = output_directory_path
        self.start_days_ago = start_days_ago
        self.schedule_interval = schedule_interval
        self.dag_name = dag_name
        self.configuration_properties_file = os.path.join(input_directory_path, "configuration.properties")
        self.job_properties_file = os.path.join(input_directory_path, "job.properties")
        self.output_dag_name = os.path.join(output_directory_path, self.dag_name) + ".py"
        params = {"user.name": user or os.environ["USER"]}
        params = self.add_properties_to_params(params)
        params = el_utils.parse_els(self.configuration_properties_file, params)
        self.params = params
        self.parser = parser.OozieParser(
            input_directory_path=input_directory_path,
            output_directory_path=output_directory_path,
            params=params,
            dag_name=dag_name,
            action_mapper=action_mapper,
            control_mapper=control_mapper,
        )

    def convert(self):
        self.parser.parse_workflow()
        relations = self.parser.get_relations()
        depends = self.parser.get_dependencies()
        nodes = self.parser.get_nodes()
        self.parser.update_trigger_rules()
        self._recreate_output_directory()
        self.create_dag_file(nodes, depends, relations)

    def _recreate_output_directory(self):
        shutil.rmtree(self.output_directory_path, ignore_errors=True)
        os.makedirs(self.output_directory_path, exist_ok=True)

    def add_properties_to_params(self, params: Dict[str, str]):
        """
        Template method, can be overridden.
        """
        return el_utils.parse_els(self.job_properties_file, params)

    def create_dag_file(self, nodes: Dict[str, ParsedNode], depends: Set[str], relations: Set[Relation]):
        """
        Writes to a file the Apache Oozie parsed workflow in Airflow's DAG format.

        :param nodes: A dictionary of {'task_id': ParsedNode object}
        :param depends: A list of strings that will be interpreted as import
            statements
        :param relations: A list of Relation corresponding to operator relations
        """
        file_name = self.output_dag_name
        with open(file_name, "w") as file:
            logging.info(f"Saving to file: {file_name}")
            self.write_dag(depends, file, nodes, relations)

    def write_dag(
        self, depends: Set[str], file: TextIO, nodes: Dict[str, ParsedNode], relations: Set[Relation]
    ):
        """
        Template method, can be overridden.
        """
        self.write_dependencies(file, depends)
        file.write("PARAMS = " + json.dumps(self.params, indent=INDENT) + "\n\n")
        self.write_dag_header(file, self.dag_name, self.schedule_interval, self.start_days_ago)
        self.write_nodes(file, nodes)
        file.write("\n\n")
        self.write_relations(file, relations)

    def write_nodes(self, file: TextIO, nodes: Dict[str, ParsedNode], indent: int = INDENT):
        """
        Writes the Airflow operators to the given opened file object.

        :param file: The file pointer to write to.
        :param nodes: Dictionary of {'task_id', ParsedNode}
        :param indent: integer of how many spaces to indent entire operator
        """
        for node in nodes.values():
            file.write(textwrap.indent(node.mapper.convert_to_text(), indent * " "))
            logging.info(f"Wrote operator corresponding to the action named: {node.mapper.name}")
            node.mapper.copy_extra_assets(
                input_directory_path=self.input_directory_path,
                output_directory_path=self.output_directory_path,
            )

    @staticmethod
    def write_relations(file, relations, indent=INDENT):
        """
        Write the relations to the given opened file object.

        These are each written on a new line.
        """
        logging.info("Writing control flow dependencies to file.")
        relations_str = render_template(template_name="relations.tpl", relations=relations)
        file.write(textwrap.indent(relations_str, indent * " "))

    @staticmethod
    def write_dependencies(file, depends, line_prefix=""):
        """
        Writes each dependency on a new line of the given file pointer.

        Of the form: from time import time, etc.
        """
        logging.info("Writing imports to file")
        file.write(f"\n{line_prefix}".join(depends))
        file.write("\n\n")

    @staticmethod
    def write_dag_header(file, dag_name, schedule_interval, start_days_ago, template="dag.tpl"):
        """
        Write the DAG header to the open file specified in the file pointer
        :param file: Opened file to write to.
        :param dag_name: Desired name of DAG
        :param schedule_interval: Desired DAG schedule interval, expressed as number of days
        :param start_days_ago: Desired DAG start date, expressed as number of days ago from the present day
        :param template: Desired template to use when creating the DAG header.
        """

        file.write(
            render_template(
                template_name=template,
                dag_name=dag_name,
                schedule_interval=schedule_interval,
                start_days_ago=start_days_ago,
            )
        )
        logging.info("Wrote DAG header.")
