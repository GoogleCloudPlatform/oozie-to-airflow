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
import sys
from collections import namedtuple
from pathlib import Path
from typing import Dict, TextIO, Set, Optional
import getpass
from datetime import datetime

from os import environ, makedirs
from os.path import basename, join

import json

import textwrap
import logging

import black
from autoflake import fix_file
from converter import parser
from converter.constants import HDFS_FOLDER
from converter.mappers import ACTION_MAP, CONTROL_MAP
from converter.parsed_node import ParsedNode
from converter.primitives import Relation
from utils import el_utils
from utils.constants import CONFIGURATION_PROPERTIES, JOB_PROPERTIES
from utils.el_utils import comma_separated_string_to_list
from utils.template_utils import render_template

INDENT = 4


class OozieConverter:
    """Converts Oozie Workflow app to Airflow's DAG
    """

    action_mapper = ACTION_MAP
    control_mapper = CONTROL_MAP

    def __init__(
        self,
        dag_name: str,
        input_directory_path: str,
        output_directory_path: str,
        user: str = None,
        start_days_ago: int = None,
        schedule_interval: str = None,
        output_dag_name: str = None,
    ):
        """
        :param input_directory_path: Oozie workflow directory.
        :param output_directory_path: Desired output directory.
        :param user: Username.
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
        self.user = user
        self.output_dag_name = (
            join(output_directory_path, output_dag_name)
            if output_dag_name
            else join(output_directory_path, self.dag_name) + ".py"
        )
        self.properties = self.get_properties()
        self.parser = parser.OozieParser(
            input_directory_path=input_directory_path,
            output_directory_path=output_directory_path,
            properties=self.properties,
            dag_name=dag_name,
            action_mapper=self.action_mapper,
            control_mapper=self.control_mapper,
        )

    def recreate_output_directory(self):
        self._recreate_output_directory()

    def convert(self):
        self.parser.parse_workflow()
        relations = self.parser.get_relations()
        depends = self.parser.get_dependencies()
        nodes = self.parser.get_nodes()
        self.create_dag_file(nodes, depends, relations)

    def _recreate_output_directory(self):
        shutil.rmtree(self.output_directory_path, ignore_errors=True)
        makedirs(self.output_directory_path, exist_ok=True)

    def get_properties(self):
        properties = {"user.name": self.user or environ["USER"]}
        properties = el_utils.parse_el_property_file_into_dictionary(
            join(self.input_directory_path, CONFIGURATION_PROPERTIES), properties
        )
        properties = el_utils.parse_el_property_file_into_dictionary(
            join(self.input_directory_path, JOB_PROPERTIES), properties
        )
        return properties

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
        black.format_file_in_place(
            Path(file_name), mode=black.FileMode(line_length=110), fast=False, write_back=black.WriteBack.YES
        )
        Args = namedtuple(
            "Args",
            "remove_all_unused_imports "
            "ignore_init_module_imports "
            "remove_duplicate_keys "
            "remove_unused_variables "
            "in_place imports "
            "expand_star_imports "
            "check",
        )
        fix_file(
            file_name,
            args=Args(
                remove_all_unused_imports=True,
                ignore_init_module_imports=False,
                imports=None,
                expand_star_imports=False,
                remove_duplicate_keys=False,
                remove_unused_variables=True,
                in_place=True,
                check=False,
            ),
            standard_out=sys.stdout,
        )

    def write_dag(
        self, depends: Set[str], file: TextIO, nodes: Dict[str, ParsedNode], relations: Set[Relation]
    ):
        """
        Template method, can be overridden.
        """
        self.write_file_header(file)
        self.write_dependencies(file, depends)
        self.write_properties(file=file, properties=self.properties)
        self.write_dag_header(
            file=file,
            dag_name=self.dag_name,
            schedule_interval=self.schedule_interval,
            start_days_ago=self.start_days_ago,
        )
        self.write_nodes(file, nodes)
        file.write("\n\n")
        self.write_relations(file, relations)

    def write_nodes(self, file: TextIO, nodes: Dict[str, ParsedNode], indent: int = INDENT):
        """
        Writes the Airflow tasks to the given opened file object.

        :param file: The file pointer to write to.
        :param nodes: Dictionary of {'task_id', ParsedNode}
        :param indent: integer of how many spaces to indent entire operator
        """
        for node in nodes.values():
            file.write(textwrap.indent(node.mapper.convert_to_text(), indent * " "))
            logging.info(f"Wrote tasks corresponding to the action named: {node.mapper.name}")
            node.mapper.copy_extra_assets(
                input_directory_path=join(self.input_directory_path, HDFS_FOLDER),
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
    def write_dag_header(
        file: TextIO,
        dag_name: str,
        schedule_interval: Optional[str],
        start_days_ago: Optional[int],
        template: str = "dag.tpl",
    ):
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

    @staticmethod
    def write_properties(file: TextIO, properties: Dict[str, str], template: str = "properties.tpl") -> None:
        """
        Write the DAG header to the open file specified in the file pointer
        :param file: Opened file to write to.
        :param properties: properties of the DAG
        :param template: Desired template to use when creating the DAG header.
        """
        converted_properties = {x: comma_separated_string_to_list(y) for x, y in properties.items()}
        file.write(
            render_template(
                template_name=template, properties=json.dumps(converted_properties, indent=INDENT)
            )
        )
        logging.info("Wrote DAG properties.")

    def write_file_header(self, file: TextIO) -> None:
        """
        Writes header of the python-generated file
        :param file: Opened file to write to
        """
        logging.info("Writing file header.")
        file.write("# -*- coding: utf-8 -*-\n")
        file.write(f'"""{basename(self.output_dag_name)}\n')
        file.write(f"  DAG generated on {datetime.now():%Y-%m-%d %H:%M:%S%z} by {getpass.getuser()}\n")
        file.write(f"     Input folder     : {self.input_directory_path}\n")
        file.write(f"     Output directory : {self.output_directory_path}\n")
        file.write(f"     Output DAG name  : {self.output_dag_name}\n")
        file.write('"""\n')
        logging.info("Wrote file header.")
