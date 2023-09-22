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
from typing import Dict, Type, Union, List

import os

import logging

from isort import SortImports

import black
from autoflake import fix_file

from o2a.converter import parser
from o2a.converter.constants import HDFS_FOLDER
from o2a.converter.parsed_action_node import ParsedActionNode
from o2a.converter.workflow import Workflow
from o2a.mappers.action_mapper import ActionMapper
from o2a.utils import el_utils
from o2a.utils.constants import CONFIG, JOB_PROPS
from o2a.utils.el_utils import comma_separated_string_to_list
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils.template_utils import render_template

AutoflakeArgs = namedtuple(
    "AutoflakeArgs",
    "remove_all_unused_imports "
    "ignore_init_module_imports "
    "remove_duplicate_keys "
    "remove_unused_variables "
    "in_place imports "
    "expand_star_imports "
    "check",
)


# pylint: disable=too-many-instance-attributes, too-many-arguments
class OozieConverter:
    """Converts Oozie Workflow app to Airflow's DAG
    """

    def __init__(
        self,
        dag_name: str,
        input_directory_path: str,
        output_directory_path: str,
        action_mapper: Dict[str, Type[ActionMapper]],
        template_name: str = "workflow.tpl",
        user: str = None,
        start_days_ago: int = None,
        schedule_interval: str = None,
        output_dag_name: str = None,
        initial_props: PropertySet = None,
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
        self.template_name = template_name
        self.config_file = os.path.join(input_directory_path, CONFIG)
        self.job_properties_file = os.path.join(input_directory_path, JOB_PROPS)
        self.output_dag_name = (
            os.path.join(output_directory_path, output_dag_name)
            if output_dag_name
            else os.path.join(output_directory_path, self.dag_name) + ".py"
        )
        # Propagate the configuration in case initial property set is passed
        self.job_properties = {} if not initial_props else initial_props.job_properties
        self.job_properties["user.name"] = user or os.environ["USER"]
        self.config: Dict[str, str] = {}
        self.props = PropertySet(
            job_properties=self.job_properties, config=self.config, action_node_properties={}
        )
        self.read_and_update_job_properties_replace_el()
        self.read_config_replace_el()
        self.parser = parser.OozieParser(
            input_directory_path=input_directory_path,
            output_directory_path=output_directory_path,
            props=self.props,
            dag_name=dag_name,
            action_mapper=action_mapper,
        )

    def recreate_output_directory(self):
        shutil.rmtree(self.output_directory_path, ignore_errors=True)
        os.makedirs(self.output_directory_path, exist_ok=True)

    def convert(self):
        self.parser.parse_workflow()

        workflow = self.parser.workflow
        self.convert_nodes(workflow.nodes)
        self.create_dag_file(workflow)
        self.format_with_black()
        self.sort_imports()
        self.remove_unused_imports()
        self.copy_extra_assets(workflow.nodes)

    @staticmethod
    def convert_nodes(nodes: Dict[str, ParsedActionNode]):
        """
        For each Oozie node, converts it into relations and internal relations.

        It uses the mapper, which is stored in ParsedActionNode. The result is saved in ParsedActionNode.tasks
        and ParsedActionNode.relations
        """
        logging.info("Converting nodes to tasks and inner relations")
        for p_node in nodes.values():
            tasks, relations = p_node.mapper.to_tasks_and_relations()
            p_node.tasks = tasks
            p_node.relations = relations

    def read_config_replace_el(self):
        """
        Reads configuration properties to config dictionary.
        Replaces EL properties within.
        :return: None
        """
        self.config = el_utils.parse_els(properties_file=self.config_file, props=self.props)

    def read_and_update_job_properties_replace_el(self):
        """
        Reads job properties and updates job_properties dictionary with the read values
        Replaces EL job_properties within.
        :return: None
        """
        self.job_properties.update(
            el_utils.parse_els(properties_file=self.job_properties_file, props=self.props)
        )

    def create_dag_file(self, workflow: Workflow):
        """
        Writes to a file the Apache Oozie parsed workflow in Airflow's DAG format.
        """
        file_name = self.output_dag_name
        with open(file_name, "w") as file:
            logging.info(f"Saving to file: {file_name}")
            dag_content = self.render_workflow(workflow)
            file.write(dag_content)

    def remove_unused_imports(self):
        fix_file(
            self.output_dag_name,
            args=AutoflakeArgs(
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

    def format_with_black(self):
        black.format_file_in_place(
            Path(self.output_dag_name),
            mode=black.FileMode(line_length=110),
            fast=False,
            write_back=black.WriteBack.YES,
        )

    def sort_imports(self):
        SortImports(self.output_dag_name)

    def copy_extra_assets(self, nodes: Dict[str, ParsedActionNode]):
        """
        Copies additional assets needed to execute a workflow, eg. Pig scripts.
        """
        for node in nodes.values():
            logging.info(f"Copies additional assets for the node: {node.mapper.name}")
            node.mapper.copy_extra_assets(
                input_directory_path=os.path.join(self.input_directory_path, HDFS_FOLDER),
                output_directory_path=self.output_directory_path,
            )

    def render_workflow(self, workflow: Workflow):
        """
        Creates text representation of the workflow.
        """
        converted_job_properties: Dict[str, Union[List[str], str]] = {
            x: comma_separated_string_to_list(y) for x, y in self.job_properties.items()
        }
        dag_file = render_template(
            template_name=self.template_name,
            dag_name=self.dag_name,
            schedule_interval=self.schedule_interval,
            start_days_ago=self.start_days_ago,
            job_properties=converted_job_properties,
            config=self.config,
            relations=workflow.relations,
            nodes=list(workflow.nodes.values()),
            dependencies=workflow.dependencies,
        )
        return dag_file
