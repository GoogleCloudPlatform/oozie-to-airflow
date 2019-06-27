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
from typing import Dict, Type, List

import os

import logging


from o2a.converter import parser
from o2a.converter.constants import HDFS_FOLDER
from o2a.converter.parsed_action_node import ParsedActionNode
from o2a.converter.relation import Relation
from o2a.converter.renderers import BaseRenderer
from o2a.converter.workflow import Workflow
from o2a.mappers.action_mapper import ActionMapper
from o2a.transformers.base_transformer import BaseWorkflowTransformer
from o2a.utils import el_utils
from o2a.utils.constants import CONFIG, JOB_PROPS
from o2a.o2a_libs.property_utils import PropertySet


# pylint: disable=too-many-instance-attributes
class OozieConverter:
    """
    Converts Oozie Workflow app to Airflow's DAG

    :param dag_name: Desired output DAG name.
    :param input_directory_path: Oozie workflow directory.
    :param output_directory_path: Desired output directory.
    :param action_mapper: List of charters that support action nodes
    :param renderer: Renderer that will be used for the output file
    :param transformers: List of transformers that will transform a workflow
    :param user: Username.  # TODO remove me and use real ${user} EL
    :param initial_props: Initial PropertySet object
    """

    def __init__(
        self,
        dag_name: str,
        input_directory_path: str,
        output_directory_path: str,
        action_mapper: Dict[str, Type[ActionMapper]],
        renderer: BaseRenderer,
        transformers: List[BaseWorkflowTransformer] = None,
        user: str = None,
        initial_props: PropertySet = None,
    ):

        # Each OozieParser class corresponds to one workflow, where one can get
        # the workflow's required dependencies (imports), operator relations,
        # and operator execution sequence.
        self.input_directory_path = input_directory_path
        self.output_directory_path = output_directory_path
        self.dag_name = dag_name
        self.config_file = os.path.join(input_directory_path, CONFIG)
        self.job_properties_file = os.path.join(input_directory_path, JOB_PROPS)
        self.renderer = renderer
        self.transformers = transformers or []
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
            action_mapper=action_mapper,
            renderer=self.renderer,
            dag_name=dag_name,
        )

    def recreate_output_directory(self):
        shutil.rmtree(self.output_directory_path, ignore_errors=True)
        os.makedirs(self.output_directory_path, exist_ok=True)

    def convert(self, as_subworkflow=False):
        self.parser.parse_workflow()

        workflow = self.parser.workflow
        self.convert_nodes(workflow.nodes)
        self.convert_relations(workflow)
        self.update_trigger_rules(workflow)
        self.apply_transformers(workflow)
        self.convert_dependencies(workflow)

        if as_subworkflow:
            self.renderer.create_subworkflow_file(workflow=workflow, props=self.props)
        else:
            self.renderer.create_workflow_file(workflow=workflow, props=self.props)
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

    @staticmethod
    def convert_dependencies(workflow: Workflow) -> None:
        logging.info("Converting dependencies.")
        for node in workflow.nodes.values():
            workflow.dependencies.update(node.mapper.required_imports())

    @staticmethod
    def convert_relations(workflow: Workflow) -> None:
        logging.info("Converting relations between nodes.")
        for p_node in workflow.nodes.values():
            for downstream in p_node.get_downstreams():
                relation = Relation(
                    from_task_id=p_node.last_task_id, to_task_id=workflow.nodes[downstream].first_task_id
                )
                workflow.relations.add(relation)
            error_downstream = p_node.get_error_downstream_name()
            if error_downstream:
                relation = Relation(
                    from_task_id=p_node.last_task_id,
                    to_task_id=workflow.nodes[error_downstream].first_task_id,
                    is_error=True,
                )
                workflow.relations.add(relation)

    @staticmethod
    def update_trigger_rules(workflow: Workflow) -> None:
        logging.info("Updating trigger rules.")
        for node in workflow.nodes.values():
            # If a task is referenced  by an "ok to=<task>", flip bit in parsed
            # node class
            for downstream in node.get_downstreams():
                workflow.nodes[downstream].is_ok = True
            error_name = node.get_error_downstream_name()
            if error_name:
                # If a task is referenced  by an "error to=<task>", flip
                # corresponding bit in the parsed node class
                workflow.nodes[error_name].is_error = True
            node.update_trigger_rule()

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

    def apply_transformers(self, workflow: Workflow):
        logging.info(f"Applying transformers")
        for transformer in self.transformers:
            transformer.process_workflow(workflow)
