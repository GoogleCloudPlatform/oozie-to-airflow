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
"""Base class for all action nappers"""
from abc import ABC
from copy import deepcopy
from typing import Any, List, Tuple
from xml.etree.ElementTree import Element

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.base_mapper import BaseMapper

from o2a.o2a_libs.property_utils import PropertySet

from o2a.utils.config_extractors import (
    TAG_JOB_XML,
    extract_properties_from_job_xml_nodes,
    TAG_CONFIGURATION,
    extract_properties_from_configuration_node,
)
from o2a.utils.xml_utils import find_nodes_by_tag, find_node_by_tag


# pylint: disable=abstract-method
# noinspection PyAbstractClass
class ActionMapper(BaseMapper, ABC):
    """Base class for all action mappers"""

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        dag_name: str,
        props: PropertySet,
        input_directory_path: str,
        **kwargs: Any,
    ):
        super().__init__(
            oozie_node=oozie_node,
            name=name,
            dag_name=dag_name,
            props=props,
            input_directory_path=input_directory_path,
            **kwargs,
        )
        self.input_directory_path = input_directory_path

    def on_parse_node(self):
        super().on_parse_node()
        self._parse_config()

    def _parse_config(self):
        job_xml_nodes = find_nodes_by_tag(self.oozie_node, TAG_JOB_XML)
        job_xml_properties = extract_properties_from_job_xml_nodes(
            job_xml_nodes=job_xml_nodes, input_directory_path=self.input_directory_path, props=self.props
        )
        self.props.action_node_properties.update(job_xml_properties)

        configuration_node = find_node_by_tag(self.oozie_node, TAG_CONFIGURATION)
        if configuration_node is not None:
            conf_node_properties = extract_properties_from_configuration_node(
                config_node=configuration_node, props=self.props
            )
            self.props.action_node_properties.update(conf_node_properties)

    @staticmethod
    def prepend_task(
        task_to_prepend: Task, tasks: List[Task], relations: List[Relation]
    ) -> Tuple[List[Task], List[Relation]]:
        """Prepends task to list of tasks and adds appropriate relation. This method does
           not modify the tasks or relations list but returns copies of those lists
           with prepended task and updated relations.

        :param task_to_prepend: task to prepend
        :param tasks: list of tasks
        :param relations: list of relations
        :return: Tuple of tasks, relations with task prepended and relations updated.
        """
        if not tasks:
            raise IndexError(f"Cannot prepend task {task_to_prepend} to an empty task list")
        new_relations: List[Relation] = deepcopy(relations)
        new_tasks: List[Task] = deepcopy(tasks)
        new_tasks.insert(0, task_to_prepend)
        new_relations.insert(0, Relation(from_task_id=task_to_prepend.task_id, to_task_id=tasks[0].task_id))
        return new_tasks, new_relations
