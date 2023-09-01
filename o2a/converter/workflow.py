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
"""Workflow"""
import os
from collections import OrderedDict
from typing import Set, Dict, Type, List

from o2a.converter.constants import HDFS_FOLDER, LIB_FOLDER
from o2a.converter.oozie_node import OozieNode
from o2a.converter.relation import Relation
from o2a.converter.task_group import TaskGroup
from o2a.utils.file_utils import get_lib_files


class Workflow:
    """Class for Workflow"""

    def __init__(
        self,
        input_directory_path: str,
        output_directory_path: str,
        dag_name: str,
        task_group_relations: Set[Relation] = None,
        nodes: Dict[str, OozieNode] = None,
        task_groups: Dict[str, TaskGroup] = None,
        dependencies: Set[str] = None,
    ) -> None:
        self.input_directory_path = input_directory_path
        self.output_directory_path = output_directory_path
        self.dag_name = dag_name
        self.task_group_relations = task_group_relations or set()
        # Dictionary is ordered purely for output being somewhat ordered the
        # same as how Oozie workflow was parsed.
        self.nodes = nodes or OrderedDict()
        self.task_groups = task_groups or OrderedDict()
        # These are the general dependencies required that every operator
        # requires.
        self.dependencies = dependencies or {
            "import shlex",
            "import datetime",
            "from o2a_lib.property_utils import PropertySet",
            "from o2a_lib import functions",
            "from airflow import models",
            "from airflow.utils.trigger_rule import TriggerRule",
            "from airflow.utils import dates",
            "from airflow.operators import bash, empty",
        }
        self.library_folder = os.path.join(self.input_directory_path, HDFS_FOLDER, LIB_FOLDER)
        self.jar_files = get_lib_files(self.library_folder, extension=".jar")

    def get_nodes_by_type(self, mapper_type: Type):
        return [node for node in self.nodes.values() if isinstance(node.mapper, mapper_type)]

    def find_upstream_nodes(self, target_node):
        result = []
        for node in self.nodes.values():
            if target_node.name in node.downstream_names or target_node.name == node.error_downstream_name:
                result.append(node)
        return result

    def find_upstream_task_group(self, target_task_group) -> List[TaskGroup]:
        result = []
        for task_group in self.task_groups.values():
            if (
                target_task_group.name in task_group.downstream_names
                or target_task_group.name == task_group.error_downstream_name
            ):
                result.append(task_group)
        return result

    def get_task_group_without_upstream(self) -> List[TaskGroup]:
        task_groups = []
        for task_group in self.task_groups.values():
            upstream_task_group = self.find_upstream_task_group(task_group)
            if not upstream_task_group:
                task_groups.append(task_group)
        return task_groups

    def get_task_group_without_ok_downstream(self):
        task_groups = []
        for task_group in self.task_groups.values():
            if not task_group.downstream_names:
                task_groups.append(task_group)
        return task_groups

    def get_task_group_without_error_downstream(self):
        task_groups = []
        for task_group in self.task_groups.values():
            if not task_group.error_downstream_name:
                task_groups.append(task_group)
        return task_groups

    def remove_node(self, node_to_delete: OozieNode):
        del self.nodes[node_to_delete.name]

        for node in self.nodes.values():
            if node_to_delete.name in node.downstream_names:
                node.downstream_names.remove(node_to_delete.name)
            if node.error_downstream_name == node_to_delete.name:
                node.error_downstream_name = None

    def __repr__(self) -> str:
        return (
            f'Workflow(dag_name="{self.dag_name}", input_directory_path="{self.input_directory_path}", '
            f'output_directory_path="{self.output_directory_path}", relations={self.task_group_relations}, '
            f"nodes={self.nodes.keys()}, dependencies={self.dependencies})"
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False
