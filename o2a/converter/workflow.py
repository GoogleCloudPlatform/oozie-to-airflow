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
from collections import OrderedDict
from typing import Set, Dict, Type

from o2a.converter.parsed_action_node import ParsedActionNode
from o2a.converter.relation import Relation


# This is a container for data, so it does not contain public methods intentionally.
class Workflow:  # pylint: disable=too-few-public-methods
    """Class for Workflow"""

    def __init__(
        self,
        input_directory_path: str,
        output_directory_path: str,
        dag_name: str,
        relations: Set[Relation] = None,
        nodes: Dict[str, ParsedActionNode] = None,
        dependencies: Set[str] = None,
    ) -> None:
        self.input_directory_path = input_directory_path
        self.output_directory_path = output_directory_path
        self.dag_name = dag_name
        self.relations = relations or set()
        # Dictionary is ordered purely for output being somewhat ordered the
        # same as how Oozie workflow was parsed.
        self.nodes = nodes or OrderedDict()
        # These are the general dependencies required that every operator
        # requires.
        self.dependencies = dependencies or {
            "import shlex",
            "import datetime",
            "from o2a.o2a_libs.el_basic_functions import *",
            "from o2a.o2a_libs.el_wf_functions import *",
            "from o2a.o2a_libs.property_utils import PropertySet",
            "from airflow import models",
            "from airflow.utils.trigger_rule import TriggerRule",
            "from airflow.utils import dates",
        }

    def get_nodes_by_type(self, mapper_type: Type):
        return [node for node in self.nodes.values() if isinstance(node.mapper, mapper_type)]

    def find_upstream_nodes(self, target_node):
        result = []
        for node in self.nodes.values():
            if target_node.name in node.downstream_names or target_node.name == node.error_downstream_name:
                result.append(node)
        return result

    def remove_node(self, node_to_delete: ParsedActionNode):
        del self.nodes[node_to_delete.name]

        for node in self.nodes.values():
            if node_to_delete.name in node.downstream_names:
                node.downstream_names.remove(node_to_delete.name)
            if node.error_downstream_name == node_to_delete.name:
                node.error_downstream_name = None

    def __repr__(self) -> str:
        return (
            f'Workflow(dag_name="{self.dag_name}", input_directory_path="{self.input_directory_path}", '
            f'output_directory_path="{self.output_directory_path}", relations={self.relations}, '
            f"nodes={self.nodes.keys()}, dependencies={self.dependencies})"
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False
