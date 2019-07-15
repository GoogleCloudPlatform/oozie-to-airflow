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
"""Remove inaccessible transformer"""

from typing import Dict

from o2a.converter.oozie_node import OozieNode
from o2a.converter.workflow import Workflow
from o2a.mappers.start_mapper import StartMapper
from o2a.transformers.base_transformer import BaseWorkflowTransformer


# pylint: disable=too-few-public-methods
class RemoveInaccessibleNodeTransformer(BaseWorkflowTransformer):
    """
    Transformer that remove nodes that are not accessible from Start node. In the case of Airflow,
    all nodes are executed. In the case of Oozie, only nodes that have a connection
    to Start node are executed.
    """

    def process_workflow_after_parse_workflow_xml(self, workflow: Workflow):
        accessible_nodes = self._find_accessible_nodes(workflow)

        for node in workflow.nodes.copy().values():
            if node not in accessible_nodes:
                workflow.remove_node(node)

    @staticmethod
    def _find_accessible_nodes(workflow: Workflow):
        """
        Finds nodes that are reachable from any Start node.
        """
        start_nodes = workflow.get_nodes_by_type(StartMapper)
        visited_node: Dict[str, OozieNode] = dict()

        def visit_node(node: OozieNode):
            if node.name in visited_node:
                return
            visited_node[node.name] = node

            all_downstream_node_names = [*node.downstream_names]
            if node.error_downstream_name:
                all_downstream_node_names.append(node.error_downstream_name)

            for node_name in all_downstream_node_names:
                visit_node(workflow.nodes[node_name])

        for start_node in start_nodes:
            visit_node(start_node)

        return visited_node.values()
