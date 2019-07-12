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
"""
Remove End Transformer
"""
from o2a.converter.workflow import Workflow
from o2a.mappers.decision_mapper import DecisionMapper
from o2a.mappers.end_mapper import EndMapper
from o2a.transformers.base_transformer import BaseWorkflowTransformer


# pylint: disable=too-few-public-methods
class RemoveEndTransformer(BaseWorkflowTransformer):
    """
    Remove End nodes with all relations when it's not connected to Decision Node.
    """

    def process_workflow_after_parse_workflow_xml(self, workflow: Workflow):
        decision_nodes = workflow.get_nodes_by_type(DecisionMapper)
        decision_node_names = {node.name for node in decision_nodes}
        end_nodes = workflow.get_nodes_by_type(EndMapper)

        for end_node in end_nodes:
            upstream_nodes = workflow.find_upstream_nodes(end_node)
            upstream_node_names = {node.name for node in upstream_nodes}

            if not decision_node_names.intersection(upstream_node_names):
                workflow.remove_node(end_node)
            else:
                for upstream_node in upstream_nodes:
                    if upstream_node.name not in decision_node_names:
                        upstream_node.downstream_names.remove(end_node.name)
