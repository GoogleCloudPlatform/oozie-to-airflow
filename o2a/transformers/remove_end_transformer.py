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

    def process_workflow(self, workflow: Workflow):
        decision_nodes = workflow.get_nodes_by_type(DecisionMapper)
        decision_node_ids = {node.last_task_id for node in decision_nodes}
        end_nodes = workflow.get_nodes_by_type(EndMapper)

        for end_node in end_nodes:
            upstream_task_ids = {
                relation.from_task_id
                for relation in workflow.relations
                if relation.to_task_id == end_node.name
            }

            if not decision_node_ids.intersection(upstream_task_ids):
                del workflow.nodes[end_node.name]

            workflow.relations -= {
                relation
                for relation in workflow.relations
                if relation.to_task_id == end_node.name and relation.from_task_id not in decision_node_ids
            }
