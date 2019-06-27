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
from o2a.converter.parsed_action_node import ParsedActionNode
from o2a.converter.workflow import Workflow
from o2a.mappers.decision_mapper import DecisionMapper
from o2a.mappers.end_mapper import EndMapper
from o2a.transformers.base_transformer import TypeNodeWorkflowTransformer


class RemoveEndTransformer(TypeNodeWorkflowTransformer):
    """
    Remove End nodes with all relations when it's not connected to Decision Node.
    """

    wanted_type = EndMapper

    def __init__(self):
        self.decision_node_ids = None

    def process_workflow(self, workflow: Workflow):
        self.decision_node_ids = {
            node.last_task_id for node in workflow.nodes.values() if isinstance(node.mapper, DecisionMapper)
        }
        super().process_workflow(workflow)

    def process_node(self, wanted_node: ParsedActionNode, workflow: Workflow):
        upstream_task_ids = {
            relation.from_task_id
            for relation in workflow.relations
            if relation.to_task_id == wanted_node.name
        }

        if not self.decision_node_ids.intersection(upstream_task_ids):
            del workflow.nodes[wanted_node.name]

        workflow.relations -= {
            relation
            for relation in workflow.relations
            if relation.to_task_id == wanted_node.name and relation.from_task_id not in self.decision_node_ids
        }
