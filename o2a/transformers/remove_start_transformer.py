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
Remove Start Transformer
"""

from o2a.converter.parsed_action_node import ParsedActionNode
from o2a.converter.workflow import Workflow
from o2a.mappers.start_mapper import StartMapper
from o2a.transformers.base_transformer import TypeNodeWorkflowTransformer


class RemoveStartTransformer(TypeNodeWorkflowTransformer):
    """
    Remove Start nodes with all relations.
    """

    wanted_type = StartMapper

    def process_node(self, wanted_node: ParsedActionNode, workflow: Workflow):
        del workflow.nodes[wanted_node.name]
        workflow.relations -= {
            relation for relation in workflow.relations if relation.from_task_id == wanted_node.name
        }
