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
Remove Kill Transformer
"""

from o2a.converter.parsed_action_node import ParsedActionNode
from o2a.converter.workflow import Workflow
from o2a.mappers.kill_mapper import KillMapper
from o2a.transformers.base_transformer import TypeNodeWorkflowTransformer


class RemoveKillTransformer(TypeNodeWorkflowTransformer):
    """
    Remove Kill nodes with all relations when it's used in error flow.
    """

    wanted_type = KillMapper

    def process_node(self, wanted_node: ParsedActionNode, workflow: Workflow):
        if workflow.nodes[wanted_node.name].is_error:
            del workflow.nodes[wanted_node.name]
            workflow.relations -= {
                relation for relation in workflow.relations if relation.to_task_id == wanted_node.name
            }
