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
Remove Join Transformer
"""

from o2a.converter.workflow import Workflow
from o2a.mappers.join_mapper import JoinMapper
from o2a.transformers.base_transformer import BaseWorkflowTransformer


# pylint: disable=too-few-public-methods
class RemoveJoinTransformer(BaseWorkflowTransformer):
    """
    Remove join nodes when there are no downstream nodes
    """

    def process_workflow_after_parse_workflow_xml(self, workflow: Workflow):
        join_nodes = workflow.get_nodes_by_type(JoinMapper)
        for join_node in join_nodes:
            if not join_node.downstream_names:
                workflow.remove_node(join_node)
