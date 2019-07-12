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
from o2a.converter.workflow import Workflow
from o2a.mappers.kill_mapper import KillMapper
from o2a.transformers.base_transformer import BaseWorkflowTransformer


# pylint: disable=too-few-public-methods
class RemoveKillTransformer(BaseWorkflowTransformer):
    """
    Remove Kill nodes with all relations when it's used in error flow.
    """

    def process_workflow_after_parse_workflow_xml(self, workflow: Workflow):
        kill_nodes = workflow.get_nodes_by_type(KillMapper)
        for kill_node in kill_nodes:
            upstream_nodes = workflow.find_upstream_nodes(kill_node)
            if not any(kill_node.name in node.downstream_names for node in upstream_nodes):
                workflow.remove_node(kill_node)
