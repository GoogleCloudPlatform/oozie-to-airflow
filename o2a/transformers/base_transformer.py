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
"""Base transformer classes"""
from abc import ABC, abstractmethod
from typing import Type

from o2a.converter.parsed_action_node import ParsedActionNode
from o2a.converter.workflow import Workflow


# pylint: disable=too-few-public-methods
class BaseWorkflowTransformer(ABC):
    """
    Base class for all transformers
    """

    @abstractmethod
    def process_workflow(self, workflow: Workflow):
        pass


class TypeNodeWorkflowTransformer(BaseWorkflowTransformer):
    """
    A transformer that lets you simplify the processing of one type of node.
    """

    @property
    @abstractmethod
    def wanted_type(self) -> Type:
        raise NotImplementedError("Not Implemented")

    def process_workflow(self, workflow: Workflow):
        wanted_nodes = [node for node in workflow.nodes.values() if isinstance(node.mapper, self.wanted_type)]

        for wanted_node in wanted_nodes:
            self.process_node(wanted_node, workflow)

    @abstractmethod
    def process_node(self, wanted_node: ParsedActionNode, workflow: Workflow):
        pass
