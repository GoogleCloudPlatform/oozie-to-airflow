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
Remove Kill Transformer tests
"""
import unittest
from unittest import mock

from o2a.converter.oozie_node import OozieNode
from o2a.converter.workflow import Workflow
from o2a.mappers.base_mapper import BaseMapper
from o2a.mappers.kill_mapper import KillMapper
from o2a.transformers.remove_kill_transformer import RemoveKillTransformer


class RemoveKillTransformerTest(unittest.TestCase):
    def test_should_remove_node_in_error_flow(self):
        transformer = RemoveKillTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        first_mapper = mock.Mock(spec=BaseMapper)
        first_mapper.name = "first_task"
        second_mapper = mock.Mock(spec=KillMapper)
        second_mapper.name = "second_task"

        first_node = OozieNode(first_mapper)
        second_node = OozieNode(second_mapper)
        first_node.error_downstream_name = second_mapper.name

        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[second_mapper.name] = second_node

        transformer.process_workflow_after_parse_workflow_xml(workflow)

        self.assertEqual({first_mapper.name}, set(workflow.nodes.keys()))

    def test_should_keep_node_in_correct_flow(self):
        transformer = RemoveKillTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        first_mapper = mock.Mock(spec=BaseMapper)
        first_mapper.name = "first_task"
        third_mapper = mock.Mock(spec=KillMapper)
        third_mapper.name = "third_task"

        first_node = OozieNode(first_mapper)
        third_node = OozieNode(third_mapper)

        first_node.downstream_names = [third_mapper.name]

        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[third_mapper.name] = third_node

        transformer.process_workflow_after_parse_workflow_xml(workflow)

        self.assertEqual({first_mapper.name, third_mapper.name}, set(workflow.nodes.keys()))
        self.assertEqual([third_node.name], first_node.downstream_names)
        self.assertEqual([], third_node.downstream_names)
