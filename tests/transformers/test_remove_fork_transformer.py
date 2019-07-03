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
Remove Fork Node transformer tests
"""
import unittest
from unittest import mock

from o2a.converter.parsed_action_node import ParsedActionNode
from o2a.converter.workflow import Workflow
from o2a.mappers.dummy_mapper import DummyMapper
from o2a.mappers.fork_mapper import ForkMapper
from o2a.mappers.join_mapper import JoinMapper
from o2a.transformers.remove_fork_transformer import RemoveForkTransformer


class RemoveForkTransformerTest(unittest.TestCase):
    def test_should_remove_join_node_when_it_does_not_have_upstream_nodes(self):
        transformer = RemoveForkTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        fork_mapper = mock.Mock(spec=ForkMapper)
        fork_mapper.name = "fork_task"
        fork_node = ParsedActionNode(fork_mapper)
        first_mapper = mock.Mock(spec=DummyMapper)
        first_mapper.name = "first_task"
        first_node = ParsedActionNode(first_mapper)
        second_mapper = mock.Mock(spec=DummyMapper)
        second_mapper.name = "second_task"
        second_node = ParsedActionNode(second_mapper)
        third_mapper = mock.Mock(spec=DummyMapper)
        third_mapper.name = "third_task"
        third_node = ParsedActionNode(third_mapper)
        join_mapper = mock.Mock(spec=JoinMapper)
        join_mapper.name = "join_task"
        join_node = ParsedActionNode(join_mapper)

        fork_node.downstream_names = [first_mapper.name, second_mapper.name, third_mapper.name]
        first_node.downstream_names = [join_mapper.name]
        second_node.downstream_names = [join_mapper.name]
        third_node.downstream_names = [join_mapper.name]

        workflow.nodes[fork_mapper.name] = fork_node
        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[second_mapper.name] = second_node
        workflow.nodes[third_mapper.name] = third_node
        workflow.nodes[join_mapper.name] = join_node

        transformer.process_workflow(workflow)

        self.assertEqual(
            {first_mapper.name, second_mapper.name, third_mapper.name, join_mapper.name},
            set(workflow.nodes.keys()),
        )
        self.assertEqual([], join_node.downstream_names)
        self.assertEqual([join_node.name], first_node.downstream_names)
        self.assertEqual([join_node.name], second_node.downstream_names)
        self.assertEqual([join_node.name], third_node.downstream_names)

    def test_should_keep_fork_node_when_it_have_upstream_nodes(self):
        # pylint: disable=too-many-locals
        transformer = RemoveForkTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        zero_mapper = mock.Mock(spec=DummyMapper)
        zero_mapper.name = "zero_task"
        zero_node = ParsedActionNode(zero_mapper)
        fork_mapper = mock.Mock(spec=ForkMapper)
        fork_mapper.name = "fork_task"
        fork_node = ParsedActionNode(fork_mapper)
        first_mapper = mock.Mock(spec=DummyMapper)
        first_mapper.name = "first_task"
        first_node = ParsedActionNode(first_mapper)
        second_mapper = mock.Mock(spec=DummyMapper)
        second_mapper.name = "second_task"
        second_node = ParsedActionNode(second_mapper)
        third_mapper = mock.Mock(spec=DummyMapper)
        third_mapper.name = "third_task"
        third_node = ParsedActionNode(third_mapper)
        join_mapper = mock.Mock(spec=JoinMapper)
        join_mapper.name = "join_task"
        join_node = ParsedActionNode(join_mapper)

        zero_node.downstream_names = [fork_node.name]
        fork_node.downstream_names = [first_mapper.name, second_mapper.name, third_mapper.name]
        first_node.downstream_names = [join_mapper.name]
        second_node.downstream_names = [join_mapper.name]
        third_node.downstream_names = [join_mapper.name]

        workflow.nodes[zero_node.name] = zero_node
        workflow.nodes[fork_mapper.name] = fork_node
        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[second_mapper.name] = second_node
        workflow.nodes[third_mapper.name] = third_node
        workflow.nodes[join_mapper.name] = join_node
        workflow.nodes[zero_node.name] = zero_node

        transformer.process_workflow(workflow)

        self.assertEqual(
            {
                zero_node.name,
                fork_mapper.name,
                first_mapper.name,
                second_mapper.name,
                third_mapper.name,
                join_mapper.name,
            },
            set(workflow.nodes.keys()),
        )
        self.assertEqual(
            [first_mapper.name, second_mapper.name, third_mapper.name], fork_node.downstream_names
        )
        self.assertEqual([fork_mapper.name], zero_node.downstream_names)
        self.assertEqual(
            [first_mapper.name, second_mapper.name, third_mapper.name], fork_node.downstream_names
        )
        self.assertEqual([join_mapper.name], first_node.downstream_names)
        self.assertEqual([join_mapper.name], second_node.downstream_names)
        self.assertEqual([join_mapper.name], third_node.downstream_names)

    def test_should_remove_multiple_fork_nodes(self):
        transformer = RemoveForkTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        fork_a_mapper = mock.Mock(spec=ForkMapper)
        fork_a_mapper.name = "join_A"
        fork_a_node = ParsedActionNode(fork_a_mapper)
        workflow.nodes[fork_a_mapper.name] = fork_a_node

        fork_b_mapper = mock.Mock(spec=ForkMapper)
        fork_b_mapper.name = "join_B"
        fork_b_node = ParsedActionNode(fork_b_mapper)
        workflow.nodes[fork_b_mapper.name] = fork_b_node

        workflow.nodes[fork_a_mapper.name] = fork_a_node
        workflow.nodes[fork_b_mapper.name] = fork_b_node

        transformer.process_workflow(workflow)

        self.assertEqual(set(), set(workflow.nodes.keys()))
