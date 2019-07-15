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
Remove Join Node transformer tests
"""
import unittest
from unittest import mock

from o2a.converter.oozie_node import OozieNode
from o2a.converter.workflow import Workflow
from o2a.mappers.dummy_mapper import DummyMapper
from o2a.mappers.fork_mapper import ForkMapper
from o2a.mappers.join_mapper import JoinMapper
from o2a.transformers.remove_join_transformer import RemoveJoinTransformer


class RemoveJoinTransformerTest(unittest.TestCase):
    def test_should_remove_join_node_when_it_does_not_have_downstream_nodes(self):
        transformer = RemoveJoinTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        fork_mapper = mock.Mock(spec=ForkMapper)
        fork_mapper.name = "fork_task"
        fork_node = OozieNode(fork_mapper)
        first_mapper = mock.Mock(spec=DummyMapper)
        first_mapper.name = "first_task"
        first_node = OozieNode(first_mapper)
        second_mapper = mock.Mock(spec=DummyMapper)
        second_mapper.name = "second_task"
        second_node = OozieNode(second_mapper)
        third_mapper = mock.Mock(spec=DummyMapper)
        third_mapper.name = "third_task"
        third_node = OozieNode(third_mapper)
        join_mapper = mock.Mock(spec=JoinMapper)
        join_mapper.name = "join_task"
        join_node = OozieNode(join_mapper)

        fork_node.downstream_names = [first_mapper.name, second_mapper.name, third_mapper.name]
        first_node.downstream_names = [join_mapper.name]
        second_node.downstream_names = [join_mapper.name]
        third_node.downstream_names = [join_mapper.name]

        workflow.nodes[fork_mapper.name] = fork_node
        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[second_mapper.name] = second_node
        workflow.nodes[third_mapper.name] = third_node
        workflow.nodes[join_mapper.name] = join_node

        transformer.process_workflow_after_parse_workflow_xml(workflow)

        self.assertEqual(
            {fork_mapper.name, first_mapper.name, second_mapper.name, third_mapper.name},
            set(workflow.nodes.keys()),
        )
        self.assertEqual(
            [first_mapper.name, second_mapper.name, third_mapper.name], fork_node.downstream_names
        )
        self.assertEqual([], first_node.downstream_names)
        self.assertEqual([], second_node.downstream_names)
        self.assertEqual([], third_node.downstream_names)

    def test_should_keep_join_node_when_it_have_downstream_nodes(self):
        # pylint: disable=too-many-locals
        transformer = RemoveJoinTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        fork_mapper = mock.Mock(spec=ForkMapper)
        fork_mapper.name = "fork_task"
        fork_node = OozieNode(fork_mapper)
        first_mapper = mock.Mock(spec=DummyMapper)
        first_mapper.name = "first_task"
        first_node = OozieNode(first_mapper)
        second_mapper = mock.Mock(spec=DummyMapper)
        second_mapper.name = "second_task"
        second_node = OozieNode(second_mapper)
        third_mapper = mock.Mock(spec=DummyMapper)
        third_mapper.name = "third_task"
        third_node = OozieNode(third_mapper)
        join_mapper = mock.Mock(spec=JoinMapper)
        join_mapper.name = "join_task"
        join_node = OozieNode(join_mapper)
        fourth_mapper = mock.Mock(spec=DummyMapper)
        fourth_mapper.name = "fourth_task"
        fourth_node = OozieNode(fourth_mapper)

        fork_node.downstream_names = [first_mapper.name, second_mapper.name, third_mapper.name]
        first_node.downstream_names = [join_mapper.name]
        second_node.downstream_names = [join_mapper.name]
        third_node.downstream_names = [join_mapper.name]
        join_node.downstream_names = [fourth_node.name]

        workflow.nodes[fork_mapper.name] = fork_node
        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[second_mapper.name] = second_node
        workflow.nodes[third_mapper.name] = third_node
        workflow.nodes[join_mapper.name] = join_node
        workflow.nodes[fourth_mapper.name] = fourth_node

        transformer.process_workflow_after_parse_workflow_xml(workflow)

        self.assertEqual(
            {
                fork_mapper.name,
                first_mapper.name,
                second_mapper.name,
                third_mapper.name,
                join_mapper.name,
                fourth_mapper.name,
            },
            set(workflow.nodes.keys()),
        )
        self.assertEqual(
            [first_mapper.name, second_mapper.name, third_mapper.name], fork_node.downstream_names
        )
        self.assertEqual([join_mapper.name], first_node.downstream_names)
        self.assertEqual([join_mapper.name], second_node.downstream_names)
        self.assertEqual([join_mapper.name], third_node.downstream_names)
        self.assertEqual([fourth_mapper.name], join_node.downstream_names)

    def test_should_remove_multiple_join_nodes(self):
        transformer = RemoveJoinTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        join_a_mapper = mock.Mock(spec=JoinMapper)
        join_a_mapper.name = "join_A"
        join_a_node = OozieNode(join_a_mapper)
        workflow.nodes[join_a_mapper.name] = join_a_node

        join_b_mapper = mock.Mock(spec=JoinMapper)
        join_b_mapper.name = "join_B"
        join_b_node = OozieNode(join_b_mapper)
        workflow.nodes[join_b_mapper.name] = join_b_node

        workflow.nodes[join_a_mapper.name] = join_a_node
        workflow.nodes[join_b_mapper.name] = join_b_node

        transformer.process_workflow_after_parse_workflow_xml(workflow)

        self.assertEqual(set(), set(workflow.nodes.keys()))
