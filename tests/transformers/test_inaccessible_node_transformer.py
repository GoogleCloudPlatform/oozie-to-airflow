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
Remove inaccessible nodes transformer tests
"""
import unittest
from unittest import mock

from o2a.converter.oozie_node import OozieNode
from o2a.converter.task import Task
from o2a.converter.workflow import Workflow
from o2a.mappers.base_mapper import BaseMapper
from o2a.mappers.start_mapper import StartMapper
from o2a.transformers.remove_inaccessible_node_transformer import RemoveInaccessibleNodeTransformer


class RemoveInaccessibleNodeTransformerTest(unittest.TestCase):
    def test_should_keep_connected_nodes_in_correct_flow(self):
        """
        Graph before:

        .. graphviz::

           digraph foo {
              S -> A
              A -> B
           }

        Graph after:

        .. graphviz::

           digraph foo {
              S -> A
              A -> B
           }

        Where:
        A - first_task
        B - second_task
        S - start_task
        """
        transformer = RemoveInaccessibleNodeTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        first_mapper = mock.Mock(spec=BaseMapper)
        first_mapper.name = "first_task"

        second_mapper = mock.Mock(spec=BaseMapper)
        second_mapper.name = "second_task"

        start_mapper = mock.Mock(spec=StartMapper)
        start_mapper.name = "start_task"

        first_node = OozieNode(mapper=first_mapper, tasks=[self._get_dummy_task(first_mapper.name)])
        second_node = OozieNode(mapper=second_mapper, tasks=[self._get_dummy_task(second_mapper.name)])
        start_node = OozieNode(mapper=start_mapper, tasks=[self._get_dummy_task(start_mapper.name)])

        start_node.downstream_names = [first_mapper.name]
        first_node.downstream_names = [second_node.name]

        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[second_mapper.name] = second_node
        workflow.nodes[start_mapper.name] = start_node

        transformer.process_workflow_after_parse_workflow_xml(workflow)

        self.assertEqual(
            {start_mapper.name, first_mapper.name, second_mapper.name}, set(workflow.nodes.keys())
        )
        self.assertEqual([first_mapper.name], start_node.downstream_names)
        self.assertEqual([second_mapper.name], first_node.downstream_names)
        self.assertEqual([], second_node.downstream_names)

    def test_should_keep_connected_nodes_in_error_state(self):
        """
        Graph before:

        .. graphviz::

           digraph foo {
              S -> A
           }

        Graph after:

        .. graphviz::

           digraph foo {
              S -> A
           }

        Where:
        A - first_task
        S - start_task
        """
        transformer = RemoveInaccessibleNodeTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        first_mapper = mock.Mock(spec=BaseMapper)
        first_mapper.name = "first_task"

        start_mapper = mock.Mock(spec=StartMapper)
        start_mapper.name = "start_task"

        first_node = OozieNode(mapper=first_mapper, tasks=[self._get_dummy_task(first_mapper.name)])
        start_node = OozieNode(mapper=start_mapper, tasks=[self._get_dummy_task(start_mapper.name)])

        start_node.error_downstream_name = first_node.name

        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[start_mapper.name] = start_node

        transformer.process_workflow_after_parse_workflow_xml(workflow)

        self.assertEqual({start_mapper.name, first_mapper.name}, set(workflow.nodes.keys()))
        self.assertEqual([], start_node.downstream_names)
        self.assertEqual([], first_node.downstream_names)
        self.assertEqual(first_mapper.name, start_node.error_downstream_name)
        self.assertEqual(None, first_node.error_downstream_name)

    def test_should_remove_inaccessible_node(self):
        """
        Graph before:

        .. graphviz::

           digraph foo {
              S -> A
              B -> A
           }

        Graph after:

        .. graphviz::

           digraph foo {
              S -> A
           }

        Where:
        A - first_task
        B - second_task
        S - start_task
        """
        transformer = RemoveInaccessibleNodeTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        first_mapper = mock.Mock(spec=BaseMapper)
        first_mapper.name = "first_task"

        second_mapper = mock.Mock(spec=BaseMapper)
        second_mapper.name = "second_task"

        start_mapper = mock.Mock(spec=StartMapper)
        start_mapper.name = "start_task"

        first_node = OozieNode(mapper=first_mapper, tasks=[self._get_dummy_task(first_mapper.name)])
        second_node = OozieNode(mapper=second_mapper, tasks=[self._get_dummy_task(second_mapper.name)])
        start_node = OozieNode(mapper=start_mapper, tasks=[self._get_dummy_task(start_mapper.name)])

        first_node.downstream_names = []
        second_node.downstream_names = [first_mapper.name]
        start_node.downstream_names = [first_mapper.name]

        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[second_mapper.name] = second_node
        workflow.nodes[start_mapper.name] = start_node
        transformer.process_workflow_after_parse_workflow_xml(workflow)

        self.assertEqual({start_mapper.name, first_mapper.name}, set(workflow.nodes.keys()))
        self.assertEqual([first_node.name], start_node.downstream_names)

    def test_should_remove_group_of_inaccessible_nodes(self):
        """
        Graph before:

        .. graphviz::

            digraph foo {
                A -> B
                B -> S
            }

        Graph after:

        .. graphviz::

            digraph foo {
                S
            }

        Where:
        A - first_task
        B - second_task
        S - start_task
        """
        transformer = RemoveInaccessibleNodeTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        first_mapper = mock.Mock(spec=BaseMapper)
        first_mapper.name = "first_task"

        second_mapper = mock.Mock(spec=BaseMapper)
        second_mapper.name = "second_task"

        start_mapper = mock.Mock(spec=StartMapper)
        start_mapper.name = "start_task"

        first_node = OozieNode(mapper=first_mapper, tasks=[self._get_dummy_task(first_mapper.name)])
        second_node = OozieNode(mapper=second_mapper, tasks=[self._get_dummy_task(second_mapper.name)])
        start_node = OozieNode(mapper=start_mapper, tasks=[self._get_dummy_task(start_mapper.name)])

        first_mapper.downstream_names = [second_mapper.name]
        second_mapper.downstream_names = [start_mapper.name]

        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[second_mapper.name] = second_node
        workflow.nodes[start_mapper.name] = start_node

        transformer.process_workflow_after_parse_workflow_xml(workflow)

        self.assertEqual({start_mapper.name}, set(workflow.nodes.keys()))
        self.assertEqual([second_mapper.name], first_mapper.downstream_names)
        self.assertEqual([start_mapper.name], second_mapper.downstream_names)

    @staticmethod
    def _get_dummy_task(task_id):
        return Task(task_id=task_id, template_name="dummy.tpl")
