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

from o2a.converter.parsed_action_node import ParsedActionNode
from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.converter.workflow import Workflow
from o2a.mappers.base_mapper import BaseMapper
from o2a.mappers.start_mapper import StartMapper
from o2a.transformers.remove_inaccessible_node_transformer import RemoveInaccessibleNodeTransformer


class RemoveEndTransformerTest(unittest.TestCase):
    def test_should_keep_connected_nodes(self):
        transformer = RemoveInaccessibleNodeTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        first_mapper = mock.Mock(spec=BaseMapper)
        first_mapper.name = "first_task"

        second_mapper = mock.Mock(spec=BaseMapper)
        second_mapper.name = "second_task"

        start_mapper = mock.Mock(spec=StartMapper)
        start_mapper.name = "start_task"

        first_node = ParsedActionNode(mapper=first_mapper, tasks=[self._get_dummy_task(first_mapper.name)])
        second_node = ParsedActionNode(mapper=second_mapper, tasks=[self._get_dummy_task(second_mapper.name)])
        start_node = ParsedActionNode(mapper=start_mapper, tasks=[self._get_dummy_task(start_mapper.name)])

        start_node.downstream_names = [first_mapper.name]
        first_node.downstream_names = [second_node.name]

        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[second_mapper.name] = second_node
        workflow.nodes[start_mapper.name] = start_node

        workflow.relations = {
            Relation(from_task_id=start_mapper.name, to_task_id=first_mapper.name),
            Relation(from_task_id=first_mapper.name, to_task_id=second_mapper.name),
        }

        transformer.process_workflow(workflow)

        self.assertEqual(
            {start_mapper.name, first_mapper.name, second_mapper.name}, set(workflow.nodes.keys())
        )
        self.assertEqual(
            {
                Relation(from_task_id=start_mapper.name, to_task_id=first_mapper.name),
                Relation(from_task_id=first_mapper.name, to_task_id=second_mapper.name),
            },
            workflow.relations,
        )

    def test_should_keep_connected_nodes_in_error_state(self):
        transformer = RemoveInaccessibleNodeTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        first_mapper = mock.Mock(spec=BaseMapper)
        first_mapper.name = "second_task"

        start_mapper = mock.Mock(spec=StartMapper)
        start_mapper.name = "start_task"

        second_node = ParsedActionNode(mapper=first_mapper, tasks=[self._get_dummy_task(first_mapper.name)])
        start_node = ParsedActionNode(mapper=start_mapper, tasks=[self._get_dummy_task(start_mapper.name)])

        start_node.error_xml = second_node.name

        workflow.nodes[first_mapper.name] = second_node
        workflow.nodes[start_mapper.name] = start_node

        workflow.relations = {
            Relation(from_task_id=start_mapper.name, to_task_id=first_mapper.name, is_error=True)
        }

        transformer.process_workflow(workflow)

        self.assertEqual({start_mapper.name, first_mapper.name}, set(workflow.nodes.keys()))
        self.assertEqual(
            {Relation(from_task_id=start_mapper.name, to_task_id=first_mapper.name, is_error=True)},
            workflow.relations,
        )

    def test_should_remove_inaccessible_node(self):
        transformer = RemoveInaccessibleNodeTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        first_mapper = mock.Mock(spec=BaseMapper)
        first_mapper.name = "first_task"

        second_mapper = mock.Mock(spec=BaseMapper)
        second_mapper.name = "second_task"

        start_mapper = mock.Mock(spec=StartMapper)
        start_mapper.name = "start_task"

        first_node = ParsedActionNode(mapper=first_mapper, tasks=[self._get_dummy_task(first_mapper.name)])
        second_node = ParsedActionNode(mapper=second_mapper, tasks=[self._get_dummy_task(second_mapper.name)])
        start_node = ParsedActionNode(mapper=start_mapper, tasks=[self._get_dummy_task(start_mapper.name)])
        start_node.downstream_names = [first_mapper.name]
        workflow.nodes[first_mapper.name] = first_node
        workflow.nodes[second_mapper.name] = second_node
        workflow.nodes[start_mapper.name] = start_node

        workflow.relations = {Relation(from_task_id=start_mapper.name, to_task_id=first_mapper.name)}

        transformer.process_workflow(workflow)

        self.assertEqual({start_mapper.name, first_mapper.name}, set(workflow.nodes.keys()))
        self.assertEqual(
            {Relation(from_task_id="start_task", to_task_id="first_task", is_error=False)}, workflow.relations
        )

    @staticmethod
    def _get_dummy_task(task_id):
        return Task(task_id=task_id, template_name="dummy.tpl")
