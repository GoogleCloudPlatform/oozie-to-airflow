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
Remove End Transformer tests
"""
import unittest
from unittest import mock

from o2a.converter.parsed_action_node import ParsedActionNode
from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.converter.workflow import Workflow

from o2a.mappers.base_mapper import BaseMapper
from o2a.mappers.decision_mapper import DecisionMapper
from o2a.mappers.end_mapper import EndMapper
from o2a.transformers.remove_end_transformer import RemoveEndTransformer


class RemoveEndTransformerTest(unittest.TestCase):
    def test_should_remove_end_node(self):
        transformer = RemoveEndTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        other_mapper = mock.Mock(spec=BaseMapper)
        other_mapper.name = "first_task"
        end_mapper = mock.Mock(spec=EndMapper)
        end_mapper.name = "second_task"

        workflow.nodes[other_mapper.name] = ParsedActionNode(mapper=other_mapper)
        workflow.nodes[end_mapper.name] = ParsedActionNode(mapper=end_mapper)

        workflow.relations = {Relation(from_task_id=other_mapper.name, to_task_id=end_mapper.name)}

        transformer.process_workflow(workflow)

        self.assertEqual({other_mapper.name}, set(workflow.nodes.keys()))
        self.assertEqual(set(), workflow.relations)

    def test_should_not_remove_end_node_when_connected_with_decision(self):
        transformer = RemoveEndTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        decision_mapper = mock.Mock(spec=DecisionMapper)
        decision_mapper.name = "first_task"
        other_mapper = mock.Mock(spec=BaseMapper)
        other_mapper.name = "second_task"
        end_mapper = mock.Mock(spec=EndMapper)
        end_mapper.name = "end_task"

        workflow.nodes[decision_mapper.name] = ParsedActionNode(
            mapper=decision_mapper, tasks=[self._get_dummy_task(decision_mapper.name)]
        )
        workflow.nodes[other_mapper.name] = ParsedActionNode(
            mapper=other_mapper, tasks=[self._get_dummy_task(other_mapper.name)]
        )
        workflow.nodes[end_mapper.name] = ParsedActionNode(
            mapper=end_mapper, tasks=[self._get_dummy_task(end_mapper.name)]
        )

        workflow.relations = {
            Relation(from_task_id=decision_mapper.name, to_task_id=end_mapper.name),
            Relation(from_task_id=other_mapper.name, to_task_id=end_mapper.name),
        }

        transformer.process_workflow(workflow)

        self.assertEqual(
            {decision_mapper.name, other_mapper.name, end_mapper.name}, set(workflow.nodes.keys())
        )
        self.assertEqual(
            {Relation(from_task_id=decision_mapper.name, to_task_id=end_mapper.name)}, workflow.relations
        )

    @staticmethod
    def _get_dummy_task(task_id):
        return Task(task_id=task_id, template_name="dummy.tpl")
