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
Remove Start Transformer tests
"""
import unittest
from unittest import mock

from o2a.converter.oozie_node import OozieNode
from o2a.converter.task import Task
from o2a.converter.workflow import Workflow
from o2a.mappers.base_mapper import BaseMapper
from o2a.mappers.start_mapper import StartMapper
from o2a.transformers.remove_start_transformer import RemoveStartTransformer


class RemoveEndTransformerTest(unittest.TestCase):
    def test_should_remove_start_node(self):
        transformer = RemoveStartTransformer()

        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")

        other_mapper = mock.Mock(spec=BaseMapper)
        other_mapper.name = "first_task"
        start_mapper = mock.Mock(spec=StartMapper)
        start_mapper.name = "start_task"

        workflow.nodes[other_mapper.name] = OozieNode(
            mapper=other_mapper, tasks=[self._get_dummy_task(other_mapper.name)]
        )
        workflow.nodes[start_mapper.name] = OozieNode(
            mapper=start_mapper, tasks=[self._get_dummy_task(start_mapper.name)]
        )

        transformer.process_workflow_after_parse_workflow_xml(workflow)

        self.assertEqual({other_mapper.name}, set(workflow.nodes.keys()))

    @staticmethod
    def _get_dummy_task(task_id):
        return Task(task_id=task_id, template_name="dummy.tpl")
