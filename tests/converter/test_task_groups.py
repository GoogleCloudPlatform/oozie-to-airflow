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
"""Tests parsed node"""
import unittest


from o2a.converter.task import Task
from o2a.converter.task_group import TaskGroup


class TestTaskGroup(unittest.TestCase):
    def setUp(self):
        self.task_group = TaskGroup(
            name="task1", tasks=[Task(task_id="first_task_id", template_name="dummy.tpl")]
        )

    def test_add_downstream_node_name(self):
        self.task_group.downstream_names.append("task1")
        self.assertIn("task1", self.task_group.downstream_names)

    def test_set_downstream_error_node_name(self):
        self.task_group.error_downstream_name = "task1"
        self.assertIn("task1", self.task_group.error_downstream_name)


class TestParserNodeMultipleOperators(unittest.TestCase):
    def test_first_task_id(self):
        task_group = TaskGroup(name="TASK_GROUP", tasks=[Task(task_id="TASK", template_name="dummy.tpl")])
        self.assertEqual("TASK", task_group.first_task_id)

    def test_last_task_id_of_error_flow(self):
        task_group = TaskGroup(
            name="TASK_GROUP",
            error_downstream_name="AAAA",
            tasks=[Task(task_id="TASK", template_name="dummy.tpl")],
        )
        task_group.add_state_handler_if_needed()
        self.assertEqual("TASK_GROUP_error", task_group.last_task_id_of_error_flow)

    def test_last_task_id_of_ok_flow(self):
        task_group = TaskGroup(
            name="task1",
            error_downstream_name="AAAA",
            tasks=[Task(task_id="TASK", template_name="dummy.tpl")],
        )
        self.assertEqual("TASK", task_group.last_task_id_of_ok_flow)

    @staticmethod
    def _get_dummy_task(task_id):
        return Task(task_id=task_id, template_name="dummy.tpl")

    @classmethod
    def _get_tasks(cls):
        return [
            cls._get_dummy_task("first_task_id"),
            cls._get_dummy_task("second_task_id"),
            cls._get_dummy_task("last_task_id"),
        ]
