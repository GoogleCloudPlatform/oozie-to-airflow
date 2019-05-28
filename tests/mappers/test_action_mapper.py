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
"""Tests action mapper"""
import unittest

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper

TEST_MAPPER_NAME = "mapper_name"
TEST_DAG_NAME = "dag_name"


class TestActionMapper(unittest.TestCase):
    def test_prepend_task_no_tasks(self):
        task_1 = Task(task_id=TEST_MAPPER_NAME + "_1", template_name="pig.tpl")
        with self.assertRaises(IndexError):
            ActionMapper.prepend_task(task_to_prepend=task_1, tasks=[], relations=[])

    def test_prepend_task_empty_relations(self):
        task_1 = Task(task_id=TEST_MAPPER_NAME + "_1", template_name="pig.tpl")
        task_2 = Task(task_id=TEST_MAPPER_NAME + "_2", template_name="pig.tpl")

        tasks, relations = ActionMapper.prepend_task(task_to_prepend=task_1, tasks=[task_2], relations=[])
        self.assertEqual([task_1, task_2], tasks)
        self.assertEqual([Relation(from_task_id="mapper_name_1", to_task_id="mapper_name_2")], relations)

    def test_prepend_task_some_relations(self):
        task_1 = Task(task_id=TEST_MAPPER_NAME + "_1", template_name="pig.tpl")
        task_2 = Task(task_id=TEST_MAPPER_NAME + "_2", template_name="pig.tpl")
        task_3 = Task(task_id=TEST_MAPPER_NAME + "_3", template_name="pig.tpl")

        tasks, relations = ActionMapper.prepend_task(
            task_to_prepend=task_1,
            tasks=[task_2, task_3],
            relations=[Relation(from_task_id="mapper_name_2", to_task_id="mapper_name_3")],
        )
        self.assertEqual([task_1, task_2, task_3], tasks)
        self.assertEqual(
            [
                Relation(from_task_id="mapper_name_1", to_task_id="mapper_name_2"),
                Relation(from_task_id="mapper_name_2", to_task_id="mapper_name_3"),
            ],
            relations,
        )
