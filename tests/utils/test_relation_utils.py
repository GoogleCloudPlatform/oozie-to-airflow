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
"""Tests Relation utils"""
import unittest

from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers import fs_mapper


# pylint: disable=invalid-name
class chainTestCase(unittest.TestCase):
    def test_empty(self):
        relations = fs_mapper.chain([])
        self.assertEqual(relations, [])

    def test_one(self):
        relations = fs_mapper.chain([Task(task_id="A", template_name="")])
        self.assertEqual(relations, [])

    def test_multiple(self):
        relations = fs_mapper.chain(
            [
                Task(task_id="task_1", template_name=""),
                Task(task_id="task_2", template_name=""),
                Task(task_id="task_3", template_name=""),
                Task(task_id="task_4", template_name=""),
            ]
        )
        self.assertEqual(
            relations,
            [
                Relation(from_task_id="task_1", to_task_id="task_2"),
                Relation(from_task_id="task_2", to_task_id="task_3"),
                Relation(from_task_id="task_3", to_task_id="task_4"),
            ],
        )
