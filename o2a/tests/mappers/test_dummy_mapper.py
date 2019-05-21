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
"""Tests for the Dummy mapper."""
import ast
import unittest
from xml.etree.ElementTree import Element
from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.task import Task
from o2a.mappers import dummy_mapper


class TestDummyMapper(unittest.TestCase):
    oozie_node = Element("dummy")

    def test_create_mapper(self):
        mapper = self._get_dummy_mapper()
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)

    def test_convert_tasks_and_relations(self):
        mapper = self._get_dummy_mapper()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(tasks, [Task(task_id="test_id", template_name="dummy.tpl")])
        self.assertEqual(relations, [])

    def test_required_imports(self):
        mapper = self._get_dummy_mapper()
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_dummy_mapper(self):
        return dummy_mapper.DummyMapper(
            oozie_node=self.oozie_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )
