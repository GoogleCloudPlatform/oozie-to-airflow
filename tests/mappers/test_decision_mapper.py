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
"""Tests decision_mapper"""
import ast
import unittest
from collections import OrderedDict

from xml.etree import ElementTree as ET
from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.task import Task
from o2a.mappers import decision_mapper


class TestDecisionMapper(unittest.TestCase):
    def setUp(self):
        # language=XML
        decision_node_str = """
<decision name="decision">
    <switch>
        <case to="task1">${firstNotNull('', '')}</case>
        <case to="task2">True</case>
        <default to="task3" />
    </switch>
</decision>
"""
        self.decision_node = ET.fromstring(decision_node_str)

    def test_create_mapper(self):
        mapper = self._get_decision_mapper()
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.decision_node, mapper.oozie_node)
        # test conversion from Oozie EL to Jinja
        self.assertEqual("first_not_null('', '')", next(iter(mapper.case_dict)))

    def test_to_tasks_and_relations(self):
        # TODO
        # decision mapper does not have the required EL parsing to correctly get
        # parsed, so once that is finished need to redo tests.
        mapper = self._get_decision_mapper()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            tasks,
            [
                Task(
                    task_id="test_id",
                    template_name="decision.tpl",
                    template_params={
                        "case_dict": OrderedDict(
                            [("first_not_null('', '')", "task1"), ("'True'", "task2"), ("default", "task3")]
                        )
                    },
                )
            ],
        )
        self.assertEqual(relations, [])

    def test_required_imports(self):
        mapper = self._get_decision_mapper()
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_decision_mapper(self):
        return decision_mapper.DecisionMapper(
            oozie_node=self.decision_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )
