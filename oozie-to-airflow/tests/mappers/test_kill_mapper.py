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
"""Tests Kill Mapper"""
import ast
import unittest
from unittest import mock
from xml.etree.ElementTree import Element
from airflow.utils.trigger_rule import TriggerRule

from converter.parsed_node import ParsedNode
from converter.primitives import Workflow, Relation, Task
from mappers import kill_mapper
from mappers.base_mapper import BaseMapper


class TestKillMapper(unittest.TestCase):

    oozie_node = Element("dummy")

    def test_create_mapper(self):
        mapper = self._get_kill_mapper()
        # make sure everything is getting initialized correctly
        self.assertEqual("test-id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)

    @mock.patch("mappers.kill_mapper.render_template", return_value="RETURN")
    def test_convert_to_text(self, render_template_mock):
        mapper = kill_mapper.KillMapper(
            oozie_node=self.oozie_node, name="test-id", trigger_rule=TriggerRule.DUMMY
        )

        res = mapper.convert_to_text()
        self.assertEqual(res, "RETURN")

        _, kwargs = render_template_mock.call_args
        tasks = kwargs["tasks"]
        relations = kwargs["relations"]

        self.assertEqual(kwargs["template_name"], "action.tpl")
        self.assertEqual(tasks, [Task(task_id="test-id", template_name="kill.tpl")])
        self.assertEqual(relations, [])

    def test_on_parse_finish(self):
        workflow = Workflow(input_directory_path=None, output_directory_path=None, dag_name=None)

        mapper = self._get_kill_mapper(name="fail_task")

        workflow.nodes["task"] = ParsedNode(mock.Mock(autospec=BaseMapper))
        workflow.nodes["fail_task"] = ParsedNode(mapper)
        workflow.nodes["success_task"] = ParsedNode(mock.Mock(autospec=BaseMapper))
        workflow.nodes["success_task"].set_is_ok(True)
        workflow.nodes["fail_task"].set_is_error(True)

        workflow.relations = {
            Relation(from_task_id="task", to_task_id="fail_task"),
            Relation(from_task_id="task", to_task_id="success_task"),
        }

        mapper.on_parse_finish(workflow)

        self.assertEqual(set(workflow.nodes.keys()), {"task", "success_task"})
        self.assertEqual(workflow.relations, {Relation(from_task_id="task", to_task_id="success_task")})

    def test_required_imports(self):
        mapper = self._get_kill_mapper()
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_kill_mapper(self, name="test-id"):
        return kill_mapper.KillMapper(oozie_node=self.oozie_node, name=name, trigger_rule=TriggerRule.DUMMY)
