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
"""Tests for the Start mapper."""
import ast
import unittest
from unittest import mock
from xml.etree.ElementTree import Element
from airflow.utils.trigger_rule import TriggerRule

from converter.parsed_node import ParsedNode
from converter.primitives import Workflow, Relation
from mappers.base_mapper import BaseMapper
from mappers.start_mapper import StartMapper


class TestStartMapper(unittest.TestCase):
    oozie_node = Element("start")

    def test_create_mapper(self):
        mapper = StartMapper(oozie_node=self.oozie_node, name="test_id", trigger_rule=TriggerRule.DUMMY)
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)

    def test_convert_to_text(self):
        mapper = StartMapper(oozie_node=self.oozie_node, name="test_id", trigger_rule=TriggerRule.DUMMY)
        self.assertEqual("", mapper.convert_to_text())

    # pylint: disable=no-self-use
    def test_required_imports(self):
        imps = StartMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def test_on_parse_finish(self):
        workflow = Workflow(input_directory_path=None, output_directory_path=None, dag_name=None)

        mapper = StartMapper(oozie_node=self.oozie_node, name="first_task", trigger_rule=TriggerRule.DUMMY)

        workflow.nodes["first_task"] = ParsedNode(mock.Mock(autospec=BaseMapper))
        workflow.nodes["second_task"] = ParsedNode(mapper)

        workflow.relations = {Relation(from_task_id="first_task", to_task_id="second_task")}

        mapper.on_parse_finish(workflow)

        self.assertEqual(set(workflow.nodes.keys()), {"second_task"})
        self.assertEqual(workflow.relations, set())
