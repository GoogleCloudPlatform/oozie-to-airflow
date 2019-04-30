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
from unittest import mock
from xml.etree.ElementTree import Element
from airflow.utils.trigger_rule import TriggerRule

from converter.primitives import Task
from mappers import dummy_mapper


class TestDummyMapper(unittest.TestCase):
    oozie_node = Element("dummy")

    def test_create_mapper(self):
        mapper = dummy_mapper.DummyMapper(
            oozie_node=self.oozie_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)

    @mock.patch("mappers.dummy_mapper.render_template", return_value="RETURN")
    def test_convert_to_text(self, render_template_mock):
        mapper = dummy_mapper.DummyMapper(
            oozie_node=self.oozie_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )

        res = mapper.convert_to_text()
        self.assertEqual(res, "RETURN")

        _, kwargs = render_template_mock.call_args
        tasks = kwargs["tasks"]
        relations = kwargs["relations"]

        self.assertEqual(kwargs["template_name"], "action.tpl")
        self.assertEqual(
            tasks,
            [Task(task_id="test_id", template_name="dummy.tpl", template_params={"trigger_rule": "dummy"})],
        )
        self.assertEqual(relations, [])

    # pylint: disable=no-self-use
    def test_required_imports(self):
        imps = dummy_mapper.DummyMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)
