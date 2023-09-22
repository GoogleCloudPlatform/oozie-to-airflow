# Copyright 2018 Google LLC
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
import unittest

from airflow.utils.trigger_rule import TriggerRule

from converter import parsed_node
from mappers import dummy_mapper


class TestParsedNode(unittest.TestCase):
    def setUp(self):
        op1 = dummy_mapper.DummyMapper(oozie_node=None, task_id="task1")
        self.p_node = parsed_node.ParsedNode(op1)

    def test_add_downstream_node_name(self):
        self.p_node.add_downstream_node_name("task1")
        self.assertIn("task1", self.p_node.get_downstreams())
        self.assertIn("task1", self.p_node.downstream_names)

    def test_set_downstream_error_node_name(self):
        self.p_node.set_error_node_name("task1")
        self.assertIn("task1", self.p_node.get_error_downstream_name())
        self.assertIn("task1", self.p_node.error_xml)

    def test_update_trigger_rule_both(self):
        self.p_node.set_is_ok(True)
        self.p_node.set_is_error(True)
        self.p_node.update_trigger_rule()
        self.assertEqual(TriggerRule.DUMMY, self.p_node.operator.trigger_rule)

    def test_update_trigger_rule_ok(self):
        self.p_node.set_is_ok(True)
        self.p_node.set_is_error(False)
        self.p_node.update_trigger_rule()
        self.assertEqual(TriggerRule.ONE_SUCCESS, self.p_node.operator.trigger_rule)

    def test_update_trigger_rule_error(self):
        self.p_node.set_is_ok(False)
        self.p_node.set_is_error(True)
        self.p_node.update_trigger_rule()
        self.assertEqual(TriggerRule.ONE_FAILED, self.p_node.operator.trigger_rule)

    def test_update_trigger_rule_(self):
        self.p_node.set_is_ok(False)
        self.p_node.set_is_error(False)
        self.p_node.update_trigger_rule()
        self.assertEqual(TriggerRule.DUMMY, self.p_node.operator.trigger_rule)
