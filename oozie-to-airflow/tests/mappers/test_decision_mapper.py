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

import ast
import unittest

from mappers import decision_mapper
from airflow.utils.trigger_rule import TriggerRule
from xml.etree import ElementTree as ET


class TestDecisionMapper(unittest.TestCase):
    def setUp(self):
        doc = ET.Element("decision", attrib={"name": "decision"})
        switch = ET.SubElement(doc, "switch")
        case1 = ET.SubElement(switch, "case", attrib={"to": "task1"})
        case2 = ET.SubElement(switch, "case", attrib={"to": "task2"})
        case3 = ET.SubElement(switch, "default", attrib={"to": "task3"})

        case1.text = "False"
        case2.text = "True"
        # default does not have text

        self.et = ET.ElementTree(doc)

    def test_create_mapper(self):
        mapper = decision_mapper.DecisionMapper(
            oozie_node=self.et.getroot(), task_id="test_id", trigger_rule=TriggerRule.DUMMY
        )
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.et.getroot(), mapper.oozie_node)

    def test_convert_to_text(self):
        # TODO
        # decision mapper does not have the required EL parsing to correctly get
        # parsed, so once that is finished need to redo tests.
        mapper = decision_mapper.DecisionMapper(
            oozie_node=self.et.getroot(), task_id="test_id", trigger_rule=TriggerRule.DUMMY
        )
        ast.parse(mapper.convert_to_text())

    def test_required_imports(self):
        imps = decision_mapper.DecisionMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)
