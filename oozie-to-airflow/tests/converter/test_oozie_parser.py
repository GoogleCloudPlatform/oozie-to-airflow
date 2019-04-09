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
"""Tests oozie parser"""
import os
import unittest
from unittest import mock
from xml.etree import ElementTree as ET

from converter import parser
from converter import parsed_node
from converter.mappers import ACTION_MAP, CONTROL_MAP
from converter.relation import Relation
from definitions import ROOT_DIR
from mappers import dummy_mapper
from mappers import ssh_mapper
from tests.utils.test_paths import EXAMPLE_DEMO_PATH


class TestOozieParser(unittest.TestCase):
    def setUp(self):
        params = {}
        self.parser = parser.OozieParser(
            input_directory_path=EXAMPLE_DEMO_PATH,
            output_directory_path="/tmp",
            params=params,
            action_mapper=ACTION_MAP,
            control_mapper=CONTROL_MAP,
        )

    def test_parse_kill_node(self):
        node_name = "kill_name"
        # language=XML
        kill_string = """
<kill name="{node_name}">
    <message>kill-text-to-log</message>
</kill>
""".format(
            node_name=node_name
        )
        self.parser.parse_kill_node(ET.fromstring(kill_string))

        self.assertIn(node_name, self.parser.nodes)
        for depend in self.parser.nodes[node_name].mapper.required_imports():
            self.assertIn(depend, self.parser.dependencies)

    def test_parse_end_node(self):
        node_name = "end_name"
        # language=XML
        end_node_str = "<end name='{node_name}'/>".format(node_name=node_name)

        end = ET.fromstring(end_node_str)
        self.parser.parse_end_node(end)

        self.assertIn(node_name, self.parser.nodes)
        for depend in self.parser.nodes[node_name].mapper.required_imports():
            self.assertIn(depend, self.parser.dependencies)

    @mock.patch("converter.parser.OozieParser.parse_node")
    def test_parse_fork_node(self, parse_node_mock):
        node_name = "fork_name"
        # language=XML
        root_string = """
<root>
    <fork name="{node_name}">
        <path start="task1" />
        <path start="task2" />
    </fork>
    <action name="task1" />
    <action name="task2" />
    <join name="join" to="end_node" />
    <end name="end_node" />
</root>
""".format(
            node_name=node_name
        )
        root = ET.fromstring(root_string)
        fork = root.find("fork")
        node1, node2 = root.findall("action")[0:2]
        self.parser.parse_fork_node(root, fork)
        node = self.parser.nodes[node_name]
        self.assertIn("task1", node.get_downstreams())
        self.assertIn("task2", node.get_downstreams())
        self.assertIn(node_name, self.parser.nodes)
        parse_node_mock.assert_any_call(root, node1)
        parse_node_mock.assert_any_call(root, node2)
        for depend in node.mapper.required_imports():
            self.assertIn(depend, self.parser.dependencies)

    def test_parse_join_node(self):
        node_name = "join_name"
        end_name = "end_name"
        # language=XML
        join_str = '<join name="{node_name}" to="{end_name}" />'.format(
            node_name=node_name, end_name=end_name
        )
        join = ET.fromstring(join_str)
        self.parser.parse_join_node(join)

        node = self.parser.nodes[node_name]
        self.assertIn(node_name, self.parser.nodes)
        self.assertIn(end_name, node.get_downstreams())
        for depend in node.mapper.required_imports():
            self.assertIn(depend, self.parser.dependencies)

    def test_parse_decision_node(self):
        node_name = "decision_node"
        # language=XML
        decision_str = """
<decision name="{node_name}">
    <switch>
        <case to="down1">${{fs:fileSize(secondjobOutputDir) gt 10 * GB}}</case>
        <case to="down2">${{fs:filSize(secondjobOutputDir) lt 100 * MB}}</case>
        <default to="end1" />
    </switch>
    </decision>
""".format(
            node_name=node_name
        )
        decision = ET.fromstring(decision_str)
        self.parser.parse_decision_node(decision)

        p_op = self.parser.nodes[node_name]
        self.assertIn(node_name, self.parser.nodes)
        self.assertIn("down1", p_op.get_downstreams())
        self.assertIn("down2", p_op.get_downstreams())
        self.assertIn("end1", p_op.get_downstreams())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.dependencies)

    @mock.patch("uuid.uuid4")
    def test_parse_start_node(self, uuid_mock):
        uuid_mock.return_value = "1234"
        node_name = "start_node_1234"
        end_name = "end_name"
        # language=XML
        start_node_str = "<start to='{end_name}'/>".format(end_name=end_name)
        start = ET.fromstring(start_node_str)
        self.parser.parse_start_node(start)

        p_op = self.parser.nodes[node_name]
        self.assertIn(node_name, self.parser.nodes)
        self.assertIn(end_name, p_op.get_downstreams())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.dependencies)

    def test_parse_action_node_ssh(self):
        self.parser.action_map = {"ssh": ssh_mapper.SSHMapper}
        node_name = "action_name"
        # language=XML
        action_string = """
<action name='{node_name}'>
    <ssh>
        <host>user@apache.org</host>
        <command>ls</command>
        <args>-l</args>
        <args>-a</args>
        <capture-output/>
    </ssh>
    <ok to='end1'/>
    <error to='fail1'/>
</action>
""".format(
            node_name=node_name
        )
        action_node = ET.fromstring(action_string)
        self.parser.parse_action_node(action_node)

        p_op = self.parser.nodes[node_name]
        self.assertIn(node_name, self.parser.nodes)
        self.assertIn("end1", p_op.get_downstreams())
        self.assertEqual("fail1", p_op.get_error_downstream_name())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.dependencies)

    def test_parse_action_node_unknown(self):
        self.parser.action_map = {"unknown": dummy_mapper.DummyMapper}
        node_name = "action_name"
        # language=XML
        action_str = """
<action name="action_name">
    <ssh><host />
    <command />
    <args />
    <args />
    <capture-output />
    </ssh>
    <ok to="end1" />
    <error to="fail1" />
</action>
""".format(
            node_name=node_name
        )
        action = ET.fromstring(action_str)
        self.parser.parse_action_node(action)

        p_op = self.parser.nodes[node_name]
        self.assertIn(node_name, self.parser.nodes)
        self.assertIn("end1", p_op.get_downstreams())
        self.assertEqual("fail1", p_op.get_error_downstream_name())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.dependencies)

    @mock.patch("converter.parser.OozieParser.parse_action_node")
    def test_parse_node_action(self, action_mock):
        root = ET.Element("root")
        action = ET.SubElement(root, "action", attrib={"name": "test_name"})
        self.parser.parse_node(root, action)
        action_mock.assert_called_once_with(action)

    @mock.patch("converter.parser.OozieParser.parse_start_node")
    def test_parse_node_start(self, start_mock):
        root = ET.Element("root")
        start = ET.SubElement(root, "start", attrib={"name": "test_name"})
        self.parser.parse_node(root, start)
        start_mock.assert_called_once_with(start)

    @mock.patch("converter.parser.OozieParser.parse_kill_node")
    def test_parse_node_kill(self, kill_mock):
        root = ET.Element("root")
        kill = ET.SubElement(root, "kill", attrib={"name": "test_name"})
        self.parser.parse_node(root, kill)
        kill_mock.assert_called_once_with(kill)

    @mock.patch("converter.parser.OozieParser.parse_end_node")
    def test_parse_node_end(self, end_mock):
        root = ET.Element("root")
        end = ET.SubElement(root, "end", attrib={"name": "test_name"})
        self.parser.parse_node(root, end)
        end_mock.assert_called_once_with(end)

    @mock.patch("converter.parser.OozieParser.parse_fork_node")
    def test_parse_node_fork(self, fork_mock):
        root = ET.Element("root")
        fork = ET.SubElement(root, "fork", attrib={"name": "test_name"})
        self.parser.parse_node(root, fork)
        fork_mock.assert_called_once_with(root, fork)

    @mock.patch("converter.parser.OozieParser.parse_join_node")
    def test_parse_node_join(self, join_mock):
        root = ET.Element("root")
        join = ET.SubElement(root, "join", attrib={"name": "test_name"})
        self.parser.parse_node(root, join)
        join_mock.assert_called_once_with(join)

    @mock.patch("converter.parser.OozieParser.parse_decision_node")
    def test_parse_node_decision(self, decision_mock):
        root = ET.Element("root")
        decision = ET.SubElement(root, "decision", attrib={"name": "test_name"})
        self.parser.parse_node(root, decision)
        decision_mock.assert_called_once_with(decision)

    def test_create_relations(self):
        oozie_node = ET.Element("dummy")
        op1 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task1"))
        op1.downstream_names = ["task2", "task3"]
        op1.error_xml = "fail1"
        op2 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task2"))
        op2.downstream_names = ["task3", "task4"]
        op2.error_xml = "fail1"
        op3 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task3"))
        op3.downstream_names = ["end1"]
        op3.error_xml = "fail1"
        op4 = mock.Mock(
            **{
                "first_task_id": "task4_first",
                "last_task_id": "task4_last",
                "get_downstreams.return_value": ["task1", "task2", "task3"],
                "get_error_downstream_name.return_value": "fail1",
            }
        )
        end = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=oozie_node, name="end1"))
        fail = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=oozie_node, name="fail1"))
        op_dict = {"task1": op1, "task2": op2, "task3": op3, "task4": op4, "end1": end, "fail1": fail}
        self.parser.nodes.update(op_dict)
        self.parser.create_relations()

        self.assertEqual(
            self.parser.relations,
            {
                Relation(from_name="task1", to_name="fail1"),
                Relation(from_name="task1", to_name="task2"),
                Relation(from_name="task1", to_name="task3"),
                Relation(from_name="task2", to_name="fail1"),
                Relation(from_name="task2", to_name="task3"),
                Relation(from_name="task2", to_name="task4_first"),
                Relation(from_name="task3", to_name="end1"),
                Relation(from_name="task3", to_name="fail1"),
                Relation(from_name="task4_last", to_name="fail1"),
                Relation(from_name="task4_last", to_name="task1"),
                Relation(from_name="task4_last", to_name="task2"),
                Relation(from_name="task4_last", to_name="task3"),
            },
        )

    def test_update_trigger_rules(self):
        oozie_node = ET.Element("dummy")
        op1 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task1"))
        op1.downstream_names = ["task2", "task3"]
        op1.error_xml = "fail1"
        op2 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task2"))
        op2.downstream_names = ["task3"]
        op2.error_xml = "fail1"
        op3 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task3"))
        op3.downstream_names = ["end1"]
        op3.error_xml = "fail1"
        end = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=oozie_node, name="end1"))
        fail = parsed_node.ParsedNode(dummy_mapper.DummyMapper(oozie_node=oozie_node, name="fail1"))
        op_dict = {"task1": op1, "task2": op2, "task3": op3, "end1": end, "fail1": fail}

        self.parser.nodes.update(op_dict)
        self.parser.create_relations()
        self.parser.update_trigger_rules()

        self.assertFalse(op1.is_ok)
        self.assertFalse(op1.is_error)
        self.assertTrue(op2.is_ok)
        self.assertFalse(op2.is_error)
        self.assertTrue(op3.is_ok)
        self.assertFalse(op2.is_error)
        self.assertTrue(end.is_ok)
        self.assertFalse(end.is_error)
        self.assertFalse(fail.is_ok)
        self.assertTrue(fail.is_error)

    def test_parse_workflow(self):
        filename = os.path.join(ROOT_DIR, "examples/demo/workflow.xml")
        self.parser.workflow = filename
        self.parser.parse_workflow()
        # Checking if names were changed to the Python syntax
        self.assertIn("cleanup_node", self.parser.nodes)
        self.assertIn("fork_node", self.parser.nodes)
        self.assertIn("pig_node", self.parser.nodes)
        self.assertIn("fail", self.parser.nodes)
