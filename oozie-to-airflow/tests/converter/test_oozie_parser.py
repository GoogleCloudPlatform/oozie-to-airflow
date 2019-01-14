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
import os
import unittest
from unittest import mock
from xml.etree import ElementTree as ET

from converter import oozie_parser
from converter import parsed_node
from definitions import ROOT_DIR
from mappers import dummy_mapper
from mappers import ssh_mapper


class TestOozieParser(unittest.TestCase):
    def setUp(self):
        params = {}
        self.parser = oozie_parser.OozieParser(None, params=params)

    def test_parse_kill_node(self):
        NODE_NAME = 'kill_name'
        kill = ET.Element('kill', attrib={'name': NODE_NAME})
        message = ET.SubElement(kill, 'message')
        message.text = 'kill-text-to-log'

        self.parser._parse_kill_node(kill)

        self.assertIn(NODE_NAME, self.parser.OPERATORS)
        for depend in self.parser.OPERATORS[
            NODE_NAME].operator.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    def test_parse_end_node(self):
        NODE_NAME = 'end_name'
        end = ET.Element('end', attrib={'name': NODE_NAME})

        self.parser._parse_end_node(end)

        self.assertIn(NODE_NAME, self.parser.OPERATORS)
        for depend in self.parser.OPERATORS[
            NODE_NAME].operator.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    @mock.patch('converter.oozie_parser.OozieParser.parse_node')
    def test_parse_fork_node(self, parse_node_mock):
        NODE_NAME = 'fork_name'
        root = ET.Element('root')
        fork = ET.SubElement(root, 'fork', attrib={'name': NODE_NAME})
        path1 = ET.SubElement(fork, 'path', attrib={'start': 'task1'})
        path2 = ET.SubElement(fork, 'path', attrib={'start': 'task2'})
        node1 = ET.SubElement(root, 'action', attrib={'name': 'task1'})
        node2 = ET.SubElement(root, 'action', attrib={'name': 'task2'})
        join = ET.SubElement(root, 'join',
                             attrib={'name': 'join', 'to': 'end_node'})
        end = ET.SubElement(root, 'end', attrib={'name': 'end_node'})

        self.parser._parse_fork_node(root, fork)

        p_op = self.parser.OPERATORS[NODE_NAME]
        self.assertIn('task1', p_op.get_downstreams())
        self.assertIn('task2', p_op.get_downstreams())
        self.assertIn(NODE_NAME, self.parser.OPERATORS)
        parse_node_mock.assert_any_call(root, node1)
        parse_node_mock.assert_any_call(root, node2)
        for depend in p_op.operator.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    def test_parse_join_node(self):
        NODE_NAME = 'join_name'
        END_NAME = 'end_name'
        join = ET.Element('join', attrib={'name': NODE_NAME, 'to': END_NAME})

        self.parser._parse_join_node(join)

        p_op = self.parser.OPERATORS[NODE_NAME]
        self.assertIn(NODE_NAME, self.parser.OPERATORS)
        self.assertIn(END_NAME, p_op.get_downstreams())
        for depend in p_op.operator.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    def test_parse_decision_node(self):
        NODE_NAME = 'decision_node'
        decision = ET.Element('decision', attrib={'name': NODE_NAME})
        switch = ET.SubElement(decision, 'switch')
        case1 = ET.SubElement(switch, 'case', attrib={'to': 'down1'})
        case1.text = '${fs:fileSize(secondjobOutputDir) gt 10 * GB}'
        case2 = ET.SubElement(switch, 'case', attrib={'to': 'down2'})
        case2.text = '${fs:filSize(secondjobOutputDir) lt 100 * MB}'
        default = ET.SubElement(switch, 'switch', attrib={'to': 'end1'})

        self.parser._parse_decision_node(decision)

        p_op = self.parser.OPERATORS[NODE_NAME]
        self.assertIn(NODE_NAME, self.parser.OPERATORS)
        self.assertIn('down1', p_op.get_downstreams())
        self.assertIn('down2', p_op.get_downstreams())
        self.assertIn('end1', p_op.get_downstreams())
        for depend in p_op.operator.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    @mock.patch('uuid.uuid4')
    def test_parse_start_node(self, uuid_mock):
        uuid_mock.return_value = '1234'
        NODE_NAME = 'start_node_1234'
        END_NAME = 'end_name'
        start = ET.Element('start', attrib={'to': END_NAME})

        self.parser._parse_start_node(start)

        p_op = self.parser.OPERATORS[NODE_NAME]
        self.assertIn(NODE_NAME, self.parser.OPERATORS)
        self.assertIn(END_NAME, p_op.get_downstreams())
        for depend in p_op.operator.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    def test_parse_action_node_ssh(self):
        self.parser.ACTION_MAP = {'ssh': ssh_mapper.SSHMapper}
        NODE_NAME = 'action_name'
        # Set up XML to mimic Oozie
        action = ET.Element('action', attrib={'name': NODE_NAME})
        ssh = ET.SubElement(action, 'ssh')
        ok = ET.SubElement(action, 'ok', attrib={'to': 'end1'})
        error = ET.SubElement(action, 'error', attrib={'to': 'fail1'})
        host = ET.SubElement(ssh, 'host')
        command = ET.SubElement(ssh, 'command')
        args1 = ET.SubElement(ssh, 'args')
        args2 = ET.SubElement(ssh, 'args')
        cap_out = ET.SubElement(ssh, 'capture-output')

        host.text = 'user@apache.org'
        command.text = 'ls'
        args1.text = '-l'
        args2.text = '-a'
        # default does not have text

        self.parser._parse_action_node(action)

        p_op = self.parser.OPERATORS[NODE_NAME]
        self.assertIn(NODE_NAME, self.parser.OPERATORS)
        self.assertIn('end1', p_op.get_downstreams())
        self.assertEqual('fail1', p_op.get_error_downstream_name())
        for depend in p_op.operator.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    def test_parse_action_node_unknown(self):
        self.parser.ACTION_MAP = {'unknown': dummy_mapper.DummyMapper}
        NODE_NAME = 'action_name'
        # Set up XML to mimic Oozie
        action = ET.Element('action', attrib={'name': NODE_NAME})
        ssh = ET.SubElement(action, 'ssh')
        ET.SubElement(action, 'ok', attrib={'to': 'end1'})
        ET.SubElement(action, 'error', attrib={'to': 'fail1'})
        ET.SubElement(ssh, 'host')
        ET.SubElement(ssh, 'command')
        ET.SubElement(ssh, 'args')
        ET.SubElement(ssh, 'args')
        ET.SubElement(ssh, 'capture-output')

        self.parser._parse_action_node(action)

        p_op = self.parser.OPERATORS[NODE_NAME]
        self.assertIn(NODE_NAME, self.parser.OPERATORS)
        self.assertIn('end1', p_op.get_downstreams())
        self.assertEqual('fail1', p_op.get_error_downstream_name())
        for depend in p_op.operator.required_imports():
            self.assertIn(depend, self.parser.DEPENDENCIES)

    @mock.patch('converter.oozie_parser.OozieParser._parse_action_node')
    def test_parse_node_action(self, action_mock):
        root = ET.Element('root')
        action = ET.SubElement(root, 'action', attrib={'name': 'test_name'})
        self.parser.parse_node(root, action)
        action_mock.assert_called_once_with(action)

    @mock.patch('converter.oozie_parser.OozieParser._parse_start_node')
    def test_parse_node_start(self, start_mock):
        root = ET.Element('root')
        start = ET.SubElement(root, 'start', attrib={'name': 'test_name'})
        self.parser.parse_node(root, start)
        start_mock.assert_called_once_with(start)

    @mock.patch('converter.oozie_parser.OozieParser._parse_kill_node')
    def test_parse_node_kill(self, kill_mock):
        root = ET.Element('root')
        kill = ET.SubElement(root, 'kill', attrib={'name': 'test_name'})
        self.parser.parse_node(root, kill)
        kill_mock.assert_called_once_with(kill)

    @mock.patch('converter.oozie_parser.OozieParser._parse_end_node')
    def test_parse_node_end(self, end_mock):
        root = ET.Element('root')
        end = ET.SubElement(root, 'end', attrib={'name': 'test_name'})
        self.parser.parse_node(root, end)
        end_mock.assert_called_once_with(end)

    @mock.patch('converter.oozie_parser.OozieParser._parse_fork_node')
    def test_parse_node_fork(self, fork_mock):
        root = ET.Element('root')
        fork = ET.SubElement(root, 'fork', attrib={'name': 'test_name'})
        self.parser.parse_node(root, fork)
        fork_mock.assert_called_once_with(root, fork)

    @mock.patch('converter.oozie_parser.OozieParser._parse_join_node')
    def test_parse_node_join(self, join_mock):
        root = ET.Element('root')
        join = ET.SubElement(root, 'join', attrib={'name': 'test_name'})
        self.parser.parse_node(root, join)
        join_mock.assert_called_once_with(join)

    @mock.patch('converter.oozie_parser.OozieParser._parse_decision_node')
    def test_parse_node_decision(self, decision_mock):
        root = ET.Element('root')
        decision = ET.SubElement(root, 'decision', attrib={'name': 'test_name'})
        self.parser.parse_node(root, decision)
        decision_mock.assert_called_once_with(decision)

    def test_create_relations(self):
        op1 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(None, 'task1'))
        op1.downstream_names = ['task2', 'task3']
        op1.error_xml = 'fail1'
        op2 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(None, 'task2'))
        op2.downstream_names = ['task3']
        op2.error_xml = 'fail1'
        op3 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(None, 'task3'))
        op3.downstream_names = ['end1']
        op3.error_xml = 'fail1'
        end = parsed_node.ParsedNode(dummy_mapper.DummyMapper(None, 'end1'))
        fail = parsed_node.ParsedNode(dummy_mapper.DummyMapper(None, 'fail1'))
        op_dict = {'task1': op1, 'task2': op2, 'task3': op3, 'end1': end,
                   'fail1': fail}
        self.parser.OPERATORS.update(op_dict)
        self.parser.create_relations()

        self.assertIn('task3.set_downstream(fail1)', self.parser.relations)
        self.assertIn('task3.set_downstream(end1)', self.parser.relations)
        self.assertIn('task1.set_downstream(task2)', self.parser.relations)
        self.assertIn('task1.set_downstream(task3)', self.parser.relations)
        self.assertIn('task2.set_downstream(task3)', self.parser.relations)
        self.assertIn('task2.set_downstream(fail1)', self.parser.relations)
        self.assertIn('task1.set_downstream(fail1)', self.parser.relations)

    def test_update_trigger_rules(self):
        op1 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(None, 'task1'))
        op1.downstream_names = ['task2', 'task3']
        op1.error_xml = 'fail1'
        op2 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(None, 'task2'))
        op2.downstream_names = ['task3']
        op2.error_xml = 'fail1'
        op3 = parsed_node.ParsedNode(dummy_mapper.DummyMapper(None, 'task3'))
        op3.downstream_names = ['end1']
        op3.error_xml = 'fail1'
        end = parsed_node.ParsedNode(dummy_mapper.DummyMapper(None, 'end1'))
        fail = parsed_node.ParsedNode(dummy_mapper.DummyMapper(None, 'fail1'))
        op_dict = {'task1': op1, 'task2': op2, 'task3': op3, 'end1': end,
                   'fail1': fail}

        self.parser.OPERATORS.update(op_dict)
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
        filename = os.path.join(ROOT_DIR, 'examples/demo/workflow.xml')
        self.parser.workflow = filename
        self.parser.parse_workflow()
        # Checking if names were changed to the Python syntax
        self.assertIn('cleanup_node', self.parser.OPERATORS)
        self.assertIn('fork_node', self.parser.OPERATORS)
        self.assertIn('pig_node', self.parser.OPERATORS)
        self.assertIn('fail', self.parser.OPERATORS)
