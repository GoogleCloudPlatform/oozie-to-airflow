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
from os import path
import typing
import unittest
from unittest import mock
from xml.etree import ElementTree as ET

from parameterized import parameterized

from o2a.converter import parser
from o2a.converter import parsed_node
from o2a.converter.mappers import ACTION_MAP, CONTROL_MAP
from o2a.converter.relation import Relation
from o2a.definitions import EXAMPLE_DEMO_PATH, EXAMPLES_PATH
from o2a.mappers import dummy_mapper, pig_mapper
from o2a.mappers import ssh_mapper


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

    @mock.patch("o2a.mappers.kill_mapper.KillMapper.on_parse_node", wraps=None)
    def test_parse_kill_node(self, on_parse_node_mock):
        node_name = "kill_name"
        # language=XML
        kill_string = f"""
<kill name="{node_name}">
    <message>kill-text-to-log</message>
</kill>
"""
        self.parser.parse_kill_node(ET.fromstring(kill_string))

        self.assertIn(node_name, self.parser.workflow.nodes)
        for depend in self.parser.workflow.nodes[node_name].mapper.required_imports():
            self.assertIn(depend, self.parser.workflow.dependencies)

        on_parse_node_mock.assert_called_once_with()

    @mock.patch("o2a.mappers.end_mapper.EndMapper.on_parse_node", wraps=None)
    def test_parse_end_node(self, on_parse_node_mock):
        node_name = "end_name"
        # language=XML
        end_node_str = f"<end name='{node_name}'/>"

        end = ET.fromstring(end_node_str)
        self.parser.parse_end_node(end)

        self.assertIn(node_name, self.parser.workflow.nodes)
        for depend in self.parser.workflow.nodes[node_name].mapper.required_imports():
            self.assertIn(depend, self.parser.workflow.dependencies)

        on_parse_node_mock.assert_called_once_with()

    @mock.patch("o2a.mappers.dummy_mapper.DummyMapper.on_parse_node", wraps=None)
    @mock.patch("o2a.converter.parser.OozieParser.parse_node")
    def test_parse_fork_node(self, parse_node_mock, on_parse_node_mock):
        node_name = "fork_name"
        # language=XML
        root_string = f"""
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
"""
        root = ET.fromstring(root_string)
        fork = root.find("fork")
        node1, node2 = root.findall("action")[0:2]
        self.parser.parse_fork_node(root, fork)
        node = self.parser.workflow.nodes[node_name]
        self.assertEqual(["task1", "task2"], node.get_downstreams())
        self.assertIn(node_name, self.parser.workflow.nodes)
        parse_node_mock.assert_any_call(root, node1)
        parse_node_mock.assert_any_call(root, node2)
        for depend in node.mapper.required_imports():
            self.assertIn(depend, self.parser.workflow.dependencies)

        on_parse_node_mock.assert_called_once_with()

    @mock.patch("o2a.mappers.dummy_mapper.DummyMapper.on_parse_node", wraps=None)
    def test_parse_join_node(self, on_parse_node_mock):
        node_name = "join_name"
        end_name = "end_name"
        # language=XML
        join_str = f"<join name='{node_name}' to='{end_name}' />"
        join = ET.fromstring(join_str)
        self.parser.parse_join_node(join)

        node = self.parser.workflow.nodes[node_name]
        self.assertIn(node_name, self.parser.workflow.nodes)
        self.assertEqual([end_name], node.get_downstreams())
        for depend in node.mapper.required_imports():
            self.assertIn(depend, self.parser.workflow.dependencies)

        on_parse_node_mock.assert_called_once_with()

    @mock.patch("o2a.mappers.decision_mapper.DecisionMapper.on_parse_node", wraps=None)
    def test_parse_decision_node(self, on_parse_node_mock):
        node_name = "decision_node"
        # language=XML
        decision_str = f"""
<decision name="{node_name}">
    <switch>
        <case to="down1">${{fs:fileSize(secondjobOutputDir) gt 10 * GB}}</case>
        <case to="down2">${{fs:filSize(secondjobOutputDir) lt 100 * MB}}</case>
        <default to="end1" />
    </switch>
    </decision>
"""
        decision = ET.fromstring(decision_str)
        self.parser.parse_decision_node(decision)

        p_op = self.parser.workflow.nodes[node_name]
        self.assertIn(node_name, self.parser.workflow.nodes)
        self.assertEqual(["down1", "down2", "end1"], p_op.get_downstreams())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.workflow.dependencies)

        on_parse_node_mock.assert_called_once_with()

    @mock.patch("o2a.mappers.start_mapper.StartMapper.on_parse_node", wraps=None)
    @mock.patch("uuid.uuid4")
    def test_parse_start_node(self, uuid_mock, on_parse_node_mock):
        uuid_mock.return_value = "1234"
        node_name = "start_node_1234"
        end_name = "end_name"
        # language=XML
        start_node_str = f"<start to='{end_name}'/>"
        start = ET.fromstring(start_node_str)
        self.parser.parse_start_node(start)

        p_op = self.parser.workflow.nodes[node_name]
        self.assertIn(node_name, self.parser.workflow.nodes)
        self.assertEqual([end_name], p_op.get_downstreams())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.workflow.dependencies)

        on_parse_node_mock.assert_called_once_with()

    @mock.patch("o2a.mappers.ssh_mapper.SSHMapper.on_parse_node", wraps=None)
    def test_parse_action_node_ssh(self, on_parse_node_mock):
        self.parser.action_map = {"ssh": ssh_mapper.SSHMapper}
        node_name = "action_name"
        # language=XML
        action_string = f"""
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
"""
        action_node = ET.fromstring(action_string)
        self.parser.parse_action_node(action_node)

        p_op = self.parser.workflow.nodes[node_name]
        self.assertIn(node_name, self.parser.workflow.nodes)
        self.assertEqual(["end1"], p_op.get_downstreams())
        self.assertEqual("fail1", p_op.get_error_downstream_name())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.workflow.dependencies)
        on_parse_node_mock.assert_called_once_with()

    def test_parse_action_node_pig_with_file_and_archive(self):
        self.parser.action_map = {"pig": pig_mapper.PigMapper}
        node_name = "pig-node"
        self.parser.params = {"nameNode": "myNameNode"}
        # language=XML
        action_string = f"""
<action name='{node_name}'>
    <pig>
        <resource-manager>myResManager</resource-manager>
        <name-node>myNameNode</name-node>
        <script>id.pig</script>
        <file>/test_dir/test.txt#test_link.txt</file>
        <archive>/test_dir/test2.zip#test_zip_dir</archive>
    </pig>
    <ok to='end1'/>
    <error to='fail1'/>
</action>
"""
        action_node = ET.fromstring(action_string)
        self.parser.parse_action_node(action_node)

        p_op = self.parser.workflow.nodes[node_name]
        self.assertIn(node_name, self.parser.workflow.nodes)
        self.assertEqual(["end1"], p_op.get_downstreams())
        self.assertEqual("fail1", p_op.get_error_downstream_name())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.workflow.dependencies)
        self.assertEqual(["myNameNode/test_dir/test.txt#test_link.txt"], p_op.mapper.hdfs_files)
        self.assertEqual(["myNameNode/test_dir/test2.zip#test_zip_dir"], p_op.mapper.hdfs_archives)

    def test_parse_mapreduce_node(self):
        self.parser.params = {
            "nameNode": "hdfs://",
            "dataproc_cluster": "mycluster",
            "gcp_region": "europe-west3",
        }
        node_name = "mr-node"
        # language=XML
        xml = f"""
<action name='{node_name}'>
    <map-reduce>
        <name-node>hdfs://</name-node>
        <prepare>
            <delete path="hdfs:///user/mapreduce/examples/apps/mapreduce/output"/>
        </prepare>
    </map-reduce>
    <ok to="end"/>
    <error to="fail"/>
</action>
"""
        action_node = ET.fromstring(xml)
        self.parser.parse_action_node(action_node)
        self.assertIn(node_name, self.parser.workflow.nodes)
        mr_node = self.parser.workflow.nodes[node_name]
        self.assertEqual(["end"], mr_node.get_downstreams())
        self.assertEqual("fail", mr_node.get_error_downstream_name())
        for depend in mr_node.mapper.required_imports():
            self.assertIn(depend, self.parser.workflow.dependencies)

    @mock.patch("o2a.mappers.dummy_mapper.DummyMapper.on_parse_node", wraps=None)
    def test_parse_action_node_unknown(self, on_parse_node_mock):
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
"""
        action = ET.fromstring(action_str)
        self.parser.parse_action_node(action)

        p_op = self.parser.workflow.nodes[node_name]
        self.assertIn(node_name, self.parser.workflow.nodes)
        self.assertEqual(["end1"], p_op.get_downstreams())
        self.assertEqual("fail1", p_op.get_error_downstream_name())
        for depend in p_op.mapper.required_imports():
            self.assertIn(depend, self.parser.workflow.dependencies)

        on_parse_node_mock.assert_called_once_with()

    @mock.patch("o2a.converter.parser.OozieParser.parse_action_node")
    def test_parse_node_action(self, action_mock):
        root = ET.Element("root")
        action = ET.SubElement(root, "action", attrib={"name": "test_name"})
        self.parser.parse_node(root, action)
        action_mock.assert_called_once_with(action)

    @mock.patch("o2a.converter.parser.OozieParser.parse_start_node")
    def test_parse_node_start(self, start_mock):
        root = ET.Element("root")
        start = ET.SubElement(root, "start", attrib={"name": "test_name"})
        self.parser.parse_node(root, start)
        start_mock.assert_called_once_with(start)

    @mock.patch("o2a.converter.parser.OozieParser.parse_kill_node")
    def test_parse_node_kill(self, kill_mock):
        root = ET.Element("root")
        kill = ET.SubElement(root, "kill", attrib={"name": "test_name"})
        self.parser.parse_node(root, kill)
        kill_mock.assert_called_once_with(kill)

    @mock.patch("o2a.converter.parser.OozieParser.parse_end_node")
    def test_parse_node_end(self, end_mock):
        root = ET.Element("root")
        end = ET.SubElement(root, "end", attrib={"name": "test_name"})
        self.parser.parse_node(root, end)
        end_mock.assert_called_once_with(end)

    @mock.patch("o2a.converter.parser.OozieParser.parse_fork_node")
    def test_parse_node_fork(self, fork_mock):
        root = ET.Element("root")
        fork = ET.SubElement(root, "fork", attrib={"name": "test_name"})
        self.parser.parse_node(root, fork)
        fork_mock.assert_called_once_with(root, fork)

    @mock.patch("o2a.converter.parser.OozieParser.parse_join_node")
    def test_parse_node_join(self, join_mock):
        root = ET.Element("root")
        join = ET.SubElement(root, "join", attrib={"name": "test_name"})
        self.parser.parse_node(root, join)
        join_mock.assert_called_once_with(join)

    @mock.patch("o2a.converter.parser.OozieParser.parse_decision_node")
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
        self.parser.workflow.nodes.update(op_dict)
        self.parser.create_relations()

        self.assertEqual(
            self.parser.workflow.relations,
            {
                Relation(from_task_id="task1", to_task_id="fail1"),
                Relation(from_task_id="task1", to_task_id="task2"),
                Relation(from_task_id="task1", to_task_id="task3"),
                Relation(from_task_id="task2", to_task_id="fail1"),
                Relation(from_task_id="task2", to_task_id="task3"),
                Relation(from_task_id="task2", to_task_id="task4_first"),
                Relation(from_task_id="task3", to_task_id="end1"),
                Relation(from_task_id="task3", to_task_id="fail1"),
                Relation(from_task_id="task4_last", to_task_id="fail1"),
                Relation(from_task_id="task4_last", to_task_id="task1"),
                Relation(from_task_id="task4_last", to_task_id="task2"),
                Relation(from_task_id="task4_last", to_task_id="task3"),
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

        self.parser.workflow.nodes.update(op_dict)
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


class WorkflowTestCase(typing.NamedTuple):
    name: str
    node_names: typing.Set[str]
    relations: typing.Set[Relation]
    params: typing.Dict[str, str]


class TestOozieExamples(unittest.TestCase):
    @parameterized.expand(
        [
            (
                WorkflowTestCase(
                    name="decision",
                    node_names={"decision_node", "first", "end", "kill"},
                    relations={
                        Relation(from_task_id="decision_node", to_task_id="end"),
                        Relation(from_task_id="decision_node", to_task_id="first"),
                        Relation(from_task_id="decision_node", to_task_id="kill"),
                    },
                    params={"nameNode": "hdfs://"},
                ),
            ),
            (
                WorkflowTestCase(
                    name="demo",
                    node_names={
                        "fork_node",
                        "pig_node",
                        "subworkflow_node",
                        "shell_node",
                        "join_node",
                        "decision_node",
                        "hdfs_node",
                        "end",
                    },
                    relations={
                        Relation(from_task_id="decision_node", to_task_id="end"),
                        Relation(from_task_id="decision_node", to_task_id="hdfs_node"),
                        Relation(from_task_id="fork_node", to_task_id="pig_node_prepare"),
                        Relation(from_task_id="fork_node", to_task_id="shell_node_prepare"),
                        Relation(from_task_id="fork_node", to_task_id="subworkflow_node"),
                        Relation(from_task_id="join_node", to_task_id="decision_node"),
                        Relation(from_task_id="pig_node", to_task_id="join_node"),
                        Relation(from_task_id="shell_node", to_task_id="join_node"),
                        Relation(from_task_id="subworkflow_node", to_task_id="join_node"),
                    },
                    params={"nameNode": "hdfs://", "dataproc_cluster": "AAA"},
                ),
            ),
            (
                WorkflowTestCase(
                    name="el",
                    node_names={"ssh"},
                    relations=set(),
                    params={"hostname": "AAAA@BBB", "nameNode": "hdfs://"},
                ),
            ),
            (
                WorkflowTestCase(
                    name="fs",
                    node_names={"chmod", "mkdir", "fs_node", "delete", "move", "touchz", "chgrp", "join"},
                    relations={
                        Relation(from_task_id="fs_node", to_task_id="chgrp_fs_0_mkdir"),
                        Relation(from_task_id="fs_node", to_task_id="delete_fs_0_mkdir"),
                        Relation(from_task_id="fs_node", to_task_id="chmod_fs_0_mkdir"),
                        Relation(from_task_id="fs_node", to_task_id="touchz"),
                        Relation(from_task_id="fs_node", to_task_id="mkdir"),
                        Relation(from_task_id="fs_node", to_task_id="move_fs_0_mkdir"),
                        Relation(from_task_id="mkdir", to_task_id="join"),
                        Relation(from_task_id="delete_fs_1_delete", to_task_id="join"),
                        Relation(from_task_id="move_fs_1_move", to_task_id="join"),
                        Relation(from_task_id="touchz", to_task_id="join"),
                        Relation(from_task_id="chgrp_fs_1_chgrp", to_task_id="join"),
                        Relation(from_task_id="chmod_fs_7_chmod", to_task_id="join"),
                    },
                    params={"hostname": "AAAA@BBB", "nameNode": "hdfs://localhost:8020/"},
                ),
            ),
            (
                WorkflowTestCase(
                    name="mapreduce",
                    node_names={"mr_node"},
                    relations=set(),
                    params={"dataproc_cluster": "A", "gcp_region": "B", "nameNode": "hdfs://"},
                ),
            ),
            (
                WorkflowTestCase(
                    name="pig",
                    node_names={"pig_node"},
                    relations=set(),
                    params={"oozie.wf.application.path": "hdfs://", "nameNode": "hdfs://"},
                ),
            ),
            (
                WorkflowTestCase(
                    name="shell", node_names={"shell_node"}, relations=set(), params={"nameNode": "hdfs://"}
                ),
            ),
            (
                WorkflowTestCase(
                    name="spark",
                    node_names={"spark_node"},
                    relations=set(),
                    params={"dataproc_cluster": "A", "gcp_region": "B", "nameNode": "hdfs://"},
                ),
            ),
            (
                WorkflowTestCase(
                    name="ssh",
                    node_names={"ssh"},
                    relations=set(),
                    params={"hostname": "AAAA@BBB", "nameNode": "hdfs://"},
                ),
            ),
            (WorkflowTestCase(name="subwf", node_names={"subworkflow_node"}, relations=set(), params={}),),
        ],
        name_func=lambda func, num, p: f"{func.__name__}_{num}_{p.args[0].name}",
    )
    @mock.patch("o2a.mappers.base_mapper.BaseMapper.on_parse_finish", wraps=None)
    @mock.patch("uuid.uuid4", return_value="1234")
    def test_parse_workflow_examples(self, case: WorkflowTestCase, _, on_parse_finish_mock):
        current_parser = parser.OozieParser(
            input_directory_path=path.join(EXAMPLES_PATH, case.name),
            output_directory_path="/tmp",
            params=case.params,
            action_mapper=ACTION_MAP,
            control_mapper=CONTROL_MAP,
        )
        current_parser.parse_workflow()
        self.assertEqual(case.node_names, set(current_parser.workflow.nodes.keys()))
        self.assertEqual(case.relations, current_parser.workflow.relations)
        on_parse_finish_mock.assert_called()
