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
"""Tests Oozie Converter"""

from unittest import mock, TestCase
from xml.etree.ElementTree import Element
from xml.etree import ElementTree as ET

from o2a import o2a
from o2a.converter.oozie_converter import OozieConverter
from o2a.converter.parsed_action_node import ParsedActionNode

from o2a.converter.task import Task
from o2a.converter.workflow import Workflow
from o2a.mappers.dummy_mapper import DummyMapper

from o2a.converter import parsed_action_node
from o2a.converter.mappers import ACTION_MAP
from o2a.converter.relation import Relation

from o2a.mappers import dummy_mapper


class TestOozieConverter(TestCase):
    def test_parse_args_input_output_file(self):
        input_dir = "/tmp/does.not.exist/"
        output_dir = "/tmp/out/"
        args = o2a.parse_args(["-i", input_dir, "-o", output_dir])
        self.assertEqual(args.input_directory_path, input_dir)
        self.assertEqual(args.output_directory_path, output_dir)

    def test_parse_args_user(self):
        input_dir = "/tmp/does.not.exist"
        output_dir = "/tmp/out/"
        user = "oozie_test"
        args = o2a.parse_args(["-i", input_dir, "-o", output_dir, "-u", user])
        self.assertEqual(args.user, user)

    @mock.patch("o2a.converter.oozie_converter.parser.OozieParser")
    def test_convert(self, oozie_parser_mock):

        # Given
        converter = self._create_converter()
        workflow = self._create_workflow()
        oozie_parser_mock.return_value.workflow = workflow

        # When
        converter.convert()

        # Then
        converter.renderer.create_subworkflow_file.assert_not_called()
        converter.renderer.create_workflow_file.assert_called_once_with(
            workflow=workflow, props=converter.props
        )

    @mock.patch("o2a.converter.oozie_converter.parser.OozieParser")
    def test_convert_as_subworkflow(self, oozie_parser_mock):

        # Given
        converter = self._create_converter()
        workflow = self._create_workflow()
        oozie_parser_mock.return_value.workflow = workflow

        # When
        converter.convert(as_subworkflow=True)

        # Then
        converter.renderer.create_workflow_file.assert_not_called()
        converter.renderer.create_subworkflow_file.assert_called_once_with(
            workflow=workflow, props=converter.props
        )

    def test_convert_nodes(self):
        converter = self._create_converter()

        tasks_1 = [
            Task(task_id="first_task", template_name="dummy.tpl"),
            Task(task_id="second_task", template_name="dummy.tpl"),
        ]
        relations_1 = {Relation(from_task_id="first_task", to_task_id="tasks_2")}
        tasks_2 = [Task(task_id="third_task", template_name="dummy.tpl")]
        relations_2 = {}

        mapper_1 = mock.MagicMock(**{"to_tasks_and_relations.return_value": (tasks_1, relations_1)})
        mapper_2 = mock.MagicMock(**{"to_tasks_and_relations.return_value": (tasks_2, relations_2)})

        node_1 = ParsedActionNode(mapper=mapper_1)
        node_2 = ParsedActionNode(mapper=mapper_2)
        nodes = dict(TASK_1=node_1, TASK_2=node_2)

        converter.convert_nodes(nodes=nodes)

        self.assertIs(node_1.tasks, tasks_1)
        self.assertIs(node_2.tasks, tasks_2)
        self.assertIs(node_1.relations, relations_1)
        self.assertIs(node_2.relations, relations_2)

    def test_convert_dependencies(self):
        converter = self._create_converter()

        mapper_1 = mock.MagicMock(**{"required_imports.return_value": {"import A", "import B"}})
        mapper_2 = mock.MagicMock(**{"required_imports.return_value": ("import B", "import C")})

        node_1 = ParsedActionNode(mapper=mapper_1)
        node_2 = ParsedActionNode(mapper=mapper_2)
        nodes = dict(TASK_1=node_1, TASK_2=node_2)

        workflow = self._create_workflow(nodes)
        converter.convert_dependencies(workflow)

        self.assertEqual({"import IMPORT", "import C", "import B", "import A"}, workflow.dependencies)

    @mock.patch("o2a.converter.oozie_converter.parser.OozieParser")
    def test_apply_transformers(self, oozie_parser_mock):
        workflow = self._create_workflow()
        oozie_parser_mock.return_value.workflow = workflow

        transformer_1 = mock.MagicMock()
        transformer_2 = mock.MagicMock()

        converter = self._create_converter()

        converter.transformers = [transformer_1, transformer_2]

        converter.apply_transformers(workflow)

        transformer_1.process_workflow.assert_called_once_with(workflow)
        transformer_2.process_workflow.assert_called_once_with(workflow)

    def test_copy_extra_assets(self):
        converter = self._create_converter()

        mock_1 = mock.MagicMock()
        mock_2 = mock.MagicMock()

        converter.copy_extra_assets(dict(mock_1=mock_1, mock_2=mock_2))

        mock_1.mapper.copy_extra_assets.assert_called_once_with(
            input_directory_path="/input_directory_path/hdfs", output_directory_path="/tmp"
        )
        mock_2.mapper.copy_extra_assets.assert_called_once_with(
            input_directory_path="/input_directory_path/hdfs", output_directory_path="/tmp"
        )

    def test_convert_relations(self):
        oozie_node = ET.Element("dummy")
        op1 = parsed_action_node.ParsedActionNode(
            dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task1", dag_name="DAG_NAME_B1"),
            tasks=[self._create_task("task1")],
        )
        op1.downstream_names = ["task2", "task3"]
        op1.error_xml = "fail1"
        op2 = parsed_action_node.ParsedActionNode(
            dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task2", dag_name="DAG_NAME_B2"),
            tasks=[self._create_task("task2")],
        )
        op2.downstream_names = ["task3", "task4"]
        op2.error_xml = "fail1"
        op3 = parsed_action_node.ParsedActionNode(
            dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task3", dag_name="DAG_NAME_B3"),
            tasks=[self._create_task("task3")],
        )
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
        end = parsed_action_node.ParsedActionNode(
            dummy_mapper.DummyMapper(oozie_node=oozie_node, name="end1", dag_name="DAG_NAME_B4"),
            tasks=[self._create_task("end1")],
        )
        fail = parsed_action_node.ParsedActionNode(
            dummy_mapper.DummyMapper(oozie_node=oozie_node, name="fail1", dag_name="DAG_NAME_B5"),
            tasks=[self._create_task("fail1")],
        )
        op_dict = {"task1": op1, "task2": op2, "task3": op3, "task4": op4, "end1": end, "fail1": fail}
        workflow = self._create_workflow(nodes=op_dict)
        converter = self._create_converter()
        workflow.relations = set()
        converter.convert_relations(workflow)

        self.assertEqual(
            workflow.relations,
            {
                Relation(from_task_id="task1", to_task_id="fail1", is_error=True),
                Relation(from_task_id="task1", to_task_id="task2"),
                Relation(from_task_id="task1", to_task_id="task3"),
                Relation(from_task_id="task2", to_task_id="fail1", is_error=True),
                Relation(from_task_id="task2", to_task_id="task3"),
                Relation(from_task_id="task2", to_task_id="task4_first"),
                Relation(from_task_id="task3", to_task_id="end1"),
                Relation(from_task_id="task3", to_task_id="fail1", is_error=True),
                Relation(from_task_id="task4_last", to_task_id="fail1", is_error=True),
                Relation(from_task_id="task4_last", to_task_id="task1"),
                Relation(from_task_id="task4_last", to_task_id="task2"),
                Relation(from_task_id="task4_last", to_task_id="task3"),
            },
        )

    def test_update_trigger_rules(self):
        oozie_node = ET.Element("dummy")
        op1 = parsed_action_node.ParsedActionNode(
            dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task1", dag_name="DAG_NAME_B"),
            tasks=[self._create_task("task1")],
        )
        op1.downstream_names = ["task2", "task3"]
        op1.error_xml = "fail1"
        op2 = parsed_action_node.ParsedActionNode(
            dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task2", dag_name="DAG_NAME_B"),
            tasks=[self._create_task("task2")],
        )
        op2.downstream_names = ["task3"]
        op2.error_xml = "fail1"
        op3 = parsed_action_node.ParsedActionNode(
            dummy_mapper.DummyMapper(oozie_node=oozie_node, name="task3", dag_name="DAG_NAME_B"),
            tasks=[self._create_task("task3")],
        )
        op3.downstream_names = ["end1"]
        op3.error_xml = "fail1"
        end = parsed_action_node.ParsedActionNode(
            dummy_mapper.DummyMapper(oozie_node=oozie_node, name="end1", dag_name="DAG_NAME_B"),
            tasks=[self._create_task("end1")],
        )
        fail = parsed_action_node.ParsedActionNode(
            dummy_mapper.DummyMapper(oozie_node=oozie_node, name="fail1", dag_name="DAG_NAME_B"),
            tasks=[self._create_task("fail1")],
        )
        op_dict = {"task1": op1, "task2": op2, "task3": op3, "end1": end, "fail1": fail}
        workflow = self._create_workflow(nodes=op_dict)
        converter = self._create_converter()

        workflow.relations = set()
        converter.convert_relations(workflow)
        converter.update_trigger_rules(workflow)

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

    @staticmethod
    def _create_converter():
        return OozieConverter(
            input_directory_path="/input_directory_path/",
            output_directory_path="/tmp",
            user="USER",
            action_mapper=ACTION_MAP,
            renderer=mock.MagicMock(),
            dag_name="test_dag",
        )

    @staticmethod
    def _create_workflow(nodes=None):
        return Workflow(
            dag_name="A",
            input_directory_path="in_dir",
            output_directory_path="out_dir",
            relations={Relation(from_task_id="DAG_NAME_A", to_task_id="DAG_NAME_B")},
            nodes=dict(
                AAA=ParsedActionNode(DummyMapper(Element("dummy"), name="DAG_NAME_A", dag_name="DAG_NAME_B"))
            )
            if not nodes
            else nodes,
            dependencies={"import IMPORT"},
        )

    @staticmethod
    def _create_task(task_id):
        return Task(task_id=task_id, template_name="dummy.tpl")
