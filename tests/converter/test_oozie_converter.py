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

import io
from pathlib import Path
from unittest import mock, TestCase
from xml.etree import ElementTree as ET

from o2a import o2a
from o2a.converter.oozie_converter import OozieConverter
from o2a.converter.mappers import CONTROL_MAP, ACTION_MAP
from o2a.converter.parsed_node import ParsedNode

from o2a.converter.task import Task
from o2a.converter.workflow import Workflow
from o2a.converter.relation import Relation
from o2a.mappers.dummy_mapper import DummyMapper


class TestOozieConverter(TestCase):
    def setUp(self):
        self.converter = OozieConverter(
            dag_name="test_dag",
            input_directory_path="/input_directory_path/",
            output_directory_path="/tmp",
            action_mapper=ACTION_MAP,
            control_mapper=CONTROL_MAP,
            user="USER",
        )

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

    @mock.patch("o2a.converter.oozie_converter.render_template", return_value="AAA")
    @mock.patch("builtins.open", return_value=io.StringIO())
    @mock.patch("o2a.converter.oozie_converter.black")
    def test_create_dag_file(self, black_mock, open_mock, _):
        workflow = Workflow(
            dag_name="A",
            input_directory_path="in_dir",
            output_directory_path="out_dir",
            relations={Relation(from_task_id="AAA", to_task_id="BBB")},
            nodes=dict(AAA=ParsedNode(DummyMapper(ET.Element("dummy"), name="AAA"))),
            dependencies={"import AAAA"},
        )

        self.converter.create_dag_file(workflow)
        open_mock.assert_called_once_with("/tmp/test_dag.py", "w")
        black_mock.format_file_in_place.assert_called_once_with(
            Path("/tmp/test_dag.py"), fast=mock.ANY, mode=mock.ANY, write_back=mock.ANY
        )

    @mock.patch("o2a.converter.oozie_converter.render_template", return_value="TEXT_CONTENT")
    def test_write_dag_file(self, render_template_mock):
        relations = {Relation(from_task_id="TASK_1", to_task_id="TASK_2")}
        nodes = dict(TASK_1=ParsedNode(DummyMapper(ET.Element("dummy"), name="TASK_1")))
        dependencies = {"import awesome_stuff"}
        workflow = Workflow(
            input_directory_path="/tmp/input_directory",
            output_directory_path="/tmp/input_directory",
            dag_name="test_dag",
            relations=relations,
            nodes=nodes,
            dependencies=dependencies,
        )

        content = self.converter.render_workflow(workflow=workflow)

        render_template_mock.assert_called_once_with(
            dag_name="test_dag",
            dependencies=["import awesome_stuff"],
            nodes=[nodes["TASK_1"]],
            params={"user.name": "USER"},
            relations={Relation(from_task_id="TASK_1", to_task_id="TASK_2")},
            schedule_interval=None,
            start_days_ago=None,
            template_name="workflow.tpl",
        )

        self.assertEqual(content, "TEXT_CONTENT")

    def test_convert_nodes(self):
        tasks_1 = [
            Task(task_id="first_task", template_name="dummy.tpl"),
            Task(task_id="second_task", template_name="dummy.tpl"),
        ]
        relations_1 = {Relation(from_task_id="first_task", to_task_id="tasks_2")}
        tasks_2 = [Task(task_id="third_task", template_name="dummy.tpl")]
        relations_2 = {}

        mapper_1 = mock.MagicMock(**{"to_tasks_and_relations.return_value": (tasks_1, relations_1)})
        mapper_2 = mock.MagicMock(**{"to_tasks_and_relations.return_value": (tasks_2, relations_2)})

        node_1 = ParsedNode(mapper=mapper_1)
        node_2 = ParsedNode(mapper=mapper_2)
        nodes = dict(TASK_1=node_1, TASK_2=node_2)

        self.converter.convert_nodes(nodes=nodes)
        self.assertIs(node_1.tasks, tasks_1)
        self.assertIs(node_2.tasks, tasks_2)
        self.assertIs(node_1.relations, relations_1)
        self.assertIs(node_2.relations, relations_2)

    def test_copy_extra_assets(self):
        mock_1 = mock.MagicMock()
        mock_2 = mock.MagicMock()

        self.converter.copy_extra_assets(dict(mock_1=mock_1, mock_2=mock_2))

        mock_1.mapper.copy_extra_assets.assert_called_once_with(
            input_directory_path="/input_directory_path/hdfs", output_directory_path="/tmp"
        )
        mock_2.mapper.copy_extra_assets.assert_called_once_with(
            input_directory_path="/input_directory_path/hdfs", output_directory_path="/tmp"
        )
