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
from os import path
from unittest import TestCase, mock
from xml.etree.ElementTree import Element

from o2a import o2a
from o2a.converter.mappers import ACTION_MAP
from o2a.converter.oozie_converter import OozieConverter
from o2a.converter.oozie_node import OozieActionNode
from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.converter.task_group import TaskGroup
from o2a.converter.workflow import Workflow
from o2a.definitions import EXAMPLES_PATH
from o2a.mappers.dummy_mapper import DummyMapper
from o2a.transformers.remove_end_transformer import RemoveEndTransformer
from o2a.transformers.remove_fork_transformer import RemoveForkTransformer
from o2a.transformers.remove_inaccessible_node_transformer import RemoveInaccessibleNodeTransformer
from o2a.transformers.remove_join_transformer import RemoveJoinTransformer
from o2a.transformers.remove_kill_transformer import RemoveKillTransformer
from o2a.transformers.remove_start_transformer import RemoveStartTransformer


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

    @mock.patch("o2a.converter.oozie_converter.workflow_xml_parser.WorkflowXmlParser")
    def test_convert(self, oozie_parser_mock):
        # Given
        converter = self._create_converter()
        workflow = self._create_workflow()
        converter.workflow = workflow

        # When
        converter.convert()

        # Then
        oozie_parser_mock.return_value.parse_workflow.assert_called_once_with()
        converter.renderer.create_subworkflow_file.assert_not_called()
        converter.renderer.create_workflow_file.assert_called_once_with(
            workflow=workflow, props=converter.props
        )

    @mock.patch("o2a.converter.oozie_converter.workflow_xml_parser.WorkflowXmlParser")
    def test_convert_as_subworkflow(self, oozie_parser_mock):
        # Given
        converter = self._create_converter()
        workflow = self._create_workflow()
        converter.workflow = workflow

        # When
        converter.convert(as_subworkflow=True)

        # Then
        oozie_parser_mock.return_value.parse_workflow.assert_called_once_with()
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

        node_1 = OozieActionNode(mapper=mapper_1)
        node_2 = OozieActionNode(mapper=mapper_2)
        nodes = dict(TASK_1=node_1, TASK_2=node_2)
        converter.workflow.nodes = nodes

        converter.convert_nodes()

        self.assertIs(node_1.tasks, tasks_1)
        self.assertIs(node_2.tasks, tasks_2)
        self.assertIs(node_1.relations, relations_1)
        self.assertIs(node_2.relations, relations_2)

    def test_apply_preconvert_transformers(self):
        workflow = self._create_workflow()

        transformer_1 = mock.MagicMock()
        transformer_2 = mock.MagicMock()

        converter = self._create_converter()
        converter.workflow = workflow

        converter.transformers = [transformer_1, transformer_2]

        converter.apply_preconvert_transformers()

        transformer_1.process_workflow_after_parse_workflow_xml.assert_called_once_with(workflow)
        transformer_2.process_workflow_after_parse_workflow_xml.assert_called_once_with(workflow)

    def test_apply_postconvert_transformers(self):
        workflow = self._create_workflow()

        transformer_1 = mock.MagicMock()
        transformer_2 = mock.MagicMock()

        converter = self._create_converter()
        converter.workflow = workflow

        converter.transformers = [transformer_1, transformer_2]

        converter.apply_postconvert_transformers()

        transformer_1.process_workflow_after_convert_nodes.assert_called_once_with(
            workflow, props=converter.props
        )
        transformer_2.process_workflow_after_convert_nodes.assert_called_once_with(
            workflow, props=converter.props
        )

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

        op1 = TaskGroup(
            name="task1",
            tasks=[self._create_task("task1")],
            downstream_names=["task2", "task3"],
            error_downstream_name="fail1",
        )
        op2 = TaskGroup(
            name="task2",
            tasks=[self._create_task("task2")],
            downstream_names=["task3", "task4"],
            error_downstream_name="fail1",
        )
        op3 = TaskGroup(
            name="task3",
            tasks=[self._create_task("task3")],
            downstream_names=["end1"],
            error_downstream_name="fail1",
        )
        op4 = mock.Mock(
            **{
                "first_task_id": "task4_first",
                "last_task_id_of_ok_flow": "task4_last",
                "last_task_id_of_error_flow": "task4_last",
                "downstream_names": ["task1", "task2", "task3"],
                "error_downstream_name": "fail1",
            }
        )
        end = TaskGroup(name="end1", tasks=[self._create_task("end1")])
        fail = TaskGroup(name="fail1", tasks=[self._create_task("fail1")])
        op_dict = {"task1": op1, "task2": op2, "task3": op3, "task4": op4, "end1": end, "fail1": fail}
        workflow = self._create_workflow(task_groups=op_dict)
        converter = self._create_converter()

        workflow.task_group_relations = set()
        converter.workflow = workflow

        converter.add_state_handlers()
        converter.convert_relations()

        self.assertEqual(
            workflow.task_group_relations,
            {
                Relation(from_task_id="task1_error", to_task_id="fail1", is_error=True),
                Relation(from_task_id="task1_ok", to_task_id="task2", is_error=False),
                Relation(from_task_id="task1_ok", to_task_id="task3", is_error=False),
                Relation(from_task_id="task2_error", to_task_id="fail1", is_error=True),
                Relation(from_task_id="task2_ok", to_task_id="task3", is_error=False),
                Relation(from_task_id="task2_ok", to_task_id="task4_first", is_error=False),
                Relation(from_task_id="task3_error", to_task_id="fail1", is_error=True),
                Relation(from_task_id="task3_ok", to_task_id="end1", is_error=False),
                Relation(from_task_id="task4_last", to_task_id="fail1", is_error=True),
                Relation(from_task_id="task4_last", to_task_id="task1", is_error=False),
                Relation(from_task_id="task4_last", to_task_id="task2", is_error=False),
                Relation(from_task_id="task4_last", to_task_id="task3", is_error=False),
            },
        )

    def test_add_error_handlers(self):
        task_groups = {"TASK_A": mock.MagicMock(), "TASK_B": mock.MagicMock()}
        converter = self._create_converter()
        workflow = self._create_workflow(task_groups=task_groups)
        converter.workflow = workflow
        converter.add_state_handlers()

        task_groups["TASK_A"].add_state_handler_if_needed.assert_called_once_with()
        task_groups["TASK_B"].add_state_handler_if_needed.assert_called_once_with()

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
    def _create_workflow(nodes=None, task_groups=None):
        return Workflow(
            dag_name="A",
            input_directory_path="in_dir",
            output_directory_path="out_dir",
            task_group_relations={Relation(from_task_id="DAG_NAME_A", to_task_id="DAG_NAME_B")},
            nodes=dict(
                AAA=OozieActionNode(DummyMapper(Element("dummy"), name="DAG_NAME_A", dag_name="DAG_NAME_B"))
            )
            if not nodes
            else nodes,
            task_groups=dict() if not task_groups else task_groups,
            dependencies={"import IMPORT"},
        )

    @staticmethod
    def _create_task(task_id):
        return Task(task_id=task_id, template_name="dummy.tpl")


class TestOozieConvertByExamples(TestCase):
    def test_should_convert_demo_workflow(self):
        renderer = mock.MagicMock()

        transformers = [
            RemoveInaccessibleNodeTransformer(),
            RemoveEndTransformer(),
            RemoveKillTransformer(),
            RemoveStartTransformer(),
            RemoveJoinTransformer(),
            RemoveForkTransformer(),
        ]

        input_directory_path = path.join(EXAMPLES_PATH, "demo")
        converter = OozieConverter(
            dag_name="demo",
            input_directory_path=input_directory_path,
            output_directory_path="/tmp/",
            action_mapper=ACTION_MAP,
            renderer=renderer,
            transformers=transformers,
            user="user",
        )
        converter.recreate_output_directory()
        converter.convert()
        _, kwargs = renderer.create_workflow_file.call_args
        workflow: Workflow = kwargs["workflow"]
        self.assertEqual(input_directory_path, workflow.input_directory_path)
        self.assertEqual("/tmp/", workflow.output_directory_path)
        self.assertEqual("demo", workflow.dag_name)
        self.assertEqual(
            {
                Relation(from_task_id="decision-node", to_task_id="end", is_error=False),
                Relation(from_task_id="decision-node", to_task_id="hdfs-node", is_error=False),
                Relation(from_task_id="join-node", to_task_id="decision-node", is_error=False),
                Relation(from_task_id="pig-node", to_task_id="join-node", is_error=False),
                Relation(from_task_id="shell-node", to_task_id="join-node", is_error=False),
                Relation(from_task_id="subworkflow-node", to_task_id="join-node", is_error=False),
            },
            workflow.task_group_relations,
        )
        self.assertEqual({}, workflow.nodes)
        self.assertEqual(
            {"pig-node", "subworkflow-node", "shell-node", "join-node", "decision-node", "hdfs-node", "end"},
            workflow.task_groups.keys(),
        )
        self.assertEqual(
            {
                "from airflow import models",
                "from airflow.contrib.operators import dataproc_operator",
                "from airflow.operators import bash_operator",
                "from airflow.operators import dummy_operator",
                "from airflow.operators import python_operator",
                "from airflow.operators.subdag_operator import SubDagOperator",
                "from airflow.operators import bash_operator, dummy_operator",
                "from airflow.utils import dates",
                "from airflow.utils.trigger_rule import TriggerRule",
                "from o2a.o2a_libs.el_basic_functions import *",
                "from o2a.o2a_libs.el_basic_functions import first_not_null",
                "from o2a.o2a_libs.el_wf_functions import *",
                "from o2a.o2a_libs.property_utils import PropertySet",
                "import datetime",
                "import shlex",
                "import subdag_childwf",
            },
            workflow.dependencies,
        )
