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
"""Tests for renderers"""
# pylint: disable=unused-argument
import sys
import unittest
from unittest import mock
from pathlib import Path
from xml.etree.ElementTree import Element

from o2a.converter.oozie_node import OozieActionNode
from o2a.converter.relation import Relation
from o2a.converter.renderers import AutoflakeArgs, DotRenderer, PythonRenderer
from o2a.converter.task import Task
from o2a.converter.task_group import TaskGroup
from o2a.converter.workflow import Workflow
from o2a.mappers.dummy_mapper import DummyMapper
from o2a.o2a_libs.property_utils import PropertySet


def _create_workflow():
    return Workflow(
        dag_name="DAG_NAME",
        input_directory_path="/tmp/input",
        output_directory_path="/tmp/output",
        task_group_relations={Relation(from_task_id="DAG_NAME_A", to_task_id="DAG_NAME_B")},
        task_groups=dict(
            TASK_NAME=TaskGroup(
                name="DAG_NAME_A", tasks=[Task(task_id="task_name", template_name="dummy.tpl")]
            )
        ),
        dependencies={"import IMPORT"},
    )


class PythonRendererTestCase(unittest.TestCase):
    @mock.patch("o2a.converter.renderers.black")
    @mock.patch("o2a.converter.renderers.fix_file")
    @mock.patch("o2a.converter.renderers.SortImports")
    @mock.patch("o2a.converter.renderers.render_template", return_value="DAG_CONTENT")
    @mock.patch("builtins.open")
    def test_create_workflow_file_should_create_file(
        self, open_mock, render_template_mock, sort_imports_mock, fix_file_mock, black_mock
    ):
        renderer = self._create_renderer()
        workflow = _create_workflow()
        props = PropertySet(config=dict(), job_properties=dict())

        renderer.create_workflow_file(workflow, props=props)
        open_mock.assert_called_once_with("/tmp/output/DAG_NAME.py", "w")
        open_mock.return_value.__enter__.return_value.write.assert_called_once_with("DAG_CONTENT")

    @mock.patch("o2a.converter.renderers.black")
    @mock.patch("o2a.converter.renderers.fix_file")
    @mock.patch("o2a.converter.renderers.SortImports")
    @mock.patch("o2a.converter.renderers.render_template", return_value="DAG_CONTENT")
    @mock.patch("builtins.open")
    def test_create_workflow_file_should_render_template(
        self, open_mock, render_template_mock, sort_imports_mock, fix_file_mock, black_mock
    ):
        renderer = self._create_renderer()
        workflow = _create_workflow()
        props = PropertySet(config=dict(), job_properties=dict())

        renderer.create_workflow_file(workflow, props=props)

        render_template_mock.assert_called_once_with(
            config={},
            dag_name="DAG_NAME",
            dependencies={"import IMPORT"},
            job_properties={},
            task_groups=list(workflow.task_groups.values()),
            relations=workflow.task_group_relations,
            schedule_interval=None,
            start_days_ago=None,
            template_name="workflow.tpl",
            task_map={"DAG_NAME_A": ["task_name"]},
        )

    @mock.patch("o2a.converter.renderers.black")
    @mock.patch("o2a.converter.renderers.fix_file")
    @mock.patch("o2a.converter.renderers.SortImports")
    @mock.patch("o2a.converter.renderers.render_template", return_value="DAG_CONTENT")
    @mock.patch("builtins.open")
    def test_create_subworkflow_file_should_be_render_template_with_different_template(
        self, open_mock, render_template_mock, sort_imports_mock, fix_file_mock, black_mock
    ):
        renderer = self._create_renderer()
        workflow = _create_workflow()
        props = PropertySet(config=dict(), job_properties=dict())

        renderer.create_subworkflow_file(workflow, props=props)
        render_template_mock.assert_called_once_with(
            config=mock.ANY,
            dag_name=mock.ANY,
            dependencies=mock.ANY,
            job_properties=mock.ANY,
            task_groups=mock.ANY,
            relations=mock.ANY,
            schedule_interval=mock.ANY,
            start_days_ago=mock.ANY,
            template_name="subworkflow.tpl",
            task_map={"DAG_NAME_A": ["task_name"]},
        )

    @mock.patch("o2a.converter.renderers.black")
    @mock.patch("o2a.converter.renderers.fix_file")
    @mock.patch("o2a.converter.renderers.SortImports")
    @mock.patch("o2a.converter.renderers.render_template", return_value="DAG_CONTENT")
    @mock.patch("builtins.open")
    def test_create_workflow_file_should_format_file(
        self, open_mock, render_template_mock, sort_imports_mock, autoflake_fix_file_mock, black_mock
    ):
        renderer = self._create_renderer()
        workflow = _create_workflow()
        props = PropertySet(config=dict(), job_properties=dict())

        renderer.create_workflow_file(workflow, props=props)
        black_mock.format_file_in_place.assert_called_once_with(
            Path("/tmp/output/DAG_NAME.py"), fast=mock.ANY, mode=mock.ANY, write_back=mock.ANY
        )
        autoflake_fix_file_mock.assert_called_once_with(
            "/tmp/output/DAG_NAME.py",
            args=AutoflakeArgs(
                remove_all_unused_imports=True,
                ignore_init_module_imports=False,
                remove_duplicate_keys=False,
                remove_unused_variables=True,
                in_place=True,
                imports=None,
                expand_star_imports=False,
                check=False,
            ),
            standard_out=sys.stdout,
        )
        sort_imports_mock.assert_called_once_with("/tmp/output/DAG_NAME.py")

    @staticmethod
    def _create_renderer():
        return PythonRenderer(
            schedule_interval=None, start_days_ago=None, output_directory_path="/tmp/output"
        )

    @staticmethod
    def _create_workflow():
        return Workflow(
            dag_name="DAG_NAME",
            input_directory_path="/tmp/input",
            output_directory_path="/tmp/output",
            task_group_relations={Relation(from_task_id="DAG_NAME_A", to_task_id="DAG_NAME_B")},
            nodes=dict(
                AAA=OozieActionNode(DummyMapper(Element("dummy"), name="DAG_NAME_A", dag_name="DAG_NAME_B"))
            ),
            dependencies={"import IMPORT"},
        )


class DotRendererTestCase(unittest.TestCase):
    @mock.patch("o2a.converter.renderers.render_template", return_value="DAG_CONTENT")
    @mock.patch("builtins.open")
    def test_create_workflow_file_should_create_file(self, open_mock, render_template_mock):
        renderer = self._create_renderer()
        workflow = _create_workflow()
        props = PropertySet(config=dict(), job_properties=dict())

        renderer.create_workflow_file(workflow, props=props)
        open_mock.assert_called_once_with("/tmp/output/DAG_NAME.dot", "w")
        open_mock.return_value.__enter__.return_value.write.assert_called_once_with("DAG_CONTENT")

    @mock.patch("o2a.converter.renderers.render_template", return_value="DAG_CONTENT")
    @mock.patch("builtins.open")
    def test_create_workflow_file_should_render_template(self, open_mock, render_template_mock):
        renderer = self._create_renderer()
        workflow = _create_workflow()
        props = PropertySet(config=dict(), job_properties=dict())

        renderer.create_workflow_file(workflow, props=props)

        render_template_mock.assert_called_once_with(
            dag_name="DAG_NAME",
            task_groups=list(workflow.task_groups.values()),
            relations=workflow.task_group_relations,
            template_name="workflow_dot.tpl",
        )

    @mock.patch("o2a.converter.renderers.render_template", return_value="DAG_CONTENT")
    @mock.patch("builtins.open")
    def test_create_subworkflow_file_should_be_render_template_with_the_same_template(
        self, open_mock, render_template_mock
    ):
        renderer = self._create_renderer()
        workflow = _create_workflow()
        props = PropertySet(config=dict(), job_properties=dict())

        renderer.create_subworkflow_file(workflow, props=props)
        render_template_mock.assert_called_once_with(
            dag_name=mock.ANY, task_groups=mock.ANY, relations=mock.ANY, template_name="workflow_dot.tpl"
        )

    @staticmethod
    def _create_renderer():
        return DotRenderer(schedule_interval=None, start_days_ago=None, output_directory_path="/tmp/output")
