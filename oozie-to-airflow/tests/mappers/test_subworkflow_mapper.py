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
"""Tests for subworkflow mapper"""
import ast
import os
from contextlib import suppress
from unittest import mock, TestCase
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from converter.mappers import CONTROL_MAP, ACTION_MAP
from converter.primitives import Task
from mappers import subworkflow_mapper
from tests.utils.test_paths import EXAMPLE_SUBWORKFLOW_PATH


class TestSubworkflowMapper(TestCase):

    subworkflow_params = {
        "dataproc_cluster": "test_cluster",
        "gcp_conn_id": "google_cloud_default",
        "gcp_region": "europe-west3",
        "gcp_uri_prefix": "gs://test_bucket/dags",
        "nameNode": "hdfs://",
        "oozie.wf.application.path": "hdfs:///user/pig/examples/pi",
    }

    main_params = {
        "examplesRoot": "examples",
        "nameNode": "hdfs://",
        "resourceManager": "localhost:8032",
        "dataproc_cluster": "cluster-o2a",
    }

    def setUp(self):
        # language=XML
        subworkflow_node_str = """
<sub-workflow>
    <app-path>${nameNode}/user/${wf:user()}/${examplesRoot}/pig</app-path>
    <propagate-configuration />
    <configuration>
        <property>
            <name>resourceManager</name>
            <value>${resourceManager}</value>
        </property>
    </configuration>
</sub-workflow>"""
        self.subworkflow_node = ET.fromstring(subworkflow_node_str)

    @mock.patch("utils.el_utils.parse_els")
    def test_create_mapper_jinja(self, parse_els):
        # Given
        parse_els.return_value = self.subworkflow_params
        # Removing subdag
        with suppress(OSError):
            os.remove("/tmp/test.test_id.py")
        # When
        mapper = subworkflow_mapper.SubworkflowMapper(
            oozie_node=self.subworkflow_node,
            name="test_id",
            dag_name="test",
            input_directory_path=EXAMPLE_SUBWORKFLOW_PATH,
            output_directory_path="/tmp",
            action_mapper=ACTION_MAP,
            control_mapper=CONTROL_MAP,
            trigger_rule=TriggerRule.DUMMY,
            params=self.main_params,
        )

        # Then
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.subworkflow_node, mapper.oozie_node)
        self.assertEqual(self.main_params, mapper.params)
        self.assertEqual("subwf.tpl", mapper.template)
        # Propagate config node is present, should forward config properties
        self.assertEqual({"resourceManager": "localhost:8032"}, mapper.get_config_properties())
        self.assertTrue(os.path.isfile("/tmp/subdag_test.py"))

    @mock.patch("utils.el_utils.parse_els")
    def test_create_mapper_jinja_no_propagate(self, parse_els):
        # Given
        parse_els.return_value = self.subworkflow_params
        # Removing subdag
        with suppress(OSError):
            os.remove("/tmp/subdag_test.py")
        # Removing the propagate-configuration node
        propagate_configuration = self.subworkflow_node.find("propagate-configuration")
        self.subworkflow_node.remove(propagate_configuration)

        # When
        mapper = subworkflow_mapper.SubworkflowMapper(
            oozie_node=self.subworkflow_node,
            name="test_id",
            dag_name="test",
            input_directory_path=EXAMPLE_SUBWORKFLOW_PATH,
            output_directory_path="/tmp",
            action_mapper=ACTION_MAP,
            control_mapper=CONTROL_MAP,
            trigger_rule=TriggerRule.DUMMY,
            params=self.main_params,
        )

        # Then
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.subworkflow_node, mapper.oozie_node)
        self.assertEqual(self.main_params, mapper.params)
        self.assertEqual("subwf.tpl", mapper.template)
        # Propagate config node is missing, should NOT forward config properties
        self.assertEqual({}, mapper.get_config_properties())
        self.assertTrue(os.path.isfile("/tmp/subdag_test.py"))

    @mock.patch("mappers.subworkflow_mapper.render_template", return_value="RETURN")
    @mock.patch("utils.el_utils.parse_els")
    def test_convert_to_text(self, parse_els_mock, render_template_mock):
        # Given
        parse_els_mock.return_value = self.subworkflow_params
        # When
        mapper = subworkflow_mapper.SubworkflowMapper(
            input_directory_path=EXAMPLE_SUBWORKFLOW_PATH,
            output_directory_path="/tmp",
            oozie_node=self.subworkflow_node,
            name="test_id",
            dag_name="test",
            trigger_rule=TriggerRule.DUMMY,
            params=self.main_params,
            action_mapper=ACTION_MAP,
            control_mapper=CONTROL_MAP,
        )

        res = mapper.convert_to_text()
        self.assertEqual(res, "RETURN")

        _, kwargs = render_template_mock.call_args
        tasks = kwargs["tasks"]
        relations = kwargs["relations"]

        self.assertEqual(kwargs["template_name"], "action.tpl")
        self.assertEqual(
            tasks,
            [Task(task_id="test_id", template_name="subwf.tpl", template_params={"trigger_rule": "dummy"})],
        )
        self.assertEqual(relations, [])

    # pylint: disable=no-self-use
    def test_required_imports(self):
        imps = subworkflow_mapper.SubworkflowMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)
