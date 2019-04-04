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
import os
import unittest
from contextlib import suppress
from unittest import mock
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from converter.mappers import CONTROL_MAP, ACTION_MAP
from mappers import subworkflow_mapper
from tests.utils.test_paths import EXAMPLE_SUBWORKFLOW_PATH


class TestSubworkflowMapper(unittest.TestCase):

    subworkflow_params = {
        "dataproc_cluster": "test_cluster",
        "gcp_conn_id": "google_cloud_default",
        "gcp_region": "europe-west3",
        "gcp_uri_prefix": "gs://test_bucket/dags",
        "nameNode": "hdfs://",
        "oozie.wf.application.path": "hdfs:///user/pig/examples/test_pig_node",
    }

    main_params = {
        "examplesRoot": "examples",
        "nameNode": "hdfs://",
        "resourceManager": "localhost:8032",
        "dataproc_cluster": "cluster-o2a",
    }

    def setUp(self):
        subwf = ET.Element("sub-workflow")
        app_path = ET.SubElement(subwf, "app-path")
        ET.SubElement(subwf, "propagate-configuration")
        configuration = ET.SubElement(subwf, "configuration")
        property1 = ET.SubElement(configuration, "property")
        name1 = ET.SubElement(property1, "name")
        value1 = ET.SubElement(property1, "value")
        self.et = ET.ElementTree(subwf)
        app_path.text = "${nameNode}/user/${wf:user()}/${examplesRoot}/pig"
        name1.text = "resourceManager"
        value1.text = "${resourceManager}"

    @mock.patch("utils.el_utils.parse_els")
    def test_create_mapper_jinja(self, parse_els):
        # Given
        parse_els.return_value = self.subworkflow_params
        # Removing subdag
        with suppress(OSError):
            os.remove("/tmp/test.test_id.py")
        # When
        mapper = subworkflow_mapper.SubworkflowMapper(
            oozie_node=self.et.getroot(),
            task_id="test_id",
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
        self.assertEqual(self.et.getroot(), mapper.oozie_node)
        self.assertEqual(self.main_params, mapper.params)
        self.assertEqual("subwf.tpl", mapper.template)
        # Propagate config node is present, should forward config properties
        self.assertEqual({"resourceManager": "localhost:8032"}, mapper.get_config_properties())
        self.assertTrue(os.path.isfile("/tmp/test.test_id.py"))

    @mock.patch("utils.el_utils.parse_els")
    def test_create_mapper_jinja_no_propagate(self, parse_els):
        # Given
        parse_els.return_value = self.subworkflow_params
        # Removing subdag
        with suppress(OSError):
            os.remove("/tmp/test.test_id.py")
        # Removing the propagate-configuration node
        pg = self.et.find("propagate-configuration")
        self.et.getroot().remove(pg)

        # When
        mapper = subworkflow_mapper.SubworkflowMapper(
            oozie_node=self.et.getroot(),
            task_id="test_id",
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
        self.assertEqual(self.et.getroot(), mapper.oozie_node)
        self.assertEqual(self.main_params, mapper.params)
        self.assertEqual("subwf.tpl", mapper.template)
        # Propagate config node is missing, should NOT forward config properties
        self.assertEqual({}, mapper.get_config_properties())
        self.assertTrue(os.path.isfile("/tmp/test.test_id.py"))

    @mock.patch("utils.el_utils.parse_els")
    def test_convert_to_text(self, parse_els):
        # Given
        parse_els.return_value = self.subworkflow_params
        # When
        mapper = subworkflow_mapper.SubworkflowMapper(
            input_directory_path=EXAMPLE_SUBWORKFLOW_PATH,
            output_directory_path="/tmp",
            oozie_node=self.et.getroot(),
            task_id="test_id",
            dag_name="test",
            trigger_rule=TriggerRule.DUMMY,
            params=self.main_params,
            action_mapper=ACTION_MAP,
            control_mapper=CONTROL_MAP,
        )

        # Then
        # Throws a syntax error if doesn't parse correctly
        ast.parse(mapper.convert_to_text())

    def test_required_imports(self):
        imps = subworkflow_mapper.SubworkflowMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)
