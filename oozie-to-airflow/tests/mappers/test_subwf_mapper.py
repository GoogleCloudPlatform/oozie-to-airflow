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
from unittest import mock
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from mappers import subwf_mapper


class TestSubworkflowMapper(unittest.TestCase):
    params = {
        "examplesRoot": "examples",
        "nameNode": "hdfs://localhost:8020",
        "resourceManager": "localhost:8032",
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

    @mock.patch("oozie_converter.OozieSubworkflowConverter")
    def test_create_mapper_jinja(self, mock_subwf_converter):
        # When
        mapper = subwf_mapper.SubworkflowMapper(
            oozie_node=self.et.getroot(),
            task_id="test_id",
            trigger_rule=TriggerRule.DUMMY,
            params=self.params,
        )

        # Then
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.et.getroot(), mapper.oozie_node)
        self.assertEqual(self.params, mapper.params)
        self.assertEqual("subwf.tpl", mapper.template)
        mock_subwf_converter.assert_called_once()
        # Propagate config node is present, should forward config properties
        self.assertEqual({"resourceManager": "localhost:8032"}, mapper.get_config_properties())

    @mock.patch("oozie_converter.OozieSubworkflowConverter")
    def test_create_mapper_jinja_no_propagate(self, mock_subwf_converter):
        # Given
        # Removing the propagate-configuration node
        pg = self.et.find("propagate-configuration")
        self.et.getroot().remove(pg)

        # When
        mapper = subwf_mapper.SubworkflowMapper(
            oozie_node=self.et.getroot(),
            task_id="test_id",
            trigger_rule=TriggerRule.DUMMY,
            params=self.params,
        )

        # Then
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.et.getroot(), mapper.oozie_node)
        self.assertEqual(self.params, mapper.params)
        self.assertEqual("subwf.tpl", mapper.template)
        mock_subwf_converter.assert_called_once()
        # Propagate config node is missing, should NOT forward config properties
        self.assertEqual({}, mapper.get_config_properties())

    @mock.patch("utils.el_utils.parse_els")
    def test_convert_to_text(self, parse_els):
        # Given
        parse_els.return_value = {
            "dataproc_cluster": "test_cluster",
            "gcp_conn_id": "google_cloud_default",
            "gcp_region": "europe-west3",
            "gcp_uri_prefix": "gs://test_bucket/dags",
        }

        # When
        mapper = subwf_mapper.SubworkflowMapper(
            oozie_node=self.et.getroot(),
            task_id="test_id",
            trigger_rule=TriggerRule.DUMMY,
            params=self.params,
        )

        # Then
        # Throws a syntax error if doesn't parse correctly
        ast.parse(mapper.convert_to_text())

    def test_required_imports(self):
        imps = subwf_mapper.SubworkflowMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)
