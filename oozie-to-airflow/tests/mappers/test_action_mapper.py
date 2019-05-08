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
"""Tests for the Dummy mapper."""
import unittest
from typing import cast

# pylint: disable=ungrouped-imports
from unittest import mock
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from converter.primitives import Workflow
from mappers.action_mapper import ActionMapper

from utils.config_extractors import TAG_CONFIGURATION, TAG_JOB_XML
from utils.xml_utils import find_nodes_by_tag, find_node_by_tag


class TestMapper(ActionMapper):
    # pylint: disable=no-self-use
    def convert_to_text(self):
        return ""

    # pylint: disable=no-self-use
    def required_imports(self):
        return set()


class ActionMapperTestCase(unittest.TestCase):
    def test_create_mapper(self):
        # language=XML
        action_node_str = """
<action name="action">
    <configuraion>
    </configuraion>
</action>
"""
        action_node = ET.fromstring(action_node_str)
        mapper = self._get_action_mapper(action_node)
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.ALL_SUCCESS, mapper.trigger_rule)

    @mock.patch(
        "mappers.action_mapper.extract_properties_from_job_xml_nodes", return_value={"KEY1": "VALUE1"}
    )
    @mock.patch(
        "mappers.action_mapper.extract_properties_from_configuration_node", return_value={"KEY2": "VALUE2"}
    )
    def test_parse_job_xml_and_configuration_minimal_green_path(
        self, extract_properties_from_configuration_node_mock, extract_properties_from_job_xml_nodes_mock
    ):
        # language=XML
        action_node_str = """
<action name="action">
    <job-xml>AAA.xml</job-xml>
    <configuration>
        <property>
            <name>KEY1</name>
            <value>VALUE1</value>
        </property>
    </configuration>
</action>
"""
        action_node = cast(ET.Element, ET.fromstring(action_node_str))
        mapper = self._get_action_mapper(action_node)

        mapper.on_parse_node(
            Workflow(
                input_directory_path="/tmp/input_directory_path",
                output_directory_path="/tmp/output_directory_path",
            )
        )

        config_node = find_node_by_tag(action_node, TAG_CONFIGURATION)
        extract_properties_from_configuration_node_mock.assert_called_once_with(
            config_node=config_node, params={}
        )

        job_xml_nodes = find_nodes_by_tag(action_node, TAG_JOB_XML)
        extract_properties_from_job_xml_nodes_mock.assert_called_once_with(
            input_directory_path="/tmp/input_directory_path", job_xml_nodes=job_xml_nodes, params={}
        )

        self.assertEqual(mapper.properties, {"KEY1": "VALUE1", "KEY2": "VALUE2"})

    @mock.patch("mappers.action_mapper.extract_properties_from_job_xml_nodes", return_value={"KEY": "VALUE1"})
    @mock.patch(
        "mappers.action_mapper.extract_properties_from_configuration_node", return_value={"KEY": "VALUE2"}
    )
    def test_parse_job_xml_and_configuration_minimal_green_path_precedense(
        self, extract_properties_from_configuration_node_mock, extract_properties_from_job_xml_nodes_mock
    ):
        # language=XML
        action_node_str = """
<action name="action">
    <job-xml>AAA.xml</job-xml>
    <configuration>
        <property>
            <name>KEY</name>
            <value>VALUE2</value>
        </property>
    </configuration>
</action>
"""
        action_node = cast(ET.Element, ET.fromstring(action_node_str))
        mapper = self._get_action_mapper(action_node)

        mapper.on_parse_node(
            Workflow(
                input_directory_path="/tmp/input_directory_path",
                output_directory_path="/tmp/output_directory_path",
            )
        )

        config_node = find_node_by_tag(action_node, TAG_CONFIGURATION)
        extract_properties_from_configuration_node_mock.assert_called_once_with(
            config_node=config_node, params={}
        )

        job_xml_nodes = find_nodes_by_tag(action_node, TAG_JOB_XML)
        extract_properties_from_job_xml_nodes_mock.assert_called_once_with(
            input_directory_path="/tmp/input_directory_path", job_xml_nodes=job_xml_nodes, params={}
        )

        self.assertEqual(mapper.properties, {"KEY": "VALUE2"})

    @mock.patch(
        "mappers.action_mapper.extract_properties_from_configuration_node", return_value={"KEY1": "VALUE1"}
    )
    @mock.patch("mappers.action_mapper.extract_properties_from_job_xml_nodes", return_value={})
    def test_parse_job_xml_and_configuration_minimal_green_path_without_configuration(
        self, extract_properties_from_configuration_node_mock, extract_properties_from_job_xml_nodes_mock
    ):
        # language=XML
        action_node_str = """
<action name="action">
    <configuration>
        <property>
            <name>KEY1</name>
            <value>VALUE1</value>
        </property>
    </configuration>
</action>
"""
        action_node = cast(ET.Element, ET.fromstring(action_node_str))
        mapper = self._get_action_mapper(action_node)

        mapper.on_parse_node(
            Workflow(
                input_directory_path="/tmp/input_directory_path",
                output_directory_path="/tmp/output_directory_path",
            )
        )

        extract_properties_from_configuration_node_mock.assert_called_once()
        extract_properties_from_job_xml_nodes_mock.assert_called_once()
        self.assertEqual(mapper.properties, {"KEY1": "VALUE1"})

    # pylint: disable=no-self-use
    def _get_action_mapper(self, action_node):
        return TestMapper(oozie_node=action_node, name="test_id")
