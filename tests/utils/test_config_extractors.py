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

"""Extractors for configuration and job-xml nodes"""
import unittest
from unittest import mock
from unittest.mock import call
from xml.etree import ElementTree as ET

from o2a.converter.exceptions import ParseException
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils.config_extractors import (
    extract_properties_from_configuration_node,
    TAG_JOB_XML,
    extract_properties_from_job_xml_nodes,
)
from o2a.utils.xml_utils import find_nodes_by_tag


# pylint: disable=invalid-name
class ConfigExtractorsModuleTestCase(unittest.TestCase):
    def test_extract_properties_from_configuration_node_miniaml_green_path(self):
        # language=XML
        config_node_str = """
    <configuration>
        <property>
            <name>mapred.mapper.new-api</name>
            <value>true</value>
        </property>
        <property>
            <name>mapred.reducer.new-api</name>
            <value>true</value>
        </property>
    </configuration>
"""
        config_node = ET.fromstring(config_node_str)
        properties = extract_properties_from_configuration_node(config_node, props=PropertySet())
        self.assertEqual(properties, {"mapred.mapper.new-api": "true", "mapred.reducer.new-api": "true"})

    def test_extract_properties_from_configuration_node_when_empty(self):
        # language=XML
        config_node_str = """
    <configuration>
    </configuration>
"""
        config_node = ET.fromstring(config_node_str)
        properties = extract_properties_from_configuration_node(config_node, props=PropertySet())
        self.assertEqual(properties, {})

    def test_extract_properties_from_configuration_node_should_el_replace(self):
        # language=XML
        config_node_str = """
    <configuration>
        <property>
            <name>mapred.reducer.new-api</name>
            <value>/user/mapred/${examplesRoot}/config/output</value>
        </property>
    </configuration>
"""
        config_node = ET.fromstring(config_node_str)
        properties = extract_properties_from_configuration_node(
            config_node, props=PropertySet(job_properties={"examplesRoot": "AAA"})
        )
        self.assertEqual(properties, {"mapred.reducer.new-api": "/user/mapred/AAA/config/output"})

    def test_extract_properties_from_configuration_node_should_raise_exception_when_name_element_is_missing(
        self
    ):
        # language=XML
        config_node_str = """
    <configuration>
        <property>
            <value>/user/mapred/${examplesRoot}/config/output</value>
        </property>
    </configuration>
"""
        config_node = ET.fromstring(config_node_str)
        with self.assertRaisesRegex(
            ParseException,
            'Element "property" should have direct children elements: name, value. One of them does not '
            "exist. Make sure the configuration element is valid.",
        ):
            extract_properties_from_configuration_node(config_node, props=PropertySet())

    def test_extract_properties_from_configuration_node_should_raise_exception_when_value_element_is_missing(
        self
    ):
        # language=XML
        config_node_str = """
    <configuration>
        <property>
            <name>mapred.reducer.new-api</name>
        </property>
    </configuration>
"""
        config_node = ET.fromstring(config_node_str)
        with self.assertRaisesRegex(
            ParseException,
            'Element "property" should have direct children elements: name, value. One of them does '
            "not exist. Make sure the configuration element is valid.",
        ):
            extract_properties_from_configuration_node(config_node, props=PropertySet())

    def test_extract_properties_from_configuration_node_should_raise_exception_when_name_element_is_empty(
        self
    ):
        # language=XML
        config_node_str = """
    <configuration>
        <property>
            <name></name>
            <value>AAA</value>
        </property>
    </configuration>
"""
        config_node = ET.fromstring(config_node_str)
        with self.assertRaisesRegex(
            ParseException,
            'Element "name" should have content, however its value is empty. Make sure the element has the '
            "correct content.",
        ):
            extract_properties_from_configuration_node(config_node, props=PropertySet())

    def test_extract_properties_from_configuration_node_should_raise_exception_when_value_element_is_empty(
        self
    ):
        # language=XML
        config_node_str = """
    <configuration>
        <property>
            <name>mapred.reducer.new-api</name>
            <value></value>
        </property>
    </configuration>
"""
        config_node = ET.fromstring(config_node_str)
        with self.assertRaisesRegex(
            ParseException,
            'Element "value" should have content, however its value is empty. Make sure the element has the '
            "correct content.",
        ):
            extract_properties_from_configuration_node(config_node, props=PropertySet())

    @mock.patch("o2a.utils.config_extractors.ET.parse")
    def test_extract_properties_from_job_xml_nodes_minimal_green_path(self, parse_mock):
        # language=XML
        action = ET.ElementTree(
            ET.fromstring(
                """
    <action>
        <job-xml>aaa.xml</job-xml>
    </action>
"""
            )
        )
        # language=XML
        parse_mock.return_value = ET.ElementTree(
            ET.fromstring(
                """
    <configuration>
        <property>
            <name>KEY1</name>
            <value>VALUE1</value>
        </property>
        <property>
            <name>KEY2</name>
            <value>VALUE2</value>
        </property>
    </configuration>
"""
            )
        )
        job_xml_nodes = find_nodes_by_tag(action, TAG_JOB_XML)
        result = extract_properties_from_job_xml_nodes(
            job_xml_nodes, input_directory_path="/tmp/no-error-path", props=PropertySet()
        )

        parse_mock.assert_called_once_with("/tmp/no-error-path/hdfs/aaa.xml")
        self.assertEqual(result, {"KEY1": "VALUE1", "KEY2": "VALUE2"})

    @mock.patch("o2a.utils.config_extractors.ET.parse")
    def test_extract_properties_from_job_xml_nodes_should_parse_multie_elements(self, parse_mock):
        # language=XML
        action = ET.ElementTree(
            ET.fromstring(
                """
    <action>
        <job-xml>aaa.xml</job-xml>
        <job-xml>bbb.xml</job-xml>
    </action>
"""
            )
        )
        # language=XML
        parse_mock.side_effect = [
            ET.ElementTree(
                ET.fromstring(
                    """
    <configuration>
        <property>
            <name>KEY1</name>
            <value>VALUE1</value>
        </property>
    </configuration>
"""
                )
            ),
            ET.ElementTree(
                ET.fromstring(
                    """
    <configuration>
        <property>
            <name>KEY2</name>
            <value>VALUE2</value>
        </property>
    </configuration>
"""
                )
            ),
        ]
        job_xml_nodes = find_nodes_by_tag(action, TAG_JOB_XML)
        result = extract_properties_from_job_xml_nodes(
            job_xml_nodes, input_directory_path="/tmp/no-error-path", props=PropertySet()
        )

        parse_mock.assert_has_calls(
            [call("/tmp/no-error-path/hdfs/aaa.xml"), call("/tmp/no-error-path/hdfs/bbb.xml")]
        )
        self.assertEqual(result, {"KEY1": "VALUE1", "KEY2": "VALUE2"})
