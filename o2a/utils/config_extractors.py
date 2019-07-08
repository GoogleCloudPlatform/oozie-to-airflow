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
from os import path
from typing import Dict, List
import xml.etree.ElementTree as ET

from o2a.converter.constants import HDFS_FOLDER
from o2a.converter.exceptions import ParseException
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils import el_utils

TAG_CONFIGURATION = "configuration"
TAG_PROPERTY = "property"
TAG_NAME = "name"
TAG_VALUE = "value"
TAG_JOB_XML = "job-xml"


def extract_properties_from_configuration_node(config_node: ET.Element, props: PropertySet) -> Dict[str, str]:
    """Extracts configuration properties from ``configuration`` node"""
    properties_dict: Dict[str, str] = dict()
    for property_node in config_node.findall(TAG_PROPERTY):
        name_node = property_node.find(TAG_NAME)
        value_node = property_node.find(TAG_VALUE)

        if name_node is None or value_node is None:
            raise ParseException(
                'Element "property" should have direct children elements: name, value. One of them does not '
                "exist. Make sure the configuration element is valid."
            )

        name = name_node.text
        value = value_node.text

        if not name:
            raise ParseException(
                'Element "name" should have content, however its value is empty. Make sure the element has '
                "the correct content."
            )

        if not value:
            raise ParseException(
                'Element "value" should have content, however its value is empty. Make sure the element has '
                "the correct content."
            )

        properties_dict[name] = el_utils.replace_el_with_var(value, props=props, quote=False)

    return properties_dict


def extract_properties_from_job_xml_nodes(
    job_xml_nodes: List[ET.Element], input_directory_path: str, props: PropertySet
):
    """Extracts configuration properties from ``job_xml`` nodes"""
    properties_dict: Dict[str, str] = dict()

    for xml_file in job_xml_nodes:
        file_name = xml_file.text
        if not file_name:
            raise ParseException(
                'Element "job-xml" should have content, however its value is empty. Make sure the element '
                "has the correct content."
            )
        file_path = path.join(input_directory_path, HDFS_FOLDER, file_name)
        config_tree = ET.parse(file_path)
        config_node = config_tree.getroot()
        if not config_node:
            raise ParseException(
                "A job-xml configuration node is specified in the workflow XML, however its value is empty."
                "Make sure the path to a configuration file is valid."
            )
        new_properties = extract_properties_from_configuration_node(config_node, props=props)
        properties_dict.update(new_properties)

    return properties_dict
