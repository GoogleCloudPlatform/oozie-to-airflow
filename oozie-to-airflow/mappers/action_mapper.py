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
"""Base class for all action nappers"""
from typing import Dict

from converter.primitives import Workflow
from mappers.base_mapper import BaseMapper

from utils.config_extractors import (
    TAG_CONFIGURATION,
    TAG_JOB_XML,
    extract_properties_from_configuration_node,
    extract_properties_from_job_xml_nodes,
)

from utils.xml_utils import find_nodes_by_tag, find_node_by_tag


# pylint: disable=abstract-method
# noinspection PyAbstractClass
class ActionMapper(BaseMapper):
    """Base class for all action mappers"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.properties: Dict[str, str] = {}

    def on_parse_node(self, workflow):
        super().on_parse_node(workflow)
        self._parse_job_xml_and_configuration(workflow)

    def _parse_job_xml_and_configuration(self, workflow: Workflow):
        job_xml_nodes = find_nodes_by_tag(self.oozie_node, TAG_JOB_XML)
        self.properties.update(
            extract_properties_from_job_xml_nodes(
                job_xml_nodes=job_xml_nodes,
                input_directory_path=workflow.input_directory_path,
                params=self.params,
            )
        )

        configuration_node = find_node_by_tag(self.oozie_node, TAG_CONFIGURATION)
        if configuration_node is not None:
            new_properties = extract_properties_from_configuration_node(
                config_node=configuration_node, params=self.params
            )
            self.properties.update(new_properties)
