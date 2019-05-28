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
from abc import ABC
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from o2a.mappers.base_mapper import BaseMapper
from o2a.utils import xml_utils, el_utils


# pylint: disable=abstract-method
# noinspection PyAbstractClass
class ActionMapper(BaseMapper, ABC):
    """Base class for all action mappers"""

    def __init__(self, oozie_node: Element, name: str, trigger_rule: str = TriggerRule.ALL_SUCCESS, **kwargs):
        super().__init__(oozie_node, name, **kwargs)
        self.properties: Dict[str, str] = {}
        self.trigger_rule: str = trigger_rule

    def on_parse_node(self):
        super().on_parse_node()
        self._parse_config()

    def _parse_config(self):
        config = self.oozie_node.find("configuration")
        if config:
            property_nodes = xml_utils.find_nodes_by_tag(config, "property")
            if property_nodes:
                for node in property_nodes:
                    name = node.find("name").text
                    value = el_utils.replace_el_with_var(
                        node.find("value").text, params=self.params, quote=False
                    )
                    self.properties[name] = value
