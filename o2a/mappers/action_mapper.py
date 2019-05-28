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
from abc import ABC
from typing import Dict, Any
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from o2a.mappers.base_mapper import BaseMapper
from o2a.utils import xml_utils, el_utils


from o2a.o2a_libs.property_utils import PropertySet


# pylint: disable=abstract-method
# noinspection PyAbstractClass
class ActionMapper(BaseMapper, ABC):
    """Base class for all action mappers"""

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        dag_name: str,
        property_set: PropertySet,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs: Any,
    ):
        super().__init__(
            oozie_node=oozie_node,
            name=name,
            dag_name=dag_name,
            property_set=property_set,
            trigger_rule=trigger_rule,
            **kwargs,
        )

    def on_parse_node(self):
        super().on_parse_node()
        self._parse_config()

    def _parse_config(self):
        action_node_properties: Dict[str, str] = {}
        config = self.oozie_node.find("configuration")
        if config:
            property_set = self.property_set
            property_nodes = xml_utils.find_nodes_by_tag(config, "property")
            if property_nodes:
                for node in property_nodes:
                    name = node.find("name").text
                    value = el_utils.replace_el_with_var(
                        node.find("value").text, property_set=property_set, quote=False
                    )
                    action_node_properties[name] = value
        self.property_set.action_node_properties = action_node_properties
