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
"""Prepare node mixin"""
from typing import List, Tuple
import xml.etree.ElementTree as ET

from o2a.converter.task import Task
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils import xml_utils
from o2a.utils.el_utils import normalize_path


class PrepareMixin:
    """Mixin used to add Prepare node capability to a node"""

    @staticmethod
    def has_prepare(oozie_node):
        return bool(xml_utils.find_nodes_by_tag(oozie_node, "prepare"))

    def get_prepare_task(self, oozie_node: ET.Element, name: str, trigger_rule: str, property_set) -> Task:
        delete_paths, mkdir_paths = self.parse_prepare_node(oozie_node, property_set=property_set)
        delete = None
        mkdir = None
        if not delete_paths and not mkdir_paths:
            raise Exception(f"There is neither delete nor mkdir in the {oozie_node}'s prepare.")
        if delete_paths:
            delete = " ".join(delete_paths)
        if mkdir_paths:
            mkdir = " ".join(mkdir_paths)
        return Task(
            task_id=name + "_prepare",
            template_name="prepare.tpl",
            trigger_rule=trigger_rule,
            template_params=dict(delete=delete, mkdir=mkdir),
        )

    @staticmethod
    def parse_prepare_node(oozie_node: ET.Element, property_set: PropertySet) -> Tuple[List[str], List[str]]:
        """
        <prepare>
            <delete path="[PATH]"/>
            ...
            <mkdir path="[PATH]"/>
            ...
        </prepare>
        """
        delete_paths = []
        mkdir_paths = []
        prepare_nodes = xml_utils.find_nodes_by_tag(oozie_node, "prepare")
        if prepare_nodes:
            # If there exists a prepare node, there will only be one, according
            # to oozie xml schema
            for node in prepare_nodes[0]:
                node_path = normalize_path(node.attrib["path"], property_set=property_set)
                if node.tag == "delete":
                    delete_paths.append(node_path)
                else:
                    mkdir_paths.append(node_path)
        return delete_paths, mkdir_paths
