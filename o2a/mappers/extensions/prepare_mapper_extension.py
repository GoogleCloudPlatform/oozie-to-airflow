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
from typing import List, Tuple, Optional

from o2a.converter.task import Task
from o2a.mappers.base_mapper import BaseMapper
from o2a.utils import xml_utils
from o2a.utils.el_utils import normalize_path


class PrepareMapperExtension:
    """Extension of mapper used to add Prepare node capability to a node in composable way"""

    def __init__(self, mapper: BaseMapper):
        self.mapper: BaseMapper = mapper

    def has_prepare(self):
        prepare_node = xml_utils.find_node_by_tag(self.mapper.oozie_node, "prepare")
        if prepare_node:
            delete_nodes = xml_utils.find_nodes_by_tag(prepare_node, "delete")
            mkdir_nodes = xml_utils.find_node_by_tag(prepare_node, "mkdir")
            if delete_nodes or mkdir_nodes:
                return True
        return False

    def get_prepare_task(self) -> Optional[Task]:
        delete_paths, mkdir_paths = self.parse_prepare_node()
        if not delete_paths and not mkdir_paths:
            return None
        delete = " ".join(delete_paths) if delete_paths else None
        mkdir = " ".join(mkdir_paths) if mkdir_paths else None
        return Task(
            task_id=self.mapper.name + "_prepare",
            template_name="prepare.tpl",
            template_params=dict(delete=delete, mkdir=mkdir),
        )

    def parse_prepare_node(self) -> Tuple[List[str], List[str]]:
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
        prepare_node = xml_utils.find_node_by_tag(self.mapper.oozie_node, "prepare")
        if prepare_node:
            # If there exists a prepare node, there will only be one, according
            # to oozie xml schema
            for node in prepare_node:
                node_path = normalize_path(node.attrib["path"], props=self.mapper.props)
                if node.tag == "delete":
                    delete_paths.append(node_path)
                elif node.tag == "mkdir":
                    mkdir_paths.append(node_path)
                else:
                    raise Exception(f"Unknown XML node in prepare: {node.tag}")
        return delete_paths, mkdir_paths
