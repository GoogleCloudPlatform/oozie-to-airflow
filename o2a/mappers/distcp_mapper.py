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
""" DistCp Mapper module """
import shlex
from typing import Dict, List, Set
from xml.etree.ElementTree import Element


from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper
from o2a.o2a_libs.src.o2a_lib.property_utils import PropertySet
from o2a.utils import xml_utils, el_utils
from o2a.utils.file_archive_extractors import ArchiveExtractor, FileExtractor


class DistCpMapper(ActionMapper):
    """
    Converts a Pig Oozie node to an Airflow task.
    """

    def __init__(self, oozie_node: Element, name: str, dag_name: str, props: PropertySet, **kwargs):
        ActionMapper.__init__(
            self, oozie_node=oozie_node, dag_name=dag_name, name=name, props=props, **kwargs
        )
        self.props = props
        self.params_dict: Dict[str, str] = {}
        self.file_extractor = FileExtractor(oozie_node=oozie_node, props=props)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, props=props)
        self.oozie_node = oozie_node
        self.args: str = ""
        self.prepare_extension: PrepareMapperExtension = PrepareMapperExtension(self)

    def _parse_args(self):
        args = []
        arg_nodes = xml_utils.find_nodes_by_tag(self.oozie_node, "arg")
        if arg_nodes:
            for node in arg_nodes:
                value: str = node.text
                if "/" in value:
                    # If an argument contains a forward slash then it's a URL.
                    # The full URL should be preserved when replacing the EL (and not just the path)
                    #   to enable copying files between two different clusters.
                    value = el_utils.replace_url_el(value, props=self.props)
                value = shlex.quote(value)
                args.append(value)
        return " ".join(args)

    def _get_distcp_command(self):
        return f"--class=org.apache.hadoop.tools.DistCp -- {self.args}"

    def on_parse_node(self):
        super().on_parse_node()
        self.args = self._parse_args()

    def to_tasks_and_relations(self):
        action_task = Task(
            task_id=self.name,
            template_name="distcp.tpl",
            template_params=dict(props=self.props, distcp_command=self._get_distcp_command()),
        )
        tasks = [action_task]
        relations: List[Relation] = []
        prepare_task = self.prepare_extension.get_prepare_task()
        if prepare_task:
            tasks, relations = self.prepend_task(prepare_task, tasks, relations)
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {"import shlex", "from airflow.operators import bash"}
