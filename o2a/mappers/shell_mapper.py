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
"""Maps Shell action into Airflow's DAG"""
from typing import List, Set
from xml.etree.ElementTree import Element


from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils.xml_utils import get_tag_el_text, get_tags_el_array_from_text
from o2a.utils.el_utils import convert_el_to_jinja


TAG_RESOURCE = "resource-manager"
TAG_NAME = "name-node"
TAG_CMD = "exec"
TAG_ARG = "argument"


class ShellMapper(ActionMapper):
    """
    Converts a Shell Oozie action to an Airflow task.
    """

    def __init__(self, oozie_node: Element, name: str, props: PropertySet, **kwargs):
        ActionMapper.__init__(self, oozie_node=oozie_node, name=name, props=props, **kwargs)
        self._parse_oozie_node()
        self.prepare_extension: PrepareMapperExtension = PrepareMapperExtension(self)

    def _parse_oozie_node(self):
        self.resource_manager = get_tag_el_text(self.oozie_node, TAG_RESOURCE, self.props)
        self.name_node = get_tag_el_text(self.oozie_node, TAG_NAME, self.props)

        cmd_txt = get_tag_el_text(self.oozie_node, TAG_CMD, self.props)
        args = get_tags_el_array_from_text(self.oozie_node, TAG_ARG, self.props)
        cmd = " ".join([cmd_txt] + [x for x in args])

        self.bash_command = convert_el_to_jinja(cmd, quote=False)
        self.pig_command = f"sh {self.bash_command}"

    def to_tasks_and_relations(self):
        action_task = Task(
            task_id=self.name,
            template_name="shell.tpl",
            template_params=dict(
                pig_command=self.pig_command, action_node_properties=self.props.action_node_properties
            ),
        )
        tasks = [action_task]
        relations: List[Relation] = []
        prepare_task = self.prepare_extension.get_prepare_task()
        if prepare_task:
            tasks, relations = self.prepend_task(prepare_task, tasks, relations)
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}
