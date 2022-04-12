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
from typing import List, Set, Type
from xml.etree.ElementTree import Element


from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils.xml_utils import get_tag_el_text, get_tags_el_array_from_text, find_nodes_by_tag
from o2a.o2a_libs import el_parser
from o2a.tasks.shell.shell_local_task import ShellLocalTask


TAG_RESOURCE = "resource-manager"
TAG_NAME = "name-node"
TAG_CMD = "exec"
TAG_ARG = "argument"


class ShellMapper(ActionMapper):
    """
    Converts a Shell Oozie action to an Airflow task.
    """

    TASK_MAPPER = {
        "local": ShellLocalTask,
        "ssh": Task,
        "gcp": Task,
    }

    def __init__(self, oozie_node: Element, name: str, props: PropertySet, **kwargs):
        ActionMapper.__init__(self, oozie_node=oozie_node, name=name, props=props, **kwargs)
        self._parse_oozie_node()
        self.prepare_extension: PrepareMapperExtension = PrepareMapperExtension(self)

    def _parse_oozie_node(self):
        self.resource_manager = get_tag_el_text(self.oozie_node, TAG_RESOURCE)
        self.name_node = get_tag_el_text(self.oozie_node, TAG_NAME)

        cmd_txt = get_tag_el_text(self.oozie_node, TAG_CMD)
        args = get_tags_el_array_from_text(self.oozie_node, TAG_ARG)
        cmd = " ".join([cmd_txt] + args)
        self.bash_command = el_parser.translate(cmd, quote=False)

        self.env_vars = self.get_env_vars()

    def get_env_vars(self):
        """
        Returns a list of environment variables to be used in the shell command.
        example: <env-var>VAR1=VALUE1</env-var>
        will be translated to {"VAR1": VALUE1}
        """
        env_var_nodes = find_nodes_by_tag(self.oozie_node, "env-var")
        env_vars = dict()
        for env_var_node in env_var_nodes:
            node_txt = el_parser.translate(env_var_node.text, quote=False)
            name, value = node_txt.split("=")
            env_vars[name] = value
        return env_vars

    def to_tasks_and_relations(self):
        task_class: Type[Task] = self.get_task_class(self.TASK_MAPPER)
        action_task = task_class(
            task_id=self.name,
            template_name="shell/shell.tpl",
            template_params=dict(
                bash_command=self.bash_command, action_node_properties=self.props.action_node_properties,
                env=self.env_vars,
            ),
        )
        tasks = [action_task]
        relations: List[Relation] = []
        prepare_task = self.prepare_extension.get_prepare_task()
        if prepare_task:
            tasks, relations = self.prepend_task(prepare_task, tasks, relations)
        return tasks, relations

    def required_imports(self) -> Set[str]:
        dependencies = self.get_task_class(self.TASK_MAPPER).required_imports()
        prepare_dependencies = self.prepare_extension.required_imports()

        return dependencies.union(prepare_dependencies)
