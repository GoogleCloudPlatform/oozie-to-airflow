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
import shlex
from typing import List, Set, Tuple
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.prepare_mixin import PrepareMixin
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils import el_utils


class ShellMapper(ActionMapper, PrepareMixin):
    """
    Converts a Shell Oozie action to an Airflow task.
    """

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        property_set: PropertySet,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        ActionMapper.__init__(
            self,
            oozie_node=oozie_node,
            name=name,
            trigger_rule=trigger_rule,
            property_set=property_set,
            **kwargs,
        )
        PrepareMixin.__init__(self, oozie_node=oozie_node)
        self._parse_oozie_node()

    def _parse_oozie_node(self):
        res_man_text = self.oozie_node.find("resource-manager").text
        name_node_text = self.oozie_node.find("name-node").text
        self.resource_manager = el_utils.replace_el_with_var(
            res_man_text, property_set=self.property_set, quote=False
        )
        self.name_node = el_utils.replace_el_with_var(
            name_node_text, property_set=self.property_set, quote=False
        )
        cmd_node = self.oozie_node.find("exec")
        arg_nodes = self.oozie_node.findall("argument")
        cmd = " ".join([cmd_node.text] + [x.text for x in arg_nodes])
        self.bash_command = el_utils.convert_el_to_jinja(cmd, quote=False)
        self.pig_command = f"sh {shlex.quote(self.bash_command)}"

    def to_tasks_and_relations(self) -> Tuple[List[Task], List[Relation]]:
        action_task = Task(
            task_id=self.name,
            template_name="shell.tpl",
            template_params=dict(
                pig_command=self.pig_command, action_node_properties=self.property_set.action_node_properties
            ),
        )
        tasks: List[Task] = [action_task]
        relations: List[Relation] = []
        prepare_task = self.get_prepare_task(
            name=self.name, trigger_rule=self.trigger_rule, property_set=self.property_set
        )
        if prepare_task:
            tasks = [prepare_task, action_task]
            relations = [Relation(from_task_id=prepare_task.task_id, to_task_id=self.name)]
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}

    @property
    def first_task_id(self) -> str:
        return f"{self.name}_prepare" if self.has_prepare() else self.name
