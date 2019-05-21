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
from typing import Dict, Set

import xml.etree.ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.prepare_mixin import PrepareMixin
from o2a.utils import el_utils


class ShellMapper(ActionMapper, PrepareMixin):
    """
    Converts a Shell Oozie action to an Airflow task.
    """

    def __init__(
        self,
        oozie_node: ET.Element,
        name: str,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        params: Dict[str, str] = None,
        **kwargs,
    ):
        ActionMapper.__init__(self, oozie_node, name, trigger_rule, **kwargs)
        if params is None:
            params = {}
        self.params = params
        self.trigger_rule = trigger_rule
        self._parse_oozie_node()

    def _parse_oozie_node(self):
        res_man_text = self.oozie_node.find("resource-manager").text
        name_node_text = self.oozie_node.find("name-node").text
        self.resource_manager = el_utils.replace_el_with_var(res_man_text, params=self.params, quote=False)
        self.name_node = el_utils.replace_el_with_var(name_node_text, params=self.params, quote=False)
        self._parse_config()
        cmd_node = self.oozie_node.find("exec")
        arg_nodes = self.oozie_node.findall("argument")
        cmd = " ".join([cmd_node.text] + [x.text for x in arg_nodes])
        self.bash_command = el_utils.convert_el_to_jinja(cmd, quote=False)
        self.pig_command = f"sh {shlex.quote(self.bash_command)}"

    def to_tasks_and_relations(self):
        prepare_command = self.get_prepare_command(self.oozie_node, self.params)
        tasks = [
            Task(
                task_id=self.name + "_prepare",
                template_name="prepare.tpl",
                template_params=dict(prepare_command=prepare_command),
            ),
            Task(
                task_id=self.name,
                template_name="shell.tpl",
                template_params=dict(pig_command=self.pig_command),
            ),
        ]
        relations = [Relation(from_task_id=self.name + "_prepare", to_task_id=self.name)]
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}

    @property
    def first_task_id(self):
        return "{task_id}_prepare".format(task_id=self.name)
