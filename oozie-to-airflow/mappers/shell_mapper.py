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

from converter.primitives import Task, Relation
from mappers.action_mapper import ActionMapper
from mappers.prepare_mixin import PrepareMixin
from utils import el_utils

from utils.template_utils import render_template


class ShellMapper(ActionMapper, PrepareMixin):
    """
    Converts a Shell Oozie action to an Airflow task.
    """

    def __init__(
        self,
        oozie_node: ET.Element,
        name: str,
        properties: Dict[str, str],
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        ActionMapper.__init__(
            self, oozie_node=oozie_node, name=name, trigger_rule=trigger_rule, properties=properties, **kwargs
        )
        self.trigger_rule = trigger_rule
        self._parse_oozie_node()

    def _parse_oozie_node(self):
        res_man_text = self.oozie_node.find("resource-manager").text
        name_node_text = self.oozie_node.find("name-node").text
        self.resource_manager = el_utils.convert_el_string_to_fstring(
            res_man_text, properties=self.properties
        )
        self.name_node = el_utils.convert_el_string_to_fstring(name_node_text, properties=self.properties)
        cmd_node = self.oozie_node.find("exec")
        arg_nodes = self.oozie_node.findall("argument")
        cmd = " ".join([cmd_node.text] + [x.text for x in arg_nodes])
        self.bash_command = el_utils.convert_el_string_to_fstring(cmd, properties=self.properties)
        quoted_command = shlex.quote(self.bash_command)
        escaped_quoted_command = el_utils.escape_string_with_python_escapes(quoted_command)
        self.pig_command = f"sh {escaped_quoted_command}"

    def convert_to_text(self) -> str:
        prepare_command, prepare_arguments = self.get_prepare_command_with_arguments(
            self.oozie_node, self.properties
        )
        tasks = [
            Task(
                task_id=self.name + "-prepare",
                template_name="prepare.tpl",
                template_params=dict(prepare_command=prepare_command, prepare_arguments=prepare_arguments),
            ),
            Task(
                task_id=self.name,
                template_name="shell.tpl",
                template_params=dict(pig_command=self.pig_command),
            ),
        ]
        relations = [Relation(from_task_id=self.name + "-prepare", to_task_id=self.name)]
        return render_template(template_name="action.tpl", tasks=tasks, relations=relations)

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}

    @property
    def first_task_id(self):
        return "{task_id}-prepare".format(task_id=self.name)
