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
from typing import Dict, Set

import xml.etree.ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

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
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        params: Dict[str, str] = None,
        template: str = "shell.tpl",
        **kwargs,
    ):
        ActionMapper.__init__(self, oozie_node, name, trigger_rule, **kwargs)
        if params is None:
            params = {}
        self.template = template
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

    def convert_to_text(self) -> str:
        prepare_command = self.get_prepare_command(self.oozie_node, self.params)
        return render_template(
            template_name=self.template, prepare_command=prepare_command, task_id=self.name, **self.__dict__
        )

    @staticmethod
    def required_imports() -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}
