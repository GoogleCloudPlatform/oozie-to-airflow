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
"""Maps SSH Oozie node to Airflow's DAG"""
import shlex
from typing import Dict, Set, List
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from converter.primitives import Task, Relation
from mappers.action_mapper import ActionMapper
from utils import el_utils

from utils.template_utils import render_template


class SSHMapper(ActionMapper):
    """
    Converts an SSH oozie node to Airflow operator.

    In order to use this, the user must specify an Airflow connection to use, and
    provide the password there.
    """

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        properties: Dict[str, str],
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        template: str = "ssh.tpl",
        **kwargs,
    ):
        ActionMapper.__init__(
            self, oozie_node=oozie_node, name=name, trigger_rule=trigger_rule, properties=properties, **kwargs
        )
        self.template = template

        self.command = self.get_command()
        self.user, self.host = self.get_user_host()

    def get_user_host(self):
        host = self.oozie_node.find("host")
        if host is None or not host.text:
            raise Exception("Missing host node in SSH action: {}".format(self.oozie_node))
        host_key = el_utils.replace_el_with_property_values(host.text, properties=self.properties)
        # the <user> node is formatted like [USER]@[HOST]
        if host_key in self.properties:
            host_key = self.properties[host_key]
        # Since airflow separates user and host, we can't use jinja templating.
        # We must check if it is in properties.
        user_host = host_key.split("@")
        return user_host[0], user_host[1]

    def get_command(self):
        cmd_node = self.oozie_node.find("command")
        arg_nodes = self.oozie_node.findall("args")
        if cmd_node is None or not cmd_node.text:
            raise Exception("Missing or empty command node in SSH action {}".format(self.oozie_node))
        cmd = cmd_node.text
        args = (x.text if x.text else "" for x in arg_nodes)
        cmd = " ".join(shlex.quote(x) for x in [cmd, *args])
        return el_utils.convert_el_string_to_fstring(cmd, properties=self.properties)

    def convert_to_text(self) -> str:
        tasks = [
            Task(
                task_id=self.name,
                template_name="ssh.tpl",
                trigger_rule=self.trigger_rule,
                template_params=dict(
                    properties=self.properties, command=self.command, user=self.user, host=self.host
                ),
            )
        ]
        relations: List[Relation] = []
        return render_template(template_name="action.tpl", tasks=tasks, relations=relations)

    def required_imports(self) -> Set[str]:
        return {
            "from airflow.utils import dates",
            "from airflow.contrib.operators import ssh_operator",
            "from airflow.contrib.hooks import ssh_hook",
        }
