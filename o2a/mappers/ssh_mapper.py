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

from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.utils import el_utils


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
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        params: Dict[str, str] = None,
        template: str = "ssh.tpl",
        **kwargs,
    ):
        ActionMapper.__init__(self, oozie_node=oozie_node, name=name, trigger_rule=trigger_rule, **kwargs)

        if params is None:
            params = {}
        self.template = template

        cmd_node = self.oozie_node.find("command")
        arg_nodes = self.oozie_node.findall("args")
        if cmd_node is None or not cmd_node.text:
            raise Exception("Missing or empty command node in SSH action {}".format(self.oozie_node))
        cmd = cmd_node.text
        args = (x.text if x.text else "" for x in arg_nodes)
        cmd = " ".join(shlex.quote(x) for x in [cmd, *args])

        self.command = el_utils.convert_el_to_jinja(cmd, quote=True)
        host = self.oozie_node.find("host")
        if host is None:
            raise Exception("Missing host node in SSH action: {}".format(self.oozie_node))
        host_key = el_utils.strip_el(host.text)
        # the <user> node is formatted like [USER]@[HOST]
        if host_key in params:
            host_key = params[host_key]

        # Since ariflow separates user and host, we can't use jinja templating.
        # We must check if it is in params.
        user_host = host_key.split("@")
        self.user = user_host[0]
        self.host = user_host[1]

    def to_tasks_and_relations(self):
        tasks = [
            Task(
                task_id=self.name,
                template_name="ssh.tpl",
                trigger_rule=self.trigger_rule,
                template_params=dict(
                    params=self.params, command=self.command, user=self.user, host=self.host
                ),
            )
        ]
        relations: List[Relation] = []
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {
            "from airflow.utils import dates",
            "from airflow.contrib.operators import ssh_operator",
            "from airflow.contrib.hooks import ssh_hook",
        }
