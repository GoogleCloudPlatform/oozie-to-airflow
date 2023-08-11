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
from typing import List, Set
from xml.etree.ElementTree import Element


from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.o2a_libs.src.o2a_lib.property_utils import PropertySet
from o2a.o2a_libs.src.o2a_lib import el_parser
from o2a.utils import el_utils, xml_utils


TAG_CMD = "command"
TAG_ARG = "args"
TAG_HOST = "host"


class SSHMapper(ActionMapper):
    """
    Converts an SSH oozie node to Airflow operator.

    In order to use this, the user must specify an Airflow connection to use, and
    provide the password there.
    """

    def __init__(
        self, oozie_node: Element, name: str, props: PropertySet, template: str = "ssh.tpl", **kwargs
    ):
        ActionMapper.__init__(self, oozie_node=oozie_node, name=name, props=props, **kwargs)
        self.template = template

        self.command = self.get_command()
        host_key = self.get_host_key()

        # Since Airflow separates user and host, we can't use jinja templating.
        # We must check if it is in job_properties.
        user_host = host_key.split("@")
        self.user = user_host[0]
        self.host = user_host[1]

    def get_command(self) -> str:
        cmd_txt = xml_utils.get_tag_el_text(self.oozie_node, TAG_CMD)
        args = xml_utils.get_tags_el_array_from_text(self.oozie_node, TAG_ARG)
        if not cmd_txt:
            raise Exception(f"Missing or empty command node in SSH action {self.oozie_node}")

        cmd = " ".join([cmd_txt] + [shlex.quote(x) for x in args])
        cmd = el_parser.translate(cmd, quote=True)
        return cmd

    def get_host_key(self) -> str:
        host = self.oozie_node.find("host")
        if host is None or not host.text:
            raise Exception(f"Missing host node in SSH action: {self.oozie_node}")
        host_key = el_utils.strip_el(host.text)
        # the <user> node is formatted like [USER]@[HOST]
        if host_key in self.props.merged:
            host_key = self.props.merged[host_key]
        return host_key

    def to_tasks_and_relations(self):
        # SSH does not support prepare node so no need for prepare task
        tasks = [
            Task(
                task_id=self.name,
                template_name="ssh.tpl",
                template_params=dict(
                    props=self.props,
                    # SSH mapper does not support <configuration></configuration> node -
                    # no action_node_properties are needed
                    command=self.command,
                    user=self.user,
                    host=self.host,
                ),
            )
        ]
        relations: List[Relation] = []
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {
            "from airflow.utils import dates",
            "from airflow.providers.ssh.operators.ssh import SSHOperator",
            "from airflow.providers.ssh.hooks.ssh import SSHHook",
        }
