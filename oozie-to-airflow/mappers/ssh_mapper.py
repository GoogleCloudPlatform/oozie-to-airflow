# Copyright 2018 Google LLC
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
import os

from airflow.contrib.operators import ssh_operator
from airflow.contrib.hooks import ssh_hook
from airflow.utils.trigger_rule import TriggerRule

from definitions import ROOT_DIR
from mappers.action_mapper import ActionMapper
from utils import el_utils
import jinja2


class SSHMapper(ActionMapper):
    """
    Converts an SSH oozie node to Airflow operator.

    In order to use this, the user must specify an Airflow connection to use, and
    provide the password there.
    """

    def __init__(
        self,
        oozie_node,
        task_id,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        params={},
        template="ssh.tpl",
        **kwargs,
    ):
        ActionMapper.__init__(self, oozie_node, task_id, trigger_rule, **kwargs)

        self.template = template

        cmd_node = self.oozie_node.find("command")
        arg_nodes = self.oozie_node.findall("args")
        cmd = " ".join([cmd_node.text] + [x.text for x in arg_nodes])
        self.command = el_utils.convert_el_to_jinja(cmd, quote=True)

        host_key = el_utils.strip_el(self.oozie_node.find("host").text)
        # the <user> node is formatted like [USER]@[HOST]
        if host_key in params:
            host_key = params[host_key]

        # Since ariflow separates user and host, we can't use jinja templating.
        # We must check if it is in params.
        user_host = host_key.split("@")
        self.user = user_host[0]
        self.host = user_host[1]

    def convert_to_text(self):
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.join(ROOT_DIR, "templates/"))
        template_env = jinja2.Environment(loader=template_loader)

        template = template_env.get_template(self.template)
        return template.render(**self.__dict__)

    def convert_to_airflow_op(self):
        """
        Oozie has 3 properties, host, arguments, command, and capture-output
        Airflow has host and command

        returns an SSH Operator
        """
        hook = ssh_hook.SSHHook(ssh_conn_id="ssh_default", username=self.user, remote_host=self.host)
        return ssh_operator.SSHOperator(
            ssh_hook=hook, task_id=self.task_id, command=self.command, trigger_rule=self.trigger_rule
        )

    @staticmethod
    def required_imports():
        return [
            "from airflow.utils import dates",
            "from airflow.contrib.operators import ssh_operator",
            "from airflow.contrib.hooks import ssh_hook",
        ]
