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
"""Tests for SSH mapper"""
import ast
import unittest
from unittest import mock

from xml.etree import ElementTree as ET
from airflow.utils.trigger_rule import TriggerRule

from converter.primitives import Task
from mappers import ssh_mapper


class TestSSHMapper(unittest.TestCase):
    def setUp(self):
        # language=XML
        ssh_node_str = """
<ssh>
    <host>user@apache.org</host>
    <command>ls</command>
    <args>-l</args>
    <args>-a</args>
    <capture-output />
</ssh>
"""
        self.ssh_node = ET.fromstring(ssh_node_str)

    def test_create_mapper_no_jinja(self):
        mapper = ssh_mapper.SSHMapper(
            oozie_node=self.ssh_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.ssh_node, mapper.oozie_node)
        self.assertEqual("user", mapper.user)
        self.assertEqual("apache.org", mapper.host)
        self.assertEqual("'ls -l -a'", mapper.command)

    def test_create_mapper_jinja(self):
        # test jinja templating
        self.ssh_node.find("host").text = "${hostname}"
        params = {"hostname": "user@apache.org"}

        mapper = ssh_mapper.SSHMapper(
            oozie_node=self.ssh_node, name="test_id", trigger_rule=TriggerRule.DUMMY, params=params
        )
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.ssh_node, mapper.oozie_node)
        self.assertEqual("user", mapper.user)
        self.assertEqual("apache.org", mapper.host)
        self.assertEqual("'ls -l -a'", mapper.command)

    @mock.patch("mappers.ssh_mapper.render_template", return_value="RETURN")
    def test_convert_to_text(self, render_template_mock):
        mapper = ssh_mapper.SSHMapper(
            oozie_node=self.ssh_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )

        res = mapper.convert_to_text()
        self.assertEqual(res, "RETURN")

        _, kwargs = render_template_mock.call_args
        tasks = kwargs["tasks"]
        relations = kwargs["relations"]

        self.assertEqual(kwargs["template_name"], "action.tpl")
        self.assertEqual(
            tasks,
            [
                Task(
                    task_id="test_id",
                    template_name="ssh.tpl",
                    template_params={
                        "params": {},
                        "trigger_rule": "dummy",
                        "command": "'ls -l -a'",
                        "user": "user",
                        "host": "apache.org",
                    },
                )
            ],
        )
        self.assertEqual(relations, [])

    # pylint: disable=no-self-use
    def test_required_imports(self):
        imps = ssh_mapper.SSHMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)
