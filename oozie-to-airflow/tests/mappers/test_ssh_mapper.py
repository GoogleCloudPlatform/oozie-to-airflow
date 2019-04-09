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

from xml.etree import ElementTree as ET
from airflow.utils.trigger_rule import TriggerRule
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

    def test_convert_to_text(self):
        mapper = ssh_mapper.SSHMapper(
            oozie_node=self.ssh_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )
        # Throws a syntax error if doesn't parse correctly
        ast.parse(mapper.convert_to_text())

    # pylint: disable=no-self-use
    def test_required_imports(self):
        imps = ssh_mapper.SSHMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)
