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

from o2a.converter.task import Task
from o2a.mappers import ssh_mapper
from o2a.o2a_libs.property_utils import PropertySet


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
        mapper = self._get_ssh_mapper(job_properties={}, config={})
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(self.ssh_node, mapper.oozie_node)
        self.assertEqual("user", mapper.user)
        self.assertEqual("apache.org", mapper.host)
        self.assertEqual("'ls -l -a'", mapper.command)

    def test_create_mapper_jinja(self):
        # test jinja templating
        self.ssh_node.find("host").text = "${hostname}"
        job_properties = {"hostname": "user@apache.org"}

        mapper = self._get_ssh_mapper(job_properties=job_properties, config={})
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(self.ssh_node, mapper.oozie_node)
        self.assertEqual("user", mapper.user)
        self.assertEqual("apache.org", mapper.host)
        self.assertEqual("'ls -l -a'", mapper.command)

    def test_to_tasks_and_relations(self):
        mapper = self._get_ssh_mapper(job_properties={}, config={})

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            [
                Task(
                    task_id="test_id",
                    template_name="ssh.tpl",
                    template_params={
                        "props": PropertySet(config={}, job_properties={}, action_node_properties={}),
                        "command": "'ls -l -a'",
                        "user": "user",
                        "host": "apache.org",
                    },
                )
            ],
            tasks,
        )
        self.assertEqual(relations, [])

    def test_required_imports(self):
        mapper = self._get_ssh_mapper(job_properties={}, config={})
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_ssh_mapper(self, job_properties, config):
        mapper = ssh_mapper.SSHMapper(
            oozie_node=self.ssh_node,
            name="test_id",
            dag_name="DAG_NAME_B",
            props=PropertySet(job_properties=job_properties, config=config),
            input_directory_path="/tmp/input-directory-path/",
        )
        return mapper
