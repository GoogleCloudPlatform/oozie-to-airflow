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
"""Tests shell mapper"""
import ast
import unittest
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from mappers import shell_mapper


class TestShellMapper(unittest.TestCase):
    def setUp(self):
        # language=XML
        shell_node_str = """
<shell>
    <resource-manager>localhost:8032</resource-manager>
    <name-node>hdfs://localhost:8020</name-node>
    <prepare>
        <delete path="${nameNode}/examples/output-data/demo/pig-node" />
        <delete path="${nameNode}/examples/output-data/demo/pig-node2" />
        <mkdir path="${nameNode}/examples/input-data/demo/pig-node" />
        <mkdir path="${nameNode}/examples/input-data/demo/pig-node2" />
     </prepare>
     <configuration>
        <property>
            <name>mapred.job.queue.name</name>
            <value>${queueName}</value>
        </property><property>
            <name>mapred.map.output.compress</name>
            <value>false</value>
        </property>
    </configuration>
    <exec>echo</exec>
    <argument>arg1</argument>
    <argument>arg2</argument>
</shell>
"""
        self.shell_node = ET.fromstring(shell_node_str)

    def test_create_mapper_no_jinja(self):
        mapper = shell_mapper.ShellMapper(
            oozie_node=self.shell_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.shell_node, mapper.oozie_node)
        self.assertEqual("localhost:8032", mapper.resource_manager)
        self.assertEqual("hdfs://localhost:8020", mapper.name_node)
        self.assertEqual("${queueName}", mapper.properties["mapred.job.queue.name"])
        self.assertEqual("echo arg1 arg2", mapper.bash_command)

    def test_create_mapper_jinja(self):
        # test jinja templating
        self.shell_node.find("resource-manager").text = "${resourceManager}"
        self.shell_node.find("name-node").text = "${nameNode}"
        params = {
            "resourceManager": "localhost:9999",
            "nameNode": "hdfs://localhost:8021",
            "queueName": "myQueue",
            "examplesRoot": "examples",
        }

        mapper = shell_mapper.ShellMapper(
            oozie_node=self.shell_node, name="test_id", trigger_rule=TriggerRule.DUMMY, params=params
        )

        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.shell_node, mapper.oozie_node)
        self.assertEqual("localhost:9999", mapper.resource_manager)
        self.assertEqual("hdfs://localhost:8021", mapper.name_node)
        self.assertEqual("myQueue", mapper.properties["mapred.job.queue.name"])
        self.assertEqual("echo arg1 arg2", mapper.bash_command)

    def test_convert_to_text(self):
        mapper = shell_mapper.ShellMapper(
            oozie_node=self.shell_node,
            name="test_id",
            trigger_rule=TriggerRule.DUMMY,
            params={"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3"},
        )
        # Throws a syntax error if doesn't parse correctly
        ast.parse(mapper.convert_to_text())

    # pylint: disable=no-self-use
    def test_required_imports(self):
        imps = shell_mapper.ShellMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)
