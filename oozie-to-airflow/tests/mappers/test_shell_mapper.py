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
from unittest import mock
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from converter.primitives import Task, Relation
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

    def test_create_mapper_jinja(self):
        # test jinja templating
        self.shell_node.find("resource-manager").text = "${resourceManager}"
        self.shell_node.find("name-node").text = "${nameNode}"
        properties = {
            "resourceManager": "localhost:9999",
            "nameNode": "hdfs://localhost:8021",
            "queueName": "myQueue",
            "examplesRoot": "examples",
        }

        mapper = self._get_shell_mapper(properties=properties)

        # make sure everything is getting initialized correctly
        self.assertEqual("test-id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.shell_node, mapper.oozie_node)
        self.assertEqual('{CTX["resourceManager"]}', mapper.resource_manager)
        self.assertEqual('{CTX["nameNode"]}', mapper.name_node)
        self.assertEqual('{CTX["queueName"]}', mapper.properties["mapred.job.queue.name"])
        self.assertEqual("echo arg1 arg2", mapper.bash_command)

    @mock.patch("mappers.shell_mapper.render_template", return_value="RETURN")
    def test_convert_to_text(self, render_template_mock):
        properties = {
            "dataproc_cluster": "my-cluster",
            "gcp_region": "europe-west3",
            "nameNode": "hdfs://localhost:9020/",
            "queueName": "default",
        }
        mapper = self._get_shell_mapper(properties=properties)
        res = mapper.convert_to_text()
        self.assertEqual(res, "RETURN")

        _, kwargs = render_template_mock.call_args
        tasks = kwargs["tasks"]
        relations = kwargs["relations"]

        self.assertEqual(kwargs["template_name"], "action.tpl")
        self.assertEqual(
            [
                Task(
                    task_id="test-id-prepare",
                    template_name="prepare.tpl",
                    template_params={
                        "prepare_command": "$DAGS_FOLDER/../data/prepare.sh -c my-cluster -r europe-west3 "
                        '-d "{} {}" -m "{} {}"',
                        "prepare_arguments": [
                            '{CTX["nameNode"]}/examples/output-data/demo/pig-node',
                            '{CTX["nameNode"]}/examples/output-data/demo/pig-node2',
                            '{CTX["nameNode"]}/examples/input-data/demo/pig-node',
                            '{CTX["nameNode"]}/examples/input-data/demo/pig-node2',
                        ],
                    },
                ),
                Task(
                    task_id="test-id",
                    template_name="shell.tpl",
                    template_params={"pig_command": "sh \\'echo arg1 arg2\\'"},
                ),
            ],
            tasks,
        )
        self.assertEqual(relations, [Relation(from_task_id="test-id-prepare", to_task_id="test-id")])

    def test_required_imports(self):
        properties = {
            "resourceManager": "localhost:9999",
            "nameNode": "hdfs://localhost:8021",
            "queueName": "myQueue",
            "examplesRoot": "examples",
        }
        mapper = self._get_shell_mapper(properties=properties)
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_shell_mapper(self, properties):
        return shell_mapper.ShellMapper(
            oozie_node=self.shell_node, name="test-id", trigger_rule=TriggerRule.DUMMY, properties=properties
        )
