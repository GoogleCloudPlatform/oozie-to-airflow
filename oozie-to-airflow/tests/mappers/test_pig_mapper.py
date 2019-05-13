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
"""Tests pig mapper"""
import ast
import unittest
from unittest import mock
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from converter.primitives import Task, Relation
from mappers import pig_mapper


class TestPigMapper(unittest.TestCase):
    def setUp(self):
        # language=XML
        pig_node_str = """
<pig>
    <resource-manager>localhost:8032</resource-manager>
    <name-node>hdfs://</name-node>
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
        </property>
        <property>
            <name>mapred.map.output.compress</name>
            <value>false</value>
        </property>
    </configuration>
    <script>id.pig</script>
    <param>INPUT=/user/${wf:conf('user.name')}/${examplesRoot}/input-data/text</param>
    <param>OUTPUT=/user/${wf:conf('user.name')}/${examplesRoot}/output-data/demo/pig-node</param>
    <file>/test_dir/test.txt#test_link.txt</file>
    <file>/user/pig/examples/pig/test_dir/test2.zip#test_link.zip</file>
    <archive>/test_dir/test2.zip#test_zip_dir</archive>
    <archive>/test_dir/test3.zip#test3_zip_dir</archive>
</pig>
"""
        self.pig_node = ET.fromstring(pig_node_str)

    def test_create_mapper_jinja(self):
        # test jinja templating
        self.pig_node.find("resource-manager").text = "${resourceManager}"
        self.pig_node.find("name-node").text = "${nameNode}"
        self.pig_node.find("script").text = "${scriptName}"
        properties = {
            "resourceManager": "localhost:9999",
            "nameNode": "hdfs://",
            "queueName": "myQueue",
            "examplesRoot": "examples",
            "scriptName": "id_el.pig",
        }

        mapper = self._get_pig_mapper(properties=properties)

        # make sure everything is getting initialized correctly
        self.assertEqual("test-id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.pig_node, mapper.oozie_node)
        self.assertEqual('{CTX["resourceManager"]}', mapper.resource_manager)
        self.assertEqual('{CTX["nameNode"]}', mapper.name_node)
        self.assertEqual("id_el.pig", mapper.script_file_name)
        self.assertEqual('{CTX["queueName"]}', mapper.properties["mapred.job.queue.name"])
        self.assertEqual(
            '/user/{el_wf_functions.wf_conf(CTX, "user.name")}/' '{CTX["examplesRoot"]}/input-data/text',
            mapper.params_dict["INPUT"],
        )
        self.assertEqual(
            '/user/{el_wf_functions.wf_conf(CTX, "user.name")}/'
            '{CTX["examplesRoot"]}/output-data/demo/pig-node',
            mapper.params_dict["OUTPUT"],
        )

    @mock.patch("mappers.pig_mapper.render_template", return_value="RETURN")
    def test_convert_to_text(self, render_template_mock):
        properties = {
            "dataproc_cluster": "my-cluster",
            "gcp_region": "europe-west3",
            "queueName": "default",
            "examplesRoot": "examples",
            "nameNode": "hdfs://",
        }
        mapper = self._get_pig_mapper(properties=properties)

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
                    template_name="pig.tpl",
                    template_params={
                        "properties": {
                            "dataproc_cluster": "my-cluster",
                            "gcp_region": "europe-west3",
                            "queueName": "default",
                            "examplesRoot": "examples",
                            "nameNode": "hdfs://",
                            "mapred.job.queue.name": '{CTX["queueName"]}',
                            "mapred.map.output.compress": "false",
                        },
                        "params_dict": {
                            "INPUT": '/user/{el_wf_functions.wf_conf(CTX, "user.name")}/'
                            '{CTX["examplesRoot"]}/input-data/text',
                            "OUTPUT": '/user/{el_wf_functions.wf_conf(CTX, "user.name")}/'
                            '{CTX["examplesRoot"]}/output-data/demo/pig-node',
                        },
                        "script_file_name": "id.pig",
                    },
                ),
            ],
            tasks,
        )
        self.assertEqual(relations, [Relation(from_task_id="test-id-prepare", to_task_id="test-id")])

    def test_first_task_id(self):
        properties = {
            "dataproc_cluster": "my-cluster",
            "gcp_region": "europe-west3",
            "queueName": "default",
            "examplesRoot": "examples",
            "nameNode": "hdfs://",
        }
        mapper = self._get_pig_mapper(properties=properties)
        self.assertEqual(mapper.first_task_id, "test-id-prepare")

    def test_required_imports(self):
        properties = {
            "dataproc_cluster": "my-cluster",
            "gcp_region": "europe-west3",
            "queueName": "default",
            "examplesRoot": "examples",
            "nameNode": "hdfs://",
        }
        mapper = self._get_pig_mapper(properties=properties)
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_pig_mapper(self, properties=None):
        mapper = pig_mapper.PigMapper(
            oozie_node=self.pig_node, name="test-id", trigger_rule=TriggerRule.DUMMY, properties=properties
        )
        return mapper
