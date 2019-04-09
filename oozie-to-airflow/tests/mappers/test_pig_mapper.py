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
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

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
    <param>INPUT=/user/${wf:user()}/${examplesRoot}/input-data/text</param>
    <param>OUTPUT=/user/${wf:user()}/${examplesRoot}/output-data/demo/pig-node</param>
</pig>
"""
        self.pig_node = ET.fromstring(pig_node_str)

    def test_create_mapper_no_jinja(self):
        mapper = pig_mapper.PigMapper(
            oozie_node=self.pig_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.pig_node, mapper.oozie_node)
        self.assertEqual("localhost:8032", mapper.resource_manager)
        self.assertEqual("hdfs://", mapper.name_node)
        self.assertEqual("id.pig", mapper.script_file_name)
        self.assertEqual("${queueName}", mapper.properties["mapred.job.queue.name"])
        self.assertEqual("/user/${wf:user()}/${examplesRoot}/input-data/text", mapper.params_dict["INPUT"])
        self.assertEqual(
            "/user/${wf:user()}/${examplesRoot}/output-data/demo/pig-node", mapper.params_dict["OUTPUT"]
        )

    def test_create_mapper_jinja(self):
        # test jinja templating
        self.pig_node.find("resource-manager").text = "${resourceManager}"
        self.pig_node.find("name-node").text = "${nameNode}"
        self.pig_node.find("script").text = "${scriptName}"
        params = {
            "resourceManager": "localhost:9999",
            "nameNode": "hdfs://",
            "queueName": "myQueue",
            "examplesRoot": "examples",
            "scriptName": "id_el.pig",
        }

        mapper = pig_mapper.PigMapper(
            oozie_node=self.pig_node, name="test_id", trigger_rule=TriggerRule.DUMMY, params=params
        )

        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.pig_node, mapper.oozie_node)
        self.assertEqual("localhost:9999", mapper.resource_manager)
        self.assertEqual("hdfs://", mapper.name_node)
        self.assertEqual("id_el.pig", mapper.script_file_name)
        self.assertEqual("myQueue", mapper.properties["mapred.job.queue.name"])
        self.assertEqual("/user/${wf:user()}/examples/input-data/text", mapper.params_dict["INPUT"])
        self.assertEqual(
            "/user/${wf:user()}/examples/output-data/demo/pig-node", mapper.params_dict["OUTPUT"]
        )

    def test_convert_to_text(self):
        mapper = pig_mapper.PigMapper(
            oozie_node=self.pig_node,
            name="test_id",
            trigger_rule=TriggerRule.DUMMY,
            params={"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3"},
        )
        # Throws a syntax error if doesn't parse correctly
        ast.parse(mapper.convert_to_text())

    # pylint: disable=no-self-use
    def test_required_imports(self):
        imps = pig_mapper.PigMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def test_first_task_id(self):
        mapper = pig_mapper.PigMapper(
            oozie_node=self.pig_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )
        self.assertEqual(mapper.first_task_id, "test_id_prepare")
