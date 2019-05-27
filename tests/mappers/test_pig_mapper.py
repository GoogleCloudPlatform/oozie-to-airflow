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

from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers import pig_mapper
from o2a.o2a_libs.property_utils import PropertySet


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
    <file>/test_dir/test.txt#test_link.txt</file>
    <file>/user/pig/examples/pig/test_dir/test2.zip#test_link.zip</file>
    <archive>/test_dir/test2.zip#test_zip_dir</archive>
    <archive>/test_dir/test3.zip#test3_zip_dir</archive>
</pig>
"""
        self.pig_node = ET.fromstring(pig_node_str)

    def test_create_mapper_no_jinja(self):
        job_properties = {"nameNode": "hdfs://"}
        configuration_properties = {}
        mapper = self._get_pig_mapper(
            job_properties=job_properties, configuration_properties=configuration_properties
        )
        mapper.on_parse_node()
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.pig_node, mapper.oozie_node)
        self.assertEqual("localhost:8032", mapper.resource_manager)
        self.assertEqual("hdfs://", mapper.name_node)
        self.assertEqual("id.pig", mapper.script_file_name)
        self.assertEqual("${queueName}", mapper.property_set.action_node_properties["mapred.job.queue.name"])
        self.assertEqual("/user/${wf:user()}/${examplesRoot}/input-data/text", mapper.params_dict["INPUT"])
        self.assertEqual(
            "/user/${wf:user()}/${examplesRoot}/output-data/demo/pig-node", mapper.params_dict["OUTPUT"]
        )

    def test_create_mapper_jinja(self):
        # test jinja templating
        self.pig_node.find("resource-manager").text = "${resourceManager}"
        self.pig_node.find("name-node").text = "${nameNode}"
        self.pig_node.find("script").text = "${scriptName}"
        job_properties = {
            "resourceManager": "localhost:9999",
            "nameNode": "hdfs://",
            "queueName": "myQueue",
            "examplesRoot": "examples",
            "scriptName": "id_el.pig",
        }
        configuration_properties = {}

        mapper = self._get_pig_mapper(
            job_properties=job_properties, configuration_properties=configuration_properties
        )
        mapper.on_parse_node()

        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.pig_node, mapper.oozie_node)
        self.assertEqual("localhost:9999", mapper.resource_manager)
        self.assertEqual("hdfs://", mapper.name_node)
        self.assertEqual("id_el.pig", mapper.script_file_name)
        self.assertEqual("myQueue", mapper.property_set.action_node_properties["mapred.job.queue.name"])
        self.assertEqual("/user/${wf:user()}/examples/input-data/text", mapper.params_dict["INPUT"])
        self.assertEqual(
            "/user/${wf:user()}/examples/output-data/demo/pig-node", mapper.params_dict["OUTPUT"]
        )

    def test_to_tasks_and_relations(self):
        job_properties = {"nameNode": "hdfs://"}
        configuration_properties = {"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3"}
        mapper = self._get_pig_mapper(
            job_properties=job_properties, configuration_properties=configuration_properties
        )
        mapper.on_parse_node()
        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            [
                Task(
                    task_id="test_id_prepare",
                    template_name="prepare.tpl",
                    template_params={
                        "delete": "/examples/output-data/demo/pig-node /examples/output-data/demo/pig-node2",
                        "mkdir": "/examples/input-data/demo/pig-node /examples/input-data/demo/pig-node2",
                    },
                ),
                Task(
                    task_id="test_id",
                    template_name="pig.tpl",
                    template_params={
                        "property_set": PropertySet(
                            configuration_properties={
                                "dataproc_cluster": "my-cluster",
                                "gcp_region": "europe-west3",
                            },
                            job_properties={"nameNode": "hdfs://"},
                            action_node_properties={
                                "mapred.job.queue.name": "${queueName}",
                                "mapred.map.output.compress": "false",
                            },
                        ),
                        "params_dict": {
                            "INPUT": "/user/${wf:user()}/${examplesRoot}/input-data/text",
                            "OUTPUT": "/user/${wf:user()}/${examplesRoot}/output-data/demo/pig-node",
                        },
                        "script_file_name": "id.pig",
                    },
                ),
            ],
            tasks,
        )
        self.assertEqual([Relation(from_task_id="test_id_prepare", to_task_id="test_id")], relations)

    def test_first_task_id(self):
        job_properties = {"nameNode": "hdfs://"}
        configuration_properties = {}
        mapper = self._get_pig_mapper(
            job_properties=job_properties, configuration_properties=configuration_properties
        )
        self.assertEqual("test_id_prepare", mapper.first_task_id)

    def test_required_imports(self):
        job_properties = {"nameNode": "hdfs://"}
        configuration_properties = {}
        mapper = self._get_pig_mapper(
            job_properties=job_properties, configuration_properties=configuration_properties
        )
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_pig_mapper(self, job_properties, configuration_properties):
        mapper = pig_mapper.PigMapper(
            oozie_node=self.pig_node,
            name="test_id",
            dag_name="BBB",
            trigger_rule=TriggerRule.DUMMY,
            property_set=PropertySet(
                job_properties=job_properties, configuration_properties=configuration_properties
            ),
        )
        return mapper
