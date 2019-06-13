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
"""Tests distcp_mapper"""
import unittest
from typing import Dict
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.distcp_mapper import DistCpMapper

# language=XML
from o2a.o2a_libs.property_utils import PropertySet

EXAMPLE_XML = """
<distcp>
    <resource-manager>${resourceManager}</resource-manager>
    <name-node>${nameNode1}</name-node>
    <prepare>
        <delete path="hdfs:///tmp/d_path"/>
    </prepare>
    <arg>-update</arg>
    <arg>-skipcrccheck</arg>
    <arg>-strategy</arg>
    <arg>dynamic</arg>
    <arg>${nameNode1}/path/to/input file.txt</arg>
    <arg>${nameNode2}/path/to/output-file.txt</arg>
    <configuration>
        <property>
            <name>oozie.launcher.mapreduce.job.hdfs-servers</name>
            <value>${nameNode1},${nameNode2}</value>
        </property>
    </configuration>
</distcp>
"""

EXAMPLE_JOB_PROPERTIES = {"nameNode1": "hdfs://localhost:8081", "nameNode2": "hdfs://localhost:8082"}

EXAMPLE_CONFIG_PROPERTIES = {"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3"}


class TestDistCpMapper(unittest.TestCase):
    def setUp(self):
        self.distcp_node: Element = ET.fromstring(EXAMPLE_XML)

    def test_setup(self):
        self.assertIsNotNone(self.distcp_node)
        self.assertTrue(isinstance(self.distcp_node, Element))

    def test_first_task_id(self):
        # Given
        mapper, _ = _get_distcp_mapper(self.distcp_node, job_properties={}, config={})

        # When
        first_task_id = mapper.first_task_id

        # Then
        self.assertEqual("distcp_prepare", first_task_id)

    def test_task_and_relations(self):
        # Given
        mapper, name = _get_distcp_mapper(
            self.distcp_node, job_properties=EXAMPLE_JOB_PROPERTIES, config=EXAMPLE_CONFIG_PROPERTIES
        )

        # When
        mapper.on_parse_node()
        tasks, relations = mapper.to_tasks_and_relations()

        # Then
        self.assertEqual(mapper.oozie_node, self.distcp_node)
        self.assertIsNotNone(tasks)
        self.assertIsNotNone(relations)
        self.assertEqual(2, len(tasks))
        self.assertEqual(1, len(relations))
        self.assertEqual(
            [
                Task(
                    task_id=f"{name}_prepare",
                    template_name="prepare.tpl",
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    template_params={"delete": "/tmp/d_path", "mkdir": None},
                ),
                Task(
                    task_id=name,
                    template_name=f"{name}.tpl",
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    template_params={
                        "props": PropertySet(
                            config={"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3"},
                            job_properties={
                                "nameNode1": "hdfs://localhost:8081",
                                "nameNode2": "hdfs://localhost:8082",
                            },
                            action_node_properties={
                                "oozie.launcher.mapreduce.job.hdfs-servers": "hdfs://localhost:8081,"
                                "hdfs://localhost:8082"
                            },
                        ),
                        "distcp_command": "--class=org.apache.hadoop.tools.DistCp -- -update -skipcrccheck "
                        "-strategy dynamic 'hdfs://localhost:8081/path/to/input file.txt' "
                        "hdfs://localhost:8082/path/to/output-file.txt",
                    },
                ),
            ],
            tasks,
        )
        self.assertEqual([Relation(from_task_id=f"{name}_prepare", to_task_id=name)], relations)


class TestDistCpMapperNoPrepare(unittest.TestCase):
    def setUp(self):
        self.distcp_node: Element = ET.fromstring(EXAMPLE_XML)
        # Removing the prepare node from the tree
        self.distcp_node.remove(self.distcp_node.find("prepare"))

    def test_setup(self):
        self.assertIsNotNone(self.distcp_node)
        self.assertTrue(isinstance(self.distcp_node, Element))

    def test_first_task_id_no_prepare(self):
        # Given
        mapper, _ = _get_distcp_mapper(self.distcp_node, job_properties={}, config={})

        # When
        first_task_id = mapper.first_task_id

        # Then
        self.assertEqual("distcp", first_task_id)

    def test_task_and_relations_no_prepare(self):
        # Given
        mapper, name = _get_distcp_mapper(
            self.distcp_node, job_properties=EXAMPLE_JOB_PROPERTIES, config=EXAMPLE_CONFIG_PROPERTIES
        )

        # When
        mapper.on_parse_node()
        tasks, relations = mapper.to_tasks_and_relations()

        # Then
        self.assertEqual(mapper.oozie_node, self.distcp_node)
        self.assertIsNotNone(tasks)
        self.assertIsNotNone(relations)
        self.assertEqual(1, len(tasks))
        self.assertEqual(0, len(relations))
        self.assertEqual(
            [
                Task(
                    task_id=name,
                    template_name=f"{name}.tpl",
                    trigger_rule=TriggerRule.ALL_SUCCESS,
                    template_params={
                        "props": PropertySet(
                            config={"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3"},
                            job_properties={
                                "nameNode1": "hdfs://localhost:8081",
                                "nameNode2": "hdfs://localhost:8082",
                            },
                            action_node_properties={
                                "oozie.launcher.mapreduce.job.hdfs-servers": "hdfs://localhost:8081,"
                                "hdfs://localhost:8082"
                            },
                        ),
                        "distcp_command": "--class=org.apache.hadoop.tools.DistCp -- -update -skipcrccheck "
                        "-strategy dynamic 'hdfs://localhost:8081/path/to/input file.txt' "
                        "hdfs://localhost:8082/path/to/output-file.txt",
                    },
                )
            ],
            tasks,
        )


def _get_distcp_mapper(distcp_node: Element, job_properties: Dict[str, str], config: Dict[str, str]):
    name = "distcp"
    mapper = DistCpMapper(
        oozie_node=distcp_node,
        dag_name=name,
        name=name,
        props=PropertySet(job_properties=job_properties, config=config),
    )
    return mapper, name
