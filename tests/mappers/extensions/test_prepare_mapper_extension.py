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
"""Tests prepare mixin"""
import unittest
from xml.etree import ElementTree as ET


from o2a.converter.task import Task
from o2a.mappers.base_mapper import BaseMapper
from o2a.mappers.extensions.prepare_mapper_extension import PrepareMapperExtension
from o2a.o2a_libs.property_utils import PropertySet

TEST_MAPPER_NAME = "mapper"


class TestPrepareMapperExtension(unittest.TestCase):
    delete_path1 = "/examples/output-data/demo/pig-node"
    delete_path2 = "/examples/output-data/demo/pig-node2"
    mkdir_path1 = "/examples/input-data/demo/pig-node"
    mkdir_path2 = "/examples/input-data/demo/pig-node2"

    @staticmethod
    def get_mapper_extension(node: ET.Element, props: PropertySet):
        mapper = BaseMapper(oozie_node=node, name=TEST_MAPPER_NAME, props=props, dag_name="dag")
        return PrepareMapperExtension(mapper)

    def test_with_prepare(self):
        cluster = "my-cluster"
        region = "europe-west3"
        job_properties = {"nameNode": "hdfs://localhost:8020"}
        config = {"dataproc_cluster": cluster, "gcp_region": region}
        # language=XML
        pig_node_prepare_str = """
<pig>
    <name-node>hdfs://</name-node>
    <prepare>
        <delete path="${nameNode}/examples/output-data/demo/pig-node" />
        <delete path="${nameNode}/examples/output-data/demo/pig-node2" />
        <mkdir path="${nameNode}/examples/input-data/demo/pig-node" />
        <mkdir path="${nameNode}/examples/input-data/demo/pig-node2" />
    </prepare>
</pig>
"""
        pig_node_prepare = ET.fromstring(pig_node_prepare_str)
        extension = self.get_mapper_extension(
            pig_node_prepare, props=PropertySet(config=config, job_properties=job_properties)
        )
        self.assertTrue(extension.has_prepare())
        task = extension.get_prepare_task()
        self.assertEqual(
            Task(
                task_id="mapper_prepare",
                template_name="prepare/prepare.tpl",
                template_params={
                    "delete": "/examples/output-data/demo/pig-node /examples/output-data/demo/pig-node2",
                    "mkdir": "/examples/input-data/demo/pig-node /examples/input-data/demo/pig-node2",
                },
            ),
            task,
        )

    def test_no_prepare(self):
        cluster = "my-cluster"
        region = "europe-west3"
        job_properties = {"nameNode": "hdfs://localhost:8020"}
        config = {"dataproc_cluster": cluster, "gcp_region": region}
        # language=XML
        pig_node_str = "<pig><name-node>hdfs://</name-node></pig>"
        pig_node = ET.fromstring(pig_node_str)
        extension = self.get_mapper_extension(
            node=pig_node, props=PropertySet(config=config, job_properties=job_properties)
        )

        self.assertFalse(extension.has_prepare())
        prepare_task = extension.get_prepare_task()
        self.assertIsNone(prepare_task)

    def test_empty_prepare(self):
        cluster = "my-cluster"
        region = "europe-west3"
        job_properties = {"nameNode": "hdfs://localhost:8020"}
        config = {"dataproc_cluster": cluster, "gcp_region": region}
        # language=XML
        pig_node_str = """
<pig>
    <name-node>hdfs://</name-node>
    <prepare>
    </prepare>
</pig>
"""
        pig_node = ET.fromstring(pig_node_str)
        extension = self.get_mapper_extension(
            node=pig_node, props=PropertySet(config=config, job_properties=job_properties)
        )
        self.assertFalse(extension.has_prepare())
        prepare_task = extension.get_prepare_task()
        self.assertIsNone(prepare_task)
