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

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.task import Task
from o2a.mappers.prepare_mixin import PrepareMixin
from o2a.o2a_libs.property_utils import PropertySet


class TestPrepareMixin(unittest.TestCase):
    delete_path1 = "/examples/output-data/demo/pig-node"
    delete_path2 = "/examples/output-data/demo/pig-node2"
    mkdir_path1 = "/examples/input-data/demo/pig-node"
    mkdir_path2 = "/examples/input-data/demo/pig-node2"

    def test_with_prepare(self):
        cluster = "my-cluster"
        region = "europe-west3"
        job_properties = {"nameNode": "hdfs://localhost:8020"}
        configuration_properties = {"dataproc_cluster": cluster, "gcp_region": region}
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
        mixin = PrepareMixin(pig_node_prepare)
        self.assertTrue(mixin.has_prepare())
        task = mixin.get_prepare_task(
            name="test_node",
            trigger_rule=TriggerRule.DUMMY,
            property_set=PropertySet(
                configuration_properties=configuration_properties, job_properties=job_properties
            ),
        )
        self.assertEqual(
            Task(
                task_id="test_node_prepare",
                template_name="prepare.tpl",
                trigger_rule="dummy",
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
        configuration_properties = {"dataproc_cluster": cluster, "gcp_region": region}
        # language=XML
        pig_node_str = "<pig><name-node>hdfs://</name-node></pig>"
        pig_node = ET.fromstring(pig_node_str)
        mixin = PrepareMixin(pig_node)
        self.assertFalse(mixin.has_prepare())
        prepare_task = mixin.get_prepare_task(
            name="task",
            trigger_rule=TriggerRule.DUMMY,
            property_set=PropertySet(
                configuration_properties=configuration_properties, job_properties=job_properties
            ),
        )
        self.assertIsNone(prepare_task)

    def test_empty_prepare(self):
        cluster = "my-cluster"
        region = "europe-west3"
        job_properties = {"nameNode": "hdfs://localhost:8020"}
        configuration_properties = {"dataproc_cluster": cluster, "gcp_region": region}
        # language=XML
        pig_node_str = """
<pig>
    <name-node>hdfs://</name-node>
    <prepare>
    </prepare>
</pig>
"""
        pig_node = ET.fromstring(pig_node_str)
        mixin = PrepareMixin(pig_node)
        self.assertFalse(mixin.has_prepare())
        prepare_task = mixin.get_prepare_task(
            name="task",
            trigger_rule=TriggerRule.DUMMY,
            property_set=PropertySet(
                configuration_properties=configuration_properties, job_properties=job_properties
            ),
        )
        self.assertIsNone(prepare_task)
