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

from o2a.mappers import prepare_mixin


class TestPrepareMixin(unittest.TestCase):
    delete_path1 = "/examples/output-data/demo/pig-node"
    delete_path2 = "/examples/output-data/demo/pig-node2"
    mkdir_path1 = "/examples/input-data/demo/pig-node"
    mkdir_path2 = "/examples/input-data/demo/pig-node2"

    def test_with_prepare(self):
        cluster = "my-cluster"
        region = "europe-west3"
        params = {"nameNode": "hdfs://localhost:8020", "dataproc_cluster": cluster, "gcp_region": region}
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

        prepare = prepare_mixin.PrepareMixin().get_prepare_command(oozie_node=pig_node_prepare, params=params)
        self.assertEqual(
            '$DAGS_FOLDER/../data/prepare.sh -c {0} -r {1} -d "{2} {3}" -m "{4} {5}"'.format(
                cluster, region, self.delete_path1, self.delete_path2, self.mkdir_path1, self.mkdir_path2
            ),
            prepare,
        )

    def test_no_prepare(self):
        cluster = "my-cluster"
        region = "europe-west3"
        params = {"nameNode": "hdfs://localhost:8020", "dataproc_cluster": cluster, "gcp_region": region}
        # language=XML
        pig_node_str = "<pig><name-node>hdfs://</name-node></pig>"
        pig_node = ET.fromstring(pig_node_str)
        prepare = prepare_mixin.PrepareMixin().get_prepare_command(oozie_node=pig_node, params=params)
        self.assertEqual("", prepare)
