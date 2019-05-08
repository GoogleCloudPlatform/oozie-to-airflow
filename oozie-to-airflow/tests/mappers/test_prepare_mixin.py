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

from mappers import prepare_mixin


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

        prepare_command, prepare_parameters = prepare_mixin.PrepareMixin().get_prepare_command_and_parameters(
            pig_node_prepare, params
        )
        self.assertEqual(
            '$DAGS_FOLDER/../data/prepare.sh -c my-cluster -r europe-west3 -d "{} {}" -m "{} {}"',
            prepare_command,
        )
        self.assertEqual(
            [
                "\"{DAG_CONTEXT.params['nameNode']}/examples/output-data/demo/pig-node\"",
                "\"{DAG_CONTEXT.params['nameNode']}/examples/output-data/demo/pig-node2\"",
                "\"{DAG_CONTEXT.params['nameNode']}/examples/input-data/demo/pig-node\"",
                "\"{DAG_CONTEXT.params['nameNode']}/examples/input-data/demo/pig-node2\"",
            ],
            prepare_parameters,
        )

    def test_no_prepare(self):
        cluster = "my-cluster"
        region = "europe-west3"
        params = {"nameNode": "hdfs://localhost:8020", "dataproc_cluster": cluster, "gcp_region": region}
        # language=XML
        pig_node_str = "<pig><name-node>hdfs://</name-node></pig>"
        pig_node = ET.fromstring(pig_node_str)
        prepare_command, prepare_parameters = prepare_mixin.PrepareMixin().get_prepare_command_and_parameters(
            pig_node, params
        )
        self.assertEqual("", prepare_command)
        self.assertEqual([], prepare_parameters)
