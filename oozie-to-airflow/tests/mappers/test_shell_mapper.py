# Copyright 2018 Google LLC
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
import ast
import unittest
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from mappers import shell_mapper


class TestShellMapper(unittest.TestCase):
    def setUp(self):
        shell = ET.Element("shell")
        res_man = ET.SubElement(shell, "resource-manager")
        name_node = ET.SubElement(shell, "name-node")
        prepare = ET.SubElement(shell, "prepare")
        delete1 = ET.SubElement(prepare, "delete")
        delete2 = ET.SubElement(prepare, "delete")
        mkdir1 = ET.SubElement(prepare, "mkdir")
        mkdir2 = ET.SubElement(prepare, "mkdir")
        config = ET.SubElement(shell, "configuration")
        property1 = ET.SubElement(config, "property")
        name1 = ET.SubElement(property1, "name")
        value1 = ET.SubElement(property1, "value")
        property2 = ET.SubElement(config, "property")
        name2 = ET.SubElement(property2, "name")
        value2 = ET.SubElement(property2, "value")
        exec_node = ET.SubElement(shell, "exec")
        arg1 = ET.SubElement(shell, "argument")
        arg2 = ET.SubElement(shell, "argument")

        self.et = ET.ElementTree(shell)

        res_man.text = "localhost:8032"
        name_node.text = "hdfs://localhost:8020"

        delete1.set("path", "${nameNode}/examples/output-data/demo/pig-node")
        delete2.set("path", "${nameNode}/examples/output-data/demo/pig-node2")
        mkdir1.set("path", "${nameNode}/examples/input-data/demo/pig-node")
        mkdir2.set("path", "${nameNode}/examples/input-data/demo/pig-node2")

        name1.text = "mapred.job.queue.name"
        value1.text = "${queueName}"
        name2.text = "mapred.map.output.compress"
        value2.text = "false"

        exec_node.text = "echo"
        arg1.text = "arg1"
        arg2.text = "arg2"

    def test_create_mapper_no_jinja(self):
        mapper = shell_mapper.ShellMapper(
            oozie_node=self.et.getroot(), task_id="test_id", trigger_rule=TriggerRule.DUMMY
        )
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.et.getroot(), mapper.oozie_node)
        self.assertEqual("localhost:8032", mapper.resource_manager)
        self.assertEqual("hdfs://localhost:8020", mapper.name_node)
        self.assertEqual("${queueName}", mapper.properties["mapred.job.queue.name"])
        self.assertEqual("echo arg1 arg2", mapper.bash_command)

    def test_create_mapper_jinja(self):
        # test jinja templating
        self.et.find("resource-manager").text = "${resourceManager}"
        self.et.find("name-node").text = "${nameNode}"
        params = {
            "resourceManager": "localhost:9999",
            "nameNode": "hdfs://localhost:8021",
            "queueName": "myQueue",
            "examplesRoot": "examples",
        }

        mapper = shell_mapper.ShellMapper(
            oozie_node=self.et.getroot(), task_id="test_id", trigger_rule=TriggerRule.DUMMY, params=params
        )

        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.et.getroot(), mapper.oozie_node)
        self.assertEqual("localhost:9999", mapper.resource_manager)
        self.assertEqual("hdfs://localhost:8021", mapper.name_node)
        self.assertEqual("myQueue", mapper.properties["mapred.job.queue.name"])
        self.assertEqual("echo arg1 arg2", mapper.bash_command)

    def test_convert_to_text(self):
        mapper = shell_mapper.ShellMapper(
            oozie_node=self.et.getroot(),
            task_id="test_id",
            trigger_rule=TriggerRule.DUMMY,
            params={"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3"},
        )
        # Throws a syntax error if doesn't parse correctly
        ast.parse(mapper.convert_to_text())

    def test_required_imports(self):
        imps = shell_mapper.ShellMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)
