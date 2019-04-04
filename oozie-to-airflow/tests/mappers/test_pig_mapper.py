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

from mappers import pig_mapper


class TestPigMapper(unittest.TestCase):
    def setUp(self):
        pig = ET.Element("pig")
        res_man = ET.SubElement(pig, "resource-manager")
        name_node = ET.SubElement(pig, "name-node")
        prepare = ET.SubElement(pig, "prepare")
        delete1 = ET.SubElement(prepare, "delete")
        delete2 = ET.SubElement(prepare, "delete")
        mkdir1 = ET.SubElement(prepare, "mkdir")
        mkdir2 = ET.SubElement(prepare, "mkdir")
        config = ET.SubElement(pig, "configuration")
        property1 = ET.SubElement(config, "property")
        name1 = ET.SubElement(property1, "name")
        value1 = ET.SubElement(property1, "value")
        property2 = ET.SubElement(config, "property")
        name2 = ET.SubElement(property2, "name")
        value2 = ET.SubElement(property2, "value")
        script = ET.SubElement(pig, "script")
        param1 = ET.SubElement(pig, "param")
        param2 = ET.SubElement(pig, "param")

        self.et = ET.ElementTree(pig)

        res_man.text = "localhost:8032"
        name_node.text = "hdfs://"
        script.text = "id.pig"

        delete1.set("path", "${nameNode}/examples/output-data/demo/pig-node")
        delete2.set("path", "${nameNode}/examples/output-data/demo/pig-node2")
        mkdir1.set("path", "${nameNode}/examples/input-data/demo/pig-node")
        mkdir2.set("path", "${nameNode}/examples/input-data/demo/pig-node2")

        name1.text = "mapred.job.queue.name"
        value1.text = "${queueName}"
        name2.text = "mapred.map.output.compress"
        value2.text = "false"

        param1.text = "INPUT=/user/${wf:user()}/${examplesRoot}/input-data/text"
        param2.text = "OUTPUT=/user/${wf:user()}/${examplesRoot}/output-data/demo/pig-node"

    def test_create_mapper_no_jinja(self):
        mapper = pig_mapper.PigMapper(
            oozie_node=self.et.getroot(), task_id="test_id", trigger_rule=TriggerRule.DUMMY
        )
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.et.getroot(), mapper.oozie_node)
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
        self.et.find("resource-manager").text = "${resourceManager}"
        self.et.find("name-node").text = "${nameNode}"
        self.et.find("script").text = "${scriptName}"
        params = {
            "resourceManager": "localhost:9999",
            "nameNode": "hdfs://",
            "queueName": "myQueue",
            "examplesRoot": "examples",
            "scriptName": "id_el.pig",
        }

        mapper = pig_mapper.PigMapper(
            oozie_node=self.et.getroot(), task_id="test_id", trigger_rule=TriggerRule.DUMMY, params=params
        )

        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.et.getroot(), mapper.oozie_node)
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
            oozie_node=self.et.getroot(),
            task_id="test_id",
            trigger_rule=TriggerRule.DUMMY,
            params={"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3"},
        )
        # Throws a syntax error if doesn't parse correctly
        ast.parse(mapper.convert_to_text())

    def test_required_imports(self):
        imps = pig_mapper.PigMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)
