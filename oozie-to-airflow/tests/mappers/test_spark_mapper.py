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
"""Tests Spark Mapper"""
import ast
import unittest
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule
from mappers import spark_mapper


class TestSparkMapper(unittest.TestCase):
    def setUp(self):
        # language=XML
        spark_node_str = """
<spark name="decision">
    <job-tracker>foo:8021</job-tracker>
    <name-node>bar:8020</name-node>
    <prepare>
        <mkdir path="/tmp/mk_path" />
        <delete path="/tmp/d_path" />
    </prepare>
    <configuration>
        <property>
            <name>mapred.compress.map.output</name>
            <value>true</value>
        </property>
    </configuration>
    <master>local[*]</master>
    <name>Spark Examples</name>
    <mode>client</mode>
    <class>org.apache.spark.examples.mllib.JavaALS</class>
    <jar>/lib/spark-examples_2.10-1.1.0.jar</jar>
    <spark-opts>--executor-memory 20G --num-executors 50 --conf spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"</spark-opts>
    <arg>inputpath=hdfs:///input/file.txt</arg>
    <arg>value=2</arg>
</spark>"""  # noqa
        self.spark_node = ET.fromstring(spark_node_str)

    def test_create_mapper(self):
        mapper = spark_mapper.SparkMapper(
            oozie_node=self.spark_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.spark_node, mapper.oozie_node)

    def test_convert_to_text(self):
        mapper = spark_mapper.SparkMapper(
            oozie_node=self.spark_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )
        res = mapper.convert_to_text()
        ast.parse(res)

    # pylint: disable=no-self-use
    def test_required_imports(self):
        imps = spark_mapper.SparkMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def test_test_and_set_found(self):
        tag = "test_tag"
        params = {"hostname": "user@apache.org"}

        spark = ET.Element("spark")
        sub_spark = ET.SubElement(spark, tag)
        sub_spark.text = "${hostname}"

        parsed = spark_mapper.SparkMapper.test_and_set(root=spark, tag=tag, default=None, params=params)
        self.assertEqual("user@apache.org", parsed)

    def test_test_and_set_not_found(self):
        tag = "test_tag"
        not_found = "not_found"
        params = {"hostname": "user@apache.org"}

        spark = ET.Element("spark")
        sub_spark = ET.SubElement(spark, tag)
        sub_spark.text = "${hostname}"

        parsed = spark_mapper.SparkMapper.test_and_set(
            root=spark, tag=not_found, default="not_here", params=params
        )
        self.assertEqual("not_here", parsed)

    def test_parse_spark_config(self):
        config = ET.Element("configuration")
        config_prop = ET.SubElement(config, "property")
        prop_name = ET.SubElement(config_prop, "name")
        prop_val = ET.SubElement(config_prop, "value")

        prop_name.text = "red_cup"
        prop_val.text = "green_drink"

        parsed = spark_mapper.SparkMapper.parse_spark_config(config)
        expected = {"red_cup": "green_drink"}

        self.assertEqual(expected, parsed)

    def test_update_spark_opts(self):
        spark_opts = ET.Element("spark-opts")
        spark_opts.text = (
            "--executor-memory 20G --verbose --num-executors 50 --conf "
            'spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError '
            '-XX:HeapDumpPath=/tmp"'
        )
        self.spark_node.remove(self.spark_node.find("spark-opts"))

        mapper = spark_mapper.SparkMapper(
            oozie_node=self.spark_node, name="test_id", trigger_rule=TriggerRule.DUMMY
        )

        mapper.update_class_spark_opts(spark_opts)

        self.assertIn("executor_memory", mapper.__dict__)
        self.assertIn("num_executors", mapper.__dict__)
        self.assertIn("verbose", mapper.__dict__)
        # --conf gets put in 'conf' class dictionary
        self.assertIn("spark.executor.extraJavaOptions", mapper.__dict__["conf"])

    def test_parse_prepared_node(self):
        exp_mkdir = ["/tmp/mk_path"]
        exp_del = ["/tmp/d_path"]
        prepare = ET.Element("prepare")
        ET.SubElement(prepare, "mkdir", attrib={"path": "/tmp/mk_path"})
        ET.SubElement(prepare, "delete", attrib={"path": "/tmp/d_path"})

        delete_list, mkdir_list = spark_mapper.SparkMapper.parse_prepare_node(prepare)

        self.assertEqual(exp_del, delete_list)
        self.assertEqual(exp_mkdir, mkdir_list)
