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

from mappers import spark_mapper
from airflow.utils.trigger_rule import TriggerRule
from xml.etree import ElementTree as ET


class TestSparkMapper(unittest.TestCase):
    def setUp(self):
        spark = ET.Element("spark", attrib={"name": "decision"})
        job_tracker = ET.SubElement(spark, "job-tracker")
        name_node = ET.SubElement(spark, "name-node")
        prepare = ET.SubElement(spark, "prepare")
        mkdir = ET.SubElement(prepare, "mkdir", attrib={"path": "/tmp/mk_path"})
        delete = ET.SubElement(prepare, "delete", attrib={"path": "/tmp/d_path"})
        # job_xml = ET.SubElement(spark, 'job-xml')
        config = ET.SubElement(spark, "configuration")
        prop = ET.SubElement(config, "property")
        prop_name = ET.SubElement(prop, "name")
        prop_value = ET.SubElement(prop, "value")
        master = ET.SubElement(spark, "master")
        name = ET.SubElement(spark, "name")
        mode = ET.SubElement(spark, "mode")
        clazz = ET.SubElement(spark, "class")
        jar = ET.SubElement(spark, "jar")
        spark_opts = ET.SubElement(spark, "spark-opts")
        arg1 = ET.SubElement(spark, "arg")
        arg2 = ET.SubElement(spark, "arg")

        job_tracker.text = "foo:8021"
        name_node.text = "bar:8020"
        # job_xml.text = '/tmp/job.xml'
        prop_name.text = "mapred.compress.map.output"
        prop_value.text = "true"
        master.text = "local[*]"
        mode.text = "client"
        name.text = "Spark Examples"
        clazz.text = "org.apache.spark.examples.mllib.JavaALS"
        jar.text = "/lib/spark-examples_2.10-1.1.0.jar"
        spark_opts.text = (
            "--executor-memory 20G --num-executors 50 --conf "
            'spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"'
        )
        arg1.text = "inputpath=hdfs://localhost/input/file.txt"
        arg2.text = "value=2"

        self.et = ET.ElementTree(spark)

    def test_create_mapper(self):
        mapper = spark_mapper.SparkMapper(
            oozie_node=self.et.getroot(), task_id="test_id", trigger_rule=TriggerRule.DUMMY
        )
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.task_id)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.et.getroot(), mapper.oozie_node)

    def test_convert_to_text(self):
        mapper = spark_mapper.SparkMapper(
            oozie_node=self.et.getroot(), task_id="test_id", trigger_rule=TriggerRule.DUMMY
        )
        ast.parse(mapper.convert_to_text())

    def test_required_imports(self):
        imps = spark_mapper.SparkMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def test_test_and_set_found(self):
        TAG = "test_tag"
        params = {"hostname": "user@apache.org"}

        spark = ET.Element("spark")
        sub_spark = ET.SubElement(spark, TAG)
        sub_spark.text = "${hostname}"

        parsed = spark_mapper.SparkMapper._test_and_set(root=spark, tag=TAG, default=None, params=params)
        self.assertEqual("user@apache.org", parsed)

    def test_test_and_set_not_found(self):
        TAG = "test_tag"
        NOT_FOUND = "not_found"
        params = {"hostname": "user@apache.org"}

        spark = ET.Element("spark")
        sub_spark = ET.SubElement(spark, TAG)
        sub_spark.text = "${hostname}"

        parsed = spark_mapper.SparkMapper._test_and_set(
            root=spark, tag=NOT_FOUND, default="not_here", params=params
        )
        self.assertEqual("not_here", parsed)

    def test_parse_spark_config(self):
        config = ET.Element("configuration")
        config_prop = ET.SubElement(config, "property")
        prop_name = ET.SubElement(config_prop, "name")
        prop_val = ET.SubElement(config_prop, "value")

        prop_name.text = "red_cup"
        prop_val.text = "green_drink"

        parsed = spark_mapper.SparkMapper._parse_spark_config(config)
        expected = {"red_cup": "green_drink"}

        self.assertEqual(expected, parsed)

    def test_update_spark_opts(self):
        spark_opts = ET.Element("spark-opts")
        spark_opts.text = (
            "--executor-memory 20G --verbose --num-executors 50 --conf "
            'spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError '
            '-XX:HeapDumpPath=/tmp"'
        )
        self.et.getroot().remove(self.et.getroot().find("spark-opts"))

        mapper = spark_mapper.SparkMapper(
            oozie_node=self.et.getroot(), task_id="test_id", trigger_rule=TriggerRule.DUMMY
        )

        mapper._update_class_spark_opts(spark_opts)

        self.assertIn("executor_memory", mapper.__dict__)
        self.assertIn("num_executors", mapper.__dict__)
        self.assertIn("verbose", mapper.__dict__)
        # --conf gets put in 'conf' class dictionary
        self.assertIn("spark.executor.extraJavaOptions", mapper.__dict__["conf"])

    def test_parse_prepared_node(self):
        EXP_MKDIR = ["/tmp/mk_path"]
        EXP_DEL = ["/tmp/d_path"]
        prepare = ET.Element("prepare")
        ET.SubElement(prepare, "mkdir", attrib={"path": "/tmp/mk_path"})
        ET.SubElement(prepare, "delete", attrib={"path": "/tmp/d_path"})

        delete_list, mkdir_list = spark_mapper.SparkMapper._parse_prepare_node(prepare)

        self.assertEqual(EXP_DEL, delete_list)
        self.assertEqual(EXP_MKDIR, mkdir_list)
