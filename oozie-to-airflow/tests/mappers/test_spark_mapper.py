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
from unittest import mock
from xml.etree import ElementTree as ET

from parameterized import parameterized
from airflow.utils.trigger_rule import TriggerRule

from mappers import spark_mapper
from utils.template_utils import render_template
from utils.xml_utils import find_nodes_by_tag

EXAMPLE_PARAMS = {"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3", "nameNode": "hdfs://"}

# language=XML
EXAMPLE_XML_WITH_PREPARE = """
<spark name="decision">
    <job-tracker>foo:8021</job-tracker>
    <name-node>bar:8020</name-node>
    <prepare>
        <mkdir path="hdfs:///tmp/mk_path" />
        <delete path="hdfs:///tmp/d_path" />
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
    <spark-opts>--executor-memory 20G --num-executors 50
 --conf spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"</spark-opts>
    <arg>inputpath=hdfs:///input/file.txt</arg>
    <arg>value=2</arg>
</spark>"""

# language=XML
EXAMPLE_XML_WITHOUT_PREPARE = """
<spark name="decision">
    <job-tracker>foo:8021</job-tracker>
    <name-node>bar:8020</name-node>
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
    <jar>/user/${userName}/${examplesRoot}/apps/spark/lib/oozie-examples-4.3.0.jar</jar>
    <spark-opts>
    --executor-memory 20G --num-executors 50
    --conf spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"
    </spark-opts>
    <arg>inputpath=hdfs:///input/file.txt</arg>
    <arg>value=2</arg>
    <arg>/user/${userName}/${examplesRoot}/apps/spark/lib/oozie-examples-4.3.0.jar</arg>
</spark>
"""
EXAMPLE_PARAMS = {
    "dataproc_cluster": "my-cluster",
    "gcp_region": "europe-west3",
    "nameNode": "hdfs://",
    "userName": "test_user",
    "examplesRoot": "examples",
}


class TestSparkMapperWithPrepare(unittest.TestCase):
    def test_create_mapper(self):
        spark_node = ET.fromstring(EXAMPLE_XML_WITH_PREPARE)
        mapper = spark_mapper.SparkMapper(
            oozie_node=spark_node, name="test_id", trigger_rule=TriggerRule.DUMMY, params=EXAMPLE_PARAMS
        )
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(spark_node, mapper.oozie_node)

    @mock.patch.object(spark_mapper, "render_template", wraps=render_template)
    def test_convert_to_text_with_prepare_node(self, render_template_mock):
        spark_node = ET.fromstring(EXAMPLE_XML_WITH_PREPARE)
        mapper = spark_mapper.SparkMapper(
            oozie_node=spark_node, name="test_id", trigger_rule=TriggerRule.DUMMY, params=EXAMPLE_PARAMS
        )
        mapper.on_parse_node()

        res = mapper.convert_to_text()

        _, kwargs = render_template_mock.call_args
        tasks = kwargs["tasks"]
        relations = kwargs["relations"]

        self.assertTrue(ast.parse(res))
        self.assertEqual(kwargs["template_name"], "action.tpl")
        self.assertEqual(
            tasks[0].template_params,
            {
                "prepare_command": "$DAGS_FOLDER/../data/prepare.sh -c my-cluster -r europe-west3 "
                '-d "/tmp/d_path" -m "/tmp/mk_path"'
            },
        )
        self.assertEqual(
            tasks[1].template_params,
            {
                "archives": [],
                "arguments": ["inputpath=hdfs:///input/file.txt", "value=2"],
                "dataproc_spark_jars": ["/lib/spark-examples_2.10-1.1.0.jar"],
                "dataproc_spark_properties": {
                    "mapred.compress.map.output": "true",
                    "spark.executor.extraJavaOptions": "-XX:+HeapDumpOnOutOfMemoryError "
                    "-XX:HeapDumpPath=/tmp",
                },
                "files": [],
                "job_name": "Spark Examples",
                "main_class": "org.apache.spark.examples.mllib.JavaALS",
                "main_jar": None,
                "trigger_rule": "dummy",
            },
        )
        self.assertEqual(len(relations), 1)
        self.assertEqual(relations[0].from_task_id, "test_id_prepare")
        self.assertEqual(relations[0].to_task_id, "test_id")

    @mock.patch.object(spark_mapper, "render_template", wraps=render_template)
    def test_convert_to_text_without_prepare_node(self, render_template_mock):
        spark_node = ET.fromstring(EXAMPLE_XML_WITHOUT_PREPARE)
        mapper = spark_mapper.SparkMapper(
            oozie_node=spark_node, name="test_id", trigger_rule=TriggerRule.DUMMY, params=EXAMPLE_PARAMS
        )
        mapper.on_parse_node()

        res = mapper.convert_to_text()

        _, kwargs = render_template_mock.call_args
        tasks = kwargs["tasks"]
        relations = kwargs["relations"]

        self.assertTrue(ast.parse(res))
        self.assertEqual(kwargs["template_name"], "action.tpl")
        self.assertEqual(
            tasks[0].template_params,
            {
                "archives": [],
                "arguments": [
                    "inputpath=hdfs:///input/file.txt",
                    "value=2",
                    "/user/test_user/examples/apps/spark/lib/oozie-examples-4.3.0.jar",
                ],
                "dataproc_spark_jars": ["/user/test_user/examples/apps/spark/lib/oozie-examples-4.3.0.jar"],
                "dataproc_spark_properties": {
                    "mapred.compress.map.output": "true",
                    "spark.executor.extraJavaOptions": "-XX:+HeapDumpOnOutOfMemoryError "
                    "-XX:HeapDumpPath=/tmp",
                },
                "files": [],
                "job_name": "Spark Examples",
                "main_class": "org.apache.spark.examples.mllib.JavaALS",
                "main_jar": None,
                "trigger_rule": "dummy",
            },
        )
        self.assertEqual(len(relations), 0)

    @parameterized.expand(
        [
            (
                "--executor-memory 20G --num-executors 50 --conf "
                'spark.executor.extraJavaOptions="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp"',
                {
                    "mapred.compress.map.output": "true",
                    "spark.executor.extraJavaOptions": "-XX:+HeapDumpOnOutOfMemoryError "
                    "-XX:HeapDumpPath=/tmp",
                },
            ),
            ("AAA", {"mapred.compress.map.output": "true"}),
            (
                '--conf key1=value --conf key2="value1 value2" '
                '--conf dup="value1 value2" --conf dup="value3 value4"',
                {
                    "dup": "value3 value4",
                    "key1": "value",
                    "key2": "value1 value2",
                    "mapred.compress.map.output": "true",
                },
            ),
        ]
    )
    @mock.patch.object(spark_mapper, "render_template", wraps=render_template)
    def test_convert_to_text_parse_spark_opts(self, spark_opts, properties, render_template_mock):
        spark_node = ET.fromstring(EXAMPLE_XML_WITHOUT_PREPARE)
        spark_opts_node = find_nodes_by_tag(spark_node, spark_mapper.SPARK_TAG_OPTS)[0]
        spark_opts_node.text = spark_opts
        mapper = spark_mapper.SparkMapper(
            oozie_node=spark_node, name="test_id", trigger_rule=TriggerRule.DUMMY, params=EXAMPLE_PARAMS
        )
        mapper.on_parse_node()

        res = mapper.convert_to_text()

        _, kwargs = render_template_mock.call_args
        tasks = kwargs["tasks"]

        self.assertTrue(ast.parse(res))
        self.assertEqual(tasks[0].template_params["dataproc_spark_properties"], properties)

    # pylint: disable=no-self-use
    def test_required_imports(self):
        imps = spark_mapper.SparkMapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)
