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
"""Tests for the MapReduce mapper"""
import ast
import unittest
from xml.etree import ElementTree as ET

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers import mapreduce_mapper

# language=XML
from o2a.o2a_libs.property_utils import PropertySet

EXAMPLE_XML = """
<map-reduce>
    <name-node>hdfs://</name-node>
    <prepare>
        <delete path="${nameNode}/examples/mapreduce/output" />
    </prepare>
    <configuration>
        <property>
            <name>mapred.mapper.new-api</name>
            <value>true</value>
        </property>
        <property>
            <name>mapred.reducer.new-api</name>
            <value>true</value>
        </property>
        <property>
            <name>mapred.job.queue.name</name>
            <value>${queueName}</value>
        </property>
        <property>
            <name>mapreduce.job.map.class</name>
            <value>WordCount$Map</value>
        </property>
        <property>
            <name>mapreduce.job.reduce.class</name>
            <value>WordCount$Reduce</value>
        </property>
        <property>
            <name>mapreduce.job.output.key.class</name>
            <value>org.apache.hadoop.io.Text</value>
        </property>
        <property>
            <name>mapreduce.job.output.value.class</name>
            <value>org.apache.hadoop.io.IntWritable</value>
        </property>
        <property>
            <name>mapreduce.input.fileinputformat.inputdir</name>
            <value>/user/mapred/${examplesRoot}/mapreduce/input</value>
        </property>
        <property>
            <name>mapreduce.output.fileoutputformat.outputdir</name>
            <value>/user/mapred/${examplesRoot}/mapreduce/output</value>
        </property>
    </configuration>
</map-reduce>
"""

# language=XML
EXAMPLE_XML_NO_PREPARE = """
<map-reduce>
    <name-node>hdfs://</name-node>
        <configuration>
        <property>
            <name>mapred.mapper.new-api</name>
            <value>true</value>
        </property>
        <property>
            <name>mapred.reducer.new-api</name>
            <value>true</value>
        </property>
        <property>
            <name>mapred.job.queue.name</name>
            <value>${queueName}</value>
        </property>
        <property>
            <name>mapreduce.job.map.class</name>
            <value>WordCount$Map</value>
        </property>
        <property>
            <name>mapreduce.job.reduce.class</name>
            <value>WordCount$Reduce</value>
        </property>
        <property>
            <name>mapreduce.job.output.key.class</name>
            <value>org.apache.hadoop.io.Text</value>
        </property>
        <property>
            <name>mapreduce.job.output.value.class</name>
            <value>org.apache.hadoop.io.IntWritable</value>
        </property>
        <property>
            <name>mapreduce.input.fileinputformat.inputdir</name>
            <value>/user/mapred/${examplesRoot}/mapreduce/input</value>
        </property>
        <property>
            <name>mapreduce.output.fileoutputformat.outputdir</name>
            <value>/user/mapred/${examplesRoot}/mapreduce/output</value>
        </property>
    </configuration>
</map-reduce>
"""


class TestMapReduceMapper(unittest.TestCase):
    def setUp(self):
        self.mapreduce_node = ET.fromstring(EXAMPLE_XML)

    def test_create_mapper(self):
        mapper = self._get_mapreduce_mapper(job_properties={}, config={})
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.mapreduce_node, mapper.oozie_node)

    def test_on_parse_node(self):
        # test jinja templating
        job_properties = {"nameNode": "hdfs://", "queueName": "myQueue", "examplesRoot": "examples"}
        config = {"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3"}

        mapper = self._get_mapreduce_mapper(job_properties=job_properties, config=config)
        mapper.on_parse_node()

        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.mapreduce_node, mapper.oozie_node)

        self.assertEqual("hdfs://", mapper.name_node)
        self.assertEqual("myQueue", mapper.props.action_node_properties["mapred.job.queue.name"])
        self.assertEqual("WordCount$Map", mapper.props.action_node_properties["mapreduce.job.map.class"])
        self.assertEqual(
            "WordCount$Reduce", mapper.props.action_node_properties["mapreduce.job.reduce.class"]
        )
        self.assertEqual(
            "org.apache.hadoop.io.Text", mapper.props.action_node_properties["mapreduce.job.output.key.class"]
        )
        self.assertEqual(
            "org.apache.hadoop.io.IntWritable",
            mapper.props.action_node_properties["mapreduce.job.output.value.class"],
        )
        self.assertEqual(
            "/user/mapred/examples/mapreduce/input",
            mapper.props.action_node_properties["mapreduce.input.fileinputformat.inputdir"],
        )
        self.assertEqual(
            "/user/mapred/examples/mapreduce/output",
            mapper.props.action_node_properties["mapreduce.output.fileoutputformat.outputdir"],
        )

    def test_to_tasks_and_relations(self):
        mapper = mapreduce_mapper.MapReduceMapper(
            oozie_node=self.mapreduce_node,
            name="test_id",
            dag_name="DAG_NAME_B",
            trigger_rule=TriggerRule.DUMMY,
            props=PropertySet(
                job_properties={"nameNode": "hdfs://"},
                config={
                    "dataproc_cluster": "my-cluster",
                    "gcp_region": "europe-west3",
                    "hadoop_jars": "hdfs:///user/mapred/examples/mapreduce/lib/wordcount.jar",
                    "hadoop_main_class": "WordCount",
                },
            ),
        )
        mapper.on_parse_node()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            [
                Task(
                    task_id="test_id_prepare",
                    template_name="prepare.tpl",
                    template_params={"delete": "/examples/mapreduce/output", "mkdir": None},
                ),
                Task(
                    task_id="test_id",
                    template_name="mapreduce.tpl",
                    template_params={
                        "props": PropertySet(
                            config={
                                "dataproc_cluster": "my-cluster",
                                "gcp_region": "europe-west3",
                                "hadoop_jars": "hdfs:///user/mapred/examples/mapreduce/lib/wordcount.jar",
                                "hadoop_main_class": "WordCount",
                            },
                            job_properties={"nameNode": "hdfs://"},
                            action_node_properties={
                                "mapred.mapper.new-api": "true",
                                "mapred.reducer.new-api": "true",
                                "mapred.job.queue.name": "${queueName}",
                                "mapreduce.job.map.class": "WordCount$Map",
                                "mapreduce.job.reduce.class": "WordCount$Reduce",
                                "mapreduce.job.output.key.class": "org.apache.hadoop.io.Text",
                                "mapreduce.job.output.value.class": "org.apache.hadoop.io.IntWritable",
                                "mapreduce.input.fileinputformat."
                                "inputdir": "/user/mapred/${examplesRoot}/mapreduce/input",
                                "mapreduce.output.fileoutputformat."
                                "outputdir": "/user/mapred/${examplesRoot}/mapreduce/output",
                            },
                        ),
                        "params_dict": {},
                        "hdfs_files": [],
                        "hdfs_archives": [],
                        "action_node_properties": {
                            "mapred.mapper.new-api": "true",
                            "mapred.reducer.new-api": "true",
                            "mapred.job.queue.name": "${queueName}",
                            "mapreduce.job.map.class": "WordCount$Map",
                            "mapreduce.job.reduce.class": "WordCount$Reduce",
                            "mapreduce.job.output.key.class": "org.apache.hadoop.io.Text",
                            "mapreduce.job.output.value.class": "org.apache.hadoop.io.IntWritable",
                            "mapreduce.input.fileinputformat.inputdir": "/user/mapred/${examplesRoot}/"
                            "mapreduce/input",
                            "mapreduce.output.fileoutputformat.outputdir": "/user/mapred/${examplesRoot}/"
                            "mapreduce/output",
                        },
                    },
                ),
            ],
            tasks,
        )
        self.assertEqual(relations, [Relation(from_task_id="test_id_prepare", to_task_id="test_id")])

    def test_required_imports(self):
        mapper = self._get_mapreduce_mapper(job_properties={}, config={})
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_mapreduce_mapper(self, job_properties, config):
        return mapreduce_mapper.MapReduceMapper(
            oozie_node=self.mapreduce_node,
            name="test_id",
            dag_name="DAG_NAME_B",
            trigger_rule=TriggerRule.DUMMY,
            props=PropertySet(job_properties=job_properties, config=config),
        )


class TestMapReduceMapperNoPrepare(unittest.TestCase):
    def setUp(self):
        self.mapreduce_node = ET.fromstring(EXAMPLE_XML_NO_PREPARE)

    def test_create_mapper(self):
        mapper = self._get_mapreduce_mapper(job_properties={}, config={})
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.DUMMY, mapper.trigger_rule)
        self.assertEqual(self.mapreduce_node, mapper.oozie_node)

    def test_to_tasks_and_relations(self):
        mapper = mapreduce_mapper.MapReduceMapper(
            oozie_node=self.mapreduce_node,
            name="test_id",
            dag_name="DAG_NAME_B",
            trigger_rule=TriggerRule.DUMMY,
            props=PropertySet(
                job_properties={"nameNode": "hdfs://"},
                config={
                    "dataproc_cluster": "my-cluster",
                    "gcp_region": "europe-west3",
                    "hadoop_jars": "hdfs:///user/mapred/examples/mapreduce/lib/wordcount.jar",
                    "hadoop_main_class": "WordCount",
                },
            ),
        )
        mapper.on_parse_node()
        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            [
                Task(
                    task_id="test_id",
                    template_name="mapreduce.tpl",
                    template_params={
                        "props": PropertySet(
                            config={
                                "dataproc_cluster": "my-cluster",
                                "gcp_region": "europe-west3",
                                "hadoop_jars": "hdfs:///user/mapred/examples/mapreduce/lib/wordcount.jar",
                                "hadoop_main_class": "WordCount",
                            },
                            job_properties={"nameNode": "hdfs://"},
                            action_node_properties={
                                "mapred.mapper.new-api": "true",
                                "mapred.reducer.new-api": "true",
                                "mapred.job.queue.name": "${queueName}",
                                "mapreduce.job.map.class": "WordCount$Map",
                                "mapreduce.job.reduce.class": "WordCount$Reduce",
                                "mapreduce.job.output.key.class": "org.apache.hadoop.io.Text",
                                "mapreduce.job.output.value.class": "org.apache.hadoop.io.IntWritable",
                                "mapreduce.input.fileinputformat."
                                "inputdir": "/user/mapred/${examplesRoot}/mapreduce/input",
                                "mapreduce.output.fileoutputformat."
                                "outputdir": "/user/mapred/${examplesRoot}/mapreduce/output",
                            },
                        ),
                        "params_dict": {},
                        "hdfs_files": [],
                        "hdfs_archives": [],
                        "action_node_properties": {
                            "mapred.mapper.new-api": "true",
                            "mapred.reducer.new-api": "true",
                            "mapred.job.queue.name": "${queueName}",
                            "mapreduce.job.map.class": "WordCount$Map",
                            "mapreduce.job.reduce.class": "WordCount$Reduce",
                            "mapreduce.job.output.key.class": "org.apache.hadoop.io.Text",
                            "mapreduce.job.output.value.class": "org.apache.hadoop.io.IntWritable",
                            "mapreduce.input.fileinputformat.inputdir": "/user/mapred/${examplesRoot}/"
                            "mapreduce/input",
                            "mapreduce.output.fileoutputformat.outputdir": "/user/mapred/${examplesRoot}/"
                            "mapreduce/output",
                        },
                    },
                )
            ],
            tasks,
        )
        self.assertEqual(relations, [])

    def test_required_imports(self):
        mapper = self._get_mapreduce_mapper(job_properties={}, config={})
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_mapreduce_mapper(self, job_properties, config):
        return mapreduce_mapper.MapReduceMapper(
            oozie_node=self.mapreduce_node,
            name="test_id",
            dag_name="DAG_NAME_B",
            trigger_rule=TriggerRule.DUMMY,
            props=PropertySet(job_properties=job_properties, config=config),
        )
