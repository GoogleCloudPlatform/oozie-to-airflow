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
"""Tests Java mapper"""
import ast
import unittest
from typing import List
from xml.etree import ElementTree as ET

from o2a.converter.task import Task
from o2a.mappers import java_mapper
from o2a.o2a_libs.property_utils import PropertySet


class TestJavaMapper(unittest.TestCase):
    def setUp(self) -> None:
        # language=XML
        java_node_with_multiple_opt_str = """
        <java>
            <resource-manager>${resourceManager}</resource-manager>
            <name-node>${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>${queueName}</value>
                </property>
            </configuration>
            <main-class>org.apache.oozie.example.DemoJavaMain</main-class>
            <java-opt>-Dtest1=val1_mult</java-opt>
            <java-opt>-Dtest2=val2_mult</java-opt>
            <arg>Hello</arg>
            <arg>Oozie!</arg>
        </java>
        """
        self.java_node_with_multiple_opt = ET.fromstring(java_node_with_multiple_opt_str)
        # language=XML
        java_node_with_single_opts = """
        <java>
            <resource-manager>${resourceManager}</resource-manager>
            <name-node>${nameNode}</name-node>
            <configuration>
                <property>
                    <name>mapred.job.queue.name</name>
                    <value>${queueName}</value>
                </property>
            </configuration>
            <main-class>org.apache.oozie.example.DemoJavaMain</main-class>
            <java-opts>-Dtest1=val1 -Dtest2=val2</java-opts>
            <arg>Hello</arg>
            <arg>Oozie!</arg>
        </java>
        """
        self.java_node_with_single_opts = ET.fromstring(java_node_with_single_opts)

    def test_arguments_are_parsed_correctly_without_jar_files(self):
        mapper = self._get_java_mapper(
            job_properties={
                "userName": "user",
                "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
            },
            config={},
        )
        mapper.on_parse_node()
        self.assertEqual("test_id", mapper.name)
        self.assertEqual("org.apache.oozie.example.DemoJavaMain", mapper.main_class)
        self.assertEqual(["-Dtest1=val1", "-Dtest2=val2"], mapper.java_opts)
        self.assertEqual(
            PropertySet(
                config={},
                job_properties={
                    "userName": "user",
                    "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
                },
                action_node_properties={"mapred.job.queue.name": "${queueName}"},
            ),
            mapper.props,
        )
        self.assertEqual([], mapper.jar_files_in_hdfs)
        self.assertEqual([], mapper.jar_files)

    def test_arguments_are_parsed_correctly_with_multiple_opts(self):
        mapper = self._get_java_mapper(
            job_properties={
                "userName": "user",
                "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
            },
            single_opts=False,
            config={},
        )
        mapper.on_parse_node()
        self.assertEqual("test_id", mapper.name)
        self.assertEqual("org.apache.oozie.example.DemoJavaMain", mapper.main_class)
        self.assertEqual(["-Dtest1=val1_mult", "-Dtest2=val2_mult"], mapper.java_opts)
        self.assertEqual(
            PropertySet(
                config={},
                job_properties={
                    "userName": "user",
                    "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
                },
                action_node_properties={"mapred.job.queue.name": "${queueName}"},
            ),
            mapper.props,
        )
        self.assertEqual([], mapper.jar_files_in_hdfs)
        self.assertEqual([], mapper.jar_files)

    def test_arguments_are_parsed_correctly_with_jar_files(self):
        mapper = self._get_java_mapper(
            job_properties={
                "userName": "user",
                "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
            },
            config={},
            jar_files=["test.jar", "test2.jar"],
        )
        mapper.on_parse_node()
        self.assertEqual("test_id", mapper.name)
        self.assertEqual("org.apache.oozie.example.DemoJavaMain", mapper.main_class)
        self.assertEqual(["-Dtest1=val1", "-Dtest2=val2"], mapper.java_opts)
        self.assertEqual(
            PropertySet(
                config={},
                job_properties={
                    "userName": "user",
                    "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
                },
                action_node_properties={"mapred.job.queue.name": "${queueName}"},
            ),
            mapper.props,
        )
        self.assertEqual(
            [
                "hdfs:///user/USER/examples/apps/java/lib/test.jar",
                "hdfs:///user/USER/examples/apps/java/lib/test2.jar",
            ],
            mapper.jar_files_in_hdfs,
        )
        self.assertEqual(["test.jar", "test2.jar"], mapper.jar_files)

    def test_mapred_ops_append_list_mapred_child(self):
        mapper = self._get_java_mapper(
            job_properties={
                "userName": "user",
                "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
                "mapred.child.java.opts": "-Dmapred1=val1 -Dmapred2=val2",
            },
            config={},
            jar_files=["test.jar", "test2.jar"],
        )
        mapper.on_parse_node()
        self.assertEqual("test_id", mapper.name)
        self.assertEqual("org.apache.oozie.example.DemoJavaMain", mapper.main_class)
        self.assertEqual(
            ["-Dmapred1=val1", "-Dmapred2=val2", "-Dtest1=val1", "-Dtest2=val2"], mapper.java_opts
        )
        self.assertEqual(
            PropertySet(
                config={},
                job_properties={
                    "userName": "user",
                    "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
                    "mapred.child.java.opts": "-Dmapred1=val1 -Dmapred2=val2",
                },
                action_node_properties={"mapred.job.queue.name": "${queueName}"},
            ),
            mapper.props,
        )
        self.assertEqual(
            [
                "hdfs:///user/USER/examples/apps/java/lib/test.jar",
                "hdfs:///user/USER/examples/apps/java/lib/test2.jar",
            ],
            mapper.jar_files_in_hdfs,
        )
        self.assertEqual(["test.jar", "test2.jar"], mapper.jar_files)

    def test_mapred_ops_append_list_mapreduce_map(self):
        mapper = self._get_java_mapper(
            job_properties={
                "userName": "user",
                "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
                "mapreduce.map.java.opts": "-Dmapreduce1=val1 -Dmapreduce2=val2",
            },
            config={},
            jar_files=["test.jar", "test2.jar"],
        )
        mapper.on_parse_node()
        self.assertEqual("test_id", mapper.name)
        self.assertEqual("org.apache.oozie.example.DemoJavaMain", mapper.main_class)
        self.assertEqual(
            ["-Dmapreduce1=val1", "-Dmapreduce2=val2", "-Dtest1=val1", "-Dtest2=val2"], mapper.java_opts
        )
        self.assertEqual(
            PropertySet(
                config={},
                job_properties={
                    "userName": "user",
                    "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
                    "mapreduce.map.java.opts": "-Dmapreduce1=val1 -Dmapreduce2=val2",
                },
                action_node_properties={"mapred.job.queue.name": "${queueName}"},
            ),
            mapper.props,
        )
        self.assertEqual(
            [
                "hdfs:///user/USER/examples/apps/java/lib/test.jar",
                "hdfs:///user/USER/examples/apps/java/lib/test2.jar",
            ],
            mapper.jar_files_in_hdfs,
        )
        self.assertEqual(["test.jar", "test2.jar"], mapper.jar_files)

    def test_to_tasks_and_relations(self):
        mapper = self._get_java_mapper(
            job_properties={
                "userName": "user",
                "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
            },
            config={},
        )
        mapper.on_parse_node()
        tasks, relations = mapper.to_tasks_and_relations()
        self.assertEqual(
            [
                Task(
                    task_id="test_id",
                    template_name="java.tpl",
                    trigger_rule="one_success",
                    template_params={
                        "props": PropertySet(
                            config={},
                            job_properties={
                                "userName": "user",
                                "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
                            },
                            action_node_properties={"mapred.job.queue.name": "${queueName}"},
                        ),
                        "hdfs_files": [],
                        "hdfs_archives": [],
                        "main_class": "org.apache.oozie.example.DemoJavaMain",
                        "jar_files_in_hdfs": [],
                        "args": ["Hello", "Oozie!"],
                    },
                )
            ],
            tasks,
        )
        self.assertEqual(relations, [])

    def test_required_imports(self):
        imps = self._get_java_mapper(
            job_properties={
                "userName": "user",
                "oozie.wf.application.path": "hdfs:///user/USER/examples/apps/java",
            },
            config={},
        ).required_imports()
        imp_str = "\n".join(imps)
        self.assertIsNotNone(ast.parse(imp_str))

    def _get_java_mapper(self, job_properties, config, single_opts: bool = True, jar_files: List[str] = None):
        mapper = java_mapper.JavaMapper(
            oozie_node=self.java_node_with_single_opts if single_opts else self.java_node_with_multiple_opt,
            name="test_id",
            dag_name="DAG_NAME_A",
            props=PropertySet(job_properties=job_properties, config=config),
            jar_files=jar_files if jar_files else [],
            input_directory_path="/tmp/input-directory-path/",
        )
        return mapper
