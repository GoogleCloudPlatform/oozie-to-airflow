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
"""Tests give mapper"""
import ast
import unittest
from typing import Dict
from xml.etree import ElementTree as ET


from o2a.converter.exceptions import ParseException
from o2a.converter.task import Task
from o2a.tasks.hive.hive_local_task import HiveLocalTask
from o2a.converter.relation import Relation
from o2a.mappers import hive_mapper
from o2a.o2a_libs.property_utils import PropertySet

# language=XML
TEST_BASE_HIVE = """
<hive xmlns="uri:oozie:hive-action:1.0">
    <resource-manager>${resourceManager}</resource-manager>
    <name-node>${nameNode}</name-node>
    <configuration>
        <property>
            <name>mapred.job.queue.name</name>
            <value>${queueName}</value>
        </property>
    </configuration>
</hive>
"""

# language=XML
FRAGMENT_PREPARE = """
<prepare> <!-- TODO: replace userName with wf:user when we have it implemented -->
    <delete path="${nameNode}/user/${userName}/${examplesRoot}/apps/pig/output"/>
    <mkdir path="${nameNode}/user/${userName}/${examplesRoot}/apps/pig/created-folder"/>
</prepare>
"""

# language=XML
FRAGMENT_SCRIPT = """
<fragment>
    <script>script.q</script>
    <param>INPUT=/user/${userName}/${examplesRoot}/apps/hive/input/</param>
    <param>OUTPUT=/user/${userName}/${examplesRoot}/apps/hive/output/</param>
</fragment>
"""


# language=XML
FRAGMENT_QUERY = """
<query>
DROP TABLE IF EXISTS test_query;
CREATE EXTERNAL TABLE test_query (a INT) STORED AS TEXTFILE
LOCATION '/user/${userName}/${examplesRoot}/input/';
INSERT OVERWRITE DIRECTORY '/user/${userName}/${examplesRoot}/output/' SELECT * FROM test_query;
</query>
"""

# language=XML
FRAGMENT_FILE = """
<fragment>
    <file>test_dir/test.txt#test_link.txt</file>
    <file>/user/${userName}/${examplesRoot}/apps/pig/test_dir/test2.zip#test_link.zip</file>
</fragment>
"""

# language=XML
FRAGMENT_ARCHIVE = """
<fragment>
    <archive>test_dir/test2.zip#test_zip_dir</archive>
    <archive>test_dir/test3.zip#test3_zip_dir</archive>
</fragment>
"""


class TestHiveMapper(unittest.TestCase):
    job_properties: Dict[str, str] = {
        "nameNode": "hdfs://",
        "queueName": "default",
        "oozie.wf.application.path": "hdfs:///user/TEST_USERNAME/apps/hive",
        "userName": "TEST_USERNAME",
        "examplesRoot": "TEST_EXAMPLE_ROOT",
    }

    config: Dict[str, str] = {
        "context_type": "local",
    }

    def setUp(self):
        self.hive_node = ET.fromstring(TEST_BASE_HIVE)

    def test_to_tasks_and_relations_should_parse_query_element(self):
        self.hive_node.append(ET.fromstring(FRAGMENT_QUERY))

        mapper = self._get_hive_mapper(job_properties=self.job_properties, config=self.config)
        mapper.on_parse_node()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            [
                HiveLocalTask(
                    task_id="test_id",
                    template_name="hive/hive.tpl",
                    trigger_rule="one_success",
                    template_params={
                        "hql": "DROP TABLE IF EXISTS test_query;\nCREATE EXTERNAL TABLE test_query (a INT) "
                        "STORED AS TEXTFILE\nLOCATION '/user/{{userName}}/{{examplesRoot}}/input/';"
                        "\nINSERT OVERWRITE DIRECTORY '/user/{{userName}}/{{examplesRoot}}/output/' "
                        "SELECT * FROM test_query;",
                        "mapred_queue": "default",
                        "hive_cli_conn_id": "hive_cli_default",
                    },
                )
            ],
            tasks,
        )

        self.assertEqual([], relations)

    def test_to_tasks_and_relations_should_parse_script_element(self):
        for element in ET.fromstring(FRAGMENT_SCRIPT):
            self.hive_node.append(element)

        mapper = self._get_hive_mapper(job_properties=self.job_properties, config=self.config)
        mapper.on_parse_node()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            [
                HiveLocalTask(
                    task_id="test_id",
                    template_name="hive/hive.tpl",
                    trigger_rule="one_success",
                    template_params={
                        "hql": "script.q",
                        "mapred_queue": "default",
                        "hive_cli_conn_id": "hive_cli_default",
                    },
                )
            ],
            tasks,
        )

        self.assertEqual([], relations)

    def test_to_tasks_and_relations_should_parse_prepare_element(self):
        self.hive_node.append(ET.fromstring(FRAGMENT_QUERY))
        self.hive_node.append(ET.fromstring(FRAGMENT_PREPARE))

        mapper = self._get_hive_mapper(job_properties=self.job_properties, config=self.config)
        mapper.on_parse_node()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(2, len(tasks))
        self.assertEqual(
            Task(
                task_id="test_id_prepare",
                template_name="prepare/prepare.tpl",
                trigger_rule="one_success",
                template_params={
                    "delete": "/user/{{userName}}/{{examplesRoot}}/apps/pig/output",
                    "mkdir": "/user/{{userName}}/{{examplesRoot}}/apps/pig/created-folder",
                },
            ),
            tasks[0],
        )

        self.assertEqual([Relation(from_task_id="test_id_prepare", to_task_id="test_id")], relations)

    def test_to_tasks_and_relations_should_parse_file_elements(self):
        self.hive_node.append(ET.fromstring(FRAGMENT_QUERY))
        for element in ET.fromstring(FRAGMENT_FILE):
            self.hive_node.append(element)

        mapper = self._get_hive_mapper(job_properties=self.job_properties, config=self.config)
        mapper.on_parse_node()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(1, len(tasks))
        self.assertEqual(
            [
                HiveLocalTask(
                    task_id="test_id",
                    template_name="hive/hive.tpl",
                    trigger_rule="one_success",
                    template_params={
                        "hql": "DROP TABLE IF EXISTS test_query;\nCREATE EXTERNAL TABLE test_query (a INT) "
                        "STORED AS TEXTFILE\nLOCATION '/user/{{userName}}/{{examplesRoot}}/input/';"
                        "\nINSERT OVERWRITE DIRECTORY '/user/{{userName}}/{{examplesRoot}}/output/' "
                        "SELECT * FROM test_query;",
                        "mapred_queue": "default",
                        "hive_cli_conn_id": "hive_cli_default",
                    },
                )
            ],
            tasks,
        )

        self.assertEqual([], relations)

    def test_to_tasks_and_relations_should_parse_archive_element(self):
        self.hive_node.append(ET.fromstring(FRAGMENT_QUERY))
        for element in ET.fromstring(FRAGMENT_ARCHIVE):
            self.hive_node.append(element)

        mapper = self._get_hive_mapper(job_properties=self.job_properties, config=self.config)
        mapper.on_parse_node()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(1, len(tasks))
        self.assertEqual(
            [
                HiveLocalTask(
                    task_id="test_id",
                    template_name="hive/hive.tpl",
                    trigger_rule="one_success",
                    template_params={
                        "hql": "DROP TABLE IF EXISTS test_query;\nCREATE EXTERNAL TABLE test_query (a INT) "
                        "STORED AS TEXTFILE\nLOCATION '/user/{{userName}}/{{examplesRoot}}/input/';"
                        "\nINSERT OVERWRITE DIRECTORY '/user/{{userName}}/{{examplesRoot}}/output/' "
                        "SELECT * FROM test_query;",
                        "mapred_queue": "default",
                        "hive_cli_conn_id": "hive_cli_default",
                    },
                )
            ],
            tasks,
        )

        self.assertEqual([], relations)

    def test_on_parse_should_raise_exception_missing_query_or_script(self):
        mapper = self._get_hive_mapper(job_properties=self.job_properties, config=self.config)
        with self.assertRaisesRegex(
            ParseException, "Action Configuration does not include script or query element"
        ):
            mapper.on_parse_node()

    def test_on_parse_should_raise_exception_when_query_and_script_are_set_at_the_same_time(self):
        self.hive_node.append(ET.fromstring(FRAGMENT_QUERY))
        for element in ET.fromstring(FRAGMENT_SCRIPT):
            self.hive_node.append(element)

        mapper = self._get_hive_mapper(job_properties=self.job_properties, config=self.config)
        with self.assertRaisesRegex(
            ParseException,
            "Action Configuration include script and query element. Only one can be set at the same time.",
        ):
            mapper.on_parse_node()

    def test_required_imports(self):
        mapper = self._get_hive_mapper(job_properties=self.job_properties, config=self.config)
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    def _get_hive_mapper(self, job_properties, config):
        mapper = hive_mapper.HiveMapper(
            oozie_node=self.hive_node,
            name="test_id",
            dag_name="DAG_NAME_B",
            props=PropertySet(job_properties=job_properties, config=config),
            input_directory_path="/tmp/input-directory-path/",
        )
        return mapper
