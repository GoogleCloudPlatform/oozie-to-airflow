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

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers import git_mapper
from o2a.mappers.git_mapper import prepare_git_command

EXAMPLE_PARAMS = {"dataproc_cluster": "my-cluster", "gcp_region": "europe-west3", "nameNode": "hdfs://"}

# language=XML
EXAMPLE_XML = """
<git name="git">
    <prepare>
        <mkdir path="hdfs:///tmp/mk_path" />
        <delete path="hdfs:///tmp/d_path" />
    </prepare>
    <git-uri>https://github.com/apache/oozie</git-uri>
    <branch>${branch}</branch>
    <destination-uri>hdfs:///my_git_repo_directory</destination-uri>
    <key-path>hdfs://name-node.second.company.com:8020/awesome-key/</key-path>
</git>"""

EXAMPLE_PARAMS = {
    "branch": "my-awesome-branch",
    "dataproc_cluster": "my-cluster",
    "gcp_region": "europe-west3",
    "nameNode": "hdfs://",
    "userName": "test_user",
    "examplesRoot": "examples",
}


# pylint: disable=invalid-name
class prepare_git_command_TestCase(unittest.TestCase):
    def test_green_path(self):
        command = prepare_git_command(
            git_uri="GIT_URI", git_branch="GIT_BRANCH", destination_path="/DEST_PATH/", key_path="/KEY/PATH"
        )
        self.assertEqual(
            command,
            "$DAGS_FOLDER/../data/git.sh --cluster {dataproc_cluster} --region {gcp_region} "
            "--git-uri GIT_URI --destination-path /DEST_PATH/ --branch GIT_BRANCH --key-path /KEY/PATH",
        )

    def test_should_escape_special_characters(self):
        command = prepare_git_command(
            git_uri="GIT_'\"URI",
            git_branch="GIT'\"_BRANCH",
            destination_path="/DEST_PA'\"TH/",
            key_path="/KEY/'\"PATH",
        )
        self.assertEqual(
            command,
            "$DAGS_FOLDER/../data/git.sh --cluster {dataproc_cluster} --region {gcp_region} "
            "--git-uri 'GIT_'\"'\"'\"URI' --destination-path '/DEST_PA'\"'\"'\"TH/' "
            "--branch 'GIT'\"'\"'\"_BRANCH' --key-path '/KEY/'\"'\"'\"PATH'",
        )

    def test_without_branch(self):
        command = prepare_git_command(
            git_uri="GIT_URI", git_branch=None, destination_path="/DEST_PATH/", key_path="/KEY/PATH"
        )
        self.assertEqual(
            command,
            "$DAGS_FOLDER/../data/git.sh --cluster {dataproc_cluster} --region {gcp_region} "
            "--git-uri GIT_URI --destination-path /DEST_PATH/ --key-path /KEY/PATH",
        )

    def test_without_key_path(self):
        command = prepare_git_command(
            git_uri="GIT_URI", git_branch="GIT_BRANCH", destination_path="/DEST_PATH/", key_path=None
        )
        self.assertEqual(
            command,
            "$DAGS_FOLDER/../data/git.sh --cluster {dataproc_cluster} --region {gcp_region} "
            "--git-uri GIT_URI --destination-path /DEST_PATH/ --branch GIT_BRANCH",
        )


class TestGitMapper(unittest.TestCase):
    def test_create_mapper(self):
        git_node = ET.fromstring(EXAMPLE_XML)
        mapper = self._get_git_mapper(git_node)
        # make sure everything is getting initialized correctly
        self.assertEqual("test_id", mapper.name)
        self.assertEqual(TriggerRule.ALL_SUCCESS, mapper.trigger_rule)
        self.assertEqual(git_node, mapper.oozie_node)

    def test_convert_to_text_with_prepare_node(self):
        git_node = ET.fromstring(EXAMPLE_XML)

        mapper = self._get_git_mapper(git_node)
        mapper.on_parse_node()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            tasks,
            [
                Task(
                    task_id="test_id_prepare",
                    template_name="prepare.tpl",
                    trigger_rule="dummy",
                    template_params={
                        "prepare_command": "$DAGS_FOLDER/../data/prepare.sh -c my-cluster "
                        '-r europe-west3 -d "/tmp/d_path" -m "/tmp/mk_path"'
                    },
                ),
                Task(
                    task_id="test_id",
                    template_name="git.tpl",
                    trigger_rule="dummy",
                    template_params={
                        "bash_command": "$DAGS_FOLDER/../data/git.sh --cluster {dataproc_cluster} "
                        "--region {gcp_region} --git-uri https://github.com/apache/oozie "
                        "--destination-path /my_git_repo_directory --branch my-awesome-branch "
                        "--key-path /awesome-key/"
                    },
                ),
            ],
        )

        self.assertEqual(relations, [Relation(from_task_id="test_id_prepare", to_task_id="test_id")])

    def test_convert_to_text_without_prepare_node(self):
        spark_node = ET.fromstring(EXAMPLE_XML)
        prepare_node = spark_node.find("prepare")
        spark_node.remove(prepare_node)
        mapper = self._get_git_mapper(spark_node)
        mapper.on_parse_node()

        tasks, relations = mapper.to_tasks_and_relations()

        self.assertEqual(
            tasks,
            [
                Task(
                    task_id="test_id",
                    template_name="git.tpl",
                    trigger_rule="dummy",
                    template_params={
                        "bash_command": "$DAGS_FOLDER/../data/git.sh --cluster {dataproc_cluster} "
                        "--region {gcp_region} --git-uri https://github.com/apache/oozie "
                        "--destination-path /my_git_repo_directory --branch my-awesome-branch "
                        "--key-path /awesome-key/"
                    },
                )
            ],
        )
        self.assertEqual(relations, [])

    def test_required_imports(self):
        spark_node = ET.fromstring(EXAMPLE_XML)
        mapper = self._get_git_mapper(spark_node)
        imps = mapper.required_imports()
        imp_str = "\n".join(imps)
        ast.parse(imp_str)

    @staticmethod
    def _get_git_mapper(spark_node):
        mapper = git_mapper.GitMapper(oozie_node=spark_node, name="test_id", params=EXAMPLE_PARAMS)
        return mapper
