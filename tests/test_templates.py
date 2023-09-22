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
"""Tests Templates"""
import ast
from copy import deepcopy
from random import randint
from typing import Dict, Any, Union, List
from unittest import TestCase

from parameterized import parameterized
from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.converter.task_group import TaskGroup
from o2a.utils.template_utils import render_template

DELETE_MARKER: Any = {}


def mutate(parent: Dict[str, Any], mutations: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mutate a dictionary. The mutation is determined using a dictionary with changes.
    The mutation may contain a special value - ``DELETE_MARKER``, which means that the key will be deleted.
    In the simplest case, this means that two dictionaries are combined.

    :Example::

    .. code-block:: pycon

        In [0]: target = { 'a': { 'b': { 'c': 3 } } }

        In [1]: mutate(target, {'a': {'b': "user"}})
        Out[1]: {'a': {'b': 'user'}}

    :Example::

    .. code-block:: pycon

        In [0]: target = { 'a': { 'b': { 'c': 3 } } }

        In [1]: mutate(target, {'a': {'b': DELETE_MARKER}})
        Out[1]: {'a': {}}

    """
    result = deepcopy(parent)
    for key, value in mutations.items():
        if value is DELETE_MARKER:
            del result[key]
        elif isinstance(value, dict):
            result[key] = mutate(parent.get(key, {}), value)
        else:
            result[key] = value
    return result


def get_value_by_path(target: Any, path: List[Union[str, int]]) -> Any:
    """
    Gets the value at path of dict or list.

    :Example:

    .. code-block:: pycon

        In [0]: target = { 'a': { 'b': { 'c': 3 } } }
        Out[0]: {'a': [{'b': {'c': 'AAA'}}]}

        In [1]: target = { 'a': [{ 'b': { 'c': 3 } }] }

        In [2]: get_value_by_path(target, ["a", 0, "b", "c"])
        Out[2]: 3

    The behavior of the function is similar to:
    https://lodash.com/docs#get
    """
    result = target
    for segment in path:
        if isinstance(result, dict):
            result = result[segment]
        elif isinstance(result, list):
            result = result[int(segment)]
        else:
            raise Exception(f"Invalid path: {path}")
    return result


def set_value_by_path(target: Any, path: List[Union[str, int]], value: Any) -> None:
    """"
    Sets the value at path of dict or list.

    :Example::

    .. code-block:: pycon

        In [0]: target = { 'a': [{ 'b': { 'c': 3 } }] }

        In [1]: set_value_by_path(target, ["a", 0, "b", "c"], "DAG_NAME_A")

        In [2]: target
        Out[2]: {'a': [{'b': {'c': 'AAA'}}]}

    The behavior of the function is similar to:
    https://lodash.com/docs#set
    """
    result = get_value_by_path(target, path[:-1])
    if isinstance(result, dict):
        result[path[-1]] = value
    elif isinstance(result, list):
        result[int(path[-1])] = value
    else:
        raise Exception(f"Invalid path: {path}")


class TemplateTestMixin:
    # noinspection PyPep8Naming
    # pylint: disable=invalid-name
    def assertValidPython(self, code):
        self.assertTrue(ast.parse(code))

    def test_all_template_parameters_must_be_correlated_with_output(self):
        """
        This test performs mutations of each value and checks if this caused a change
        in result of the template rendering. The new value is selected randomly. The operation is
        performed recursively.

        This test allows you to check if all the parameters specified in the `DEFAULT_TEMPLATE_PARAMS` field
        are used in the template specified by the `TEMPLATE_NAME` field.
        """
        original_view = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)

        def walk_recursively_and_mutate(path: List[Union[str, int]]):
            current_value = get_value_by_path(self.DEFAULT_TEMPLATE_PARAMS, path)
            if isinstance(current_value, str):
                template_params = deepcopy(self.DEFAULT_TEMPLATE_PARAMS)
                set_value_by_path(template_params, path, f"no_error_value_{randint(0, 100)}")
                mutated_view = render_template(self.TEMPLATE_NAME, **template_params)
                self.assertNotEqual(
                    original_view,
                    mutated_view,
                    f"Uncorrelated template job_properties: {path}, Mutated view: {mutated_view}",
                )
            elif isinstance(current_value, int):
                template_params = deepcopy(self.DEFAULT_TEMPLATE_PARAMS)
                set_value_by_path(template_params, path, randint(0, 100))
                mutated_view = render_template(self.TEMPLATE_NAME, **template_params)
                self.assertNotEqual(
                    original_view,
                    mutated_view,
                    f"Uncorrelated template job_properties: {path}, Mutated view: {mutated_view}",
                )

            elif isinstance(current_value, dict):
                for key, _ in current_value.items():
                    walk_recursively_and_mutate([*path, key])
            elif isinstance(current_value, list):
                for i in range(len(current_value)):
                    walk_recursively_and_mutate([*path, i])

        walk_recursively_and_mutate([])


class DecisionTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "decision.tpl"

    DEFAULT_TEMPLATE_PARAMS = dict(
        task_id="DAG_NAME_A",
        trigger_rule=TriggerRule.DUMMY,
        case_dict={"first_not_null('', '')": "task1", "'True'": "task2", "default": "task3"},
    )

    def test_minimal_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand(
        [({"task_id": 'AA"AA"\''},), ({"trigger_rule": 'AA"AA"\''},), ({"case_dict": {"default": 'tas"k3'}},)]
    )
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutations=mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class DummyTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "dummy.tpl"

    DEFAULT_TEMPLATE_PARAMS = dict(task_id="DAG_NAME_A", trigger_rule=TriggerRule.DUMMY)

    def test_minimal_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand([({"task_id": 'AA"AA"\''},), ({"trigger_rule": 'AA"AA"\''},)])
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutations=mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class FsOpTempalteTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "fs_op.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "DAG_NAME_A",
        "pig_command": "DAG_NAME_A",
        "trigger_rule": TriggerRule.DUMMY,
        "action_node_properties": {"key": "value"},
    }

    def test_minimal_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand(
        [
            ({"pig_command": 'AA"AA"\''},),
            ({"task_id": 'AA"AA"\''},),
            ({"trigger_rule": 'AA"AA"\''},),
            ({"action_node_properties": {"key": 'value""\''}},),
        ]
    )
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutations=mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class GitTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "git.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "TASK_ID",
        "trigger_rule": "dummy",
        "git_uri": "https://github.com/apache/oozie",
        "git_branch": "my-awesome-branch",
        "destination_path": "/my_git_repo_directory",
        "key_path": "/awesome-key/",
    }

    def test_minimal_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand(
        [
            ({"git_uri": None},),
            ({"git_branch": None},),
            ({"destination_uri": None},),
            ({"destination_path": None},),
            ({"key_path_uri": None},),
            ({"key_path": None},),
        ]
    )
    def test_optional_parameters(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)

    @parameterized.expand([({"task_id": 'AA"AA"\''},), ({"trigger_rule": 'AA"AA"\''},)])
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutations=mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class HiveTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "hive.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "AA",
        "trigger_rule": "dummy",
        "script": "id.q",
        "query": "SELECT 1",
        "variables": {
            "INPUT": "/user/${wf:user()}/${examplesRoot}/input-data/text",
            "OUTPUT": "/user/${wf:user()}/${examplesRoot}/output-data/demo/hive-node",
        },
        "action_node_properties": {"key": "value"},
    }

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand([({"variables": {"OUTPUT": None}},), ({"variables": {"INPUT": None}},)])
    def test_optional_parameters(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)

    @parameterized.expand(
        [
            ({"task_id": 'A"'},),
            ({"trigger_rule": 'A"'},),
            ({"script": 'A"'},),
            ({"query": 'A"'},),
            ({"variables": {'AA"': "DAG_NAME_A"}},),
            ({"variables": {"AA": 'A"AA'}},),
            ({"action_node_properties": {'AA"': "DAG_NAME_A"}},),
            ({"action_node_properties": {"AA": 'A"AA'}},),
        ]
    )
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class KillTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "kill.tpl"

    DEFAULT_TEMPLATE_PARAMS = dict(task_id="DAG_NAME_A", trigger_rule=TriggerRule.DUMMY)

    def test_minimal_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand([({"task_id": 'AA"AA"\''},), ({"trigger_rule": 'AA"AA"\''},)])
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutations=mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class MapReduceTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "mapreduce.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "AA",
        "trigger_rule": "dummy",
        "action_node_properties": {
            "mapred.mapper.new-api": "true",
            "mapred.reducer.new-api": "true",
            "mapred.job.queue.name": "${queueName}",
            "mapreduce.job.map.class": "WordCount$Map",
            "mapreduce.job.reduce.class": "WordCount$Reduce",
            "mapreduce.job.output.key.class": "org.apache.hadoop.io.Text",
            "mapreduce.job.output.value.class": "org.apache.hadoop.io.IntWritable",
            "mapreduce.input.fileinputformat.inputdir": "/user/mapred/${examplesRoot}/mapreduce/input",
            "mapreduce.output.fileoutputformat.outputdir": "/user/mapred/${examplesRoot}/mapreduce/output",
        },
        "hdfs_files": ["B"],
        "hdfs_archives": ["D"],
    }

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand([({"hdfs_files": None},), ({"hdfs_archives": None},)])
    def test_optional_parameters(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)

    @parameterized.expand(
        [
            ({"task_id": 'AA"AA"\''},),
            ({"trigger_rule": 'AA"AA"\''},),
            ({"action_node_properties": {"mapred.mapper.new-api": 'tr"ue'}},),
            ({"hdfs_files": ['val"ue']},),
            ({"hdfs_archives": ['val"ue']},),
        ]
    )
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutations=mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class PigTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "pig.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "AA",
        "trigger_rule": "dummy",
        "params_dict": {
            "INPUT": "/user/${wf:user()}/${examplesRoot}/input-data/text",
            "OUTPUT": "/user/${wf:user()}/${examplesRoot}/output-data/demo/pig-node",
        },
        "script_file_name": "id.pig",
        "action_node_properties": {"key": "value"},
    }

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand([({"params_dict": {"OUTPUT": None}},), ({"params_dict": {"INPUT": None}},)])
    def test_optional_parameters(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)

    @parameterized.expand(
        [
            ({"job_properties": {'AA"': "DAG_NAME_A"}},),
            ({"job_properties": {"AA": 'A"AA'}},),
            ({"command": 'A"'},),
            ({"user": 'A"'},),
            ({"host": 'A"'},),
            ({"script_file_name": 'AAAA"AAA'},),
            ({"params_dict": {"key": 'val"ue'}},),
        ]
    )
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template("pig.tpl", **template_params)
        self.assertValidPython(res)


class PrepareTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "prepare.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "DAG_NAME_A",
        "trigger_rule": "dummy",
        "delete": "file1 file2",
        "mkdir": "file3 file4",
        "action_node_properties": {"key": "value"},
    }

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand(
        [({"task_id": 'AA"AA"\''},), ({"trigger_rule": 'AA"AA"\''},), ({"prepare_command": 'PREPARE_CMD"'},)]
    )
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class ShellTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "shell.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "DAG_NAME_A",
        "pig_command": "PIG_CMD",
        "trigger_rule": "dummy",
        "action_node_properties": {"key": "value"},
    }

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand(
        [
            ({"task_id": 'AA"AA"\''},),
            ({"trigger_rule": 'AA"AA"\''},),
            ({"pig_command": 'A"'},),
            ({"pig_command": 'PIG_CMD"'},),
        ]
    )
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class SparkTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "spark.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "AA",
        "hdfs_archives": [],
        "arguments": ["inputpath=hdfs:///input/file.txt", "value=2"],
        "dataproc_spark_jars": ["/lib/spark-examples_2.10-1.1.0.jar"],
        "spark_opts": {
            "mapred.compress.map.output": "true",
            "spark.executor.extraJavaOptions": "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp",
        },
        "hdfs_files": [],
        "job_name": "Spark Examples",
        "main_class": "org.apache.spark.examples.mllib.JavaALS",
        "main_jar": None,
        "trigger_rule": "dummy",
        "action_node_properties": {"key": "value"},
    }

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand(
        [
            ({"archives": None},),
            ({"dataproc_spark_jars": None},),
            ({"dataproc_spark_properties": None},),
            ({"files": None},),
            ({"main_class": None},),
            ({"main_jar": None},),
        ]
    )
    def test_optional_parameters(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)

    @parameterized.expand(
        [
            ({"task_id": 'AA"AA"\''},),
            ({"trigger_rule": 'AA"AA"\''},),
            ({"name": 'A"'},),
            ({"command": 'A"'},),
            ({"user": 'A"'},),
            ({"host": 'A"'},),
        ]
    )
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class SshTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "ssh.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "AA",
        "job_properties": {},
        "trigger_rule": "dummy",
        "command": "ls -l -a",
        "user": "user",
        "host": "apache.org",
        "action_node_properties": {"key": "value"},
    }

    def test_minimal_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand([({"action_node_properties": {}},)])
    def test_optional_parameters(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)

    @parameterized.expand(
        [
            ({"task_id": 'AA"AA"\''},),
            ({"trigger_rule": 'AA"AA"\''},),
            ({"action_node_properties": {'AA"': "DAG_NAME_A"}},),
            ({"action_node_properties": {"AA": 'A"AA'}},),
            ({"command": 'A"'},),
            ({"user": 'A"'},),
            ({"host": 'A"'},),
        ]
    )
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class SubwfTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "subwf.tpl"

    DEFAULT_TEMPLATE_PARAMS = {"task_id": "test_id", "trigger_rule": "dummy", "app_name": "DAG_NAME_A"}

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand(
        [({"task_id": 'AA"AA"\''},), ({"trigger_rule": 'AA"AA"\''},), ({"app_name": "APP\"'NAME"},)]
    )
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class WorkflowTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "workflow.tpl"

    DEFAULT_TEMPLATE_PARAMS = dict(
        dag_name="test_dag",
        dependencies={"import awesome_stuff"},
        task_groups=[
            TaskGroup(
                name="TASK_GROUP",
                tasks=[
                    Task(task_id="first_task", template_name="dummy.tpl"),
                    Task(task_id="second_task", template_name="dummy.tpl"),
                ],
            )
        ],
        job_properties={"user.name": "USER"},
        config={},
        relations={Relation(from_task_id="TASK_1", to_task_id="TASK_2")},
        schedule_interval=3,
        start_days_ago=3,
    )

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)


class SubWorkflowTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "subworkflow.tpl"

    DEFAULT_TEMPLATE_PARAMS = dict(
        dependencies={"import awesome_stuff"},
        task_groups=[
            TaskGroup(
                name="AAA",
                tasks=[
                    Task(task_id="first_task", template_name="dummy.tpl"),
                    Task(task_id="second_task", template_name="dummy.tpl"),
                ],
                relations=[Relation(from_task_id="first_task", to_task_id="second_task")],
            )
        ],
        job_properties={"user.name": "USER"},
        config={"key": "value"},
        relations={Relation(from_task_id="TASK_1", to_task_id="TASK_2")},
    )

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)
