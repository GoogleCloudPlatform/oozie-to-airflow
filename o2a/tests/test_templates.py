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
from unittest import mock, TestCase

from parameterized import parameterized
from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.parsed_node import ParsedNode
from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.dummy_mapper import DummyMapper
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

        In [1]: mutate(target, {'a': {'b': "AAAA"}})
        Out[1]: {'a': {'b': 'AAAA'}}

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

        In [1]: set_value_by_path(target, ["a", 0, "b", "c"], "AAA")

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
                    f"Uncorrelated template params: {path}, Mutated view: {mutated_view}",
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
        task_id="AAA",
        trigger_rule=TriggerRule.DUMMY,
        case_dict={"first_not_null('', '')": "task1", "'True'": "task2", "default": "task3"},
    )

    def test_minimal_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)


class DummyTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "dummy.tpl"

    DEFAULT_TEMPLATE_PARAMS = dict(task_id="AAA", trigger_rule=TriggerRule.DUMMY)

    def test_minimal_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)


class FsOpTempalteTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "fs_op.tpl"

    DEFAULT_TEMPLATE_PARAMS = {"task_id": "AAA", "pig_command": "AAA", "trigger_rule": TriggerRule.DUMMY}

    def test_minimal_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    def test_quote_escape(self):
        template_params = {**self.DEFAULT_TEMPLATE_PARAMS, **dict(pig_command='AA"AA"')}
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class KillTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "kill.tpl"

    DEFAULT_TEMPLATE_PARAMS = dict(task_id="AAA", trigger_rule=TriggerRule.DUMMY)

    def test_minimal_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)


class MapReduceTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "mapreduce.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "AA",
        "trigger_rule": "dummy",
        "properties": {
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
        "params_dict": {},
        "hdfs_files": ["B"],
        "hdfs_archives": ["D"],
    }

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand([({"hdfs_files": None},), ({"hdfs_archives": None},)])
    def test_optional_parameters(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template("mapreduce.tpl", **template_params)
        self.assertValidPython(res)


class PigTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "pig.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "AA",
        "trigger_rule": "dummy",
        "properties": {"mapred.job.queue.name": "${queueName}", "mapred.map.output.compress": "false"},
        "params_dict": {
            "INPUT": "/user/${wf:user()}/${examplesRoot}/input-data/text",
            "OUTPUT": "/user/${wf:user()}/${examplesRoot}/output-data/demo/pig-node",
        },
        "script_file_name": "id.pig",
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
            ({"params": {'AA"': "AAA"}},),
            ({"params": {"AA": 'A"AA'}},),
            ({"command": 'A"'},),
            ({"user": 'A"'},),
            ({"host": 'A"'},),
        ]
    )
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template("pig.tpl", **template_params)
        self.assertValidPython(res)


class PrepareTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "prepare.tpl"

    DEFAULT_TEMPLATE_PARAMS = {"task_id": "AAA", "prepare_command": "AAAA", "trigger_rule": "dummy"}

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand([({"prepare_command": 'A"'},), ({"prepare_command": 'AAAAA"'},)])
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class ShellTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "shell.tpl"

    DEFAULT_TEMPLATE_PARAMS = {"task_id": "AAA", "pig_command": "AAAA", "trigger_rule": "dummy"}

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand([({"pig_command": 'A"'},), ({"pig_command": 'AAAAA"'},)])
    def test_escape_character(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)


class SparkTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "spark.tpl"

    DEFAULT_TEMPLATE_PARAMS = {
        "task_id": "AA",
        "archives": [],
        "arguments": ["inputpath=hdfs:///input/file.txt", "value=2"],
        "dataproc_spark_jars": ["/lib/spark-examples_2.10-1.1.0.jar"],
        "dataproc_spark_properties": {
            "mapred.compress.map.output": "true",
            "spark.executor.extraJavaOptions": "-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp",
        },
        "files": [],
        "job_name": "Spark Examples",
        "main_class": "org.apache.spark.examples.mllib.JavaALS",
        "main_jar": None,
        "trigger_rule": "dummy",
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
            ({"params": {'AA"': "AAA"}},),
            ({"params": {"AA": 'A"AA'}},),
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
        "params": {},
        "trigger_rule": "dummy",
        "command": "ls -l -a",
        "user": "user",
        "host": "apache.org",
    }

    def test_minimal_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)

    @parameterized.expand([({"params": None},)])
    def test_optional_parameters(self, mutation):
        template_params = mutate(self.DEFAULT_TEMPLATE_PARAMS, mutation)
        res = render_template(self.TEMPLATE_NAME, **template_params)
        self.assertValidPython(res)

    @parameterized.expand(
        [
            ({"params": {'AA"': "AAA"}},),
            ({"params": {"AA": 'A"AA'}},),
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

    DEFAULT_TEMPLATE_PARAMS = {"task_id": "test_id", "app_name": "AAA", "trigger_rule": "dummy"}

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)


class WorkflowTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "workflow.tpl"

    DEFAULT_TEMPLATE_PARAMS = dict(
        dag_name="test_dag",
        dependencies=["import awesome_stuff"],
        nodes=[
            ParsedNode(
                mock.MagicMock(spec=DummyMapper),
                tasks=[
                    Task(task_id="first_task", template_name="dummy.tpl"),
                    Task(task_id="second_task", template_name="dummy.tpl"),
                ],
            )
        ],
        params={"user.name": "USER"},
        relations={Relation(from_task_id="TASK_1", to_task_id="TASK_2")},
        schedule_interval=None,
        start_days_ago=None,
    )

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)


class SubWorkflowTemplateTestCase(TestCase, TemplateTestMixin):
    TEMPLATE_NAME = "subworkflow.tpl"

    DEFAULT_TEMPLATE_PARAMS = dict(
        dependencies=["import awesome_stuff"],
        nodes=[
            ParsedNode(
                mock.MagicMock(spec=DummyMapper),
                tasks=[
                    Task(task_id="first_task", template_name="dummy.tpl"),
                    Task(task_id="second_task", template_name="dummy.tpl"),
                ],
                relations=[Relation(from_task_id="first_task", to_task_id="second_task")],
            )
        ],
        params={"user.name": "USER"},
        relations={Relation(from_task_id="TASK_1", to_task_id="TASK_2")},
        schedule_interval=None,
        start_days_ago=None,
    )

    def test_green_path(self):
        res = render_template(self.TEMPLATE_NAME, **self.DEFAULT_TEMPLATE_PARAMS)
        self.assertValidPython(res)
