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
"""Tests fs mapper"""

import ast
import unittest
from unittest import mock
from xml.etree import ElementTree as ET

from parameterized import parameterized
from airflow.utils.trigger_rule import TriggerRule

from converter.primitives import Task, Relation
from mappers import fs_mapper

TEST_PARAMS = {"user.name": "pig", "nameNode": "hdfs://localhost:8020"}


# pylint: disable=invalid-name
class PrepareMkdirCommandTest(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "<mkdir path='hdfs://localhost:8020/home/pig/test-fs/test-mkdir-1'/>",
                "fs -mkdir -p \\'{}\\'",
                ["/home/pig/test-fs/test-mkdir-1"],
            ),
            (
                "<mkdir path='${nameNode}/home/pig/test-fs/DDD-mkdir-1'/>",
                "fs -mkdir -p \\'{}\\'",
                ["/home/pig/test-fs/DDD-mkdir-1"],
            ),
        ]
    )
    def test_result(self, xml, expected_command, expected_arguments):
        node = ET.fromstring(xml)
        command, arguments = fs_mapper.prepare_mkdir_command(node, TEST_PARAMS)
        self.assertEqual(expected_command, command)
        self.assertEqual(expected_arguments, arguments)


class PrepareDeleteCommandTest(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "<delete path='hdfs://localhost:8020/home/pig/test-fsXXX/test-delete-3'/>",
                "fs -rm -r \\'{}\\'",
                ["/home/pig/test-fsXXX/test-delete-3"],
            ),
            (
                "<delete path='${nameNode}/home/${wf:conf(\"user.name\")}/test-fs/test-delete-3'/>",
                "fs -rm -r \\'{}\\'",
                ['/home/{el_wf_functions.wf_conf(CTX, "user.name")}/test-fs/test-delete-3'],
            ),
        ]
    )
    def test_result(self, xml, expected_command, expected_arguments):
        node = ET.fromstring(xml)
        command, arguments = fs_mapper.prepare_delete_command(node, TEST_PARAMS)
        self.assertEqual(expected_command, command)
        self.assertEqual(expected_arguments, arguments)


class PrepareMoveCommandTest(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "<move source='hdfs://localhost:8020/home/pig/test-fs/test-move-1' "
                "target='/home/pig/test-fs/test-move-2' />",
                "fs -mv \\'{}\\' \\'{}\\'",
                ["/home/pig/test-fs/test-move-1", "/home/pig/test-fs/test-move-2"],
            ),
            (
                "<move source='${nameNode}/home/pig/test-fs/test-move-1' "
                "target='/home/pig/test-DDD/test-move-2' />",
                "fs -mv \\'{}\\' \\'{}\\'",
                ["/home/pig/test-fs/test-move-1", "/home/pig/test-DDD/test-move-2"],
            ),
        ]
    )
    def test_result(self, xml, expected_command, expected_arguments):
        node = ET.fromstring(xml)
        command, arguments = fs_mapper.prepare_move_command(node, TEST_PARAMS)
        self.assertEqual(expected_command, command)
        self.assertEqual(expected_arguments, arguments)


class PrepareChmodCommandTest(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "<chmod path='hdfs://localhost:8020/home/pig/test-fs/test-chmod-1' "
                "permissions='777' dir-files='false' />",
                "fs -chmod {} \\'{}\\' \\'{}\\'",
                ["", "777", "/home/pig/test-fs/test-chmod-1"],
            ),
            (
                "<chmod path='hdfs://localhost:8020/home/pig/test-fs/test-chmod-2' "
                "permissions='777' dir-files='true' />",
                "fs -chmod {} \\'{}\\' \\'{}\\'",
                ["", "777", "/home/pig/test-fs/test-chmod-2"],
            ),
            (
                "<chmod path='${nameNode}/home/pig/test-fs/test-chmod-3' permissions='777' />",
                "fs -chmod {} \\'{}\\' \\'{}\\'",
                ["", "777", "/home/pig/test-fs/test-chmod-3"],
            ),
            (
                """<chmod path='hdfs://localhost:8020/home/pig/test-fs/test-chmod-4'
                permissions='777' dir-files='false' >
         <recursive/>
         </chmod>""",
                "fs -chmod {} \\'{}\\' \\'{}\\'",
                ["-R", "777", "/home/pig/test-fs/test-chmod-4"],
            ),
        ]
    )
    def test_result(self, xml, expected_command, expected_arguments):
        node = ET.fromstring(xml)
        command, arguments = fs_mapper.prepare_chmod_command(node, TEST_PARAMS)
        self.assertEqual(expected_command, command)
        self.assertEqual(expected_arguments, arguments)


class PrepareTouchzCommandTest(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "<touchz path='hdfs://localhost:8020/home/pig/test-fs/test-touchz-1' />",
                "fs -touchz \\'{}\\'",
                ["/home/pig/test-fs/test-touchz-1"],
            ),
            (
                "<touchz path='${nameNode}/home/pig/test-fs/DDDD-touchz-1' />",
                "fs -touchz \\'{}\\'",
                ["/home/pig/test-fs/DDDD-touchz-1"],
            ),
        ]
    )
    def test_result(self, xml, expected_command, expected_arguments):
        node = ET.fromstring(xml)
        command, arguments = fs_mapper.prepare_touchz_command(node, TEST_PARAMS)
        self.assertEqual(expected_command, command)
        self.assertEqual(expected_arguments, arguments)


class PrepareChgrpCommandTest(unittest.TestCase):
    @parameterized.expand(
        [
            (
                "<chgrp path='hdfs://localhost:8020/home/pig/test-fs/test-chgrp-1' group='hadoop' />",
                "fs -chgrp {} \\'{}\\' \\'{}\\'",
                ["", "hadoop", "/home/pig/test-fs/test-chgrp-1"],
            ),
            (
                "<chgrp path='${nameNode}0/home/pig/test-fs/DDD-chgrp-1' group='hadoop' />",
                "fs -chgrp {} \\'{}\\' \\'{}\\'",
                ["", "hadoop", "/home/pig/test-fs/DDD-chgrp-1"],
            ),
        ]
    )
    def test_result(self, xml, expected_command, expected_arguments):
        node = ET.fromstring(xml)
        command, arguments = fs_mapper.prepare_chgrp_command(node, TEST_PARAMS)
        self.assertEqual(expected_command, command)
        self.assertEqual(expected_arguments, arguments)


class FsMapperSingleTestCase(unittest.TestCase):
    def setUp(self):
        # language=XML
        node_str = """
            <fs>
                <mkdir path='hdfs://localhost:9200/home/pig/test-delete-1'/>
            </fs>"""
        self.node = ET.fromstring(node_str)

        self.mapper = _get_fs_mapper(oozie_node=self.node)
        self.mapper.on_parse_node()

    @mock.patch("mappers.fs_mapper.render_template", return_value="RETURN")
    def test_convert_to_text(self, render_template_mock):
        res = self.mapper.convert_to_text()
        self.assertEqual(res, "RETURN")

        _, kwargs = render_template_mock.call_args
        tasks = kwargs["tasks"]
        relations = kwargs["relations"]

        self.assertEqual(kwargs["template_name"], "action.tpl")
        self.assertEqual(
            tasks,
            [
                Task(
                    task_id="test-id",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mkdir -p \\'{}\\'",
                        "arguments": ["/home/pig/test-delete-1"],
                    },
                )
            ],
        )
        self.assertEqual(relations, [])

    def test_required_imports(self):
        imps = self.mapper.required_imports()
        imp_str = "\n".join(imps)
        self.assertIsNotNone(ast.parse(imp_str))

    def test_get_first_task_id(self):
        self.assertEqual(self.mapper.first_task_id, "test-id")

    def test_get_last_task_id(self):
        self.assertEqual(self.mapper.last_task_id, "test-id")


class FsMapperEmptyTestCase(unittest.TestCase):
    def setUp(self):
        self.node = ET.Element("fs")
        self.mapper = _get_fs_mapper(oozie_node=self.node)
        self.mapper.on_parse_node()

    @mock.patch("mappers.fs_mapper.render_template")
    def test_convert_to_text(self, render_template_mock):
        self.mapper.convert_to_text()

        _, kwargs = render_template_mock.call_args
        tasks = kwargs["tasks"]
        relations = kwargs["relations"]

        self.assertEqual(kwargs["template_name"], "action.tpl")
        self.assertEqual([Task(task_id="test-id", template_name="dummy.tpl")], tasks)
        self.assertEqual(relations, [])

    def test_required_imports(self):
        imps = self.mapper.required_imports()
        imp_str = "\n".join(imps)
        self.assertIsNotNone(ast.parse(imp_str))

    def test_get_first_task_id(self):
        self.assertEqual(self.mapper.first_task_id, "test-id")

    def test_get_last_task_id(self):
        self.assertEqual(self.mapper.last_task_id, "test-id")


class FsMapperComplexTestCase(unittest.TestCase):
    def setUp(self):
        # language=XML
        node_str = """
            <fs>
                <!-- mkdir -->
                <mkdir path='hdfs://localhost:9200/home/pig/test-delete-1'/>
                <mkdir path='hdfs:///home/pig/test-delete-2'/>
                <!-- delete -->
                <mkdir path='hdfs://localhost:9200/home/pig/test-delete-1'/>
                <mkdir path='hdfs://localhost:9200/home/pig/test-delete-2'/>
                <mkdir path='hdfs://localhost:9200/home/pig/test-delete-3'/>
                <delete path='hdfs://localhost:9200/home/pig/test-delete-1'/>

                <!-- move -->
                <mkdir path='hdfs://localhost:9200/home/pig/test-delete-1'/>
                <move source='hdfs://localhost:9200/home/pig/test-chmod-1' target='/home/pig/test-chmod-2' />

                <!-- chmod -->
                <mkdir path='hdfs://localhost:9200/home/pig/test-chmod-1'/>
                <mkdir path='hdfs://localhost:9200/home/pig/test-chmod-2'/>
                <mkdir path='hdfs://localhost:9200/home/pig/test-chmod-3'/>
                <mkdir path='hdfs://localhost:9200/home/pig/test-chmod-4'/>
                <chmod path='hdfs://localhost:9200/home/pig/test-chmod-1'
                    permissions='-rwxrw-rw-' dir-files='false' />
                <chmod path='hdfs://localhost:9200/home/pig/test-chmod-2'
                    permissions='-rwxrw-rw-' dir-files='true' />
                <chmod path='hdfs://localhost:9200/home/pig/test-chmod-3'
                    permissions='-rwxrw-rw-' />
                <chmod path='hdfs://localhost:9200/home/pig/test-chmod-4'
                    permissions='-rwxrw-rw-' dir-files='false' >
                    <recursive/>
                </chmod>

                <!-- touchz -->
                <touchz path='hdfs://localhost:9200/home/pig/test-touchz-1' />

                <!-- chgrp -->
                <chgrp path='hdfs://localhost:9200/home/pig/test-touchz-1' group='pig' />
            </fs>"""
        self.node = ET.fromstring(node_str)

        self.mapper = _get_fs_mapper(oozie_node=self.node)
        self.mapper.on_parse_node()

    @mock.patch("mappers.fs_mapper.render_template")
    def test_convert_to_text(self, render_template_mock):
        self.mapper.convert_to_text()

        _, kwargs = render_template_mock.call_args
        tasks = kwargs["tasks"]
        relations = kwargs["relations"]

        self.assertEqual(kwargs["template_name"], "action.tpl")
        self.assertEqual(
            [
                Task(
                    task_id="test-id-fs-0-mkdir",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mkdir -p \\'{}\\'",
                        "arguments": ["/home/pig/test-delete-1"],
                    },
                ),
                Task(
                    task_id="test-id-fs-1-mkdir",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mkdir -p \\'{}\\'",
                        "arguments": ["/home/pig/test-delete-2"],
                    },
                ),
                Task(
                    task_id="test-id-fs-2-mkdir",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mkdir -p \\'{}\\'",
                        "arguments": ["/home/pig/test-delete-1"],
                    },
                ),
                Task(
                    task_id="test-id-fs-3-mkdir",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mkdir -p \\'{}\\'",
                        "arguments": ["/home/pig/test-delete-2"],
                    },
                ),
                Task(
                    task_id="test-id-fs-4-mkdir",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mkdir -p \\'{}\\'",
                        "arguments": ["/home/pig/test-delete-3"],
                    },
                ),
                Task(
                    task_id="test-id-fs-5-delete",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -rm -r \\'{}\\'",
                        "arguments": ["/home/pig/test-delete-1"],
                    },
                ),
                Task(
                    task_id="test-id-fs-6-mkdir",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mkdir -p \\'{}\\'",
                        "arguments": ["/home/pig/test-delete-1"],
                    },
                ),
                Task(
                    task_id="test-id-fs-7-move",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mv \\'{}\\' \\'{}\\'",
                        "arguments": ["/home/pig/test-chmod-1", "/home/pig/test-chmod-2"],
                    },
                ),
                Task(
                    task_id="test-id-fs-8-mkdir",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mkdir -p \\'{}\\'",
                        "arguments": ["/home/pig/test-chmod-1"],
                    },
                ),
                Task(
                    task_id="test-id-fs-9-mkdir",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mkdir -p \\'{}\\'",
                        "arguments": ["/home/pig/test-chmod-2"],
                    },
                ),
                Task(
                    task_id="test-id-fs-10-mkdir",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mkdir -p \\'{}\\'",
                        "arguments": ["/home/pig/test-chmod-3"],
                    },
                ),
                Task(
                    task_id="test-id-fs-11-mkdir",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -mkdir -p \\'{}\\'",
                        "arguments": ["/home/pig/test-chmod-4"],
                    },
                ),
                Task(
                    task_id="test-id-fs-12-chmod",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -chmod {} \\'{}\\' \\'{}\\'",
                        "arguments": ["", "-rwxrw-rw-", "/home/pig/test-chmod-1"],
                    },
                ),
                Task(
                    task_id="test-id-fs-13-chmod",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -chmod {} \\'{}\\' \\'{}\\'",
                        "arguments": ["", "-rwxrw-rw-", "/home/pig/test-chmod-2"],
                    },
                ),
                Task(
                    task_id="test-id-fs-14-chmod",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -chmod {} \\'{}\\' \\'{}\\'",
                        "arguments": ["", "-rwxrw-rw-", "/home/pig/test-chmod-3"],
                    },
                ),
                Task(
                    task_id="test-id-fs-15-chmod",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -chmod {} \\'{}\\' \\'{}\\'",
                        "arguments": ["-R", "-rwxrw-rw-", "/home/pig/test-chmod-4"],
                    },
                ),
                Task(
                    task_id="test-id-fs-16-touchz",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -touchz \\'{}\\'",
                        "arguments": ["/home/pig/test-touchz-1"],
                    },
                ),
                Task(
                    task_id="test-id-fs-17-chgrp",
                    template_name="fs_op.tpl",
                    template_params={
                        "pig_command": "fs -chgrp {} \\'{}\\' \\'{}\\'",
                        "arguments": ["", "pig", "/home/pig/test-touchz-1"],
                    },
                ),
            ],
            tasks,
        )
        self.assertEqual(
            relations,
            [
                Relation(from_task_id="test-id-fs-0-mkdir", to_task_id="test-id-fs-1-mkdir"),
                Relation(from_task_id="test-id-fs-1-mkdir", to_task_id="test-id-fs-2-mkdir"),
                Relation(from_task_id="test-id-fs-2-mkdir", to_task_id="test-id-fs-3-mkdir"),
                Relation(from_task_id="test-id-fs-3-mkdir", to_task_id="test-id-fs-4-mkdir"),
                Relation(from_task_id="test-id-fs-4-mkdir", to_task_id="test-id-fs-5-delete"),
                Relation(from_task_id="test-id-fs-5-delete", to_task_id="test-id-fs-6-mkdir"),
                Relation(from_task_id="test-id-fs-6-mkdir", to_task_id="test-id-fs-7-move"),
                Relation(from_task_id="test-id-fs-7-move", to_task_id="test-id-fs-8-mkdir"),
                Relation(from_task_id="test-id-fs-8-mkdir", to_task_id="test-id-fs-9-mkdir"),
                Relation(from_task_id="test-id-fs-9-mkdir", to_task_id="test-id-fs-10-mkdir"),
                Relation(from_task_id="test-id-fs-10-mkdir", to_task_id="test-id-fs-11-mkdir"),
                Relation(from_task_id="test-id-fs-11-mkdir", to_task_id="test-id-fs-12-chmod"),
                Relation(from_task_id="test-id-fs-12-chmod", to_task_id="test-id-fs-13-chmod"),
                Relation(from_task_id="test-id-fs-13-chmod", to_task_id="test-id-fs-14-chmod"),
                Relation(from_task_id="test-id-fs-14-chmod", to_task_id="test-id-fs-15-chmod"),
                Relation(from_task_id="test-id-fs-15-chmod", to_task_id="test-id-fs-16-touchz"),
                Relation(from_task_id="test-id-fs-16-touchz", to_task_id="test-id-fs-17-chgrp"),
            ],
        )

    def test_required_imports(self):
        imps = self.mapper.required_imports()
        imp_str = "\n".join(imps)
        self.assertIsNotNone(ast.parse(imp_str))

    def test_get_first_task_id(self):
        self.assertEqual(self.mapper.first_task_id, "test-id-fs-0-mkdir")

    def test_get_last_task_id(self):
        self.assertEqual(self.mapper.last_task_id, "test-id-fs-17-chgrp")


def _get_fs_mapper(oozie_node):
    return fs_mapper.FsMapper(
        oozie_node=oozie_node, name="test-id", trigger_rule=TriggerRule.DUMMY, properties={}
    )
