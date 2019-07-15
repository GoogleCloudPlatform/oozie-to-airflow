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
"""Tests action mapper"""
import unittest
from unittest import mock
from typing import List, Tuple, cast
from xml.etree import ElementTree as ET

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils.config_extractors import TAG_CONFIGURATION, TAG_JOB_XML
from o2a.utils.xml_utils import find_node_by_tag, find_nodes_by_tag

TEST_MAPPER_NAME = "mapper_name"
TEST_DAG_NAME = "dag_name"


class ActionMapperTestImpl(ActionMapper):
    def to_tasks_and_relations(self) -> Tuple[List[Task], List[Relation]]:
        tasks: List[Task] = [Task(task_id="TEST_TASK", template_name="dummy.tpl", template_params={})]
        relations: List[Relation] = []
        return tasks, relations

    # pylint: disable=no-self-use
    def required_imports(self):
        return set()


class TestActionMapper(unittest.TestCase):
    def test_prepend_task_no_tasks(self):
        task_1 = Task(task_id=TEST_MAPPER_NAME + "_1", template_name="pig.tpl")
        with self.assertRaises(IndexError):
            ActionMapper.prepend_task(task_to_prepend=task_1, tasks=[], relations=[])

    def test_prepend_task_empty_relations(self):
        task_1 = Task(task_id=TEST_MAPPER_NAME + "_1", template_name="pig.tpl")
        task_2 = Task(task_id=TEST_MAPPER_NAME + "_2", template_name="pig.tpl")

        tasks, relations = ActionMapper.prepend_task(task_to_prepend=task_1, tasks=[task_2], relations=[])
        self.assertEqual([task_1, task_2], tasks)
        self.assertEqual([Relation(from_task_id="mapper_name_1", to_task_id="mapper_name_2")], relations)

    def test_prepend_task_some_relations(self):
        task_1 = Task(task_id=TEST_MAPPER_NAME + "_1", template_name="pig.tpl")
        task_2 = Task(task_id=TEST_MAPPER_NAME + "_2", template_name="pig.tpl")
        task_3 = Task(task_id=TEST_MAPPER_NAME + "_3", template_name="pig.tpl")

        tasks, relations = ActionMapper.prepend_task(
            task_to_prepend=task_1,
            tasks=[task_2, task_3],
            relations=[Relation(from_task_id="mapper_name_2", to_task_id="mapper_name_3")],
        )
        self.assertEqual([task_1, task_2, task_3], tasks)
        self.assertEqual(
            [
                Relation(from_task_id="mapper_name_1", to_task_id="mapper_name_2"),
                Relation(from_task_id="mapper_name_2", to_task_id="mapper_name_3"),
            ],
            relations,
        )

    @mock.patch(
        "o2a.mappers.action_mapper.extract_properties_from_job_xml_nodes", return_value={"KEY1": "VALUE1"}
    )
    @mock.patch(
        "o2a.mappers.action_mapper.extract_properties_from_configuration_node",
        return_value={"KEY2": "VALUE2"},
    )
    def test_parse_job_xml_and_configuration_minimal_green_path(
        self, extract_properties_from_configuration_node_mock, extract_properties_from_job_xml_nodes_mock
    ):
        # language=XML
        action_node_str = """
<action name="action">
    <job-xml>AAA.xml</job-xml>
    <configuration>
        <property>
            <name>KEY1</name>
            <value>VALUE1</value>
        </property>
    </configuration>
</action>
"""
        action_node: ET.Element = cast(ET.Element, ET.fromstring(action_node_str))
        mapper = self._get_action_mapper(action_node)

        mapper.on_parse_node()

        config_node = find_node_by_tag(action_node, TAG_CONFIGURATION)
        extract_properties_from_configuration_node_mock.assert_called_once_with(
            config_node=config_node, props=mapper.props
        )

        job_xml_nodes = find_nodes_by_tag(action_node, TAG_JOB_XML)
        extract_properties_from_job_xml_nodes_mock.assert_called_once_with(
            input_directory_path="/tmp/input_directory_path", job_xml_nodes=job_xml_nodes, props=mapper.props
        )

        self.assertEqual({"KEY1": "VALUE1", "KEY2": "VALUE2"}, mapper.props.action_node_properties)

    @mock.patch(
        "o2a.mappers.action_mapper.extract_properties_from_job_xml_nodes", return_value={"KEY": "VALUE1"}
    )
    @mock.patch(
        "o2a.mappers.action_mapper.extract_properties_from_configuration_node", return_value={"KEY": "VALUE2"}
    )
    def test_parse_job_xml_and_configuration_minimal_green_path_precedense(
        self, extract_properties_from_configuration_node_mock, extract_properties_from_job_xml_nodes_mock
    ):
        # language=XML
        action_node_str = """
<action name="action">
    <job-xml>AAA.xml</job-xml>
    <configuration>
        <property>
            <name>KEY</name>
            <value>VALUE2</value>
        </property>
    </configuration>
</action>
"""
        action_node = cast(ET.Element, ET.fromstring(action_node_str))
        mapper = self._get_action_mapper(action_node)

        mapper.on_parse_node()

        config_node = find_node_by_tag(action_node, TAG_CONFIGURATION)
        extract_properties_from_configuration_node_mock.assert_called_once_with(
            config_node=config_node, props=mapper.props
        )

        job_xml_nodes = find_nodes_by_tag(action_node, TAG_JOB_XML)
        extract_properties_from_job_xml_nodes_mock.assert_called_once_with(
            input_directory_path="/tmp/input_directory_path", job_xml_nodes=job_xml_nodes, props=mapper.props
        )

        self.assertEqual({"KEY": "VALUE2"}, mapper.props.action_node_properties)

    @mock.patch(
        "o2a.mappers.action_mapper.extract_properties_from_configuration_node",
        return_value={"KEY1": "VALUE1"},
    )
    @mock.patch("o2a.mappers.action_mapper.extract_properties_from_job_xml_nodes", return_value={})
    def test_parse_job_xml_and_configuration_minimal_green_path_without_job_xml(
        self, extract_properties_from_configuration_node_mock, extract_properties_from_job_xml_nodes_mock
    ):
        # language=XML
        action_node_str = """
<action name="action">
    <configuration>
        <property>
            <name>KEY1</name>
            <value>VALUE1</value>
        </property>
    </configuration>
</action>
"""
        action_node = cast(ET.Element, ET.fromstring(action_node_str))
        mapper = self._get_action_mapper(action_node)

        mapper.on_parse_node()

        extract_properties_from_configuration_node_mock.assert_called_once()
        extract_properties_from_job_xml_nodes_mock.assert_called_once()
        self.assertEqual({"KEY1": "VALUE1"}, mapper.props.action_node_properties)

    @staticmethod
    def _get_action_mapper(action_node: ET.Element, props: PropertySet = None):
        if not props:
            props = PropertySet()
        return ActionMapperTestImpl(
            oozie_node=action_node,
            name="test_id",
            dag_name="DAG_NAME",
            props=props,
            input_directory_path="/tmp/input_directory_path",
        )
