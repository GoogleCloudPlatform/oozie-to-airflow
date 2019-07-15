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
"""Tests for AddWorkflowNotificationTransformer"""
import unittest

from o2a.converter.task import Task
from o2a.converter.task_group import TaskGroup
from o2a.converter.workflow import Workflow
from o2a.o2a_libs.property_utils import PropertySet
from o2a.transformers.add_workflow_notificaton_transformer import (
    AddWorkflowNotificationTransformer,
    PROP_WORKFLOW_NOTIFICATION_URL,
    START_TASK_GROUP_NAME,
    END_SUCCESS_TASK_GROUP_NAME,
    END_FAIL_TASK_GROUP_NAME,
)


class AddWorkflowNotificationTransformerTest(unittest.TestCase):
    def test_should_do_nothing_when_notification_url_not_configured(self):
        # Given
        transformer = AddWorkflowNotificationTransformer()
        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")
        props = PropertySet()
        first_task_group = TaskGroup(
            name="first_task", tasks=[Task(task_id="first_task", template_name="dummy.tpl")]
        )
        workflow.task_groups[first_task_group.name] = first_task_group

        # When
        transformer.process_workflow_after_convert_nodes(workflow, props)

        # Then
        self.assertEqual({first_task_group.name}, workflow.task_groups.keys())

    def test_should_add_start_workflow_node(self):
        # Given
        transformer = AddWorkflowNotificationTransformer()
        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")
        props = PropertySet(job_properties={PROP_WORKFLOW_NOTIFICATION_URL: "http://example.com/workflow"})
        first_task_group = TaskGroup(
            name="first_task", tasks=[Task(task_id="first_task", template_name="dummy.tpl")]
        )
        workflow.task_groups[first_task_group.name] = first_task_group

        # When
        transformer.process_workflow_after_convert_nodes(workflow, props)

        # Then
        self.assertIn(START_TASK_GROUP_NAME, workflow.task_groups.keys())
        self.assertEqual([first_task_group.name], workflow.task_groups["start_workflow"].downstream_names)
        self.assertEqual(
            [
                Task(
                    task_id=START_TASK_GROUP_NAME,
                    template_name="http.tpl",
                    trigger_rule="one_success",
                    template_params={"url": "http://example.com/workflow", "error_code": 0},
                )
            ],
            workflow.task_groups[START_TASK_GROUP_NAME].tasks,
        )

    def test_should_add_end_success_workflow_node(self):
        # Given
        transformer = AddWorkflowNotificationTransformer()
        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")
        props = PropertySet(job_properties={PROP_WORKFLOW_NOTIFICATION_URL: "http://example.com/workflow"})
        first_task_group = TaskGroup(
            name="first_task", tasks=[Task(task_id="first_task", template_name="dummy.tpl")]
        )

        # When
        workflow.task_groups[first_task_group.name] = first_task_group

        # Then
        transformer.process_workflow_after_convert_nodes(workflow, props)
        self.assertIn(END_SUCCESS_TASK_GROUP_NAME, workflow.task_groups.keys())
        self.assertIn(END_SUCCESS_TASK_GROUP_NAME, first_task_group.downstream_names)
        self.assertEqual(
            [
                Task(
                    task_id=END_SUCCESS_TASK_GROUP_NAME,
                    template_name="http.tpl",
                    trigger_rule="one_success",
                    template_params={"url": "http://example.com/workflow", "error_code": 0},
                )
            ],
            workflow.task_groups[END_SUCCESS_TASK_GROUP_NAME].tasks,
        )

    def test_should_add_end_fail_workflow_node(self):
        # Given
        transformer = AddWorkflowNotificationTransformer()
        workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME_B")
        props = PropertySet(job_properties={PROP_WORKFLOW_NOTIFICATION_URL: "http://example.com/workflow"})
        first_task_group = TaskGroup(
            name="first_task", tasks=[Task(task_id="first_task", template_name="dummy.tpl")]
        )
        workflow.task_groups[first_task_group.name] = first_task_group

        # When
        transformer.process_workflow_after_convert_nodes(workflow, props)

        # Then
        self.assertIn(END_FAIL_TASK_GROUP_NAME, workflow.task_groups.keys())
        self.assertEqual(END_FAIL_TASK_GROUP_NAME, first_task_group.error_downstream_name)
        self.assertEqual(
            [
                Task(
                    task_id=END_FAIL_TASK_GROUP_NAME,
                    template_name="http.tpl",
                    trigger_rule="one_success",
                    template_params={"url": "http://example.com/workflow", "error_code": 1},
                )
            ],
            workflow.task_groups[END_FAIL_TASK_GROUP_NAME].tasks,
        )
