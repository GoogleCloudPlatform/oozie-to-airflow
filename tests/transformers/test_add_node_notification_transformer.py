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
"""Tests for AddNodeNotificationTransformer"""
import unittest

from o2a.converter.task import Task
from o2a.converter.task_group import ActionTaskGroup, ControlTaskGroup
from o2a.converter.workflow import Workflow
from o2a.o2a_libs.property_utils import PropertySet
from o2a.transformers.add_node_notificaton_transformer import (
    AddNodeNotificationTransformer,
    NODE_STATUS_SUFFIX,
    NODE_TRANSITION_SUFFIX,
    PROP_KEY_NODE_NOTIFICATION_URL,
)

NODE_NOTIFICATION_URL_TPL = "http://example.com/action?job-id=$jobId&node-name=$nodeName&status=$status"


def new_task(task_id):
    return Task(task_id=task_id, template_name="dummy.tpl")


class AddNodeNotificationTransformerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.transformer = AddNodeNotificationTransformer()
        self.workflow = Workflow(input_directory_path="", output_directory_path="", dag_name="DAG_NAME")
        self.action_task_group = ActionTaskGroup(name="action_task_group", tasks=[new_task("action_task")])
        self.workflow.task_groups[self.action_task_group.name] = self.action_task_group
        self.props = PropertySet(job_properties={PROP_KEY_NODE_NOTIFICATION_URL: NODE_NOTIFICATION_URL_TPL})

    def test_should_do_nothing_when_notification_url_not_configured(self):
        """
        Input:

        ACTION

        Expected output:

        ACTION
        """
        # Given
        props = PropertySet()

        # When
        self.transformer.process_workflow_after_convert_nodes(self.workflow, props)

        # Then
        self.assertEqual({self.action_task_group.name}, self.workflow.task_groups.keys())

    def test_should_add_status_notification_to_single_action_task_group(self):
        """
        Input:

        ACTION

        Expected output:

        STATUS
           |
        ACTION
        """
        # When
        self.transformer.process_workflow_after_convert_nodes(self.workflow, self.props)

        # Then
        exp_status_notification_name = f"{self.action_task_group.name}{NODE_STATUS_SUFFIX}"
        self.assertEqual(2, len(self.workflow.task_groups))
        self.assertEqual(
            {exp_status_notification_name, self.action_task_group.name}, self.workflow.task_groups.keys()
        )

    def test_should_add_transition_and_status_between_two_action_task_groups(self):
        """
        Input:

        ACTION
           |
        ACTION

        Expected output:

        STATUS
           |
        ACTION
           |
        TRANSITION
           |
        STATUS
           |
        ACTION
        """
        # Given
        second_action_task_group = ActionTaskGroup(
            name="second_action_task_group", tasks=[new_task("control_task")]
        )
        self.workflow.task_groups[second_action_task_group.name] = second_action_task_group
        self.action_task_group.downstream_names.append(second_action_task_group.name)
        exp_first_action_status_notification_name = f"{self.action_task_group.name}{NODE_STATUS_SUFFIX}"
        exp_second_action_status_notification_name = f"{second_action_task_group.name}{NODE_STATUS_SUFFIX}"
        exp_second_action_transition_notification_name = (
            f"{self.action_task_group.name}{NODE_TRANSITION_SUFFIX}_T_{second_action_task_group.name}"
        )

        # When
        self.transformer.process_workflow_after_convert_nodes(self.workflow, self.props)

        # Then
        self.assertEqual(5, len(self.workflow.task_groups))
        self.assertEqual(
            {
                exp_first_action_status_notification_name,
                self.action_task_group.name,
                exp_second_action_transition_notification_name,
                exp_second_action_status_notification_name,
                second_action_task_group.name,
            },
            self.workflow.task_groups.keys(),
        )

    def test_should_add_transition_between_action_and_control_task_groups(self):
        """
        Input:

        ACTION
           |
        CONTROL

        Expected output:

        STATUS
           |
        ACTION
           |
        TRANSITION
           |
        CONTROL
        """
        # Given
        control_task_group = ControlTaskGroup(name="control_task_group", tasks=[new_task("control_task")])
        self.workflow.task_groups[control_task_group.name] = control_task_group
        self.action_task_group.downstream_names.append(control_task_group.name)
        exp_action_status_notification_name = f"{self.action_task_group.name}{NODE_STATUS_SUFFIX}"
        exp_control_transition_notification_name = (
            f"{self.action_task_group.name}{NODE_TRANSITION_SUFFIX}_T_{control_task_group.name}"
        )

        # When
        self.transformer.process_workflow_after_convert_nodes(self.workflow, self.props)

        # Then
        self.assertEqual(4, len(self.workflow.task_groups))
        self.assertEqual(
            {
                exp_action_status_notification_name,
                self.action_task_group.name,
                exp_control_transition_notification_name,
                control_task_group.name,
            },
            self.workflow.task_groups.keys(),
        )

    def test_should_add_transition_between_two_control_task_groups(self):
        """
        Input:

        CONTROL
           |
        CONTROL

        Expected output:

        CONTROL
           |
        TRANSITION
           |
        CONTROL
        """
        # Given
        self.workflow.task_groups.clear()  # Reset workflow
        first_control_task_group = ControlTaskGroup(
            name="first_control_task_group", tasks=[new_task("first_control_task")]
        )
        second_control_task_group = ControlTaskGroup(
            name="second_control_task_group", tasks=[new_task("second_control_task")]
        )
        self.workflow.task_groups[first_control_task_group.name] = first_control_task_group
        self.workflow.task_groups[second_control_task_group.name] = second_control_task_group
        first_control_task_group.downstream_names = [second_control_task_group.name]
        exp_second_control_transition_notification_name = (
            f"{first_control_task_group.name}{NODE_TRANSITION_SUFFIX}_T_{second_control_task_group.name}"
        )

        # When
        self.transformer.process_workflow_after_convert_nodes(self.workflow, self.props)

        # Then
        self.assertEqual(3, len(self.workflow.task_groups))
        self.assertEqual(
            {
                first_control_task_group.name,
                exp_second_control_transition_notification_name,
                second_control_task_group.name,
            },
            self.workflow.task_groups.keys(),
        )

    def test_should_add_transition_and_status_between_control_and_action_task_groups(self):
        """
        Input:

        CONTROL
           |
        ACTION

        Expected output:

        CONTROL
           |
        TRANSITION
           |
        STATUS
           |
        ACTION
        """
        # Given
        control_task_group = ControlTaskGroup(name="control_task_group", tasks=[new_task("control_task")])
        self.workflow.task_groups[control_task_group.name] = control_task_group
        control_task_group.downstream_names = [self.action_task_group.name]
        exp_transition_notification_name = (
            f"{control_task_group.name}{NODE_TRANSITION_SUFFIX}_T_{self.action_task_group.name}"
        )
        exp_status_notification_name = f"{self.action_task_group.name}{NODE_STATUS_SUFFIX}"

        # When
        self.transformer.process_workflow_after_convert_nodes(self.workflow, self.props)

        # Then
        self.assertEqual(4, len(self.workflow.task_groups))
        self.assertEqual(
            {
                control_task_group.name,
                exp_transition_notification_name,
                exp_status_notification_name,
                self.action_task_group.name,
            },
            self.workflow.task_groups.keys(),
        )

    def test_should_handle_fork_type_case(self):
        """
        Input:

                 ACTION
                 |    |
        CONTROL <     > ACTION

        Expected output:

                 STATUS
                    |
                 ACTION
                 |    |
        TRANSITION    TRANSITION
                 |    |
                 |    STATUS
                 |    |
        CONTROL <     > ACTION
        """
        # Given
        control_task_group = ControlTaskGroup(
            name="control_task_group", tasks=[Task(task_id="control_task", template_name="dummy.tpl")]
        )
        self.workflow.task_groups[control_task_group.name] = control_task_group
        self.action_task_group.downstream_names.append(control_task_group.name)

        second_action_task_group = ActionTaskGroup(
            name="second_action_task_group", tasks=[new_task("second_action_task")]
        )
        self.workflow.task_groups[second_action_task_group.name] = second_action_task_group
        self.action_task_group.downstream_names.append(second_action_task_group.name)

        exp_first_action_status_notification_name = f"{self.action_task_group.name}{NODE_STATUS_SUFFIX}"
        exp_second_action_status_notification_name = f"{second_action_task_group.name}{NODE_STATUS_SUFFIX}"
        exp_action_control_transition_notification_name = (
            f"{self.action_task_group.name}{NODE_TRANSITION_SUFFIX}_T_{control_task_group.name}"
        )
        exp_action_action_transition_notification_name = (
            f"{self.action_task_group.name}{NODE_TRANSITION_SUFFIX}_T_{second_action_task_group.name}"
        )

        # When
        self.transformer.process_workflow_after_convert_nodes(self.workflow, self.props)

        # Then
        self.assertEqual(7, len(self.workflow.task_groups))
        self.assertEqual(
            {
                exp_first_action_status_notification_name,
                self.action_task_group.name,
                exp_action_control_transition_notification_name,
                control_task_group.name,
                exp_action_action_transition_notification_name,
                exp_second_action_status_notification_name,
                second_action_task_group.name,
            },
            self.workflow.task_groups.keys(),
        )

    def test_should_handle_join_type_case(self):
        """
        Input:

        CONTROL >      < ACTION
                |     |
                CONTROL


        Expected output:

                         STATUS
                         |
        CONTROL >      < ACTION
                |     |
        TRANSITION    TRANSITION
                |     |
                CONTROL
        """
        # Given
        control_task_group = ControlTaskGroup(
            name="control_task_group", tasks=[Task(task_id="control_task", template_name="dummy.tpl")]
        )
        join_task_group = ControlTaskGroup(
            name="join_task_group", tasks=[Task(task_id="join_task", template_name="dummy.tpl")]
        )
        self.workflow.task_groups[control_task_group.name] = control_task_group
        self.workflow.task_groups[join_task_group.name] = join_task_group
        self.action_task_group.downstream_names = [join_task_group.name]
        control_task_group.downstream_names = [join_task_group.name]

        exp_action_status_notification_name = f"{self.action_task_group.name}{NODE_STATUS_SUFFIX}"
        exp_action_control_transition_notification_name = (
            f"{self.action_task_group.name}{NODE_TRANSITION_SUFFIX}_T_{join_task_group.name}"
        )
        exp_control_control_transition_notification_name = (
            f"{control_task_group.name}{NODE_TRANSITION_SUFFIX}_T_{join_task_group.name}"
        )

        # When
        self.transformer.process_workflow_after_convert_nodes(self.workflow, self.props)

        # Then
        self.assertEqual(6, len(self.workflow.task_groups))
        self.assertEqual(
            {
                exp_action_status_notification_name,
                control_task_group.name,
                self.action_task_group.name,
                exp_action_control_transition_notification_name,
                exp_control_control_transition_notification_name,
                join_task_group.name,
            },
            self.workflow.task_groups.keys(),
        )
