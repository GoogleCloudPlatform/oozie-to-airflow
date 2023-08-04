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
"""
Remove Start Transformer
"""
from o2a.converter.task import Task
from o2a.converter.task_group import TaskGroup
from o2a.converter.workflow import Workflow
from o2a.o2a_libs.src.o2a_lib.property_utils import PropertySet
from o2a.transformers.base_transformer import BaseWorkflowTransformer

PROP_WORKFLOW_NOTIFICATION_URL = "oozie.wf.workflow.notification.url"

START_TASK_GROUP_NAME = "start_workflow"
END_SUCCESS_TASK_GROUP_NAME = "end_success_workflow"
END_FAIL_TASK_GROUP_NAME = "end_fail_workflow"

NOTIFICATION_TASK_GROUP_NAMES = {START_TASK_GROUP_NAME, END_SUCCESS_TASK_GROUP_NAME, END_FAIL_TASK_GROUP_NAME}


# pylint: disable=too-few-public-methods
class AddWorkflowNotificationTransformer(BaseWorkflowTransformer):
    """
    Add workflow notification task group
    """

    def process_workflow_after_convert_nodes(self, workflow: Workflow, props: PropertySet):
        notification_url = props.job_properties.get(PROP_WORKFLOW_NOTIFICATION_URL, None)
        if not notification_url:
            return

        self._add_start_task_group(notification_url, workflow)
        self._add_end_success_task_group(notification_url, workflow)
        self._add_end_fail_task_group(notification_url, workflow)

    @classmethod
    def _add_start_task_group(cls, notification_url, workflow):
        start_workflow_task_group = cls._create_notification_task_group(
            url_template=notification_url, start_task_name=START_TASK_GROUP_NAME, status="RUNNING"
        )
        start_workflow_task_group.downstream_names = [
            task_group.name
            for task_group in workflow.get_task_group_without_upstream()
            if task_group.name not in NOTIFICATION_TASK_GROUP_NAMES
        ]
        workflow.task_groups[start_workflow_task_group.name] = start_workflow_task_group

    @classmethod
    def _add_end_success_task_group(cls, notification_url, workflow):
        end_success_workflow_task_group = cls._create_notification_task_group(
            url_template=notification_url, start_task_name=END_SUCCESS_TASK_GROUP_NAME, status="SUCCEEDED"
        )
        for task_group in workflow.get_task_group_without_ok_downstream():
            if task_group.name in NOTIFICATION_TASK_GROUP_NAMES:
                continue
            task_group.downstream_names.append(end_success_workflow_task_group.name)
        workflow.task_groups[end_success_workflow_task_group.name] = end_success_workflow_task_group

    @classmethod
    def _add_end_fail_task_group(cls, notification_url, workflow):
        end_fail_workflow_task_group = cls._create_notification_task_group(
            url_template=notification_url,
            start_task_name=END_FAIL_TASK_GROUP_NAME,
            status="FAILED",
            error_code=1,
        )
        for task_group in workflow.get_task_group_without_error_downstream():
            if task_group.name in NOTIFICATION_TASK_GROUP_NAMES:
                continue
            task_group.error_downstream_name = end_fail_workflow_task_group.name
        workflow.task_groups[end_fail_workflow_task_group.name] = end_fail_workflow_task_group

    @staticmethod
    def _create_notification_task_group(url_template, start_task_name, status, error_code=0):
        url = (
            url_template.replace("$parentId", "").replace("$jobId", "{{ run_id }}").replace("$status", status)
        )
        start_workflow_node = TaskGroup(
            name=start_task_name,
            tasks=[
                Task(
                    task_id=start_task_name,
                    template_name="http.tpl",
                    template_params=dict(url=url, error_code=error_code),
                )
            ],
        )
        return start_workflow_node
