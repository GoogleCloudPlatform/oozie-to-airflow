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
Adds node notifications
"""
from typing import List

from o2a.converter.task import Task
from o2a.converter.task_group import (
    ActionTaskGroup,
    ControlTaskGroup,
    StatusNotificationTaskGroup,
    TaskGroup,
    TransitionNotificationTaskGroup,
)
from o2a.converter.workflow import Workflow
from o2a.o2a_libs.src.o2a_lib.property_utils import PropertySet
from o2a.transformers.add_workflow_notificaton_transformer import NOTIFICATION_TASK_GROUP_NAMES
from o2a.transformers.base_transformer import BaseWorkflowTransformer

PROP_KEY_NODE_NOTIFICATION_URL = "oozie.wf.action.notification.url"

NODE_STATUS_SUFFIX = "_status_notification"
NODE_TRANSITION_SUFFIX = "_transition_notification"


# pylint: disable=unidiomatic-typecheck
# pylint: disable=missing-docstring
class AddNodeNotificationTransformer(BaseWorkflowTransformer):
    """
    Add node notification task group
    """

    def __init__(self):
        self.notification_url = None

    def process_workflow_after_convert_nodes(self, workflow: Workflow, props: PropertySet):
        self.notification_url = props.job_properties.get(PROP_KEY_NODE_NOTIFICATION_URL, None)
        if not self.notification_url:
            return
        self.add_all_notifications(workflow)

    def add_all_notifications(self, workflow: Workflow):
        task_groups_snapshot = workflow.task_groups.copy().values()
        for task_group in task_groups_snapshot:
            upstreams: List[TaskGroup] = workflow.find_upstream_task_group(task_group)
            if not upstreams:
                # TG with no upstream
                if isinstance(task_group, ActionTaskGroup):
                    self._add_status(task_group, workflow)
            elif len(upstreams) == 1 and upstreams[0].name in NOTIFICATION_TASK_GROUP_NAMES:
                # TG with only the start workflow upstream
                if isinstance(task_group, ActionTaskGroup):
                    self._add_status(task_group, workflow, upstreams[0])
            else:
                # There is at least 1 upstream TG
                pass

        for task_group in task_groups_snapshot:
            # Now we go downstream only
            # For correct execution (of e.g. decision) we need to make a snapshot (copy) of downstream names
            downstream_names_snapshot = task_group.downstream_names.copy()
            for downstream_name in downstream_names_snapshot:
                downstream: TaskGroup = workflow.task_groups[downstream_name]
                if isinstance(task_group, ActionTaskGroup) and isinstance(downstream, ActionTaskGroup):
                    # action -> action = T: S:
                    self._add_transition_and_status(downstream_name, task_group, workflow)
                if isinstance(task_group, ActionTaskGroup) and isinstance(downstream, ControlTaskGroup):
                    # action -> control = T:
                    self._add_transition(downstream_name, task_group, workflow)
                if isinstance(task_group, ControlTaskGroup) and isinstance(downstream, ActionTaskGroup):
                    # control -> action = T: S:
                    self._add_transition_and_status(downstream_name, task_group, workflow)
                if isinstance(task_group, ControlTaskGroup) and isinstance(downstream, ControlTaskGroup):
                    # control -> control = T:
                    self._add_transition(downstream_name, task_group, workflow)

    def _add_status(self, task_group, workflow, upstream=None):
        status_notification = self._create_status_notification_task_group(
            self.notification_url, f"{task_group.name}{NODE_STATUS_SUFFIX}", task_group.name, "S:RUNNING"
        )
        status_notification.downstream_names = [task_group.name]
        workflow.task_groups[status_notification.name] = status_notification
        if upstream:
            upstream.downstream_names.remove(task_group.name)
            upstream.downstream_names.append(status_notification.name)

    def _add_transition(self, downstream_name, task_group, workflow):
        transition_notification = self._create_transition_notification_task_group(
            self.notification_url,
            f"{task_group.name}{NODE_TRANSITION_SUFFIX}_T_{downstream_name}",
            task_group.name,
            f"T:{downstream_name}",
        )
        transition_notification.downstream_names = [downstream_name]
        task_group.downstream_names.remove(downstream_name)
        task_group.downstream_names.append(transition_notification.name)
        workflow.task_groups[transition_notification.name] = transition_notification

    def _add_transition_and_status(self, downstream_name, task_group, workflow):
        transition_notification = self._create_transition_notification_task_group(
            self.notification_url,
            f"{task_group.name}{NODE_TRANSITION_SUFFIX}_T_{downstream_name}",
            task_group.name,
            f"T:{downstream_name}",
        )
        status_notification = self._create_status_notification_task_group(
            self.notification_url, f"{downstream_name}{NODE_STATUS_SUFFIX}", downstream_name, "S:RUNNING"
        )
        workflow.task_groups[transition_notification.name] = transition_notification
        workflow.task_groups[status_notification.name] = status_notification
        task_group.downstream_names.remove(downstream_name)
        task_group.downstream_names.append(transition_notification.name)
        transition_notification.downstream_names = [status_notification.name]
        status_notification.downstream_names = [downstream_name]

    @staticmethod
    def _create_status_notification_task_group(url_template, task_name, node_name, status):
        url = (
            url_template.replace("$jobId", "{{ dag.dag_id }}")
            .replace("$nodeName", node_name)
            .replace("$status", status)
        )
        notification_tgrp = StatusNotificationTaskGroup(
            name=task_name,
            tasks=[
                Task(
                    task_id=f"{task_name}_STATUS",
                    template_name="http.tpl",
                    template_params=dict(url=url, error_code=0),
                )
            ],
        )
        return notification_tgrp

    @staticmethod
    def _create_transition_notification_task_group(url_template, task_name, node_name, status):
        url = (
            url_template.replace("$jobId", "{{ dag.dag_id }}")
            .replace("$nodeName", node_name)
            .replace("$status", status)
        )
        notification_tgrp = TransitionNotificationTaskGroup(
            name=task_name,
            tasks=[
                Task(
                    task_id=f"{task_name}_TRANSITION",
                    template_name="http.tpl",
                    template_params=dict(url=url, error_code=0),
                )
            ],
        )
        return notification_tgrp
