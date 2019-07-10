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
"""Class for Airflow's task group"""
# noinspection PyPackageRequirements
from typing import List, Optional

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.exceptions import O2AException
from o2a.converter.task import Task
from o2a.converter.relation import Relation


class TaskGroup:
    """Airflow's tasks group

    It is created as a result of converting the ParsedActionNode object. It contains all of its
    information except the mapper.

    Additionally, it contains an Airflow tasks, relations and python imports statements.
    """

    def __init__(
        self,
        name,
        tasks,
        relations=None,
        downstream_names=None,
        error_downstream_name=None,
        dependencies=None,
    ):
        self.name = name
        self.tasks: List[Task] = tasks or []
        self.relations: List[Relation] = relations or []
        self.downstream_names: List[str] = downstream_names or []
        self.error_downstream_name: Optional[str] = error_downstream_name
        self.dependencies: List[str] = dependencies or []
        self.error_handler_task: Optional[Task] = None
        self.ok_handler_task: Optional[Task] = None

    @property
    def first_task_id(self) -> str:
        """
        Returns task_id of first task in group
        """
        return self.tasks[0].task_id

    @property
    def last_task_id_of_ok_flow(self) -> str:
        """
        Returns task_id of last task in group
        """
        if self.ok_handler_task:
            return self.ok_handler_task.task_id
        return self.tasks[-1].task_id

    @property
    def last_task_id_of_error_flow(self) -> str:
        """
        Returns task_id of last task in group
        """
        if not self.error_handler_task:
            raise O2AException(
                "Unsupported state. The Error handler task ID was requested before it was created."
            )
        return self.error_handler_task.task_id

    def add_state_handler_if_needed(self):
        """
        Add additional tasks and relations to handle error and ok flow.

        If the error path is specified, additional relations and task are added to handle
        the error state.
        If the error path and the ok path is specified, additional relations and task are added
        to handle the ok path and the error path.
        If the error path and the ok path is not-specified, no action is performed.
        """
        if not self.error_downstream_name:
            return
        error_handler_task_id = self.name + "_error"
        error_handler = Task(
            task_id=error_handler_task_id, template_name="dummy.tpl", trigger_rule=TriggerRule.ONE_FAILED
        )
        self.error_handler_task = error_handler
        new_relations = (
            Relation(from_task_id=t.task_id, to_task_id=error_handler_task_id, is_error=True)
            for t in self.tasks
        )
        self.relations.extend(new_relations)

        if not self.downstream_names:
            return
        ok_handler_task_id = self.name + "_ok"
        ok_handler = Task(
            task_id=ok_handler_task_id, template_name="dummy.tpl", trigger_rule=TriggerRule.ONE_SUCCESS
        )
        self.ok_handler_task = ok_handler
        self.relations.append(Relation(from_task_id=self.tasks[-1].task_id, to_task_id=ok_handler_task_id))

    @property
    def all_tasks(self):
        all_tasks = [*self.tasks]
        if self.error_handler_task:
            all_tasks.append(self.error_handler_task)
        if self.ok_handler_task:
            all_tasks.append(self.ok_handler_task)
        return all_tasks

    def __repr__(self) -> str:
        return (
            f"TaskGroup(name={self.name}, "
            f"downstream_names={self.downstream_names}, "
            f"error_downstream_name={self.error_downstream_name}, "
            f"tasks={self.tasks}, relations={self.relations})"
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False


class ActionTaskGroup(TaskGroup):
    pass


class ControlTaskGroup(TaskGroup):
    pass


class NotificationTaskGroup(TaskGroup):
    pass


class StatusNotificationTaskGroup(NotificationTaskGroup):
    pass


class TransitionNotificationTaskGroup(NotificationTaskGroup):
    pass
