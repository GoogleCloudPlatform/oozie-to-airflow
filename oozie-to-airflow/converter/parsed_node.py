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
"""Class for parsed Oozie workflow node"""
# noinspection PyPackageRequirements
from typing import List, Optional
import logging

from airflow.utils.trigger_rule import TriggerRule

from converter import primitives

# Pylint and flake8 does not understand forward references
# https://www.python.org/dev/peps/pep-0484/#forward-references
from mappers import base_mapper  # noqa: F401 pylint: disable=unused-import


class ParsedNode:
    """Class for parsed Oozie workflow node"""

    def __init__(self, mapper: "base_mapper.BaseMapper"):
        self.mapper = mapper
        self.downstream_names: List[str] = []
        self.is_error: bool = False
        self.is_ok: bool = False
        self.error_xml: Optional[str] = None
        self.tasks: List["primitives.Task"] = []
        self.relations: List["primitives.Relation"] = []
        self.error_task: Optional["primitives.Task"] = None
        self.error_relation: Optional["primitives.Relation"] = None

    def add_downstream_node_name(self, node_name: str):
        """
        Adds a single downstream name string to the list `downstream_names`.
        :param node_name: The name to append to the list
        """
        self.downstream_names.append(node_name)

    def set_error_node_name(self, error_name: str):
        """
        Sets the error_xml class variable to the supplied `error_name`
        :param error_name: The downstream error node, Oozie nodes can only have
            one error downstream.
        """
        self.error_xml = error_name

    def get_downstreams(self) -> List[str]:
        return self.downstream_names

    def get_error_downstream_name(self) -> Optional[str]:
        return self.error_xml

    def __repr__(self) -> str:
        return (
            f"ParsedNode(mapper={self.mapper}, downstream_names={self.downstream_names}, "
            f"is_error={self.is_error}, is_ok={self.is_ok}, error_xml={self.error_xml}, tasks={self.tasks}, "
            f"relations={self.relations})"
        )

    @property
    def first_task_id_in_correct_flow(self) -> str:
        """
        Returns task_id of first task in correct flow
        """
        return self.tasks[0].task_id

    @property
    def first_task_id_in_error_flow(self) -> str:
        """
        Returns task_id of first task in error
        """
        if self.error_task:
            return self.error_task.task_id
        return self.tasks[0].task_id

    def get_all_tasks(self):
        if self.error_task:
            return [*self.tasks, self.error_task]
        return [*self.tasks]

    def get_all_relations(self):
        if self.error_relation:
            return [*self.relations, self.error_relation]
        return [*self.relations]

    @property
    def last_task_id(self) -> str:
        """
        Returns task_id of last task in mapper
        """
        return self.tasks[-1].task_id

    def set_is_error(self, is_error: bool):
        """
        A bit that switches when the node is the error downstream of any other
        node.

        :param is_error: Boolean of is_error or not.
        """
        self.is_error = is_error

    def set_is_ok(self, is_ok: bool):
        """
        A bit that switches when the node is the ok downstream of any other
        node.

        :param is_ok: Boolean of is_ok or not.
        """
        self.is_ok = is_ok

    def update_trigger_rule(self):
        """
        The trigger rule gets determined by if it is error or ok.

        OK only
            the trigger rules for the first task equals to TriggerRule.ALL_SUCCESS

        ERROR only
            the trigger rules for the first task equals to TriggerRule.ONE_FAILED

        neither
            no changes

        both
            the trigger rules for the first task equals to TriggerRule.ALL_SUCCESS
            An additional task is also created on the second position with
            TriggerRule equals to TriggerRule.ONE_FAILED

        """
        if self.is_ok and self.is_error:
            error_task_name = self.mapper.name + "_error_handle"
            self.error_task = primitives.Task(
                task_id=error_task_name, template_name="dummy.tpl", trigger_rule=TriggerRule.ONE_FAILED
            )
            self.error_relation = primitives.Relation(
                to_task_id=self.tasks[0].task_id, from_task_id=error_task_name
            )
            for task in self.tasks:
                task.trigger_rule = TriggerRule.ALL_SUCCESS
        elif not self.is_ok and not self.is_error:
            logging.warning(f"The Node {self.mapper.name} is not used in the correct and error flow.")
            for task in self.tasks:
                task.trigger_rule = TriggerRule.DUMMY
        elif self.is_ok:
            for task in self.tasks:
                task.trigger_rule = TriggerRule.ALL_SUCCESS
        else:
            self.tasks[0].trigger_rule = TriggerRule.ONE_FAILED
            for task in self.tasks[1:]:
                task.trigger_rule = TriggerRule.ALL_SUCCESS
