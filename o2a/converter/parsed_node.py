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

from o2a.mappers.base_mapper import BaseMapper


class ParsedNode:
    """Class for parsed Oozie workflow node"""

    def __init__(self, mapper: BaseMapper, tasks=None, relations=None):
        from o2a.converter.task import Task
        from o2a.converter.relation import Relation

        self.mapper = mapper
        self.downstream_names: List[str] = []
        self.is_error: bool = False
        self.is_ok: bool = False
        self.error_xml: Optional[str] = None
        self.tasks: List[Task] = tasks or []
        self.relations: List[Relation] = relations or []

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
            f"ParsedNode(mapper={repr(self.mapper)}, downstream_names={self.downstream_names}, "
            f"is_error={self.is_error}, is_ok={self.is_ok}, error_xml={self.error_xml})"
        )

    @property
    def first_task_id(self) -> str:
        """
        Returns task_id of first task in mapper
        """
        return self.mapper.first_task_id

    @property
    def last_task_id(self) -> str:
        """
        Returns task_id of last task in mapper
        """
        return self.mapper.last_task_id

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

        OK only -> TriggerRule.ONE_SUCCESS
        ERROR only -> TriggerRule.ONE_FAILED
        both -> TriggerRule.DUMMY and a warning log.
        neither -> TriggerRule.DUMMY

        Eventually this if it is both error and ok, then
        we can extend it into two Airflow Operators where one
        is a python branch operator, and make a decision there.
        """
        if self.is_ok and self.is_error:
            logging.warning(f"Task {self.mapper.name} is both an error node and a ok node.")
            self.mapper.trigger_rule = TriggerRule.DUMMY
        elif not self.is_ok and not self.is_error:
            # Sets to dummy, but does not warn user about it.
            self.mapper.trigger_rule = TriggerRule.DUMMY
        elif self.is_ok:
            self.mapper.trigger_rule = TriggerRule.ALL_SUCCESS
        else:
            self.mapper.trigger_rule = TriggerRule.ONE_FAILED
