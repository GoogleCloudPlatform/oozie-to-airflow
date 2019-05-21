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
"""Base mapper - it is a base class for all mappers actions, and logic alike"""
from typing import Set, Tuple, List
from xml.etree.ElementTree import Element

import airflow.utils.trigger_rule

from o2a.converter.relation import Relation
from o2a.converter.task import Task


class BaseMapper:
    """The Base Mapper class - parent for all mappers."""

    # pylint: disable=unused-argument
    def __init__(
        self,
        oozie_node: Element,
        name: str,
        trigger_rule=airflow.utils.trigger_rule.TriggerRule.ALL_SUCCESS,
        params=None,
        **kwargs,
    ):
        if params is None:
            params = {}
        self.params = params
        self.oozie_node = oozie_node
        self.name = name
        self.trigger_rule = trigger_rule

    def to_tasks_and_relations(self) -> Tuple[List[Task], List[Relation]]:
        """
        Convert oozie node to tasks and relations.
        """
        raise NotImplementedError("Not Implemented")

    def required_imports(self) -> Set[str]:
        """
        Returns a set of strings that are the import statement that python will
        write to use.

        Ex: returns {'from airflow.operators import bash_operator']}
        """
        raise NotImplementedError("Not Implemented")

    @property
    def first_task_id(self) -> str:
        """
        Returns task_id of first task in mapper
        """
        return self.name

    @property
    def last_task_id(self) -> str:
        """
        Returns task_id of last task in mapper
        """
        return self.name

    def on_parse_node(self):
        """
        Called when processing a node.
        """

    def on_parse_finish(self, workflow):
        """
        Called when processing of all nodes is finished.

        This is a good time to copy additional files, or to perform additional operations on the workflow.
        """

    # pylint: disable=unused-argument,no-self-use
    def copy_extra_assets(self, input_directory_path: str, output_directory_path: str) -> None:
        """
        Copies extra assets required by the generated DAG - such as script files, jars etc.

        :param input_directory_path: oozie workflow application directory
        :param output_directory_path: output directory for the generated DAG and assets
        :return: None
        """
        return None
