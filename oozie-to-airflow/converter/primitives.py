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
"""Class for Airflow relation"""
from collections import OrderedDict
from typing import Any, Dict, Optional, Set

from orderedset import OrderedSet

from airflow.utils.trigger_rule import TriggerRule

from utils.template_utils import render_template
from utils.variable_name import convert_to_python_variable


class Relation:
    """Class for Airflow relation"""

    def __init__(self, from_task_id: str, to_task_id: str):
        self.from_task_id: str = from_task_id
        self.to_task_id: str = to_task_id

    @property
    def from_task_variable_name(self):
        return convert_to_python_variable(self.from_task_id)

    @property
    def to_task_variable_name(self):
        return convert_to_python_variable(self.to_task_id)

    def __eq__(self, other):
        if not isinstance(other, Relation):
            # don't attempt to compare against unrelated types
            return NotImplemented

        return self.from_task_id == other.from_task_id and self.to_task_id == other.to_task_id

    def __hash__(self):
        return hash((self.from_task_id, self.to_task_id))

    def __repr__(self):
        items = ("%s = %r" % (k, v) for k, v in self.__dict__.items())
        return "<%s: {%s}>" % (self.__class__.__name__, ", ".join(items))


# This is a container for data, so it does not contain public methods intentionally.
class Workflow:  # pylint: disable=too-few-public-methods
    """Class for Workflow"""

    def __init__(self, input_directory_path, output_directory_path, dag_name=None) -> None:
        self.input_directory_path: str = input_directory_path
        self.output_directory_path: str = output_directory_path
        self.dag_name: Optional[str] = dag_name
        self.relations: Set[Relation] = set()
        # Dictionary is ordered purely for output being somewhat ordered the
        # same as how Oozie workflow was parsed.
        from converter.parsed_node import ParsedNode

        self.nodes: Dict[str, ParsedNode] = OrderedDict()
        # These are the general dependencies required that every operator
        # requires.
        self.dependencies: OrderedSet[str] = OrderedSet(
            [
                "from typing import NamedTuple, Dict",
                "import datetime",
                "from o2a_libs import el_basic_functions, el_wf_functions, ctx",
                "from airflow import models",
                "from airflow.utils.trigger_rule import TriggerRule",
                "from airflow.utils import dates",
            ]
        )

    def __repr__(self) -> str:
        return (
            f"Workflow(dag_name={self.dag_name}, input_directory_path={self.input_directory_path}, "
            f"output_directory_path={self.output_directory_path}, relations={self.relations}, "
            f"nodes={self.nodes.keys()}, dependencies={self.dependencies})"
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False


# This is a container for data, so it does not contain public methods intentionally.
class Task:  # pylint: disable=too-few-public-methods
    """Class for Airflow Task"""

    def __init__(
        self,
        task_id: str,
        template_name: str,
        trigger_rule=TriggerRule.DUMMY,
        template_params: Dict[str, Any] = None,
    ):
        self.task_id: str = task_id
        self.template_name: str = template_name
        self.trigger_rule = trigger_rule
        self.template_params: Dict[str, Any] = template_params or {}

    @property
    def task_variable_name(self):
        return convert_to_python_variable(self.task_id)

    @property
    def rendered_template(self):
        return render_template(
            template_name=self.template_name,
            task_id=self.task_id,
            trigger_rule=self.trigger_rule,
            task_variable_name=self.task_variable_name,
            **self.template_params,
        )

    def __repr__(self) -> str:
        return (
            f'Task(task_id="{self.task_id}", '
            f'template_name="{self.template_name}", '
            f'trigger_rule="{self.trigger_rule}", '
            f"template_params={self.template_params}, "
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False
