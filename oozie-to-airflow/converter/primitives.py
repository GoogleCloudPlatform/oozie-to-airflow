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
from typing import Set, Optional, Dict, NamedTuple, Any

# Pylint and flake8 does not understand forward references
# https://www.python.org/dev/peps/pep-0484/#forward-references
from converter import parsed_node  # noqa: F401 pylint: disable=unused-import
from utils.template_utils import render_template


class Relation(NamedTuple):
    """Class for Airflow relation"""

    from_task_id: str
    to_task_id: str


# This is a container for data, so it does not contain public methods intentionally.
class Workflow:  # pylint: disable=too-few-public-methods
    """Class for Workflow"""

    dag_name: Optional[str]
    input_directory_path: str
    output_directory_path: str
    relations: Set[Relation]
    nodes: Dict[str, "parsed_node.ParsedNode"]
    dependencies: Set[str]  # TODO: Check is set likely maintain insertion order (Python 3.6 ?)

    def __init__(self, input_directory_path, output_directory_path, dag_name=None) -> None:
        self.input_directory_path = input_directory_path
        self.output_directory_path = output_directory_path
        self.dag_name = dag_name
        self.relations = set()
        # Dictionary is ordered purely for output being somewhat ordered the
        # same as how Oozie workflow was parsed.
        self.nodes = OrderedDict()
        # These are the general dependencies required that every operator
        # requires.
        self.dependencies = {
            "import datetime",
            "from airflow import models",
            "from airflow.utils.trigger_rule import TriggerRule",
            "from airflow.utils import dates",
            "from o2a_libs.el_basic_functions import * ",
            "from o2a_libs.el_wf_functions import * ",
            "from airflow.utils import dates",
        }

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

    task_id: str
    template_name: str
    template_params: Dict[str, Any]

    def __init__(self, task_id, template_name, template_params=None):
        self.task_id = task_id
        self.template_name = template_name
        self.template_params = template_params or {}

    @property
    def rendered_template(self):
        return render_template(template_name=self.template_name, task_id=self.task_id, **self.template_params)

    def __repr__(self) -> str:
        return (
            f'Task(task_id="{self.task_id}", template_name="{self.template_name}", '
            f"template_params={self.template_params})"
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False
