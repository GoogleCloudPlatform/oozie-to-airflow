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
from abc import ABC
from copy import deepcopy
from typing import Any, List, Set, Tuple
from xml.etree.ElementTree import Element


from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.o2a_libs.src.o2a_lib.property_utils import PropertySet


class BaseMapper(ABC):
    """The Base Mapper class - parent for all mappers."""

    # pylint: disable = unused-argument
    def __init__(self, oozie_node: Element, name: str, dag_name: str, props: PropertySet, **kwargs: Any):
        self.props = deepcopy(props)
        self.oozie_node = oozie_node
        self.dag_name = dag_name
        self.name = name

    def to_tasks_and_relations(self) -> Tuple[List[Task], List[Relation]]:
        """
        Convert oozie node to tasks and relations.
        """
        raise NotImplementedError("Not Implemented")

    def required_imports(self) -> Set[str]:
        """
        Returns a set of strings that are the import statement that python will
        write to use.

        Ex: returns {'from airflow.operators import bash']}
        """
        raise NotImplementedError("Not Implemented")

    def on_parse_node(self):
        """
        Called when processing a node.
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

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(name={self.name}, "
            f"dag_name={self.dag_name}, "
            f"oozie_node={self.oozie_node}, "
            f"props={self.props}) "
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False
