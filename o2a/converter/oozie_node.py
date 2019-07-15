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

from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.base_mapper import BaseMapper


class OozieNode:
    """Class for parsed Oozie workflow node"""

    def __init__(self, mapper: BaseMapper, tasks=None, relations=None):
        self.mapper = mapper
        self.downstream_names: List[str] = []
        self.error_downstream_name: Optional[str] = None
        self.tasks: List[Task] = tasks or []
        self.relations: List[Relation] = relations or []
        self.error_handler_task: Optional[Task] = None
        self.ok_handler_task: Optional[Task] = None

    @property
    def name(self) -> str:
        """
        Returns name of node
        """
        return self.mapper.name

    def __repr__(self) -> str:
        return (
            f"ParsedActionNode(mapper={self.mapper}, "
            f"downstream_names={self.downstream_names}, "
            f"error_downstream_name={self.error_downstream_name}, "
            f"tasks={self.tasks}, relations={self.relations})"
        )

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return False


class OozieActionNode(OozieNode):
    def __repr__(self) -> str:
        return (
            f"OozieActionNode(mapper={self.mapper}, "
            f"downstream_names={self.downstream_names}, "
            f"error_downstream_name={self.error_downstream_name}, "
            f"tasks={self.tasks}, relations={self.relations})"
        )


class OozieControlNode(OozieNode):
    def __repr__(self) -> str:
        return (
            f"OozieControlNode(mapper={self.mapper}, "
            f"downstream_names={self.downstream_names}, "
            f"error_downstream_name={self.error_downstream_name}, "
            f"tasks={self.tasks}, relations={self.relations})"
        )
