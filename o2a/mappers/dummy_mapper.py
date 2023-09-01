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
"""Dummy Mapper that is used as temporary solution while we are implementing the real mappers.
"""
from typing import List, Optional, Set
from xml.etree.ElementTree import Element

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.base_mapper import BaseMapper
from o2a.o2a_libs.src.o2a_lib.property_utils import PropertySet


class DummyMapper(BaseMapper):
    """Dummy mapper. It always returns one tasks that does nothing.

    It's used in place of not-yet-implemented mappers and as the base class for control nodes.
    """

    def __init__(
        self, oozie_node: Element, name: str, dag_name: str, props: Optional[PropertySet] = None, **kwargs
    ):
        super().__init__(
            oozie_node=oozie_node,
            name=name,
            dag_name=dag_name,
            props=props or PropertySet(job_properties={}, config={}),
            **kwargs,
        )

    def to_tasks_and_relations(self):
        tasks: List[Task] = [Task(task_id=self.name, template_name="dummy.tpl")]
        relations: List[Relation] = []
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {"from airflow.operators import empty"}
