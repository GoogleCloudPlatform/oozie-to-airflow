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
"""Null Mapper - used when there is a need to insert no-op node"""
from typing import Set, Tuple, List

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.base_mapper import BaseMapper


class NullMapper(BaseMapper):

    # pylint: disable=no-self-use
    def to_tasks_and_relations(self) -> Tuple[List[Task], List[Relation]]:
        return [], []

    # pylint: disable=no-self-use
    def convert_to_airflow_op(self) -> None:
        return

    def required_imports(self) -> Set[str]:
        return set()
