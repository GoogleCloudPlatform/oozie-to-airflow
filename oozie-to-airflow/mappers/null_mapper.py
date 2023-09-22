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
from typing import Set
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule
from mappers.base_mapper import BaseMapper


class NullMapper(BaseMapper):
    def __init__(self, oozie_node: Element, task_id: str):
        BaseMapper.__init__(self, oozie_node=oozie_node, task_id=task_id, trigger_rule=TriggerRule.DUMMY)
        self.task_id = task_id

    # pylint: disable=no-self-use
    def convert_to_text(self) -> str:
        return ""

    # pylint: disable=no-self-use
    def convert_to_airflow_op(self) -> None:
        return

    @staticmethod
    def required_imports() -> Set[str]:
        return set()
