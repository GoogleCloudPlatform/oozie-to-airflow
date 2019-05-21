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
from typing import Set

from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper


class DummyMapper(ActionMapper):
    def to_tasks_and_relations(self):
        tasks = [Task(task_id=self.name, trigger_rule=self.trigger_rule, template_name="dummy.tpl")]
        relations = []
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {"from airflow.operators import dummy_operator"}
