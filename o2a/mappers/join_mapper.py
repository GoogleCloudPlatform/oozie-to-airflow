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
"""Maps Oozie join node to Airflow's task"""
from typing import List

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.dummy_mapper import DummyMapper


class JoinMapper(DummyMapper):
    """
    Converts a Join node to an Airflow's task.
    """

    def to_tasks_and_relations(self):
        tasks: List[Task] = [
            Task(task_id=self.name, template_name="dummy.tpl", trigger_rule=TriggerRule.ALL_SUCCESS)
        ]
        relations: List[Relation] = []
        return tasks, relations
