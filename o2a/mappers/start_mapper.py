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
"""Maps Oozie start node to Airflow's DAG"""
from typing import Set

from o2a.mappers.base_mapper import BaseMapper


class StartMapper(BaseMapper):
    def required_imports(self) -> Set[str]:
        return set()

    def to_tasks_and_relations(self):
        return [], []

    def on_parse_finish(self, workflow):
        super().on_parse_finish(self)
        del workflow.nodes[self.name]
        workflow.relations -= {
            relation for relation in workflow.relations if relation.from_task_id == self.name
        }
