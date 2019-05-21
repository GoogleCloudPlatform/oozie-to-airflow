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
"""Maps Oozie end node to Airflow's DAG"""
from o2a.mappers.decision_mapper import DecisionMapper
from o2a.mappers.dummy_mapper import DummyMapper


class EndMapper(DummyMapper):
    def on_parse_finish(self, workflow):
        super().on_parse_finish(self)
        decision_node_ids = {
            node.last_task_id for node in workflow.nodes.values() if isinstance(node.mapper, DecisionMapper)
        }
        upstream_task_ids = {
            relation.from_task_id for relation in workflow.relations if relation.to_task_id == self.name
        }

        if not decision_node_ids.intersection(upstream_task_ids):
            del workflow.nodes[self.name]

        workflow.relations -= {
            relation
            for relation in workflow.relations
            if relation.to_task_id == self.name and relation.from_task_id not in decision_node_ids
        }
