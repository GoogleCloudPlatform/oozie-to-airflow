# Copyright 2018 Google LLC
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
from typing import Set

from airflow.utils.trigger_rule import TriggerRule
from mappers.base_mapper import BaseMapper


# noinspection PyAbstractClass
class NullMapper(BaseMapper):
    def __init__(self, oozie_node: str = None, task_id: str = None):
        BaseMapper.__init__(self, oozie_node=oozie_node, task_id=task_id, trigger_rule=TriggerRule.DUMMY)
        self.task_id = task_id

    def convert_to_text(self) -> str:
        return ""

    @staticmethod
    def required_imports() -> Set[str]:
        return set()
