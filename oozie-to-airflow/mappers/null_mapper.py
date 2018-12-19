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

from airflow.utils.trigger_rule import TriggerRule
from mappers.base_mapper import BaseMapper


class NullMapper(BaseMapper):
    def __init__(self, task_id):
        BaseMapper.__init__(self, None, task_id, TriggerRule.DUMMY)
        self.task_id = task_id

    def convert_to_text(self):
        return ''

    @staticmethod
    def required_imports():
        return []
