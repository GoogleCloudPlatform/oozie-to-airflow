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


from mappers.action_mapper import ActionMapper
from utils.template_utils import render_template


class DummyMapper(ActionMapper):
    def convert_to_text(self):
        return render_template(template_name="dummy.tpl", task_id=self.name, trigger_rule=self.trigger_rule)

    def required_imports(self) -> Set[str]:
        return {"from airflow.operators import dummy_operator"}
