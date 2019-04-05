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
import os
from typing import Set

import jinja2

from airflow.operators import dummy_operator

from mappers.action_mapper import ActionMapper
from definitions import ROOT_DIR


class DummyMapper(ActionMapper):
    def convert_to_text(self):
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.join(ROOT_DIR, "templates/"))
        template_env = jinja2.Environment(loader=template_loader)

        template = template_env.get_template("dummy.tpl")
        return template.render(task_id=self.task_id, trigger_rule=self.trigger_rule)

    def convert_to_airflow_op(self):
        return dummy_operator.DummyOperator(task_id=self.task_id, trigger_rule=self.trigger_rule)

    @staticmethod
    def required_imports() -> Set[str]:
        return {"from airflow.operators import dummy_operator"}

    # noinspection PyMethodMayBeStatic
    @property
    def has_prepare(self) -> bool:
        """ Required for Unknown Action Mapper """
        return False
