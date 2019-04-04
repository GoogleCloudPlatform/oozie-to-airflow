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
import os
from typing import Set

import jinja2
from airflow.operators.bash_operator import BashOperator

from definitions import ROOT_DIR
from mappers.base_mapper import BaseMapper


class KillMapper(BaseMapper):
    def convert_to_text(self) -> str:
        template_loader = jinja2.FileSystemLoader(searchpath=os.path.join(ROOT_DIR, "templates/"))
        template_env = jinja2.Environment(loader=template_loader)

        template = template_env.get_template("kill.tpl")
        return template.render(task_id=self.task_id, trigger_rule=self.trigger_rule)

    def convert_to_airflow_op(self) -> BashOperator:
        return BashOperator(bash_command="exit 1", task_id=self.task_id, trigger_rule=self.trigger_rule)

    @staticmethod
    def required_imports() -> Set[str]:
        return {"from airflow.operators import bash_operator"}
