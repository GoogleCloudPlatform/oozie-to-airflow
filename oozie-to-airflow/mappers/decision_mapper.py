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

import collections
import os
from typing import Dict, Set
from xml.etree.ElementTree import Element

import jinja2
from airflow.utils.trigger_rule import TriggerRule

from definitions import ROOT_DIR
from mappers.base_mapper import BaseMapper
from utils.el_utils import convert_el_to_jinja


# noinspection PyAbstractClass
class DecisionMapper(BaseMapper):
    """
    Decision nodes have multiple paths, where they evaluate EL functions
    until it finds the first one that's true, else it goes to default.

    XML Example:
    <workflow-app name="foo-wf" xmlns="uri:oozie:workflow:0.1">
    ...
    <decision name="mydecision">
        <switch>
            <case to="reconsolidatejob">
              ${fs:fileSize(secondjobOutputDir) gt 10 * GB}
            </case>
            <case to="rexpandjob">
              ${fs:filSize(secondjobOutputDir) lt 100 * MB}
            </case>
            <case to="recomputejob">
              ${ hadoop:counters('secondjob')[RECORDS][REDUCE_OUT] lt 1000000 }
            </case>
            <default to="end"/>
        </switch>
    </decision>
    ...
    </workflow-app>
    """

    def __init__(
        self,
        oozie_node: Element,
        task_id: str,
        trigger_rule: str = TriggerRule.ALL_DONE,
        params: Dict[str, str] = None,
    ):
        BaseMapper.__init__(self, oozie_node=oozie_node, task_id=task_id, trigger_rule=trigger_rule)
        if params is None:
            params = {}
        self.oozie_node = oozie_node
        self.task_id = task_id
        self.trigger_rule = trigger_rule
        self.params = params
        self.case_dict = self._get_cases()

    def _get_cases(self) -> Dict[str, str]:
        switch_node = self.oozie_node[0]
        case_dict = collections.OrderedDict()
        for case in switch_node:
            if "case" in case.tag:
                case_text = convert_el_to_jinja(case.text.strip(), quote=True)
                case_dict[case_text] = case.attrib["to"]
            else:  # Default return value
                case_dict["default"] = case.attrib["to"]
        return case_dict

    def convert_to_text(self) -> str:
        template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(searchpath=os.path.join(ROOT_DIR, "templates/")),
            trim_blocks=True,
            lstrip_blocks=True,
        )

        template = template_env.get_template("decision.tpl")
        return template.render(
            task_id=self.task_id, trigger_rule=self.trigger_rule, case_dict=self.case_dict.items()
        )

    @staticmethod
    def required_imports() -> Set[str]:
        return {
            "from airflow.operators import python_operator",
            "from airflow.utils import dates",
            "from o2a_libs.el_basic_functions import first_not_null",
        }
