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

import jinja2
from airflow.utils.trigger_rule import TriggerRule

from definitions import ROOT_DIR
from mappers.base_mapper import BaseMapper


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

    def __init__(self, oozie_node, task_id, trigger_rule=TriggerRule.ALL_DONE,
                 params={}):
        BaseMapper.__init__(self, oozie_node, task_id, trigger_rule)
        self.oozie_node = oozie_node
        self.task_id = task_id
        self.trigger_rule = trigger_rule
        self.params = params

    def convert_to_text(self):
        switch_node = self.oozie_node[0]

        case_dict = collections.OrderedDict()
        for case in switch_node:
            if 'case' in case.tag:
                case_dict[case.text] = case.attrib['to']
            else:  # Default return value
                case_dict['default'] = case.attrib['to']

        template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(
                searchpath=os.path.join(ROOT_DIR, 'templates/')),
            trim_blocks=True,
            lstrip_blocks=True)

        template = template_env.get_template('decision.tpl')
        return template.render(task_id=self.task_id,
                               trigger_rule=self.trigger_rule,
                               case_dict=case_dict.items())

    @staticmethod
    def required_imports():
        return ['from airflow.operators import python_operator']
