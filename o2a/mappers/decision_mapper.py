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
"""Maps decision node to Airflow's DAG"""
import collections
from typing import Dict, List, Set

from xml.etree.ElementTree import Element


from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.base_mapper import BaseMapper
from o2a.o2a_libs.src.o2a_lib.property_utils import PropertySet
from o2a.o2a_libs.src.o2a_lib import el_parser


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
        self, oozie_node: Element, name: str, dag_name: str, props: PropertySet = None, **kwargs: Dict
    ):
        BaseMapper.__init__(
            self,
            oozie_node=oozie_node,
            name=name,
            dag_name=dag_name,
            props=props or PropertySet(job_properties={}, config={}),
            **kwargs,
        )
        self._get_cases()

    def _get_cases(self):
        switch_node = self.oozie_node[0]
        self.case_dict: Dict[str, str] = collections.OrderedDict()
        self.default_case = None
        for case in switch_node:
            if "case" in case.tag:
                case_text = el_parser.translate(case.text.strip(), quote=False)
                self.case_dict[case_text] = case.attrib["to"]
            else:  # Default return value
                self.default_case = case.attrib["to"]

    def to_tasks_and_relations(self):
        tasks = [
            Task(
                task_id=self.name,
                template_name="decision.tpl",
                template_params=dict(case_dict=self.case_dict, default_case=self.default_case),
            )
        ]
        relations: List[Relation] = []
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {"from airflow.operators import python", "from airflow.utils import dates"}
