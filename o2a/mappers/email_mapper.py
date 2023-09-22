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
"""Maps Email action into Airflow's DAG"""

from typing import List, Optional, Set, Tuple
from xml.etree.ElementTree import Element

from o2a.converter.relation import Relation
from o2a.converter.task import Task
from o2a.mappers.action_mapper import ActionMapper
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils import xml_utils


class EmailMapper(ActionMapper):
    """
    Converts an Email Oozie action node to an Airflow task.
    """

    def __init__(self, oozie_node: Element, name: str, dag_name: str, props: PropertySet, **kwargs):
        ActionMapper.__init__(
            self, oozie_node=oozie_node, dag_name=dag_name, name=name, props=props, **kwargs
        )
        self.to: Optional[str] = None
        self.cc: Optional[str] = None
        self.bcc: Optional[str] = None
        self.subject: Optional[str] = None
        self.body: Optional[str] = None

    def on_parse_node(self):
        super().on_parse_node()
        self.__extract_email_data()

    def to_tasks_and_relations(self) -> Tuple[List[Task], List[Relation]]:
        action_task = Task(
            task_id=self.name,
            template_name="email.tpl",
            template_params=dict(
                props=self.props, to=self.to, cc=self.cc, bcc=self.bcc, subject=self.subject, body=self.body
            ),
        )
        tasks = [action_task]
        relations: List[Relation] = []  # no prepare node in email action
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {"from airflow.operators import email_operator"}

    def __extract_email_data(self):
        root = self.oozie_node
        props = self.props
        self.to = xml_utils.get_tag_el_text(root=root, tag="to", props=props)
        self.cc = xml_utils.get_tag_el_text(root=root, tag="cc", props=props)
        self.bcc = xml_utils.get_tag_el_text(root=root, tag="bcc", props=props)
        self.subject = xml_utils.get_tag_el_text(root=root, tag="subject", props=props)
        self.body = xml_utils.get_tag_el_text(root=root, tag="body", props=props)
