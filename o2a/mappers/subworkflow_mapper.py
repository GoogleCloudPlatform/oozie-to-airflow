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
"""Maps subworkflow of Oozie to Airflow's sub-dag"""
import logging
import os
from typing import Dict, Set, Type, Tuple, List

from xml.etree.ElementTree import Element

from o2a.converter.oozie_converter import OozieConverter
from o2a.converter.relation import Relation
from o2a.converter.renderers import BaseRenderer
from o2a.converter.task import Task
from o2a.definitions import EXAMPLES_PATH
from o2a.mappers.action_mapper import ActionMapper
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils import el_utils


# pylint: disable=too-many-instance-attributes
class SubworkflowMapper(ActionMapper):
    """
    Converts a Sub-workflow Oozie node to an Airflow task.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        oozie_node: Element,
        name: str,
        dag_name: str,
        input_directory_path: str,
        output_directory_path: str,
        props: PropertySet,
        action_mapper: Dict[str, Type[ActionMapper]],
        renderer: BaseRenderer,
        **kwargs,
    ):
        ActionMapper.__init__(
            self, oozie_node=oozie_node, name=name, dag_name=dag_name, props=props, **kwargs
        )
        self.task_id = name
        self.input_directory_path = input_directory_path
        self.output_directory_path = output_directory_path
        self.dag_name = dag_name
        self.action_mapper = action_mapper
        self.renderer = renderer
        self._parse_oozie_node()

    def _parse_oozie_node(self):
        app_path = self.oozie_node.find("app-path").text
        app_path = el_utils.replace_el_with_var(app_path, props=self.props, quote=False)
        _, _, self.app_name = app_path.rpartition("/")
        # TODO: hacky: we should calculate it deriving from input_directory_path and comparing app-path
        # TODO: but for now we assume app is in "examples"
        app_path = os.path.join(EXAMPLES_PATH, self.app_name)
        logging.info(f"Converting subworkflow from {app_path}")
        converter = OozieConverter(
            input_directory_path=app_path,
            output_directory_path=self.output_directory_path,
            renderer=self.renderer,
            action_mapper=self.action_mapper,
            dag_name=self.app_name,
            initial_props=self.get_props(),
        )
        converter.convert(as_subworkflow=True)

    def get_props(self) -> PropertySet:
        propagate_configuration = self.oozie_node.find("propagate-configuration")
        # Below the `is not None` is necessary due to Element's __bool__() return value:
        # `len(self._children) != 0`,
        # and `propagate_configuration` is an empty node so __bool__() will always return False.
        return (
            self.props if propagate_configuration is not None else PropertySet(config={}, job_properties={})
        )

    def to_tasks_and_relations(self) -> Tuple[List[Task], List[Relation]]:
        tasks: List[Task] = [
            Task(task_id=self.name, template_name="subwf.tpl", template_params=dict(app_name=self.app_name))
        ]
        relations: List[Relation] = []
        return tasks, relations

    def required_imports(self) -> Set[str]:
        return {
            "from airflow.utils import dates",
            "from airflow.contrib.operators import dataproc_operator",
            "from airflow.operators.subdag_operator import SubDagOperator",
            f"import subdag_{self.app_name}",
        }
