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
from typing import Set, Dict, Type
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from converter.subworkflow_converter import OozieSubworkflowConverter
from mappers.action_mapper import ActionMapper
from mappers.base_mapper import BaseMapper
from tests.utils.test_paths import EXAMPLES_PATH
from utils import el_utils, xml_utils
from utils.template_utils import render_template


class SubworkflowMapper(ActionMapper):
    """
    Converts a Sub-workflow Oozie node to an Airflow task.
    """

    properties: Dict[str, str]

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        oozie_node: Element,
        name: str,
        dag_name: str,
        input_directory_path: str,
        output_directory_path: str,
        action_mapper: Dict[str, Type[ActionMapper]],
        control_mapper: Dict[str, Type[BaseMapper]],
        trigger_rule=TriggerRule.ALL_SUCCESS,
        params=None,
        template="subwf.tpl",
        **kwargs,
    ):
        ActionMapper.__init__(self, oozie_node=oozie_node, name=name, trigger_rule=trigger_rule, **kwargs)
        if params is None:
            params = {}
        self.template = template
        self.params = params
        self.task_id = name
        self.trigger_rule = trigger_rule
        self.properties = {}
        self.input_directory_path = input_directory_path
        self.output_directory_path = output_directory_path
        self.dag_name = dag_name
        self.action_mapper = action_mapper
        self.control_mapper = control_mapper
        self._parse_oozie_node()

    def _parse_oozie_node(self):
        app_path = self.oozie_node.find("app-path").text
        app_path = el_utils.replace_el_with_var(app_path, params=self.params, quote=False)
        # TODO: hacky: we should calculate it deriving from input_directory_path and comparing app-path
        # TODO: but for now we assume app is in "examples"
        app_path = os.path.join(EXAMPLES_PATH, app_path.rsplit("/", 1)[1])
        logging.info(f"Converting subworkflow from {app_path}")
        self._parse_config()
        converter = OozieSubworkflowConverter(
            input_directory_path=app_path,
            output_directory_path=self.output_directory_path,
            start_days_ago=0,
            action_mapper=self.action_mapper,
            control_mapper=self.control_mapper,
            dag_name=f"{self.dag_name}.{self.task_id}",
            output_dag_name="subdag_test.py",  # TODO: do not use hard-coded name for subdaag
        )
        converter.convert()

    def get_config_properties(self):
        propagate_configuration = self.oozie_node.find("propagate-configuration")
        # Below the `is not None` is necessary due to Element's __bool__() return value:
        # `len(self._children) != 0`,
        # and `propagate_configuration` is an empty node so __bool__() will always return False.
        return self.properties if propagate_configuration is not None else {}

    def _parse_config(self):
        config = self.oozie_node.find("configuration")
        if config:
            property_nodes = xml_utils.find_nodes_by_tag(config, "property")
            if property_nodes:
                for node in property_nodes:
                    name = node.find("name").text
                    value = el_utils.replace_el_with_var(
                        node.find("value").text, params=self.params, quote=False
                    )
                    self.properties[name] = value

    def convert_to_text(self):
        return render_template(template_name=self.template, **self.__dict__)

    def required_imports(self) -> Set[str]:
        return {
            "from airflow.utils import dates",
            "from airflow.contrib.operators import dataproc_operator",
            "from airflow.operators.subdag_operator import SubDagOperator",
            f"from subdag_test import sub_dag",
        }
