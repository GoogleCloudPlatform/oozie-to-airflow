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
"""Converts sub-workflows of Oozie to Airflow"""
import json
import textwrap
from typing import TextIO, Dict, Type, Set

from converter.oozie_converter import OozieConverter, INDENT
from converter.parsed_node import ParsedNode
from converter.relation import Relation
from mappers.action_mapper import ActionMapper
from mappers.base_mapper import BaseMapper


# pylint: disable=too-few-public-methods
class OozieSubworkflowConverter(OozieConverter):
    """Converts Oozie Subworkflow to Airflow's DAG"""

    def __init__(
        self,
        dag_name: str,
        input_directory_path: str,
        output_directory_path: str,
        action_mapper: Dict[str, Type[ActionMapper]],
        control_mapper: Dict[str, Type[BaseMapper]],
        user: str = None,
        start_days_ago: int = None,
        schedule_interval: str = None,
    ):
        OozieConverter.__init__(
            self,
            dag_name=dag_name,
            input_directory_path=input_directory_path,
            output_directory_path=output_directory_path,
            action_mapper=action_mapper,
            control_mapper=control_mapper,
            user=user,
            start_days_ago=start_days_ago,
            schedule_interval=schedule_interval,
        )

    def write_dag(
        self, depends: Set[str], file: TextIO, nodes: Dict[str, ParsedNode], relations: Set[Relation]
    ) -> None:
        self.write_dependencies(file, depends)
        file.write("PARAMS = " + json.dumps(self.params, indent=INDENT) + "\n\n")
        file.write("\ndef sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):\n")
        self.write_dag_header(
            file, self.dag_name, self.schedule_interval, self.start_days_ago, template="dag_subwf.tpl"
        )
        self.write_nodes(file, nodes, indent=INDENT + 4)
        file.write("\n\n")
        self.write_relations(file, relations, indent=INDENT + 4)
        file.write(textwrap.indent("\nreturn dag\n", INDENT * " "))
