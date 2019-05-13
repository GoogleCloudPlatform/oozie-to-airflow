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
import textwrap
from typing import Dict, Set, TextIO

from converter.oozie_converter import OozieConverter, INDENT
from converter.parsed_node import ParsedNode
from converter.primitives import Relation


# pylint: disable=too-few-public-methods
class OozieSubworkflowConverter(OozieConverter):
    """Converts Oozie Subworkflow to Airflow's DAG"""

    def __init__(
        self,
        dag_name: str,
        input_directory_path: str,
        output_directory_path: str,
        user: str = None,
        start_days_ago: int = None,
        schedule_interval: str = None,
        output_dag_name: str = None,
    ):
        OozieConverter.__init__(
            self,
            dag_name=dag_name,
            input_directory_path=input_directory_path,
            output_directory_path=output_directory_path,
            user=user,
            start_days_ago=start_days_ago,
            schedule_interval=schedule_interval,
            output_dag_name=output_dag_name,
        )

    def write_dag(
        self, depends: Set[str], file: TextIO, nodes: Dict[str, ParsedNode], relations: Set[Relation]
    ) -> None:
        self.write_dependencies(file, depends)
        self.write_properties(file=file, properties=self.properties)
        file.write("\ndef sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):\n")
        self.write_dag_header(
            file=file,
            dag_name=self.dag_name,
            schedule_interval=self.schedule_interval,
            start_days_ago=self.start_days_ago,
            template="dag_subwf.tpl",
        )
        self.write_nodes(file, nodes, indent=INDENT + 4)
        file.write("\n\n")
        self.write_relations(file, relations, indent=INDENT + 4)
        file.write(textwrap.indent("\nreturn dag\n", INDENT * " "))
