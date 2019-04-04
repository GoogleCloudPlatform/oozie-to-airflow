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

import json
import textwrap
from typing import TextIO, Dict, Type

from converter.converter import OozieConverter, INDENT
from converter.parsed_node import ParsedNode
from mappers.action_mapper import ActionMapper
from mappers.base_mapper import BaseMapper


class OozieSubworkflowConverter(OozieConverter):
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
        self, depends: [str], f: TextIO, operators: Dict[str, ParsedNode], relations: [str]
    ) -> None:
        self.write_dependencies(f, depends)
        f.write("PARAMS = " + json.dumps(self.params, indent=INDENT) + "\n\n")
        f.write("\ndef sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval):\n")
        self.write_dag_header(
            f, self.dag_name, self.schedule_interval, self.start_days_ago, template="dag_subwf.tpl"
        )
        self.write_operators(f, operators, indent=INDENT + 4)
        f.write("\n\n")
        self.write_relations(f, relations, indent=INDENT + 4)
        f.write(textwrap.indent("\nreturn dag\n", INDENT * " "))
