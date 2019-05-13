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
"""Maps Oozie pig node to Airflow's DAG"""
from typing import Dict, Set
from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from converter.primitives import Relation, Task
from mappers.action_mapper import ActionMapper
from mappers.prepare_mixin import PrepareMixin
from utils import el_utils, xml_utils
from utils.file_archive_extractors import ArchiveExtractor, FileExtractor
from utils.template_utils import render_template


# pylint: disable=too-many-instance-attributes
class MapReduceMapper(ActionMapper, PrepareMixin):
    """
    Converts a MapReduce Oozie node to an Airflow task.
    """

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        properties=None,
        **kwargs,
    ):
        ActionMapper.__init__(
            self, oozie_node=oozie_node, name=name, trigger_rule=trigger_rule, properties=properties, **kwargs
        )
        self.params_dict: Dict[str, str] = {}
        self.trigger_rule = trigger_rule
        self.file_extractor = FileExtractor(oozie_node=oozie_node, properties=properties)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, properties=properties)
        self._parse_oozie_node()

    def _parse_oozie_node(self):
        name_node_text = self.oozie_node.find("name-node").text
        self.name_node = el_utils.convert_el_string_to_fstring(name_node_text, properties=self.properties)
        self._parse_params()
        self.files, self.hdfs_files = self.file_extractor.parse_node()
        self.archives, self.hdfs_archives = self.archive_extractor.parse_node()

    def _parse_params(self):
        param_nodes = xml_utils.find_nodes_by_tag(self.oozie_node, "param")
        if param_nodes:
            self.params_dict = {}
            for node in param_nodes:
                param = el_utils.replace_el_with_property_values(node.text, properties=self.properties)
                key, value = param.split("=", 1)
                self.params_dict[key] = value

    def convert_to_text(self) -> str:
        prepare_command, prepare_arguments = self.get_prepare_command_with_arguments(
            self.oozie_node, self.properties
        )
        tasks = [
            Task(
                task_id=self.name + "-prepare",
                template_name="prepare.tpl",
                trigger_rule=self.trigger_rule,
                template_params=dict(prepare_command=prepare_command, prepare_arguments=prepare_arguments),
            ),
            Task(
                task_id=self.name,
                template_name="mapreduce.tpl",
                trigger_rule=self.trigger_rule,
                template_params=dict(
                    properties=self.properties,
                    params_dict=self.params_dict,
                    hdfs_files=self.hdfs_files,
                    hdfs_archives=self.hdfs_archives,
                ),
            ),
        ]
        relations = [Relation(from_task_id=self.name + "-prepare", to_task_id=self.name)]
        return render_template(template_name="action.tpl", tasks=tasks, relations=relations)

    @staticmethod
    def _validate_paths(input_directory_path, output_directory_path):
        if not input_directory_path:
            raise Exception("The input_directory_path should be set and is {}".format(input_directory_path))
        if not output_directory_path:
            raise Exception("The output_directory_path should be set and is {}".format(output_directory_path))

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}
