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
import os
from typing import Dict, Set, Tuple, List

from xml.etree.ElementTree import Element

from airflow.utils.trigger_rule import TriggerRule

from o2a.converter.task import Task
from o2a.converter.relation import Relation
from o2a.mappers.action_mapper import ActionMapper
from o2a.mappers.prepare_mixin import PrepareMixin
from o2a.o2a_libs.property_utils import PropertySet
from o2a.utils import el_utils, xml_utils
from o2a.utils.file_archive_extractors import ArchiveExtractor, FileExtractor


# pylint: disable=too-many-instance-attributes
class PigMapper(ActionMapper, PrepareMixin):
    """
    Converts a Pig Oozie node to an Airflow task.
    """

    def __init__(
        self,
        oozie_node: Element,
        name: str,
        property_set: PropertySet,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        **kwargs,
    ):
        ActionMapper.__init__(
            self,
            oozie_node=oozie_node,
            name=name,
            trigger_rule=trigger_rule,
            property_set=property_set,
            **kwargs,
        )
        PrepareMixin.__init__(self, oozie_node=oozie_node)
        self.params_dict: Dict[str, str] = {}
        self.file_extractor = FileExtractor(oozie_node=oozie_node, property_set=self.property_set)
        self.archive_extractor = ArchiveExtractor(oozie_node=oozie_node, property_set=self.property_set)
        self._parse_oozie_node()

    def _parse_oozie_node(self):
        res_man_text = self.oozie_node.find("resource-manager").text
        name_node_text = self.oozie_node.find("name-node").text
        script = self.oozie_node.find("script").text
        self.resource_manager = el_utils.replace_el_with_var(
            res_man_text, property_set=self.property_set, quote=False
        )
        self.name_node = el_utils.replace_el_with_var(
            name_node_text, property_set=self.property_set, quote=False
        )
        self.script_file_name = el_utils.replace_el_with_var(
            script, property_set=self.property_set, quote=False
        )
        self._parse_params()
        self.files, self.hdfs_files = self.file_extractor.parse_node()
        self.archives, self.hdfs_archives = self.archive_extractor.parse_node()

    def _parse_params(self):
        param_nodes = xml_utils.find_nodes_by_tag(self.oozie_node, "param")
        if param_nodes:
            self.params_dict = {}
            for node in param_nodes:
                param = el_utils.replace_el_with_var(node.text, property_set=self.property_set, quote=False)
                key, value = param.split("=")
                self.params_dict[key] = value

    def to_tasks_and_relations(self) -> Tuple[List[Task], List[Relation]]:
        action_task = Task(
            task_id=self.name,
            template_name="pig.tpl",
            trigger_rule=self.trigger_rule,
            template_params=dict(
                property_set=self.property_set,
                params_dict=self.params_dict,
                script_file_name=self.script_file_name,
            ),
        )
        tasks: List[Task] = [action_task]
        relations: List[Relation] = []
        prepare_task = self.get_prepare_task(
            name=self.name, trigger_rule=self.trigger_rule, property_set=self.property_set
        )
        if prepare_task:
            tasks = [prepare_task, action_task]
            relations = [Relation(from_task_id=prepare_task.task_id, to_task_id=self.name)]
        return tasks, relations

    def _add_symlinks(self, destination_pig_file):
        destination_pig_file.write("set mapred.create.symlink yes;\n")
        if self.files:
            destination_pig_file.write("set mapred.cache.file {};\n".format(",".join(self.hdfs_files)))
        if self.archives:
            destination_pig_file.write("set mapred.cache.archives {};\n".format(",".join(self.hdfs_archives)))

    def copy_extra_assets(self, input_directory_path: str, output_directory_path: str):
        self._validate_paths(input_directory_path, output_directory_path)
        source_pig_file_path = os.path.join(input_directory_path, self.script_file_name)
        destination_pig_file_path = os.path.join(output_directory_path, self.script_file_name)
        self._copy_pig_script_with_path_injection(destination_pig_file_path, source_pig_file_path)

    def _copy_pig_script_with_path_injection(self, destination_pig_file_path, source_pig_file_path):
        os.makedirs(os.path.dirname(destination_pig_file_path), exist_ok=True)
        with open(destination_pig_file_path, "w") as destination_pig_file:
            with open(source_pig_file_path, "r") as source_pig_file:
                pig_script = source_pig_file.read()
                if self.files or self.archives:
                    self._add_symlinks(destination_pig_file)
                destination_pig_file.write(pig_script)

    @staticmethod
    def _validate_paths(input_directory_path, output_directory_path):
        if not input_directory_path:
            raise Exception("The input_directory_path should be set and is {}".format(input_directory_path))
        if not output_directory_path:
            raise Exception("The output_directory_path should be set and is {}".format(output_directory_path))

    def required_imports(self) -> Set[str]:
        return {"from airflow.utils import dates", "from airflow.contrib.operators import dataproc_operator"}

    @property
    def first_task_id(self) -> str:
        return f"{self.name}_prepare" if self.has_prepare() else self.name
